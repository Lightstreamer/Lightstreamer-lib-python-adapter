import base64
import logging
from urllib import parse
from collections import OrderedDict

from lightstreamer_adapter.interfaces.metadata import (MetadataProviderError,
                                                       NotificationError,
                                                       AccessError,
                                                       ItemsError,
                                                       SchemaError,
                                                       CreditsError,
                                                       ConflictingSessionError,
                                                       MpnPlatformType, Mode)

from lightstreamer_adapter.interfaces.data import (DataProviderError,
                                                   SubscribeError,
                                                   FailureError)

log = logging.getLogger(__name__)

EMPTY_VALUE = "$"
NULL_VALUE = "#"
MODES = OrderedDict([("RAW", "R"),
                     ("MERGE", "M"),
                     ("DISTINCT", "D"),
                     ("COMMAND", "C")])

_reverse_modes = {v: k for k, v in MODES.items()}

_exceptions_map = {str(MetadataProviderError): 'M',
                   str(NotificationError): 'N',
                   str(AccessError): 'A',
                   str(ItemsError): 'I',
                   str(SchemaError): 'S',
                   str(CreditsError): 'C',
                   str(ConflictingSessionError): 'X',
                   str(DataProviderError): 'D',
                   str(SubscribeError): 'U',
                   str(FailureError): 'F'}


class RemotingException(Exception):
    """
    Issued by the Remote Server upon an unexpected error
    """


def join(*args, append=False):
    members = [str(i) for i in args]
    suffix = "|" if append else ''
    return "|".join(members) + suffix


def remoting_exception(method):
    def wrap(protocol_func):
        def _wrap(*args, **kwargs):
            try:
                return protocol_func(*args, **kwargs)
            except RemotingException as err:
                msg = "{} while parsing a {} request".format(str(err),
                                                             str(method))
                raise Exception(msg) from err
            except Exception as err:
                msg = ("An unexpected exception caught while parsing a {} "
                       "request")
                raise RemotingException(msg.format(str(method))) from err

        return _wrap
    return wrap


def encoding_exception(arg1):
    def wrap(protocol_func):
        def _wrap(*args, **kwargs):
            try:
                return protocol_func(*args, **kwargs)
            except RemotingException as err:
                msg = "{} while parsing a {} request".format(str(err), arg1)
                raise Exception(msg) from err
            except Exception as err:
                msg = ("An unexpected exception caught while parsing a {} "
                       "request")
                raise Exception(msg.format(arg1)) from err

        return _wrap
    return wrap


def parse_request(request):
    log.debug("Received request %s", request)
    tokens = request.rstrip().split('|')
    not_empty_tokens = [t for t in tokens if t.rstrip()]
    if len(not_empty_tokens) <= 1:
        return None
    method_id = tokens[0]
    return dict([("id", method_id),
                 ("method", not_empty_tokens[1]),
                 ("test_data", not_empty_tokens[2:])])


def read_token(tokens, index):
    try:
        return tokens[index]
    except IndexError as err:
        raise RemotingException("Token not found") from err


def read(tokens, token_type, index):
    current_token_type = read_token(tokens, index)

    if current_token_type == token_type:
        current_token = read_token(tokens, index + 1)
        if current_token_type == 'S':
            return decode_string(current_token)
        elif current_token_type == 'M':
            return decode_modes(current_token)
        elif current_token_type == "I":
            return int(current_token)
        elif current_token_type == "P":
            return decode_nobile_platform_type(current_token)
        else:
            # Never happens (hoping!)
            raise RemotingException("Unknown type!")
    else:
        raise RemotingException("Unknown type '{}' found".
                                format(current_token_type))


def read_map(tokens, start, length=None):
    stop = start + length if length else None
    data = tokens[start:stop]
    if len(data) % 2 != 0:
        raise RemotingException("Invalid number of tokens")

    return {read(data, "S", i): read(data, "S", i + 2)
            for i in range(0, len(data) - 2, 4)}


def read_seq(tokens, offset, length=None):
    stop = offset + length if length else None
    data = tokens[offset:stop]
    sequence = [read(data, 'S', i) for i in range(0, len(data), 2)]
    return sequence


def decode_string(string):
    if string == NULL_VALUE:
        return None

    if string == EMPTY_VALUE:
        return ''

    return parse.unquote_plus(string)


def encode_string(string):
    if string is None:
        return NULL_VALUE

    if not string:
        return EMPTY_VALUE

    try:
        return parse.quote_plus(string)
    except Exception as err:
        raise RemotingException("Unknown error while quoting string") from err


def encode_boolean(boolean):
    return str(int(boolean))


def encode_double(double):
    return float(double)


def encode_modes(modes):
    if modes is None:
        return NULL_VALUE

    if not modes:
        return EMPTY_VALUE

    res = [mode.value for mode in modes]
    return ''.join(res)


def encode_byte(byte_str):
    try:
        return base64.b64encode(byte_str).decode('utf-8')
    except Exception as err:
        raise RemotingException("Error while base64-encoding bytes") from err


def encode_value(value):
    if value is None or isinstance(value, str):
        return "S|" + encode_string(value)
    elif isinstance(value, bytes):
        return "Y|" + encode_byte(value)



def decode_modes(modes):
    if modes == NULL_VALUE or modes == EMPTY_VALUE:
        return None

    if modes == EMPTY_VALUE:
        return []

    for mode in modes:
        if mode in list(m.value for m in Mode):
            return Mode(mode)
        else:
            raise RemotingException("Unknown mode '{}' found".format(mode))

    raise RemotingException("Unknown mode '{}' found".format(modes))


def decode_nobile_platform_type(platform_type):
    if platform_type == NULL_VALUE:
        return None

    if platform_type == EMPTY_VALUE:
        return ''

    if platform_type in [c.value for c in MpnPlatformType]:
        return MpnPlatformType(platform_type)
    else:
        raise RemotingException("Unknown platform type '{}'"
                                .format(platform_type))


def _append_exceptions(response, error, subtype=True):
    tokens = []
    error_type = str(type(error))
    error_id = None
    if subtype and error_type in _exceptions_map:
        error_id = _exceptions_map[error_type]
        tokens.append(error_id)
    else:
        response += '|'

    tokens.append(encode_string(str(error)))

    # Handle ConflictingSessionError as sub-case of CreditsError
    if error_id in ['C', 'X']:
        tokens.append(str(error.client_error_code))
        tokens.append(encode_string(error.client_user_msg))
        if error_id == 'X':
            tokens.append(encode_string(error.conflicting_session_id))

    return response + '|'.join(tokens)


def _handle_exception(exception, method, *excepted_errors):
    try:
        raise exception
    except excepted_errors:
        return _append_exceptions(method, exception)
    except:
        return _append_exceptions(method, exception, False)
