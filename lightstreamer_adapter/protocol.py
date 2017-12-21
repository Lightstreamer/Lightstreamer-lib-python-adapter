"""Commons functionalities implementing the ARI Protocol."""
import base64
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
EMPTY_VALUE = "$"
NULL_VALUE = "#"
METHOD_KEEP_ALIVE = "KEEPALIVE"
MODES = OrderedDict([("RAW", "R"),
                     ("MERGE", "M"),
                     ("DISTINCT", "D"),
                     ("COMMAND", "C")])

_EXCEPTIONS_MAP = {str(MetadataProviderError): 'M',
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
    """Issued by the Remote Server upon an unexpected error
    """


def join(*args, append_separator=False):
    """Returns a string which is a concatenation of all arguments, separated by
    a '|'.
    """
    members = [i for i in args]
    suffix = "|" if append_separator else ''
    return "|".join(members) + suffix


def remoting_exception_on_parse(method):
    """Decorator function which executes the provided method and may raise a
    RemotingException with a detailed message about the current parsing
    operation.
    """
    def wrap(protocol_func):
        def _wrap(*args, **kwargs):
            try:
                return protocol_func(*args, **kwargs)
            except RemotingException as err:
                msg = "{} while parsing {} request".format(str(err),
                                                           str(method))
                raise RemotingException(msg)
            except Exception as err:
                msg = ("An unexpected exception caught while parsing {} "
                       "request")
                raise RemotingException(msg.format(str(method))) from err
        return _wrap
    return wrap


def parse_request(request):
    """Operates a first-level parsing, retrieving the following three required
    components:
      id: the request id
      method the method to invoke on the Remote Adapter
      data: the arguments for the method
    """
    packet = request.rstrip().split('|')
    not_empty_tokens = [t for t in packet if t.rstrip()]
    if len(not_empty_tokens) <= 1:
        return None
    method_id = packet[0]
    return {"id": method_id,
            "method": not_empty_tokens[1],
            "data": not_empty_tokens[2:]}


def read_token(packet, index):
    """Reads a the token at the specified index of the provided packet.
    """
    try:
        return packet[index]
    except IndexError as err:
        raise RemotingException("Token not found") from err


def read(packet, data_type, index):
    """Reads and decode a single sequence of '<type>|<segment>', located at
    the specified index in the provided packet, where:

    type is the native type of the segment and must match the provided
    data_type;
    segment is the content of a field or argument of a method.
    """
    current_data_type = read_token(packet, index)

    if current_data_type == data_type:
        current_token = read_token(packet, index + 1)
        if current_data_type == 'S':
            return decode_string(current_token)
        elif current_data_type == 'M':
            return decode_modes(current_token)
        elif current_data_type == "I":
            return int(current_token)
        elif current_data_type == "P":
            return decode_nobile_platform_type(current_token)
    else:
        raise RemotingException("Unknown type '{}' found".
                                format(current_data_type))


def read_map(tokens, start, length=None):
    """Reads and decodes <length> sequences of '|S|<key>|S|<value>' from the
    provided tokens, starting at <start> index, and returns a dict made up of
    <length> key:value pairs.
    """
    stop = start + length if length else None
    data = tokens[start:stop]
    if len(data) % 2 != 0:
        raise RemotingException("Invalid number of tokens")

    return {read(data, "S", i): read(data, "S", i + 2)
            for i in range(0, len(data) - 2, 4)}


def read_seq(tokens, offset, length=None):
    """Reads and decodes '|S|value' from the provided tokens, starting at
    <offset> index, and returns a list of <length> values.
    """
    stop = offset + length if length else None
    data = tokens[offset:stop]
    sequence = [read(data, 'S', i) for i in range(0, len(data), 2)]
    return sequence


def decode_string(string):
    """Decodes the provided URL-encoded string.

    The method also handles special cases as follows:
    returns None in case of '#' (a null value)
    returns an empty string in case of '$' (an empty value)
    """
    if string == NULL_VALUE:
        return None

    if string == EMPTY_VALUE:
        return ''

    return parse.unquote_plus(string)


def encode_string(string):
    """Returns the URL-encoding of the provided string.

    The method also handles the special cases:
    returns '#' in case of None;
    returns '$' in case of empty string.
    """
    if string is None:
        return NULL_VALUE

    if not string:
        return EMPTY_VALUE

    try:
        return parse.quote_plus(string)
    except TypeError as err:
        raise RemotingException("Unknown error while url-encoding string") \
            from err


def encode_boolean(boolean):
    """Returns a string representation of the provided boolean value."""
    if isinstance(boolean, bool):
        return str(int(boolean))
    raise RemotingException("Not a bool value: '{}'".format(str(boolean)))


def encode_integer(integer):
    """Returns a string representation of the provided integer value."""
    if isinstance(integer, int) and not isinstance(integer, bool):
        return str(integer)
    raise RemotingException("Not an int value: '{}'".format(str(integer)))


def encode_double(double):
    """Returns a string representation of the provided float value."""
    if isinstance(double, float):
        return str(float(double))
    raise RemotingException("Not a float value: '{}'".format(str(double)))


def encode_modes(modes):
    """Returns a string which is a concatenation of the Modes in the provided
    list.
    """
    if modes is None:
        return NULL_VALUE

    if not modes:
        return EMPTY_VALUE

    res = [mode.value for mode in modes]
    return ''.join(res)


def encode_byte(byte_str):
    """Returns the Base 64 encoding of the provide byte string."""
    try:
        return base64.b64encode(byte_str).decode('utf-8')
    except Exception as err:
        raise RemotingException("Error while base64-encoding bytes") from err


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
    if subtype and error_type in _EXCEPTIONS_MAP:
        error_id = _EXCEPTIONS_MAP[error_type]
        tokens.append(error_id)
    else:
        response += '|'

    tokens.append(encode_string(str(error)))

    # Handles ConflictingSessionError as sub-case of CreditsError.
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
