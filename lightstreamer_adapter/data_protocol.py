"""Data Provider Protocol implementation."""
from enum import Enum
from lightstreamer_adapter.protocol import (join,
                                            encode_string as enc_str,
                                            encode_byte as enc_byte,
                                            encode_boolean as enc_bool,
                                            read,
                                            read_map,
                                            remoting_exception_on_parse,
                                            _handle_exception, _write_init,
                                            RemotingException)

from lightstreamer_adapter.interfaces.data import (DataProviderError,
                                                   SubscribeError,
                                                   FailureError)


class Method(Enum):
    """Enum for representing the methods of the Data Provider Protocol."""
    DPI = 1
    DPNI = 2
    SUB = 3
    USB = 4
    UD3 = 5
    EOS = 6
    CLS = 7
    FAL = 8

    def __str__(self):
        return self.name


@remoting_exception_on_parse(Method.DPI)
def read_init(data):
    """Reads and parses a DPI ('Data Init') request."""
    return read_map(data, 0)


def write_init(proxy_parameters=None, exception=None):
    """Encodes and returns a DPI ('Data Init') response."""
#     if not exception:
#         if proxy_parameters:
#             parameters = []
#             for key, value in proxy_parameters.items():
#                 parameters.append(key)
#                 parameters.append(enc_str(value))
#             return join(str(Method.DPI), 'S|') + '|S|'.join(parameters)
#         return join(str(Method.DPI), "V")
#     return _handle_exception(exception, join(str(Method.DPI), 'E'),
#                              DataProviderError)
    return _write_init(Method.DPI, DataProviderError, proxy_parameters,
                       exception)


def write_notify_init(proxy_parameters):
    """Encodes and returns a DNPI ('Data Notifications Init') response."""
    parameters = []
    for key, value in proxy_parameters.items():
        parameters.append(key)
        parameters.append(enc_str(value))
    return join(str(Method.DPNI), 'S|') + '|S|'.join(parameters)


@remoting_exception_on_parse(Method.SUB)
def read_sub(data):
    """Reads and parses a SUB ('Subscribe') request."""
    return read(data, "S", 0)


def write_sub(exception=None):
    """Encodes and returns a SUB ('Subscribe') response."""
    if not exception:
        return join(str(Method.SUB), "V")
    return _handle_exception(exception, join(str(Method.SUB), 'E'),
                             SubscribeError, FailureError)


@remoting_exception_on_parse(Method.USB)
def read_usub(data):
    """Reads and parses a USB ('Unsubscribe') request."""
    return read(data, "S", 0)


def write_unsub(exception=None):
    """Encodes and returns a USB ('Unsubscribe') response."""
    if not exception:
        return join(str(Method.USB), 'V')
    return _handle_exception(exception, join(str(Method.USB), 'E'),
                             SubscribeError, FailureError)


def _encode_value(value):
    """Encodes a value passed in an update map to be write in ad UD3 response.
    """
    if value is None or isinstance(value, str):
        return "S|" + enc_str(value)
    elif isinstance(value, bytes):
        return "Y|" + enc_byte(value)
    raise RemotingException(("Found value '{}' of an unsupported type while "
                             "building a {} request").format(str(value),
                                                             str(Method.UD3)))


def write_update_map(item_name, request_id, issnapshot, events_map):
    """Encodes and returns a UD3 ('Update By Map') response."""
    update = join(str(Method.UD3),
                  'S', enc_str(item_name),
                  'S', enc_str(request_id),
                  'B', enc_bool(issnapshot),
                  append_separator=events_map)
    if events_map:
        tokens = [join('S', enc_str(field), _encode_value(value))
                  for field, value in events_map.items()]
        return update + "|".join(tokens)
    return update


def write_eos(item, request_id):
    """Encodes and returns an EOS ('End Of Snapshot') response."""
    return join(str(Method.EOS), "S", enc_str(item), "S", enc_str(request_id))


def write_cls(item, request_id):
    """Encodes and returns a CLS ('Clear Snapshot') response string."""
    return join(str(Method.CLS), "S", enc_str(item), "S", enc_str(request_id))


def write_failure(exception):
    """Encodes and returns a FAL ('Failure') response string."""
    return join(str(Method.FAL), 'E', enc_str(str(exception)))
