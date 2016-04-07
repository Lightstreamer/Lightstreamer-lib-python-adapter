from enum import Enum
from lightstreamer_adapter.protocol import (join,
                                            encode_string as enc_str,
                                            encode_byte as enc_byte,
                                            encode_boolean as enc_bool,
                                            read,
                                            read_map,
                                            remoting_exception_on_parse,
                                            _handle_exception,
                                            RemotingException)

from lightstreamer_adapter.interfaces.data import (DataProviderError,
                                                   SubscribeError,
                                                   FailureError)


class Method(Enum):
    """Enum for representing the methods of the Data Provider protocol.
    """
    DPI = 1
    SUB = 2
    USB = 3
    UD3 = 4
    EOS = 5
    CLS = 6
    FAL = 7

    def __str__(self):
        return self.name


def write_init(exception=None):
    if not exception:
        return join(str(Method.DPI), "V")
    else:
        return _handle_exception(exception, join(str(Method.DPI), 'E'),
                                 DataProviderError)


@remoting_exception_on_parse(Method.DPI)
def read_init(data):
    return read_map(data, 0)


@remoting_exception_on_parse(Method.SUB)
def read_sub(data):
    return read(data, "S", 0)


def write_sub(exception=None):
    if not exception:
        return join(str(Method.SUB), "V")
    else:
        return _handle_exception(exception, join(str(Method.SUB), 'E'),
                                 SubscribeError,
                                 FailureError)


@remoting_exception_on_parse(Method.USB)
def read_usub(data):
    return read(data, "S", 0)


def write_unsub(exception=None):
    if not exception:
        return join(str(Method.USB), 'V')
    else:
        return _handle_exception(exception, join(str(Method.USB), 'E'),
                                 SubscribeError,
                                 FailureError)


def encode_value(value):
    if value is None or isinstance(value, str):
        return "S|" + enc_str(value)
    elif isinstance(value, bytes):
        return "Y|" + enc_byte(value)
    else:
        raise RemotingException(("Found value '{}' of an unsupported type "
                                 "while building a {} request")
                                .format(str(value), str(Method.UD3)))


def write_update_map(item_name, request_id, issnapshot, events_map):
    update = join(str(Method.UD3),
                  'S', enc_str(item_name),
                  'S', enc_str(request_id),
                  'B', enc_bool(issnapshot),
               append_separator=events_map)
    if events_map:
        tokens = [join('S', enc_str(field), encode_value(value))
                  for field, value in events_map.items()]
        return update + "|".join(tokens)
    else:
        return update


def write_eos(item, request_id):
    return join(str(Method.EOS), "S", enc_str(item), "S", enc_str(request_id))


def write_cls(item, request_id):
    return join(str(Method.CLS), "S", enc_str(item), "S", enc_str(request_id))


def write_failure(exception):
    return join(str(Method.FAL), 'E', enc_str(str(exception)))
