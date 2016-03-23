from enum import Enum
from lightstreamer_adapter.protocol import (join,
                                            encode_string as enc_str,
                                            encode_boolean as enc_bool,
                                            encode_value as enc_val,
                                            read,
                                            read_map,
                                            remoting_exception,
                                            _handle_exception)

from lightstreamer_adapter.interfaces.data import (DataProviderError,
                                                   SubscribeError,
                                                   FailureError)


class Method(Enum):
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
        return join(Method.DPI, "V")
    else:
        return _handle_exception(exception, join(Method.DPI, 'E'),
                                 DataProviderError)


@remoting_exception(Method.DPI)
def read_init(data):
    return read_map(data, 0)


@remoting_exception(Method.SUB)
def read_sub(data):
    return read(data, "S", 0)


def write_sub(exception=None):
    if not exception:
        return join(Method.SUB, "V")
    else:
        return _handle_exception(exception, join(Method.SUB, 'E'),
                                 SubscribeError,
                                 FailureError)


@remoting_exception(Method.USB)
def read_usub(data):
    return read(data, "S", 0)


def write_unsub(exception=None):
    if not exception:
        return join(Method.USB, 'V')
    else:
        return _handle_exception(exception, join(Method.USB, 'E'),
                                 SubscribeError,
                                 FailureError)


# throw new RemotingException("Found value '" + value.toString() + "'
# of an unsupported type while building a " + METHOD_UPDATE_BY_MAP +
# " request");
def write_update_map(item_name, request_id, issnapshot, events_map):
    qry = join(Method.UD3,
               'S', enc_str(item_name),
               'S', enc_str(request_id),
               'B', enc_bool(issnapshot),
               append=True)
    try:
        tokens = [join('S', enc_str(field), enc_val(value))
                  for field, value in events_map.items()]
        return qry + "|".join(tokens)
    except Exception:
        raise Exception("Error while base64-encoding bytes")


def write_eos(item, request_id):
    return join(Method.EOS, "S", enc_str(item), "S", enc_str(request_id))


def write_cls(item, request_id):
    return join(Method.CLS, "S", enc_str(item), "S", enc_str(request_id))


def write_failure(exception):
    return join(Method.FAL, 'E', enc_str(str(exception)))
