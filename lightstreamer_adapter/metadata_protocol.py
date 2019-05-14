"""Metadata Provider Protocol implementation."""
from enum import Enum
from lightstreamer_adapter.protocol import (join,
                                            read,
                                            read_seq,
                                            read_map,
                                            remoting_exception_on_parse,
                                            encode_string as enc_str,
                                            encode_double as enc_double,
                                            encode_boolean as enc_bool,
                                            encode_integer as enc_int,
                                            encode_modes as enc_modes,
                                            _handle_exception, _write_init)

from lightstreamer_adapter.protocol import (MetadataProviderError,
                                            NotificationError,
                                            AccessError,
                                            ItemsError,
                                            SchemaError,
                                            CreditsError)

from lightstreamer_adapter.interfaces.metadata import (MpnDeviceInfo,
                                                       TableInfo,
                                                       MpnSubscriptionInfo)


class Method(Enum):
    """Enum for representing the methods of the Metadata Provider protocol.
    """
    MPI = 1
    NUS = 2
    NUA = 3
    NNS = 4
    NSC = 5
    GIS = 6
    GSC = 7
    GIT = 8
    GUI = 9
    NUM = 10
    NNT = 11
    NTC = 12
    MDA = 13
    MSA = 14
    MDC = 15

    def __str__(self):
        return self.name


@remoting_exception_on_parse(Method.MPI)
def read_init(data):
    """Reads and parses a MPI ('Metadata Init') request."""
    return read_map(data, 0)


def write_init(proxy_parameters=None, exception=None):
    """Encodes and returns an MPI ('Metadata Init') response."""
    return _write_init(Method.MPI, MetadataProviderError, proxy_parameters,
                       exception)


@remoting_exception_on_parse(Method.NUS)
def read_notify_user(data):
    """Reads and parses a NUS ('Notify User') request."""
    return {"user": read(data, "S", 0),
            "password": read(data, "S", 2),
            "httpHeaders": read_map(data, 4)}


@remoting_exception_on_parse(Method.NUA)
def read_notify_user_auth(data):
    """Reads and parses a NUA (extended version of 'Notify User', to carry
    identification data included in the client SSL certificate)) request.
    """
    return {"user": read(data, "S", 0),
            "password": read(data, "S", 2),
            "clientPrincipal": read(data, "S", 4),
            "httpHeaders": read_map(data, 6)}


def write_notiy_user(method, allowed_max_bandwidth=None,
                     wants_tables_notification=None, exception=None):
    """Encodes and returns a NUS ('Notify User') response."""
    if not exception:
        return join(str(method),
                    'D', enc_double(allowed_max_bandwidth),
                    'B', enc_bool(wants_tables_notification))
    return _handle_exception(exception, join(str(method), 'E'), AccessError,
                             CreditsError)


@remoting_exception_on_parse(Method.NNS)
def read_notify_new_session(data):
    """Reads and parses a NNS ('Notify New Session') request."""
    return {"user": read(data, "S", 0),
            "session_id": read(data, "S", 2),
            "clientContext": read_map(data, 4)}


def write_notify_new_session(exception=None):
    """Encodes and returns a NNS ('Notify User') response."""
    method = str(Method.NNS)
    if not exception:
        return join(method, "V")
    return _handle_exception(exception, join(method, "E"), CreditsError,
                             NotificationError)


@remoting_exception_on_parse(Method.NSC)
def read_notifiy_session_close(data):
    """Reads and parses a NSC ('Notify Session Close') request."""
    return read(data, "S", 0)


def write_notify_session_close(exception=None):
    """Encodes and returns a NSC ('Notify Session Close') response."""
    method = str(Method.NSC)
    if not exception:
        return join(method, "V")
    return _handle_exception(exception, join(method, "E"), NotificationError)


@remoting_exception_on_parse(Method.GIS)
def read_get_items(data):
    """Reads and parses a GIS ('Get Items') request."""
    return {"user": read(data, "S", 0),
            "group": read(data, "S", 2),
            "session_id": read(data, "S", 4)}


def write_get_items(items=None, exception=None):
    """Encodes and returns a GIS ('Get Items') response."""
    method = str(Method.GIS)
    if exception:
        return _handle_exception(exception, join(method, 'E'),
                                 ItemsError)
    if items:
        return join(method, 'S|') + '|S|'.join([enc_str(item_name) for
                                                item_name in items])
    return join(method)


@remoting_exception_on_parse(Method.GSC)
def read_get_schema(data):
    """Reads and parses a GSC ('Get Schema') request."""
    return {"user": read(data, "S", 0), "group": read(data, "S", 2),
            "schema": read(data, "S", 4),
            "session_id": read(data, "S", 6)}


def write_get_schema(fields=None, exception=None):
    """Encodes and returns a GSC ('Get Items') response."""
    method = str(Method.GSC)
    if exception:
        return _handle_exception(exception, join(method, 'E'),
                                 ItemsError, SchemaError)
    if fields:
        return join(method, 'S|') + '|S|'.join([enc_str(field) for
                                                field in fields])
    return join(method)


@remoting_exception_on_parse(Method.GIT)
def read_get_item_data(data):
    """Reads and parses a GIT ('Get Item Data') request."""
    return read_seq(data, 0)


def write_get_item_data(items_data=None, exception=None):
    """Encodes and returns a GIT ('Get Item Data') response."""
    method = str(Method.GIT.name)
    if not exception:
        if items_data:
            encoded_items = [join('I', enc_int(data["distinctSnapshotLength"]),
                                  'D', enc_double(data["minSourceFrequency"]),
                                  'M', enc_modes(data["allowedModeList"]))
                             for data in items_data]
            return join(method, append_separator=True) + \
                "|".join(encoded_items)
        return join(method)
    return _handle_exception(exception, join(method, 'E'))


@remoting_exception_on_parse(Method.GUI)
def read_get_user_item_data(data):
    """Reads and parses a GUI ('Get User Item Data') request."""
    return {"user": read(data, 'S', 0),
            "items": read_seq(data, 2)}


def write_get_user_item_data(items_data=None, exception=None):
    """Encodes and returns a GUI ('Get User Item Data') response."""
    method = str(Method.GUI)
    if not exception:
        if items_data:
            encoded_items = [join('I', enc_int(data["allowedBufferSize"]),
                                  'D', enc_double(data["allowedMaxFrequency"]),
                                  'M', enc_modes(data["allowedModeList"]))
                             for data in items_data]

            return join(method, append_separator=True) + \
                "|".join(encoded_items)
        return join(method)
    return _handle_exception(exception, join(method, 'E'))


@remoting_exception_on_parse(Method.NUM)
def read_notify_user_message(data):
    """Reads and parses a NUM ('Notify User Message') request."""
    return {"user": read(data, 'S', 0),
            "session_id": read(data, 'S', 2),
            "message": read(data, 'S', 4)}


def write_notify_user_message(exception=None):
    """Encodes and returns a NUM ('Notify User Message') response."""
    method = str(Method.NUM)
    if not exception:
        return join(method, "V")
    return _handle_exception(exception, join(method, "E"), CreditsError,
                             NotificationError)


def _read_table(table, offset, with_selector=True):
    table_info = {'winIndex': read(table, "I", offset),
                  'mode': read(table, "M", offset + 2),
                  'group': read(table, "S", offset + 4),
                  'schema': read(table, "S", offset + 6),
                  'min': read(table, "I", offset + 8),
                  'max': read(table, "I", offset + 10),
                  'selector': (read(table, "S", offset + 12) if with_selector
                               else None)}

    return TableInfo(table_info['winIndex'], table_info['mode'],
                     table_info['group'], table_info['schema'],
                     table_info['min'], table_info['max'],
                     table_info['selector'])


def _read_tables(data, offset):
    tb_segs = data[offset:]
    tb_chunks = [tb_segs[i:i + 14] for i in range(0, len(tb_segs), 14)]
    table_infos = [_read_table(table, 0) for table in tb_chunks]
    return table_infos


@remoting_exception_on_parse(Method.NNT)
def read_notify_new_tables(data):
    """Reads and parses a NNT ('Notify New Table') request."""
    return {"user": read(data, "S", 0),
            "session_id": read(data, "S", 2),
            "tableInfos": _read_tables(data, 4)}


def write_notify_new_tables(exception=None):
    """Encodes and returns a NNT ('Notify New Table') response."""
    method = str(Method.NNT)
    if not exception:
        return join(method, "V")
    return _handle_exception(exception, join(method, "E"), CreditsError,
                             NotificationError)


@remoting_exception_on_parse(Method.NTC)
def read_notify_tables_close(data):
    """Reads and parses a NTC ('Notify Table Close') request."""
    return {"session_id": read(data, "S", 0),
            "tableInfos": _read_tables(data, 2)}


def write_notify_tables_close(exception=None):
    """Encodes and returns a NTC ('Notify Table Close') response."""
    method = str(Method.NTC)
    if not exception:
        return join(method, "V")
    return _handle_exception(exception, join(method, "E"), NotificationError)


def _read_mpn_device_info(data, offset=0):
    mobile_platform_type = read(data, "P", offset)
    application_id = read(data, "S", offset + 2)
    device_token = read(data, "S", offset + 4)

    return MpnDeviceInfo(mobile_platform_type,
                         application_id,
                         device_token)


@remoting_exception_on_parse(Method.MDA)
def read_notify_device_access(data):
    """Reads and parses a MDA ('Notify MPN Device Access') request."""
    return {"user": read(data, "S", 0),
            "sessionId": read(data, "S", 2),
            "mpnDeviceInfo": _read_mpn_device_info(data, 4)}


def write_notify_device_acces(exception=None):
    """Encodes and returns a MDA ('Notify MPN Device Access') response."""
    method = str(Method.MDA)
    if not exception:
        return join(method, "V")
    return _handle_exception(exception, join(method, "E"), CreditsError,
                             NotificationError)


def _read_subscription_info(data, offset):

    return MpnSubscriptionInfo(
        device=_read_mpn_device_info(data, offset),
        trigger=read(data, "S", offset + 6),
        notification_format=read(data, "S", offset + 8))


@remoting_exception_on_parse(Method.MSA)
def read_subscription_activation(data):
    """Reads and parses a MSA ('Notify MPN Subscription Activation') request.
    """
    values = dict()

    values["user"] = read(data, "S", 0)
    values["session_id"] = read(data, "S", 2)
    values["table"] = _read_table(data, 4, False)
    values["subscription"] = _read_subscription_info(data, 16)
    return values


def write_subscription_activation(exception=None):
    """Encodes and returns a MSA ('Notify MPN Subscription Activation')
    response.
    """
    method = str(Method.MSA)
    if not exception:
        return join(method, "V")
    return _handle_exception(exception, join(method, "E"), CreditsError,
                             NotificationError)


@remoting_exception_on_parse(Method.MDC)
def read_device_token_change(data):
    """Reads and parses a MDC ('Notify MPN Subscription Activation') request.
    """
    return {"user": read(data, "S", 0),
            "sessionId": read(data, "S", 2),
            "mpnDeviceInfo": _read_mpn_device_info(data, 4),
            "newDeviceToken": read(data, "S", 10)}


def write_device_token_change(exception=None):
    """Reads and parses a MDC ('Notify MPN Subscription Activation') request.
    """
    method = str(Method.MDC)
    if not exception:
        return join(method, "V")
    return _handle_exception(exception, join(method, "E"), CreditsError,
                             NotificationError)
