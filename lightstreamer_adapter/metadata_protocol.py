from enum import Enum
from lightstreamer_adapter.protocol import (join,
                                            read,
                                            read_token,
                                            read_seq,
                                            read_map,
                                            remoting_exception_on_parse,
                                            encode_string as enc_str,
                                            encode_double as enc_double,
                                            encode_boolean as enc_bool,
                                            encode_integer as enc_int,
                                            encode_modes as enc_modes,
                                            _handle_exception,
                                            RemotingException)

from lightstreamer_adapter.interfaces.metadata import (MetadataProviderError,
                                                       NotificationError,
                                                       AccessError,
                                                       ItemsError,
                                                       SchemaError,
                                                       CreditsError,
                                                       MpnDeviceInfo,
                                                       TableInfo,
                                                       MpnApnsSubscriptionInfo,
                                                       MpnGcmSubscriptionInfo)


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


MPN_SUBSCRIPTION_TYPES = ["PA", "PG"]


def write_init(exception=None):
    if not exception:
        return join(str(Method.MPI), "V")
    else:
        return _handle_exception(exception, join(str(Method.MPI), 'E'),
                                 MetadataProviderError)


@remoting_exception_on_parse(Method.MPI)
def read_init(data):
    return read_map(data, 0)


@remoting_exception_on_parse(Method.NUS)
def read_notify_user(data):
    return {"user": read(data, "S", 0),
            "password": read(data, "S", 2),
            "httpHeaders": read_map(data, 4)}


def write_notiy_user(method, allowed_max_bandwidth=None,
                     wants_tables_notification=None, exception=None):
    if not exception:
        return join(str(method),
                    'D', enc_double(allowed_max_bandwidth),
                    'B', enc_bool(wants_tables_notification))
    else:
        return _handle_exception(exception, join(str(method), 'E'),
                                 AccessError, CreditsError)


@remoting_exception_on_parse(Method.NUA)
def read_notify_user_auth(data):
    return {"user": read(data, "S", 0),
            "password": read(data, "S", 2),
            "clientPrincipal": read(data, "S", 4),
            "httpHeaders": read_map(data, 6)}


@remoting_exception_on_parse(Method.NNS)
def read_notify_new_session(data):
    return {"user": read(data, "S", 0),
            "session_id": read(data, "S", 2),
            "clientContext": read_map(data, 4)}


def write_notify_new_session(exception=None):
    method = str(Method.NNS)
    if not exception:
        return join(method, "V")
    else:
        return _handle_exception(exception, join(method, "E"),
                                 CreditsError,
                                 NotificationError)


@remoting_exception_on_parse(Method.NSC)
def read_notifiy_session_close(data):
    return read(data, "S", 0)


def write_notify_session_close(exception=None):
    method = str(Method.NSC)
    if not exception:
        return join(method, "V")
    else:
        return _handle_exception(exception, join(method, "E"),
                                 NotificationError)


@remoting_exception_on_parse(Method.GIS)
def read_get_items(data):
    return {"user": read(data, "S", 0),
            "group": read(data, "S", 2),
            "session_id": read(data, "S", 4)}


def write_get_items(items=None, exception=None):
    method = str(Method.GIS)
    if exception:
        return _handle_exception(exception, join(method, 'E'),
                                 ItemsError)
    else:
        if items:
            return join(method, 'S|') + '|S|'.join([enc_str(item) for
                                                    item in items])
        else:
            return join(method)


@remoting_exception_on_parse(Method.GSC)
def read_get_schema(data):
    return {"user": read(data, "S", 0), "group": read(data, "S", 2),
            "schema": read(data, "S", 4),
            "session_id": read(data, "S", 6)}


def write_get_schema(fields=None, exception=None):
    method = str(Method.GSC)
    if exception:
        return _handle_exception(exception, join(method, 'E'),
                                 ItemsError, SchemaError)
    else:
        if fields:
            return join(method, 'S|') + '|S|'.join([enc_str(field) for
                                                    field in fields])
        else:
            return join(method)


@remoting_exception_on_parse(Method.GIT)
def read_get_item_data(data):
    return read_seq(data, 0)


def write_get_item_data(items_data=None, exception=None):
    method = str(Method.GIT.name)
    if not exception:
        if items_data:
            encoded_items = [join('I', enc_int(data["distinctSnapshotLength"]),
                                  'D', enc_double(data["minSourceFrequency"]),
                                  'M', enc_modes(data["allowedModeList"]))
                             for data in items_data]
            return join(method, append_separator=True) + \
                "|".join(encoded_items)
        else:
            return join(method)
    else:
        return _handle_exception(exception, join(method, 'E'))


@remoting_exception_on_parse(Method.GUI)
def read_get_user_item_data(data):
    return {"user": read(data, 'S', 0),
            "items": read_seq(data, 2)}


def write_get_user_item_data(items_data=None, exception=None):
    method = str(Method.GUI)
    if not exception:
        if items_data:
            encoded_items = [join('I', enc_int(data["allowedBufferSize"]),
                                  'D', enc_double(data["allowedMaxFrequency"]),
                                  'M', enc_modes(data["allowedModeList"]))
                             for data in items_data]

            return join(method, append_separator=True) + \
                "|".join(encoded_items)
        else:
            return join(method)
    else:
        return _handle_exception(exception, join(method, 'E'))


@remoting_exception_on_parse(Method.NUM)
def read_notify_user_message(data):
    return {"user": read(data, 'S', 0),
            "session_id": read(data, 'S', 2),
            "message": read(data, 'S', 4)}


def write_notify_user_message(exception=None):
    method = str(Method.NUM)
    if not exception:
        return join(method, "V")
    else:
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
    return {"user": read(data, "S", 0),
            "session_id": read(data, "S", 2),
            "tableInfos": _read_tables(data, 4)}


def write_notify_new_tables_data(exception=None):
    method = str(Method.NNT)
    if not exception:
        return join(method, "V")
    else:
        return _handle_exception(exception, join(method, "E"), CreditsError,
                                 NotificationError)


@remoting_exception_on_parse(Method.NTC)
def read_notify_tables_close(data):
    return {"session_id": read(data, "S", 0),
            "tableInfos": _read_tables(data, 2)}


def write_notify_tables_close(exception=None):
    method = str(Method.NTC)
    if not exception:
        return join(method, "V")
    else:
        return _handle_exception(exception, join(method, "E"),
                                 NotificationError)


def _read_mpn_device_info(data, offset=0):
    mobile_platform_type = read(data, "P", offset)
    application_id = read(data, "S", offset + 2)
    device_token = read(data, "S", offset + 4)

    return MpnDeviceInfo(mobile_platform_type,
                         application_id,
                         device_token)


@remoting_exception_on_parse(Method.MDA)
def read_notify_device_access(data):
    return {"user": read(data, "S", 0),
            "mpnDeviceInfo": _read_mpn_device_info(data, 2)}


def write_notify_device_acces(exception=None):
    method = str(Method.MDA)
    if not exception:
        return join(method, "V")
    else:
        return _handle_exception(exception, join(method, "E"),
                                 CreditsError, NotificationError)


def _read_apn_subscription_info(data, offset):
    arg_length = int(read_token(data, offset))
    arguments = read_seq(data, offset + 22, arg_length * 2)
    cust_data_size = int(read_token(data, offset + 1))
    custom_data = read_map(data, offset + 22 + arg_length * 2,
                           cust_data_size * 4)

    return MpnApnsSubscriptionInfo(
        device=_read_mpn_device_info(data, offset + 2),
        trigger=read(data, "S", offset + 8),
        sound=read(data, "S", offset + 10),
        badge=read(data, "S", offset + 12),
        localized_action_key=read(data, "S", offset + 14),
        launch_image=read(data, "S", offset + 16),
        txt_format=read(data, "S", offset + 18),
        localized_format_key=read(data, "S", offset + 20),
        arguments=arguments,
        custom_data=custom_data)


def _read_gcm_subscription_info(data, offset):
    num_of_data = int(read_token(data, offset))
    map_start_idx = offset + 11
    map_length = num_of_data * 4

    return MpnGcmSubscriptionInfo(
        device=_read_mpn_device_info(data, offset + 1),
        trigger=read(data, "S", offset + 7),
        collapse_key=read(data, "S", offset + 9),
        data=read_map(data, map_start_idx, map_length),
        delay_while_idle=read(data, "S", map_start_idx + map_length),
        time_to_live=read(data, "S", map_start_idx + map_length + 2))


@remoting_exception_on_parse(Method.MSA)
def read_subscription_activation(data):
    values = dict()

    values["user"] = read(data, "S", 0)
    values["session_id"] = read(data, "S", 2)
    values["table"] = _read_table(data, 4, False)

    subscription_type = read_token(data, 16)
    if subscription_type == 'PA':
        values["subscription"] = _read_apn_subscription_info(data, 17)
    elif subscription_type == 'PG':
        values["subscription"] = _read_gcm_subscription_info(data, 17)
    else:
        raise RemotingException("Unsupported MPN subscription type")
    return values


def write_subscription_activation(exception=None):
    method = str(Method.MSA)
    if not exception:
        return join(method, "V")
    else:
        return _handle_exception(exception, join(method, "E"),
                                 CreditsError, NotificationError)


@remoting_exception_on_parse(Method.MDC)
def read_device_token_change(data):
    return {"user": read(data, "S", 0),
            "mpnDeviceInfo": _read_mpn_device_info(data, 2),
            "newDeviceToken": read(data, "S", 8)}


def write_device_token_change(exception=None):
    method = str(Method.MDC)
    if not exception:
        return join(method, "V")
    else:
        return _handle_exception(exception, join(method, "E"), CreditsError,
                                 NotificationError)
