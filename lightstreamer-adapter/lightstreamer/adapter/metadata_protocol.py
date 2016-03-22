from enum import Enum
from lightstreamer.adapter.protocol import (join,
                                            read,
                                            read_token,
                                            toMap,
                                            seq,
                                            remoting_exception,
                                            encode_string as enc_str,
                                            encode_double as enc_double,
                                            encode_boolean as enc_bool,
                                            encode_modes as enc_modes,
                                            _handle_exception,
                                            RemotingException)

from lightstreamer.interfaces.metadata import (MetadataProviderError,
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
        return join(Method.MPI, "V")
    else:
        return _handle_exception(exception, join(Method.MPI, 'E'),
                                 MetadataProviderError)


@remoting_exception(Method.MPI)
def read_init(data):
    return toMap(data, 0)


@remoting_exception(Method.NUS)
def read_notify_user(data):
    return {"user": read(data, "S", 0), "password": read(data, "S", 2),
            "httpHeaders": toMap(data, 4)}


def write_notiy_user(method, allowed_max_bandwidth=None,
                     wants_tables_notification=None, exception=None):
    if not exception:
        return join(method,
                    'D', enc_double(allowed_max_bandwidth),
                    'B', enc_bool(wants_tables_notification))
    else:
        return _handle_exception(exception, join(method, 'E'), AccessError,
                                 CreditsError)


@remoting_exception(Method.NUA)
def read_notify_user_auth(data):
    return {"user": read(data, "S", 0), "password": read(data, "S", 2),
            "clientPrincipal": read(data, "S", 4),
            "httpHeaders": toMap(data, 6)}


@remoting_exception(Method.NNS)
def read_notify_new_session(data):
    return {"user": read(data, "S", 0), "session_id": read(data, "S", 2),
            "clientContext": toMap(data, 4)}


def write_notify_new_session(exception=None):
    if not exception:
        return join(Method.NNS, "V")
    else:
        return _handle_exception(exception, join(Method.NNS, "E"),
                                 CreditsError,
                                 NotificationError)


@remoting_exception(Method.NSC)
def read_notifiy_session_close(data):
    return read(data, "S", 0)


def write_notify_session_close(exception=None):
    if not exception:
        return join(Method.NSC, "V")
    else:
        return _handle_exception(exception, join(Method.NSC, "E"),
                                 NotificationError)


@remoting_exception(Method.GIS)
def read_get_items(data):
    return {"user": read(data, "S", 0), "group": read(data, "S", 2),
            "session_id": read(data, "S", 4)}


def write_get_items(items=None, exception=None):
    if items:
        return join(Method.GIS, 'S|') + '|S|'.join([enc_str(item) for item in
                                                    items])
    elif exception:
        return _handle_exception(exception, join(Method.GIS, 'E'), ItemsError)


@remoting_exception(Method.GSC)
def read_get_schema(data):
    return {"user": read(data, "S", 0), "group": read(data, "S", 2),
            "schema": read(data, "S", 4),
            "session_id": read(data, "S", 6)}


def write_get_schema(fields=None, exception=None):
    if fields:
        return join(Method.GSC, 'S|') + '|S|'.join([enc_str(field) for field
                                                    in fields])
    elif exception:
        return _handle_exception(exception, join(Method.GSC, 'E'), ItemsError,
                                 SchemaError)


@remoting_exception(Method.GIT)
def read_get_item_data(data):
    return seq(data, 0)


def write_get_item_data(items_data=None, exception=None):
    if not exception:
        encoded_items = [join('I', data["distinctSnapshotLength"],
                              'D', enc_double(data["minSourceFrequency"]),
                              'M', enc_modes(data["allowedModeList"]))
                         for data in items_data]
        if encoded_items:
            return join(Method.GIT, append=True) + "|".join(encoded_items)
        else:
            return join(Method.GIT)
    else:
        return _handle_exception(exception, join(Method.GIT, 'E'))


@remoting_exception(Method.GUI)
def read_get_user_item_data(data):
    return {"user": read(data, 'S', 0), "items": seq(data, 2)}


def write_get_user_item_data(items_data=None, exception=None):
    if not exception:
        encoded_items = [join('I', data["allowedBufferSize"],
                              'D', enc_double(data["allowedMaxFrequency"]),
                              'M', enc_modes(data["allowedModeList"]))
                         for data in items_data]
        if encoded_items:
            return join(Method.GUI, append=True) + "|".join(encoded_items)
        else:
            return join(Method.GUI)
    else:
        return _handle_exception(exception, join(Method.GUI, 'E'))


@remoting_exception(Method.NUM)
def read_notify_user_message(data):
    return {"user": read(data, 'S', 0), "session_id": read(data, 'S', 2),
            "message": read(data, 'S', 4)}


def write_notify_user_message(exception=None):
    if not exception:
        return join(Method.NUM, "V")
    else:
        return _handle_exception(exception, join(Method.NUM, "E"),
                                 CreditsError, NotificationError)


def _read_table(table, offset, with_selector=True):
    table_info = {'winIndex': read(table, "I", offset),
                  'mode': read(table, "M", offset + 2),
                  'group': read(table, "S", offset + 4),
                  'schema': read(table, "S", offset + 6),
                  'min': read(table, "I", offset + 8),
                  'max': read(table, "I", offset + 10),
                  'selector': read(table, "S", offset + 12) if with_selector
                  else None}

    return TableInfo(table_info['winIndex'], table_info['mode'],
                     table_info['group'], table_info['schema'],
                     table_info['min'], table_info['max'],
                     table_info['selector'])


def _read_tables(data, offset):
    tb_segs = data[offset:]
    tb_chunks = [tb_segs[i:i + 14] for i in range(0, len(tb_segs), 14)]
    tableInfos = [_read_table(table, 0) for table in tb_chunks]
    return tableInfos


@remoting_exception(Method.NNT)
def read_notify_new_tables(data):
    return {"user": read(data, "S", 0), "session_id": read(data, "S", 2),
            "tableInfos": _read_tables(data, 4)}


def write_notify_new_tables_data(exception=None):
    if not exception:
        return join(Method.NNT, "V")
    else:
        return _handle_exception(exception, join(Method.NNT, "E"),
                                 CreditsError, NotificationError)


@remoting_exception(Method.NTC)
def read_notify_tables_close(data):
    return {"session_id": read(data, "S", 0),
            "tableInfos": _read_tables(data, 2)}


def write_notify_tables_close(exception=None):
    if not exception:
        return join(Method.NTC, "V")
    else:
        return _handle_exception(exception, join(Method.NTC, "E"),
                                 NotificationError)


def _read_mpn_device_info(data, offset=0):
    mobile_platform_type = read(data, "P", offset)
    application_id = read(data, "S", offset + 2)
    device_token = read(data, "S", offset + 4)

    return MpnDeviceInfo(mobile_platform_type,
                         application_id,
                         device_token)


@remoting_exception(Method.MDA)
def read_notify_device_access(data):
    return {"user": read(data, "S", 0),
            "mpnDeviceInfo": _read_mpn_device_info(data, 2)}


def write_notify_device_acces(exception=None):
    if not exception:
        return join(Method.MDA, "V")
    else:
        return _handle_exception(exception, join(Method.MDA, "E"),
                                 CreditsError, NotificationError)


def _read_mobile_apn_subscriptionInfo(data, offset):
    arg_length = int(read_token(data, offset))
    arguments = seq(data, offset + 22, arg_length * 2)
    cust_data_size = int(read_token(data, offset + 1))
    custom_data = toMap(data, offset + 22 + arg_length * 2, cust_data_size * 4)

    return MpnApnsSubscriptionInfo(
                            device=_read_mpn_device_info(data, offset + 2),
                            trigger=read(data, "S", offset + 8),
                            sound=read(data, "S", offset + 10),
                            badge=read(data, "S", offset + 12),
                            localized_action_key=read(data, "S", offset + 14),
                            launch_image=read(data, "S", offset + 16),
                            format=read(data, "S", offset + 18),
                            localized_format_key=read(data, "S", offset + 20),
                            arguments=arguments,
                            custom_data=custom_data)


def _read_mobile_gcm_subscriptionInfo(data, offset):
    num_of_data = int(read_token(data, offset))
    map_start_idx = offset + 11
    map_length = num_of_data * 4

    return MpnGcmSubscriptionInfo(
                  device=_read_mpn_device_info(data, offset + 1),
                  trigger=read(data, "S", offset + 7),
                  collapse_key=read(data, "S", offset + 9),
                  data=toMap(data, map_start_idx, map_length),
                  delay_while_idle=read(data, "S", map_start_idx + map_length),
                  time_to_live=read(data, "S", map_start_idx + map_length + 2))


@remoting_exception(Method.MSA)
def read_notify_subscription_activation(data):
    values = dict()

    values["user"] = read(data, "S", 0)
    values["session_id"] = read(data, "S", 2)
    values["table"] = _read_table(data, 4, False)

    subscriptionType = read_token(data, 16)
    if subscriptionType == 'PA':
        values["subscription"] = _read_mobile_apn_subscriptionInfo(data, 17)
    elif subscriptionType == 'PG':
        values["subscription"] = _read_mobile_gcm_subscriptionInfo(data, 17)
    else:
        raise RemotingException("Unsupported MPN subscription type")

    return values


def write_notify_subscription_activation(exception=None):
    if not exception:
        return join(Method.MSA, "V")
    else:
        return _handle_exception(exception, join(Method.MSA, "E"),
                                 CreditsError, NotificationError)


@remoting_exception(Method.MDC)
def read_notify_device_token_change(data):
    return {"user": read(data, "S", 0),
            "mpnDeviceInfo": _read_mpn_device_info(data, 2),
            "newDeviceToken": read(data, "S", 8)}


def write_notify_device_token_change(exception=None):
    if not exception:
        return join(Method.MDC, "V")
    else:
        return _handle_exception(exception, join(Method.MDC, "E"),
                                 CreditsError, NotificationError)
