"""
This module contains all classes, enums and exceptions needed to create and
manage a Remote Metadata Adapter.
"""

from enum import Enum

__all__ = ['MetadataProvider', 'Mode', 'MpnPlatformType', 'MpnDeviceInfo',
           'MpnSubscriptionInfo', 'TableInfo', 'MetadataError',
           'MetadataProviderError', 'NotificationError', 'AccessError',
           'ItemsError', 'SchemaError', 'CreditsError',
           'ConflictingSessionError']


class MetadataProvider():
    """Provides a base class to be extended by a Remote Metadata Adapter in
    order to attach a Metadata Provider to Lightstreamer.

    To facilitate the coding of the Adapter, each method provides a default
    implementation to allow a simple default behavoiur, which can be overridden
    by extending this class and supplying the custom implementation.

    Such default behavoiur will be specified in this documentation trough the
    use of the marker: **IMPLEMENTATION NOTE**.

    An instance of a Remote Metadata Adapter is supplied to Lightstreamer
    through a :class:`lightstreamer_adapter.server.MetadataProviderServer`
    instance.

    A Metadata Provider is used by Lightstreamer Kernel in combination with one
    or multiple Data Providers, uniquely associated with it; it is consulted
    in order to manage the push Requests intended for the associated Data
    Providers. A Metadata Provider supplies information for several different
    goals:

    * the resolution of the Group/Schema names used in the Requests;
    * the check of the User accessibility to the requested Items;
    * the check of the resource level granted to the User;
    * the request for specific characteristics of the Items.

    Note: Each Item may be supplied by one or more of the associated Data
    Adapters and each client Request must reference to a specific Data Adapter.
    However, in the current version of the interface, no Data Adapter
    information is supplied to the Metadata Adapter methods. Hence, the Item
    names must provide enough information for the methods to give an answer.
    As a consequence, for instance, the frequency, snapshot length and other
    characteristics of an item are the same regardless of the Data Adapter
    it is requested from. More likely, for each item name defined, only one
    of the Data Adapters in the set is responsible for supplying that item.

    All implementation methods should perform as fast as possible. See the
    notes on the corresponding methods in the Java In-Process interface for the
    method-related details. Also consider that the roundtrip time involved in
    the remote call adds up to each call time anyway.

    In order to avoid that delays on calls for one session propagate to other
    sessions, the size of the thread pool devoted to the management of the
    client requests should be properly set, through the <server_pool_max_size>
    flag, in the Server configuration file.

    Alternatively, a dedicated pool, properly sized, can be defined for the
    involved Adapter Set in the adapters.xml. Still more restricted dedicated
    pools can be defined for the authorization-related calls and for each Data
    Adapter in the Adapter Set. The latter pool would also run any Metadata
    Adapter method related to the items supplied by the specified Data Adapter.
    """

    def initialize(self, parameters, config_file=None):
        """Called by Lightstreamer Kernel through the Remote Server to provide
        initialization information to the Metadata Adapter.

        The call must not be blocking; any polling cycle or similar must be
        started in a different thread. Any delay in returning from this call
        will in turn delay the Server initialization. If an exception occurs in
        this method, Lightstreamer Kernel can't complete the startup and must
        exit.

        :param dict parameters: A dictionary object that contains key-value
         pairs corresponding to the parameters elements supplied for the
         Metadata Adapter configuration. Both key and values are represented as
         string objects. The parameters can be supplied through the
         :meth:`lightstreamer_adapter.server.MetadataProviderServer.adapter_params`
         property of the ``MetadataProviderServer`` instance. More parameters
         can be added by leveraging the ``init_remote`` parameter in the Proxy
         Adapter configuration.
        :param str config_file: The path on the local disk of the Metadata
         Adapter configuration file. The file path can be supplied through the
         :meth:`lightstreamer_adapter.server.MetadataProviderServer.adapter_config`
         propery of the used ``MetadataProviderServer`` instance.
        :raises \
        lightstreamer_adapter.interfaces.metadata.MetadataProviderError:
         (never raised in the default implementation) in case an error occurs
         that prevents the correct behavior of the Metadata Adapter.

        **IMPLEMENTATION NOTE:** does nothing.
        """

    def notify_user(self, user, password, http_headers):
        """Called by Lightstreamer Kernel through the Remote Server as a
        preliminary check that a user is enabled to make Requests to the
        related Data Providers. It is invoked upon each session request and it
        is called prior to any other session-related request. So, any other
        method with a User  argument can assume that the supplied User argument
        has already been checked.

        The User authentication should be based on the user and password
        arguments supplied by the client. The full report of the request HTTP
        headers is also available; they could be used in order to gather
        information about the client, but should not be used for
        authentication, as they may not be under full control by client code.
        See also the discussion about the <use_protected_js> Server
        configuration element, if available.

        This method runs in the Server authentication thread pool, if defined.

        :param str user: A User name.
        :param str password: A password optionally required to validate the
         User.
        :param dict http_headers: A dictionary object that contains a
         key-value pair for each header found in the HTTP request that
         originated the call. The header names are reported in lower-case form.

         For headers defined multiple times, a unique key-value pair is
         reported, where the value is a concatenation of all the supplied
         header values, separated by a ",". One pair is added by Lightstreamer
         Server; the name is "REQUEST_ID" and the value is a unique id assigned
         to the client request.
        :raises lightstreamer_adapter.interfaces.metadata.AccessError: if the
         User name is not known or the supplied password is not correct.
        :raises lightstreamer_adapter.interfaces.metadata.CreditsError: if the
         User is known but is not enabled to make further Requests at the
         moment.

        **IMPLEMENTATION NOTE:** does nothing.
        """

    def notify_user_with_principal(self, user, password, http_headers,
                                   client_principal=None):
        """Called by Lightstreamer Kernel, through the Remote Server, instead
        of calling the :meth:`notify_user` method, in case the Server has been
        instructed to acquire the client principal from the client TLS/SSL
        certificate through the <use_client_auth> configuration flag.

        Note that the above flag can be set for each listening port
        independently (and it can be set for TLS/SSL ports only), hence, both
        methods may be invoked, depending on the port used by the client. Also
        note that in case client certificate authentication is not forced on a
        listening port through <force_client_auth>, a client request issued on
        that port may not be authenticated, hence it may have no principal
        associated. In that case, if <use_client_auth> is set, this method will
        still be invoked, with ``None`` principal.

        See
        :meth:`notify_user` for other notes.

        **Edition Note:** https is an optional feature, available depending
        on Edition and License Type.
        To know what features are enabled by your license, please see the
        License tab of the Monitoring Dashboard (by default, available at
        /dashboard).

        :param str user: A User name.
        :param str password: A password optionally required to validate the
         User.
        :param dict http_headers: A dictionary object that contains a
         key-value pair for each header found in the HTTP request that
         originated the call. The header names are reported in lower-case form.

         For headers defined multiple times, a unique key-value pair is
         reported, where the value is a concatenation of all the supplied
         header values, separated by a ",". One pair is added by Lightstreamer
         Server; the name is "REQUEST_ID" and the value is a unique id assigned
         to the client request.
        :param str client_principal: the identification name reported in the
         client TLS/SSL certificate supplied on the socket connection used to
         issue the request that originated the call; it can be not specified
         if client has not authenticated itself or the authentication has
         failed.
        :raises lightstreamer_adapter.interfaces.metadata.AccessError: if the
         User name is not known or the supplied password is not correct.
        :raises lightstreamer_adapter.interfaces.metadata.CreditsError: if the
         User is known but is not enabled to make further Requests at the
         moment.


        **IMPLEMENTATION NOTE:** invokes the :meth:`notify_user` method, where
        the ``client_principal`` argument is discarded.
        """
        self.notify_user(user, password, http_headers)

    def get_items(self, user, session_id, group):
        """
        Called by Lightstreamer Kernel through the Remote Server to resolve an
        Item Group name (or Item List specification) supplied in a Request. The
        names of the Items in the Group must be returned. For instance, the
        client could be allowed to specify the "NASDAQ100" Group name and,
        upon that, the list of all items corresponding to the stocks included
        in that index could be returned.

        Possibly, the content of an Item Group may be dependant on the User who
        is issuing the Request or on the specific Session instance.

        When an Item List specification is supplied, it is made of a
        space-separated list of the names of the Items in the List. This
        convention is used by some of the subscription methods provided by the
        various client libraries.

        When any of these interface methods is used by client code accessing
        this Remote Metadata Adapter, the supplied "group" argument should be
        inspected as a space-separated list of Item names and an array with
        these names in the same order should be returned.

        Another typical case is when the same Item has different contents
        depending on the User that is issuing the request. On the Data Adapter
        side, different Items (one for each User) can be used; nevertheless, on
        the client side, the same name can be specified in the subscription
        request and the actual user-related name can be determined and
        returned here. For instance::

         if group == "portfolio":
             item_name = "PF_" + user
             return [item_name]
         elif group.startswith("PF"):
             # protection from unauthorized use of user-specific items
             raise ItemsError("Unexpected group name");

        Obviously, the two above techniques can be combined, hence any element
        of an Item List can be replaced with a decorated or alternative Item
        name: the related updates will be associated to the original name used
        in the supplied Item List specification by client library _code.

        This method runs in the Server thread pool specific for the Data
        Adapter that supplies the involved Items, if defined.

        :param str user: A User name.
        :param str session_id: The ID of a Session owned by the User.
        :param str group: An Item Group name (or Item List specification).
        :return: A list of the names of the Items in the Group.
        :rtype: list of strings
        :raises lightstreamer_adapter.interfaces.metadata.ItemsError: if the
         supplied Item Group name (or Item List specification) is not
         recognized.

        **IMPLEMENTATION NOTE:** handles Item List specifications, therefore
        the result is simply a list of all space-separated names forming the
        Item Group Name.
        """
        return group.split(' ')

    def get_schema(self, user, session_id, group, schema):
        """Called by Lightstreamer Kernel through the Remote Server to resolve
        a Field Schema name (or Field List specification) supplied in a
        Request. The names of the Fields in the Schema must be returned.

        Possibly, the content of a Field Schema may be dependent on the User
        who is issuing the Request, on the specific Session instance or on the
        Item Group (or Item List) to which the Request is related.

        When a Field List specification is supplied, it is made of a
        space-separated list of the names of the Fields in the Schema. This
        convention is used by some of the subscription methods provided by the
        various client libraries.

        When any of these interface methods is used by client code accessing
        this Remote Metadata Adapter, the supplied "schema" argument should be
        inspected as a space-separated list of Field names and an array with
        these names in the same order should be returned; returning decorated
        or alternative Field names is also possible: they will be associated to
        the corresponding names used in the supplied Field List specification
        by client library client_error_code.

        This method runs in the Server thread pool specific for the Data
        Adapter that supplies the involved Items, if defined.

        :param str user: A User name.
        :param str session_id: The ID of a Session owned by the User.
        :param str group: The name of the Item Group (or specification of the
         Item List) whose Items the Schema is to be applied to.
        :param str schema: A Field Schema name (or Field List specification).
        :return: A list of the names of the Fields in the Schema
        :rtype: list of string
        :raises lightstreamer_adapter.interfaces.metadata.ItemsError: if the
         supplied Item Group name (or Item List specification) is not
         recognized.
        :raises lightstreamer_adapter.interfaces.metadata.SchemaError: if the
         supplied Field Schema name (or Field List specification) is not
         recognized.

        **IMPLEMENTATION NOTE:** handles Field List specifications, therefore
        the result is simply a list of all space-separated names forming the
        Field Schema.
        """
        return schema.split(' ')

    def get_allowed_max_bandwidth(self, user):
        """Called by Lightstreamer Kernel through the Remote Server to ask for
        the bandwidth level to be allowed to a User for a push Session.

        This method runs in the Server authentication thread pool, if defined.

        **Edition Note:** Bandwidth Control is an optional feature,
        available depending on Edition and License Type.
        To know what features are enabled by your license, please see the
        License tab of the Monitoring Dashboard (by default, available at
        /dashboard).

        :param str user: An User
        :return: The allowed bandwidth, in Kbit/sec. A zero return value means
         an unlimited bandwidth.
        :rtype: float

        **IMPLEMENTATION NOTE:** Always returns a zero value.
        """
        return 0.0

    def get_allowed_max_item_frequency(self, user, item):
        """Called by Lightstreamer Kernel through the Remote Server to ask for
        the ItemUpdate frequency to be allowed to a User for a specific Item.
        An unlimited frequency can also be specified. Such filtering applies
        only to Items requested with publishing Mode MERGE, DISTINCT and
        COMMAND (in the latter case, the frequency limitation applies to the
        UPDATE events for each single key). If an Item is requested with
        publishing Mode MERGE, DISTINCT or COMMAND and unfiltered dispatching
        has been specified, then returning any limited maximum frequency will
        cause the refusal of the request by the Kernel.

        This method runs in the Server thread pool specific or the Data Adapter
        that supplies the involved items, if defined.

        **Edition Note:** A further global frequency limit could also
        be imposed by the Server, depending on Edition and License Type; this
        specific limit also applies to RAW mode and to unfiltered dispatching.
        To know what features are enabled by your license, please see the
        License tab of the Monitoring Dashboard (by default, available at
        /dashboard).

        :param str user: An User
        :param str item: An Item name
        :return: The allowed Update frequency, in Updates/sec. A zero return
         value means no frequency restriction.
        :rtype: float

        **IMPLEMENTATION NOTE:** always returns zero, to mean no frequency
        limit. This also enables unfiltered dispatching for Items subscribed
        in MERGE or DISTINCT mode.
        """
        return 0.0

    def get_allowed_buffer_size(self, user, item):
        """Called by Lightstreamer Kernel through the Remote Server to ask for
        the maximum size allowed for the buffer internally used to enqueue
        subsequent ItemUpdates for the same Item. If this buffer is more than 1
        element deep, a short burst of ItemEvents from the Data Adapter can be
        forwarded to the Client without losses, though with some delay. The
        buffer size is specified in the Request. Its maximum allowed size can
        be different for different Users. Such buffering applies only to Items
        requested with publishing Mode MERGE or DISTINCT. However, if the Item
        has been requested with unfiltered dispatching, then the buffer size is
        always unlimited and buffer size settings are ignored.

        This method runs in the Server thread pool specific for the Data
        Adapter that supplies the involved items, if defined.

        :param str user: An User
        :param str item: A Item Name
        :return: The allowed buffer size. A zero return value means a
         potentially unlimited buffer.
        :rtype: int

        **IMPLEMENTATION NOTE:** always returns zero, to mean no size limit.
        """
        return 0

    def ismode_allowed(self, user, item, mode):
        """Called by Lightstreamer Kernel through the Remote Server to ask for
        the allowance of a publishing Mode for an Item. A publishing Mode can
        or cannot be allowed depending on the User. The Metadata Adapter should
        ensure that conflicting Modes are not both allowed for the same Item
        (even for different Users), otherwise some Requests will be eventually
        refused by Lightstreamer Kernel. The conflicting Modes are MERGE,
        DISTINCT and COMMAND.

        This method runs in the Server thread pool specific for the Data
        Adapter that supplies the involved items, if defined.

        :param str user: A User name.
        :param str item: An Item Name.
        :param Mode mode: A publishing Mode.
        :return: ``True`` if the publishing Mode is allowed.
        :rtype: bool

        **IMPLEMENTATION NOTE:** always return ``True``. As a consequence,
        conflicting Modes may be both allowed for the same Item, so the Clients
        should ensure that the same Item cannot be requested in two conflicting
        Modes.
        """
        return True

    def mode_may_be_allowed(self, item, mode):
        """Called by Lightstreamer Kernel through the Remote Server to ask for
        the allowance of a publishing Mode for an Item (for at least one User).
        The Metadata Adapter should ensure that conflicting Modes are not both
        allowed for the same Item. The conflicting Modes are MERGE, DISTINCT
        and COMMAND.

        This method runs in the Server thread pool specific for the Data
        Adapter that supplies the involved items, if defined.

        :param str item: An Item Name.
        :param Mode mode: A publishing Mode.
        :return: ``True`` if the publishing Mode is allowed.
        :rtype: bool

        **IMPLEMENTATION NOTE:** always return ``True``. As a consequence,
        conflicting Modes may be both allowed for the same Item, so the Clients
        should ensure that the same Item cannot be requested in two conflicting
        Modes.
        This is just to simplify the development phase; the implementation of
        the overriding method MUST be different, to ensure that conflicting
        modes (i.e. MERGE, DISTINCT and COMMAND) are not both allowed for the
        same Item.
        """
        return True

    def get_min_source_frequency(self, item):
        """Called by Lightstreamer Kernel through the Remote Server to ask for
        the minimum ItemEvent frequency from the Data Adapter at which the
        events for an Item are guaranteed to be delivered to the Clients
        without loss of information. In case of an incoming ItemEvent frequency
        greater than this value, Lightstreamer Kernel may prefilter the events.
        Such prefiltering applies only for Items requested with publishing Mode
        MERGE or DISTINCT. The frequency set should be greater than the
        ItemUpdate frequencies allowed to the different Users for that Item.
        Moreover, because this filtering is made without buffers, the frequency
        set should be far greater than the ItemUpdate frequencies allowed for
        that Item for which buffering of event bursts is desired. If an Item is
        requested with publishing Mode MERGE or DISTINCT and unfiltered
        dispatching, then specifying any limited source frequency will cause
        the refusal of the request by the Kernel. This feature is just for
        ItemEventBuffers protection against Items with a very fast flow on the
        Data Adapter and a very slow flow allowed to the Clients. If this is
        the case, but just a few Clients need a fast or unfiltered flow for the
        same MERGE or DISTINCT Item, the use of two differently named Items
        that receive the same flow from the Data Adapter is suggested.

        This method runs in the Server thread pool specific for the Data
        Adapter that supplies the involved items, if defined.

        :param item: An Item Name.
        :return: The minimum ItemEvent frequency that must be processed without
         loss of information, in ItemEvents/sec. A zero return value indicates
         that incoming ItemEvents must not be prefiltered. If the ItemEvents
         frequency for the Item is known to be very low, returning zero allows
         Lightstreamer Kernel to save any prefiltering effort.
        :rtype: float

        **IMPLEMENTATION NOTE:** The Metadata Adapter can't set any minimum
        frequency; this also enables unfiltered dispatching for Items
        subscribed in MERGE or DISTINCT mode. Therefore the method always
        returns zero, to mean that incoming ItemEvents must not be prefiltered.
        """
        return 0.0

    def get_distinct_snapshot_length(self, item):
        """Called by Lightstreamer Kernel through the Remote Server to ask for
        the maximum allowed length for a Snapshot of an Item that has been
        requested with publishing Mode DISTINCT. In fact, in DISTINCT
        publishing Mode, the Snapshot for an Item is made by the last events
        received for the Item and the Client can specify how many events it
        would like to receive. Thus, Lightstreamer Kernel must always keep a
        buffer with some of the last events received for the Item and the
        length of the buffer is limited by the value returned by this method.
        The maximum Snapshot size cannot be unlimited.

        This method runs in the Server thread pool specific for the Data
        Adapter hat supplies the involved items, if defined.

        :param str item: An Item Name.
        :return: The maximum allowed length for the Snapshot; a zero return
         value means that no Snapshot information should be kept.
        :rtype: int

        **IMPLEMENTATION NOTE:** always return a value of 0, to mean that no
        events are specified, so snapshot will not be managed
        """
        return 0

    def notify_user_message(self, user, session_id, message):
        """Called by Lightstreamer Kernel through the Remote Server to forward
        a message received by a User. The interpretation of the message is up
        to the Metadata Adapter. A message can also be refused.

        This method runs in the Server thread pool specific for the Adapter
        Set, if defined.

        :param str user: A User name.
        :param str session_id: The ID of a Session owned by the User.
        :param str message: A string.
        :raises lightstreamer_adapter.interfaces.metadata.CreditsError: in case
         the User is not enabled to send the message or the message cannot be
         correctly managed.
        :raises lightstreamer_adapter.interfaces.metadata.NotificationError: in
         case something is wrong in the parameters, such as a nonexistent
         Session ID.

        **IMPLEMENTATION NOTE:** the Metadata Adapter does never accept the
         message, therefore a :class:`CreditsError` is raised.
        """
        raise CreditsError(0, "Unsupported function")

    def notify_new_session(self, user, session_id, client_context):
        """Called by Lightstreamer Kernel through the Remote Server to check
        that a User is enabled to open a new push Session. If the check
        succeeds, this also notifies the Metadata Adapter that the Session is
        being assigned to the User.

        Request context information is also available; this allows for
        differentiating group, schema and message management based on specific
        Request characteristics.

        This method runs in the Server thread pool specific for the Adapter
        Set, if defined.

        :param str user: A User name.
        :param str session_id: The ID of a new Session.
        :param dict client_context: A dictionary object that contains key-value
         pairs with various information about the request context. All values
         are supplied as strings. Information related to a client connection
         refers to the HTTP request that originated the call.
         Available keys are:

         * "REMOTE_IP" - string representation of the remote IP
           related to the current connection; it may be a proxy address
         * "REMOTE_PORT" - string representation of the remote port related to
           the current connection
         * "USER_AGENT" - the user-agent as declared in the current connection
           HTTP header
         * "FORWARDING_INFO" - the content of the X-Forwarded-For HTTP header
           related to the current connection; intermediate proxies usually set
           this header to supply connection routing information
         * "LOCAL_SERVER" - the name of the specific server socket that handles
           the current connection, as configured through the <http_server> or
           <https_server> element
         * "CLIENT_TYPE" - the type of client API in use; the value may be null
           for some old client APIs
         * "CLIENT_VERSION" - the signature, including version and build
           number, of the client API in use; the signature may be only
           partially complete, or even null, for some old client APIs and for
           some custom clients
         * "REQUEST_ID" - the same id that has just been supplied to
           :meth:`notify_user` method for the current client request instance;
           this allows for using local authentication-related details for the
           authorization task. Note: the Remote Adapter is responsible for
           disposing any cached information in case this method is not called
           because of any early error during request management.

        :raises lightstreamer_adapter.interfaces.metadata.CreditsError: in case
         the User is not enabled to open the new Session. If it's possible that
         the User would be enabled as soon as another Session were closed, then
         a :class:`ConflictingSessionError` can be raised, in which the ID of
         the other Session must be specified. In this case, a second invocation
         of the method with the same "REQUEST_ID" and a different Session ID
         will be received.
        :raises lightstreamer_adapter.interfaces.metadata.NotificationError: in
         case something is wrong in the parameters, such as the ID of a Session
         already open for this or a different User.

        **IMPLEMENTATION NOTE:** does nothing.
        """

    def notify_session_close(self, session_id):
        """Called by Lightstreamer Kernel through the Remote Server to notify
        the Metadata Adapter that a push Session has been closed.

        This method is called by the Server asynchronously and does not consume
        a pooled thread on the Server. As a consequence, it is not guaranteed
        that no more calls related with this sessionID, like
        ``notify_new_tables``, ``notify_tables_close``, and ``get_items`` can
        occur after its invocation on parallel threads. Accepting them would
        have no effect.
        However, if the method may have side-effects on the Adapter, like
        ``notify_user_message``, the Adapter is responsible for checking if
        the session is still valid.

        :param str session_id: A Session ID.
        :raises lightstreamer_adapter.interfaces.metadata.NotificationError: in
         case something is wrong in the parameters, such as the ID of a Session
         that is not currently open.

        **IMPLEMENTATION NOTE:** does nothing, because the Metadata Adapter
        doesn't need to remember the open Sessions
        """

    def wants_tables_notification(self, user):
        """Called by Lightstreamer Kernel through the Remote Server to know
        whether the Metadata Adapter must or must not be notified any time a
        Table (i.e. Subscription) is added or removed from a push Session owned
        by a supplied User. If this method returns ``False``, the methods
        :meth:`notify_new_tables` and :meth:`notify_tables_close` will never be
        called for this User, saving some processing time. In this case, the
        User will be allowed to add to his Sessions any Tables (i.e.
        Subscriptions) he wants.

        This method runs in the Server authentication thread pool, if defined.

        :param str user: A User name.
        :return: ``True`` if the Metadata Adapter must be notified any time a
         Table (i.e. Subscription) is added or removed from a Session owned by
         the User.
        :rtype: bool

        **IMPLEMENTATION NOTE:** always return ``False``, to prevent being
        notified with
        :meth:`notify_new_tables` and :meth:`notify_tables_close`, as the the
        Metadata Adapter doesn't require such notifications in this default
        implementation.
        """
        return False

    def notify_new_tables(self, user, session_id, tables):
        """Called by Lightstreamer Kernel through the Remote Server to check
        that a User is enabled to add some Tables (i.e. Subscriptions) to a
        push Session. If the check succeeds, this also notifies the Metadata
        Adapter that the Tables are being added to the Session.

        The method is invoked only if enabled for the User through
        :meth:`wants_tables_notification`.

        This method runs in the Server thread pool specific for the Data
        Adapter that supplies the involved items, if defined.

        :param str user: A User name.
        :param str session_id: The ID of a Session owned by the User.
        :param tables: A list of :class:`TableInfo` instances, each of them
         containing the details of a Table (i.e. Subscription) to be added to
         the Session. The elements in the list represent Tables (i.e.:
         Subscriptions) whose subscription is requested atomically by the
         client. A single element should be expected in the list, unless
         clients based on a very old version of a client library or text
         protocol may be in use.
        :type tables: list
        :raises lightstreamer_adapter.interfaces.metadata.CreditsError: in case
         the User is not allowed to add the specified Tables (i.e.
         Subscriptions) to the Session.
        :raises lightstreamer_adapter.interfaces.metadata.NotificationError: in
         case something is wrong in the parameters, such as the ID of a Session
         that is not currently open or inconsistent information about a Table
         (i.e. Subscription).

        **IMPLEMENTATION NOTE:** unless the :meth:`wants_tables_notification`
        method is overridden, this method will never be called.
        """

    def notify_tables_close(self, session_id, tables):
        """Called by Lightstreamer Kernel through the Remote Server to notify
        the Metadata Adapter that some Tables (i.e. Subscriptions) have been
        removed from a push Session.

        The method is invoked only if enabled for the User through
        :meth:`wants_tables_notification`.

        This method is called by the Server asynchronously and does not consume
        a pooled thread on the Server.

        :param str session_id: A Session ID.
        :param tables: A list of :class:`TableInfo` instance each of them
         containing the details of a Table (i.e. Subscription) that has been
         removed from the Session. The supplied list is in 1:1 correspondence
         with the list supplied by :meth:`notify_new_tables` in a previous
         call; the correspondence can be recognized by matching the
         :meth:`TableInfo.win_index` property of the included TableInfo objects
         if multiple objects are included, it must be the same for all of
         them).
        :type tables: list
        :raises lightstreamer_adapter.interfaces.metadata.NotificationError: in
         case something is wrong in the parameters, such as the ID of a Session
         that is not currently open or a Table (i.e. Subscription) that is not
         contained in the Session.

        **IMPLEMENTATION NOTE:** does nothing, because the Metadata Adapter
        doesn't need to remember the open Sessions.
        """

    def notify_mpn_device_access(self, user, session_id, device):
        """Called by Lightstreamer Kernel through the Remote Server to check
        that a User is enabled to access the specified MPN device. The success
        of this method call is a prerequisite for all MPN operations, including
        the activation of a subscription, the deactivation of a subscription,
        the change of a device token, etc. Some of these operations have a
        subsequent specific notification, i.e.
        :meth:`notify_mpn_subscription_activation` and
        :meth:`notify_mpn_device_token_change`.

        Take particular precautions when authorizing device access, if possible
        ensure the user is entitled to the specific platform, device token and
        application ID.

        **Edition Note:** Push Notifications is an optional feature,
        available depending on Edition and License Type.
        To know what features are enabled by your license, please see the
        License tab of the Monitoring Dashboard (by default, available at
        /dashboard).

        :param str user:  A User name.
        :param str session_id: The ID of a Session owned by the User.
        :param MpnDeviceInfo device: Specifies an MPN device.

        **IMPLEMENTATION NOTE:** does nothing.
        """

    def notify_mpn_subscription_activation(self, user, session_id, table,
                                           mpn_subscription):
        """Called by Lightstreamer Kernel through the Remote Server to check
        that a User is enabled to activate a Push Notification subscription. If
        the check succeeds, this also notifies the Metadata Adapter that Push
        Notifications are being activated.

        Take particular precautions when authorizing subscriptions, if possible
        check for validity the trigger expression reported by the
        :meth:`MpnSubscriptionInfo.trigger` property, as it may contain
        maliciously crafted code. The MPN notifiers configuration file contains
        a first-line validation mechanism based on regular expression that may
        also be used for this purpose.

        **Edition Note:** Push Notifications is an optional feature,
        available depending on Edition and License Type.
        To know what features are enabled by your license, please see the
        License tab of the Monitoring Dashboard (by default, available at
        /dashboard).

        :param str user: User name.
        :param str session_id: The ID of a Session owned by the User. The
         session ID is provided for a thorough validation of the Table
         information, but Push Notification subscriptions are persistent and
         survive the session. Thus, any association between this Session ID and
         this Push Notification subscription should be considered temporary.
        :param TableInfo table: A ``TableInfo`` instance, containing the
         details of a Table (i.e.: Subscription) for which Push Notification
         have to be activated.
        :param MpnSubscriptionInfo mpn_subscription:
         A ``MpnSubscriptionInfo`` instance, containing the platform
         specific details of a PushNotification to be activated.
        :type mpn_subscription: :class:`MpnSubscriptionInfo`
        :raises lightstreamer_adapter.interfaces.metadata.CreditsError: if the
         User is not allowed to activate the specified Push Notification in the
         Session.
        :raises lightstreamer_adapter.interfaces.metadata.NotificationError: if
         something is wrong in the parameters, such as inconsistent
         information about a Table (i.e.: Subscription) or a Push Notification.

        **IMPLEMENTATION NOTE:** does nothing.
        """

    def notify_mpn_device_token_change(self, user, session_id, device,
                                       new_device_token):
        """ Called by Lightstreamer Kernel through the Remote Server to check
        that a User is enabled to change the token of an MPN device. If the
        check succeeds, this also notifies the Metadata Adapter that future
        client requests should be issued by specifying the new device token.

        Take particular precautions when authorizing device token changes, if
        possible ensure the user is entitled to the new device token.

        **Edition Note:** Push Notifications is an optional feature,
        available depending on Edition and License Type.
        To know what features are enabled by your license, please see the
        License tab of the Monitoring Dashboard (by default, available at
        /dashboard).

        :param str user: A User name.
        :param str session_id: The ID of a Session owned by the User.
        :param MpnDeviceInfo device: Specifies an MPN device.
        :param str new_device_token: The new token being assigned to the
         device.
        :raises lightstreamer_adapter.interfaces.metadata.CreditsError: if the
         User is not allowed to change the specified device token.
        :raises lightstreamer_adapter.interfaces.metadata.NotificationError: if
         something is wrong in the parameters, such as inconsistent information
         about the device.

        **IMPLEMENTATION NOTE:** does nothing.
        """


class Mode(Enum):
    """Encapsulates a publishing Mode. The different Modes handled by
    Lightstreamer Kernel can be uniquely identified by the static constants
    defined in this class. See the technical documents for a detailed
    description of Modes.
    """

    RAW = 'R'
    """The RAW Mode"""

    MERGE = 'M'
    """The MERGE Mode"""

    DISTINCT = 'D'
    """The DISTINCT Mode"""

    COMMAND = 'C'
    """The COMMAND Mode"""


class MpnPlatformType(Enum):
    """Identifies a Push Notifications platform type, used with MPN-related
    requests of the MetadataProvider. It is used by Lightstreamer to specify
    the platform associated with the notified client requests

    The available constants refer to the platform types currently supported.

    **Edition Note:** Push Notifications is an optional feature,
    available depending on Edition and License Type.
    To know what features are enabled by your license, please see the License
    tab of the Monitoring Dashboard (by default, available at /dashboard).
    """

    APPLE = 'A'
    """Refers to Push Notifications for APPLE platforms, such as iOS, macOS and
    tvOS.
    The back-end service for APPLE platforms is APNs ("APPLE Push Notification
    service").
    APPLE, iOS, macOS and tvOS are registered trademarks of APPLE, Inc.
    """

    GOOGLE = 'G'
    """Refers to Push Notifications for GOOGLE platforms, such as Android and
    Chrome.
    The back-end service for GOOGLE platforms is FCM ("Firebase Cloud
    Messaging").
    GOOGLE, Android and Chrome are registered trademarks of GOOGLE Inc.
    """


class MpnDeviceInfo():
    """Specifies a target device for Push Notifications, used with MPN-related
    requests for the MetadataProvider. Note that the processing and the
    authorization of Push Notifications is per-device and per-application.
    While a physical device is uniquely identified by the platform type and a
    platform dependent device token, Lightstreamer considers the same device
    used by two different applications as two different MPN devices. Thus, an
    ``MpnDeviceInfo`` instance uniquely identifies both the physical device and
    the application for which it is being used.

    An ``MpnDeviceInfo`` always provides the following identifiers:

    * The platform type.
    * The application ID.
    * The device token.
    """

    def __init__(self, platform_type, application_id, device_token):
        self._type = platform_type
        self._application_id = application_id
        self._device_token = device_token

    def __eq__(self, other):
        if type(other) is type(self):
            return self.__dict__ == other.__dict__
        return False

    @property
    def mpn_platform_type(self):
        """The platform type of the device.

        :type: :class:`MpnPlatformType`
        """
        return self._type

    @property
    def application_id(self):
        """The application ID, also known as the package name or bundle ID on
        some platforms.

        :type: str
        """
        return self._application_id

    @property
    def device_token(self):
        """The token of the device, also known as the registration ID on some
        platforms.

        :type: str
        """
        return self._device_token


class TableInfo():
    """Used by MetadataProvider to provide value objects to the calls to
    methods :meth:`notify_new_tables()` and :meth:`notify_tables_close()`. The
    attributes of every Table (i.e.: Subscription) to be added or removed to a
    Session have to be written to a ``TableInfo`` instance.
    """

    def __init__(self, win_index, mode, group, schema, first_idx, last_idx,
                 selector=None):
        self._win_index = win_index
        self._mode = mode
        self._group = group
        self._schema = schema
        self._min = first_idx
        self._max = last_idx
        self._selector = selector

    def __eq__(self, other):
        if type(other) is type(self):
            return self.__dict__ == other.__dict__
        return False

    @property
    def win_index(self):
        """The unique identifier of the client subscription request within
        the session. This allows for matching the corresponding subscription
        and unsubscription requests.
        Note that, for clients based on a very old version of a
        client library or text protocol, subscription requests may involve
        multiple Tables (i.e.: Subscriptions), hence multiple objects of this
        type can be supplied in a single array by MetadataProvider through
        :meth:`notify_new_tables` :meth:`notify_tables_close`. In this case,
        the value returned is the same for all these objects and the single
        Tables (i.e.: Subscriptions) can be identified by their relative
        position in the array.

        :type: int
        """
        return self._win_index

    @property
    def mode(self):
        """The publishing Mode for the Items in the Table (i.e. Subscription)
        (it must be the same across all the Table).

        :type: :class:`Mode`
        """
        return self._mode

    @property
    def group(self):
        """The name of the Item Group (or specification of the Item List) to
        which the subscribed Items belong.

        :type: str
        """
        return self._group

    @property
    def schema(self):
        """The name of the Field Schema (or specification of the Field List)
        used for the subscribed Items.

        :type: str
        """
        return self._schema

    @property
    def min(self):
        """The index of the first Item in the Group to be considered in the
        Table (i.e. Subscription).

        :type: int
        """
        return self._min

    @property
    def max(self):
        """The index of the last Item in the Group to be considered in the
        Table (i.e. Subscription).

        :type: int
        """
        return self._max

    @property
    def selector(self):
        """The name of the optional Selector associated to the Table
        (i.e. Subscription).

        :type: str
        """
        return self._selector


class MpnSubscriptionInfo:
    """Specifies a Push Notifications subscription, used with MPN-related
    requests of the ``MetadataProvider``.

    For the actual description of the subscription we rely on a generic
    descriptor accessible via the notification_format property, where the
    structure of the descriptor depends on the platform.

    **Edition Note:** Push Notifications is an optional feature,
    available depending on Edition and License Type.
    To know what features are enabled by your license, please see the License
    tab of the Monitoring Dashboard (by default, available at /dashboard).
    """

    def __init__(self, device, notification_format, trigger):
        self._device = device
        self._notification_format = notification_format
        self._trigger = trigger

    def __eq__(self, other):
        if type(other) is type(self):
            return self.__dict__ == other.__dict__
        return False

    @property
    def device(self):
        """MPN device of this subscription.

        :type: :class:`MpnDeviceInfo`
        """
        return self._device

    @property
    def notification_format(self):
        """The descriptor of the push notifications format of this
        subscription. The structure of the format descriptor depends on the
        platform type and it is represented in json.

        :type: str
        """
        return self._notification_format

    @property
    def trigger(self):
        """The optional expression that triggers the delivery of push
        notification.

        :type: str
        """
        return self._trigger


class MetadataError(Exception):
    """Base exception class for all exceptions directly raised by the
    Metadata Adapter.
    """

    def __init__(self, msg):
        """Constructs a MetadataError with the supplied detail message.

        :param str msg: the detail message.
        """
        self._msg = msg
        super(MetadataError, self).__init__(msg)

    @property
    def msg(self):
        """The detail message

        :type: str
        """
        return self._msg


class MetadataProviderError(MetadataError):
    """Raised by the :meth:`MetadataProvider.initialize` method if there is
    some problem that prevents the correct behavior of the Metadata Adapter. If
    this  exception occurs, Lightstreamer Kernel must give up the startup.
    """


class NotificationError(MetadataError):
    """Raised by the ``notify_*`` methods in MetadataProvider if there is some
    inconsistency in the supplied parameters. Lightstreamer Kernel ensures
    that such conditions will never occur, but they may be checked for
    debugging or documentation reasons.
    """


class AccessError(MetadataError):
    """Raised by the ``notify_*`` methods in MetadataProvider if the supplied
    User is not recognized or a functionality is not implemented for this User.
    """


class ItemsError(MetadataError):
    """Thrown by the :meth:`MetadataProvider.get_items` and
    :meth:`MetadataProvider.get_schema` methods if the supplied Item Group name
    (or Item List specification) is not recognized or cannot be resolved.
    """


class SchemaError(MetadataError):
    """Raised by the
    :meth:`get_schema`
    method in MetadataProvider if the supplied Field Schema name (or Field List
    specification) is not recognized or cannot be resolved.
    """


class CreditsError(MetadataError):
    """Thrown by the ``notify_*`` methods in MetadataProvider if some
    functionality cannot be allowed to the supplied User. This may occur if the
    user is not granted some resource or if the user would exceed the granted
    amount. Different kinds of problems can be distinguished by an error
    client_error_code. Both the error message detail and the error
    client_error_code will be forwarded by Lightstreamer Kernel to the
    Client.
    """

    def __init__(self, client_error_code, msg, user_msg=None):
        """Constructs a CreditsError with supplied error client_error_code and
        message text to be forwarded to the Client.  An internal error message
        text can also be specified.

        :param int client_error_code: Error code that can be used to
         distinguish the kind of problem. It must be a negative integer, or
         zero to mean an unspecified problem.
        :param str msg: the detail message.
        :param str user_msg: A detail message to be forwarded to the Client.
         If ``None`` or missing, an empty string message will be forwarded.
         The message is free, but if it is not in simple ASCII or if it is
         multiline, it might be altered in order to be sent to very old
         non-TLCP clients.
        """
        super(CreditsError, self).__init__(msg)
        self._code = client_error_code
        self._user_msg = user_msg

    @property
    def client_error_code(self):
        """The error client_error_code to be forwarded to the client.

        :type: str
        """
        return self._code

    @property
    def client_user_msg(self):
        """The error detail message to be forwarded to the client.

        :type: str
        """
        return self._user_msg


class ConflictingSessionError(CreditsError):
    """
    Thrown by the :meth:`MetadataProvider.notify_new_session` method f a User
    is not enabled to open a new Session but he would be enabled as soon as
    another Session were closed. By using this exception, the ID of the other
    Session is also supplied. After receiving this exception, the Server may
    try to close the specified session and invoke
    :meth:`MetadataProvider.notify_new_session` again.
    """

    def __init__(self, code, msg, conflicting_session_id, user_msg=None):
        """Constructs a ConflictingSessionError with supplied error
        client_error_code and message text that will be forwarded to the Client
        in case the Server can't solve the issue by closing the conflicting
        session. An internal error message text can also be specified.

        :param int client_error_code:  Error code that can be used to
         distinguish the kind of problem. It must be a negative integer, or
         zero to mean an unspecified problem.
        :param str msg: The detail message.
        :param str conflicting_session_id: ID of a Session that can be closed
         in order to eliminate the reported problem. It must not be null.
        :param str user_msg: A detail message to be forwarded to the Client.
         If ``None`` or missing, an empty string message will be forwarded.
         The message is free, but if it is not in simple ASCII or if it is
         multiline, it might be altered in order to be sent to very old
         non-TLCP clients.
        """
        super(ConflictingSessionError, self).__init__(code, msg, user_msg)
        self._conflicting_session_id = conflicting_session_id

    @property
    def conflicting_session_id(self):
        """The ID of a Session that can be closed in order to eliminate the
        problem reported in this exception.

        :type: str
        """
        return self._conflicting_session_id
