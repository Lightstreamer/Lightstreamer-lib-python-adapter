"""
This module contains all ABC classes and exceptions needed to create and manage
a Remote Data Adapter.
"""
from abc import ABCMeta, abstractmethod

__all__ = ['DataProvider', 'ItemEventListener', 'DataError',
           'DataProviderError', 'SubscribeError', 'FailureError']


class DataProvider(metaclass=ABCMeta):
    """Provides an abstract class meant to be extended by a Remote Data Adapter
    in order to attach a Data Provider to Lightstreamer. An instance of a
    Remote Data Adapter is supplied to Lightstreamer through a
    :class:`lightstreamer_adapter.server.DataProviderServer` instance. After
    initialization, Lightstreamer sets itself as the Remote Data Adapter
    listener, by calling the :meth:`set_listener` method.

    Data Providers are used by Lightstreamer Kernel to obtain all data to be
    pushed to the Clients. Any Item requested by a Client must refer to one
    supplied by the configured Data Adapters.

    A Data Provider supplies data in a publish/subscribe way. Lightstreamer
    asks for data by calling the
    :meth:`subscribe` and :meth:`unsubscribe` methods for various Items and
    the Data Adapter sends ItemEvents to its listener in an asynchronous way.

    A Data Adapter can also support Snapshot management. Upon subscription to
    an Item, the current state of the Item data can be sent to the Server
    before the updates. This allows the Server to maintain the Item state, by
    integrating the new ItemEvents into the state (in a way that depends on the
    Item type) and to make this state available to the Clients.

    Note that the interaction between the Server and the Data Adapter and the
    interaction between the Server and any Client are independent activities.
    As a consequence, the very first ItemEvents sent by the Data Adapter to the
    Server for an Item just subscribed to might be processed before the Server
    starts feeding any client, even the client that caused the subscription to
    the Item to be invoked; then, such events would not be forwarded to any
    client. If it is desirable that a client receives all the ItemEvents that
    have been produced for an Item by the Data Adapter since subscription time,
    then the support for the Item Snapshot can be leveraged.

    Lightstreamer Kernel ensures that calls to
    :meth:`.subscribe` and
    :meth:`.unsubscribe` for
    the same Item will be interleaved, without redundant calls; whenever
    subscribe raises an exception, the corresponding unsubscribe call is not
    issued.
    """

    @abstractmethod
    def initialize(self, parameters, config_file=None):
        """Called by the Remote Server to provide initialization information to
        the Data Adapter. The call must not be blocking; any polling cycle or
        similar must be started in a different thread. If an exception occurs
        in this method, Lightstreamer Kernel can't complete the startup and
        must exit. The initialization information can be supplied in different
        ways, depending on the way the Remote Server is launched.

        The call must not be blocking; any polling cycle or similar must be
        started in a different thread. Any delay in returning from this call
        will in turn delay the Server initialization. If an exception occurs in
        this method, Lightstreamer Server can't complete the startup and must
        exit.

        :param dict parameters: A dictionary object that contains key-value
         pairs corresponding to the parameters elements supplied for the Data
         Adapter configuration. Both key and values are represented as string
         objects.

         The parameters can be supplied through the
         :meth:`lightstreamer_adapter.server.DataProviderServer.adapter_params`
         property of the DataProviderServer instance. More parameters can be
         added by leveraging the ``init_remote`` parameter in the Proxy Adapter
         configuration.
        :param str config_file: The path on the local disk of the Data Adapter
         configuration file.

         The file path can be supplied by assigning the
         :meth:`lightstreamer_adapter.server.DataProviderServer.adapter_config`
         property of the DataProviderServer instance used.
        :raises lightstreamer_adapter.interfaces.data.DataProviderError: in
         case an error occurs that prevents the correct behavior of the Data
         Adapter.
        """

    @abstractmethod
    def set_listener(self, event_listener):
        """Called by the Remote Server to provide a listener to receive the
        Item Events carrying data and asynchronous error notifications for
        Lightstreamer Kernel. The listener is set before any subscribe is
        called and is never changed.

        :param ItemEventListener event_listener: A listener.
        """

    @abstractmethod
    def subscribe(self, item_name):
        """Called by Lightstreamer Remote Server to request data for an Item.
        If the request succeeds, the Remote Data Adapter can start sending an
        ItemEvent to the listener for any update of the Item value. Before
        sending the updates, the Remote Data Adapter may optionally send one or
        more ItemEvents to supply the current Snapshot.

        The general rule to be followed for event dispatching is::

         if issnapshot_available(itemName) == true
             SNAP* [EOS] UPD*
         else
             UPD*

        where:

        * SNAP represents an :meth:`ItemEventListener.update` call with the
          ``issnapshot`` flag set to ``True``
        * EOS represents an :meth:`ItemEventListener.end_of_snapshot` call
        * UPD represents an :meth:`ItemEventListener.update` call with the
          ``issnapshot`` flag set to ``False``; in this case, the special
          :meth:`clear_snapshot` call can also be issued.

        The composition of the snapshot depends on the Mode in which the Item
        is to be processed. In particular, for MERGE mode, the snapshot
        consists of one event and the first part of the rule becomes::

        [SNAP] [EOS] UPD*

        where a missing snapshot is considered as an empty snapshot.

        If an Item can be requested only in RAW mode, then
        :meth:`DataProvider.issnapshot_available` should always return
        ``False``; anyway, when an Item is requested in RAW mode, any snapshot
        is discarded.

        Note that calling :meth:`ItemEventListener.end_of_snapshot` is not
        mandatory; however, not calling it in DISTINCT or COMMAND mode may
        cause the server to keep the snapshot and forward it to the clients
        only after the first no-shapshot event has been received. The same
        happens for MERGE mode if neither the snapshot nor the endOfSnapshot
        call are supplied.

        Unexpected snapshot events are converted to non-snapshot events (but
        for RAW mode, where they are ignored); unexpected
        :meth:`ItemEventListener.end_of_snapshot` calls are ignored.

        The method can be blocking, but, as the Proxy Adapter implements
        subscribe and unsubscribe asynchronously, subsequent
        subscribe-unsubscribe-subscribe-unsubscribe requests can still be
        issued by Lightstreamer Server to the Proxy Adapter. When this happens,
        the requests may be queued on the Remote Adapter, hence some Subscribe
        calls may be delayed.

        :param str item_name: Name of an Item.
        :raises lightstreamer_adapter.interfaces.data.SubscribeError: in case
         the request cannot be satisfied
        :raises lightstreamer_adapter.interfaces.data.FailureError: in case the
         method execution has caused a severe problem that can compromise
         future operation of the Data Adapter.
        """

    @abstractmethod
    def unsubscribe(self, item_name):
        """Called by Lightstreamer Kernel through the Remote Server to end a
        previous request of data for an Item. After the call has returned, no
        more ItemEvents for the Item should be sent to the listener until
        requested by a new subscription for the same Item.

        The method can be blocking, but, as the Proxy Adapter implements
        subscribe and unsubscribe asynchronously, subsequent
        subscribe-unsubscribe-subscribe-unsubscribe requests can still be
        issued by Lightstreamer Server to the Proxy Adapter. When this happens,
        the requests may be queued on the Remote Adapter, hence some Subscribe
        calls may be delayed.

        :param str item_name: Name of an Item.
        :raises lightstreamer_adapter.interfaces.data.SubscribeError: in case
         the request cannot be satisfied
        :raises lightstreamer_adapter.interfaces.data.FailureError: in case the
         method execution has caused a severe problem that can compromise
         future operation of the Data Adapter.
        """

    @abstractmethod
    def issnapshot_available(self, item_name):
        """Called by Lightstreamer Kernel through the Remote Server to know
        whether the Data Adapter, after a subscription for an Item, will send
        some Snapshot Item Events before sending the updates. An Item Snapshot
        can be represented by zero, one or more Item Events, also depending on
        the Item type. The decision whether to supply or not to supply Snapshot
        information is entirely up to the Data Adapter.

        The method should be nonblocking. The availability of the snapshot for
        an Item should be a known architectural property. When the snapshot,
        though expected, cannot be obtained at subscription time, then it can
        only be considered as empty.

        :param str item_name: Name of an Item.
        :return bool: ``True`` if Snapshot information will be sent for this
         Item before the updates.
        :raises lightstreamer_adapter.interfaces.data.SubscribeError: in case
         the Data Adapter is unable to answer to the request.
        """


class ItemEventListener(metaclass=ABCMeta):
    """Internally implemented abstract interface, used by Lightstreamer Kernel
    to receive the Item Events and any asynchronous severe error notification
    from the Data Adapter. The listener instance is supplied to the Data
    Adapter by Lightstreamer Kernel (through the Remote Server) through a
    :meth:`DataProvider.set_listener` call. The listener can manage Item Events
    implemented as dictionary objects, whose values can be expressed either as
    a string or as a bytes (the special mandatory fields for COMMAND Mode named
    ``key`` and ``command`` must be encoded as string).

    When an Item Event instance has been sent to the listener, it is totally
    owned by Lightstreamer and it must not be anymore changed by the Data
    Adapter. The Remote Server may also hold the object for some time after the
    listener call has returned.
    """

    @abstractmethod
    def update(self, item_name, item_event, issnapshot):
        """Called by a Data Adapter to send an Item Event implemented as
        dictionary object to Lightstreamer Kernel.

        The Remote Adapter should ensure that, after an
        :meth:`DataProvider.unsubscribe()` call for the Item has returned, no
        more Update calls are issued, until requested by a new subscription for
        the same Item. This assures that, upon a new subscription for the Item,
        no trailing events due to the previous subscription can be received by
        the Remote Server. Note that the method is nonblocking; moreover, it
        only takes locks to first order mutexes; so, it can safely be called
        while holding a custom lock.

        :param str item_name: The name of the Item whose values are carried by
         the Item Event.
        :param dict item_event: A dictionary object, in which every key-value
         pair represents a Field of the Item Event. A Field value can be
         expressed as either a string or a bytes, the latter case being the
         most efficient, though restricted to the ISO-8859-1 (ISO-LATIN-1)
         character set. A Field value can be None or missing if the Field is
         not to be reported in the event.
        :param bool issnapshot: ``True`` if the Item Event carries the Item
         Snapshot.
        """

    @abstractmethod
    def end_of_snapshot(self, item_name):
        """Called by a Data Adapter to signal to Lightstreamer Kernel that no
        more Item Event belonging to the Snapshot are expected for an Item.
        This call is optional, because the Snapshot completion can also be
        inferred from the ``issnapshot`` flag in the update calls. However,
        this call allows Lightstreamer Kernel to be informed of the Snapshot
        completion before the arrival of the first non-snapshot event. Calling
        this function is recommended if the Item is to be subscribed in
        DISTINCT mode. In case the Data Adapter returned ``False`` to
        :meth:`DataProvider.issnapshot_available` for the same Item, this
        function should not be called.

        The Remote Adapter should ensure that, after an
        :meth:`DataProvider.unsubscribe` call for the Item has returned, a
        possible pending ``end_of_snapshot`` call related with the previous
        subscription request is no longer issued. This assures that, upon a new
        subscription for the Item, no trailing events due to the previous
        subscription can be received by the Remote Server. Note that the method
        is nonblocking; moreover, it only takes locks to first order mutexes;
        so, it can safely be called while holding a custom lock.

        :param str item_name: The name of the Item whose snapshot has been
         completed
        """

    @abstractmethod
    def clear_snapshot(self, item_name):
        """Called by a Data Adapter to signal to Lightstreamer Kernel that the
        current Snapshot of the Item has suddenly become empty. More precisely:

        * for subscriptions in MERGE mode, the current state of the Item will
          be cleared, as though an update with all fields valued as null were
          issued;

        * for subscriptions in COMMAND mode, the current state of the Item will
          be cleared, as though a DELETE event for each key were issued;

        * for subscriptions in DISTINCT mode, a suitable notification that
          the Snapshot for the Item should be cleared will be sent to all the
          clients currently subscribed to the Item (clients based on some old
          client library versions may not be notified); at the same time, the
          current recent update history kept by the Server for the Item will be
          cleared and this will affect the Snapshot for new subscriptions;

        * for subscriptions in RAW mode, there will be no effect.

        Note that this is a real-time event, not a Snapshot event; hence, in
        order to issue this call, it is not needed that the Data Adapter has
        returned ``True`` to
        :meth:`DataProvider.issnapshot_available` for the specified Item;
        moreover, if invoked while the Snapshot is being supplied, the Kernel
        will infer that the Snapshot has been completed.

        The Adapter should ensure that, after an
        :meth:`DataProvider.unsubscribe` call for the Item has returned, a
        possible pending :meth:`DataProvider.clear_snapshot` call related with
        the previous subscription request is no longer issued. This assures
        that, upon a new subscription for the Item, no trailing events due to
        the previous subscription can be received by the Kernel. Note that the
        method is nonblocking; moreover, it only takes locks to first order
        mutexes; so, it can safely be called while holding a custom lock.

        :param str item_name: The name of the Item whose Snapshot has become
         empty.
        """

    def failure(self, exception):
        """Called by a Data Adapter to notify Lightstreamer Kernel of the
        occurrence of a severe problem that can compromise future operation of
        the Data Adapter.

        :param Exception exception: Any Excetion object, with the description
         of the problem.
        """


class DataError(Exception):
    """Base class for all exceptions directly raised by the Data Adapter."""

    def __init__(self, msg):
        self._msg = msg
        super(DataError, self).__init__(msg)

    @property
    def msg(self):
        """The detail message

        :type: str
        """
        return self._msg


class DataProviderError(DataError):
    """Raised by the :meth:`DataProvider.initialize` method if there is some
    problem that prevents the correct behavior of the Data Adapter. If this
    exception occurs, Lightstreamer Kernel must give up the
    startup.
    """


class SubscribeError(DataError):
    """Raised by the :meth:`DataProvider.subscribe` and
    :meth:`DataProvider.unsubscribe` methods if the request cannot be
    satisfied.
    """


class FailureError(DataError):
    """Raised by the :meth:`DataProvider.subscribe` and
    :meth:`DataProvider.unsubscribe` methods if the method execution has caused
    a severe problem that can compromise future operation of the Data Adapter.
    """
