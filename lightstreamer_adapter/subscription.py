"""
Helper classes for the management of subscription and unsubscription of the
Items handled by the Remote Data Adapter.
"""
from contextlib import contextmanager
import threading
from _collections import deque

from lightstreamer_adapter.protocol import RemotingException

from . import DATA_PROVIDER_LOGGER


class _ItemTaskManager():
    """Helper class which schedules the execution of the tasks relative to a
    specified Item.

    This class manages a queue of tasks to be executed for an Item. Tasks
    are dequeued by a single thread, which is submitted to the Executor
    configured by the Data Provider Server and then propagated to the
    Subscription Manager. The single thread ensures that each unique task is
    is executed in a sequentialized way, in order to avoid any synchronisation
    issue that may affect the Item consistency.
    """

    def __init__(self, item_name, subscription_mgr):
        self._item_name = item_name
        self._tasks_deq = deque()
        self._code = None
        self._isrunning = False
        self._lock = threading.Lock()
        self._queued = 0
        self._last_subscribe_outcome = False
        self._subscription_mgr = subscription_mgr

    def inc_queued(self):
        """Increments the total number of task submitted to this _ItemManager.
        """
        self._queued += 1

    def add_task(self, task):
        """Add to the queue the provided task to be asynchronously by the
        Executor.
        """
        with self._lock:
            self._tasks_deq.append(task)
            # Starts only a single dequeuing task, which is submitted to the
            # Executor.
            if not self._isrunning:
                self._isrunning = True
                self._subscription_mgr.execute_task(self._deque)

    @property
    def code(self):
        """"The current Request Id.
        """
        return self._code

    def _deque(self):
        """Dequeuing task submitted to the Executor.

        This task dequeues all _ItemTask instances submitted to this
        _ItemManager, and executes the wrapped task and 'late' task, the latter
        if required.
        """
        dequeued = 0
        last_subscribe_outcome = True
        while True:
            with self._lock:
                if dequeued == 0:
                    last_subscribe_outcome = self._last_subscribe_outcome
                if len(self._tasks_deq) == 0:
                    self._isrunning = False
                    self._last_subscribe_outcome = last_subscribe_outcome
                    break
                # Gets the next _ItemTask.
                item_task = self._tasks_deq.popleft()
                islast = len(self._tasks_deq) == 0
                dequeued += 1
            try:
                if item_task.issubscribe:
                    # Current scheduled task is a Subscription
                    if not islast:
                        item_task.do_late_task()
                        last_subscribe_outcome = False
                    else:
                        with self._subscription_mgr.sync_items():
                            # The current Request Id is set to the one of
                            # the current scheduled task.
                            self._code = item_task.code
                        last_subscribe_outcome = item_task.do_task()
                else:
                    # Current scheduled task is an Unsubscription
                    if last_subscribe_outcome:
                        # Previous subscription with success, so execute the
                        # the unsubscription task,
                        item_task.do_task()
                    else:
                        # Issue in the previuos subscription, so execute the
                        # 'late task'.
                        item_task.do_late_task()
                    with self._subscription_mgr.sync_items():
                        # In case of unsubscription, putting the current
                        # Request Id to None indicates that no more updates are
                        # expected for this Item.
                        self._code = None
            except RemotingException:
                DATA_PROVIDER_LOGGER.error("Caught an exception")

        # Invokes the _dec_dequeued method through the SubscriptionManager,
        # while the RLock associated with the Subscription Manager is kept.
        with self._subscription_mgr.sync_items():
            self._dec_queued(dequeued)

    def _dec_queued(self, dequeued):
        """Decrements the total number of enqueued tasks, until it will be
        necessary to remove this _ItemTaskManager from the SubscriptionManager.
        """
        self._queued -= dequeued
        if not self._code and self._queued == 0:
            item_manager = self._subscription_mgr.get_item_mgr(self._item_name)
            if not item_manager:
                pass
            elif item_manager != self:
                pass
            else:
                self._subscription_mgr.del_active_item(self._item_name)


class ItemTask():
    """Simple class which wraps the execution of a task relative to the
    provided Request Id.

    Each instance of ItemTask wraps both the task and the "late" task. The
    "late" task has to be submitted in case the execution task has been
    requested too late by the Lightstreamer Server or its outcome was a
    failure.
    """

    def __init__(self, request_id, issubscribe, do_task, do_late_task):
        self._request_id = request_id
        self._issubscribe = issubscribe
        self._do_task = do_task
        self._do_late_task = do_late_task

    def do_task(self):
        """Executea the task.
        """
        return self._do_task()

    def do_late_task(self):
        """Executea the late task.
        """
        self._do_late_task()

    @property
    def code(self):
        """The Request Id originating this task execution.
        """
        return self._request_id

    @property
    def issubscribe(self):
        """Indicates if this task is a Subscription (True) or an Unsubscription
        (False).
        """
        return self._issubscribe


class SubscriptionManager():
    """Helper class for the subscription management.

    This class hides the complexity related with the synchronization required
    to handle in a properly way the subscription and unsubscription operations
    for the items.

    Subscriptions and unsubscription operations are managed asynchronously
    through the submission of related tasks to an Executor.
    """

    def __init__(self, executor):
        self._executor = executor
        self._active_items = {}
        self._active_items_lock = threading.RLock()

    def execute_task(self, task):
        """Executes the provided task.

        The task is submitted to the Executor configured bye the Data Provider
        Server and then propagated to this Subscription Manager.
        """
        self._executor.submit(task)

    def do_subscription(self, item_name, sub_task):
        """Schedules the execution of the 'sub_task' function, provided by the
        DataProvider server for managing the subscription of the 'item_name'
        Item.

        The sub_task is a sequence of operations which involve the Remote Data
        Adapter attached to the Data Provider Server.
        """
        with self._active_items_lock:
            if item_name not in self._active_items:
                # Initializes a new _ItemTaskManager for the provided
                # item_name.
                self._active_items[item_name] = _ItemTaskManager(item_name,
                                                                 self)
            item_manager = self._active_items[item_name]
            item_manager.inc_queued()
        # Submits the task to the _ItemTaskManager.
        item_manager.add_task(sub_task)

    def do_unsubscription(self, item_name, unsub_task):
        """Schedules the execution of the 'ubsub_task' function, provided by
        the DataProvider server for managing the unsubscription of the
        'item_name' Item.

        The ubsub_task is a sequence of operations which involve the Remote
        Data Adapter attached to the Data Provider Server.
        """
        with self._active_items_lock:
            if item_name not in self._active_items:
                DATA_PROVIDER_LOGGER.error("Task list expected for item %s",
                                           item_name)
                return
            item_manager = self._active_items[item_name]
            item_manager.inc_queued()
        # Submits the task to the _ItemTaskManager.
        item_manager.add_task(unsub_task)

    @contextmanager
    def sync_items(self):
        """Defines the function for the 'with' statement, in order to execute a
        block while the RLock associated with the internal items dictionary
        is acquired.
        """
        with self._active_items_lock:
            yield

    def get_item_mgr(self, item_name):
        """Retrieves the _ItemTaskManager associated with the provided
        item_name.

        This method is used only internally by the _ItemTaskManager to decide
        whether to remove itself from the SubscriptionManager, trough an
        invocation to the 'del_active_item' method.
        """
        return self._active_items.get(item_name)

    def get_active_item(self, item_name):
        """Retrieves the 'item_name' Item.
        """
        with self._active_items_lock:
            if item_name in self._active_items:
                item_manager = self._active_items[item_name]
                return item_manager.code
        return None

    def del_active_item(self, item_name):
        """Removes the 'item_name' Item from this Susbcription Manager.
        """
        if item_name in self._active_items:
            del self._active_items[item_name]
