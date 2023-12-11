=====================================
Lightstreamer SDK for Python Adapters
=====================================

A Python library to  write Data Adapters and Metadata Adapters for `Lightstreamer Server`_.
The adapters will run in a separate process, communicating with the Server through the Adapter Remoting Infrastructure.

.. _Lightstreamer Server: http://www.lightstreamer.com

.. image:: https://raw.githubusercontent.com/Lightstreamer/Lightstreamer-lib-python-adapter/master/architecture.png

Use
===
Install the package:

.. code-block:: bash

   $ pip install lightstreamer-adapter

Configure Lightstreamer
-----------------------

1) Download and install Lightstreamer
2) Go to the ``adapters`` folder of your Lightstreamer Server installation. Create a new folder to deploy the remote adapters in, let's call it ``PythonAdapter``
3) Create an ``adapters.xml`` file inside the ``PythonAdapter`` folder and use the following contents (this is an example configuration, you can modify it to your liking by using the generic template, https://lightstreamer.com/docs/ls-server/latest/remote_adapter_conf_template/adapters.xml or https://lightstreamer.com/docs/ls-server/latest/remote_adapter_robust_conf_template/adapters.xml, as a reference):

   .. code-block:: xml

      <?xml version="1.0"?>
      <adapters_conf id="PROXY_PYTHON">
         <metadata_provider>
            <adapter_class>ROBUST_PROXY_FOR_REMOTE_ADAPTER</adapter_class>
            <classloader>log-enabled</classloader>
            <param name="request_reply_port">8003</param>
            <param name="timeout">36000000</param>
         </metadata_provider>
         <data_provider>
            <adapter_class>ROBUST_PROXY_FOR_REMOTE_ADAPTER</adapter_class>
            <classloader>log-enabled</classloader>
            <param name="request_reply_port">8001</param>
            <param name="timeout">36000000</param>
         </data_provider>
      </adapters_conf>

4) Take note of the ports configured in the adapters.xml file as those are needed to write the remote part of the adapters.

Write the Adapters
------------------

Create a new python module, let's call it ``adapters.py``, where we will put  the minimal logic required to write a basic Adapter Set.

1) Import the server classes needed to setup the connection to the Lightstreamer server, and the adapter classes to be extended to write your own Remote Adapters:

   .. code-block:: python
   
      from lightstreamer_adapter.server import (DataProviderServer, MetadataProviderServer)
      from lightstreamer_adapter.interfaces.data import DataProvider
      from lightstreamer_adapter.interfaces.metadata import MetadataProvider
   
2) Create a new Remote Data Adapter by subclassing the DataProvider abstract class:

   .. code-block:: python
   
      class MyDataAdapter(DataProvider):
          """This Remote Data Adapter sample class shows a simple implementation of
          the DataProvider abstract class."""
      
          def __init__(self):
              # Reference to the provided ItemEventListener instance
              self._listener = None

          def issnapshot_available(self, item_name):
              """Returns True if Snapshot information will be sent for the item_name
              item before the updates."""
              snapshot = False  # May be based on the item_name item
              return snapshot
         
          def set_listener(self, event_listener):
              """Caches the reference to the provided ItemEventListener instance."""
              self._listener = event_listener
              
          def subscribe(self, item_name):
              """Invoked to request data for an item. From now on you can start
              sending real time updates for item_name item, through invocations like
              the following:
              
              self._listener.update(item_name, {'field1': valField1,
                                                'field2': valField2}, False)
              """
              
          def unsubscribe(self, item_name):
              """Invoked to end a previous request of data for an item. From now on,
              you should stop sending updates for item_name item."""


3) Create a new Remote Metadata Adapter by subclassing the MetadataProvider class, if the latter's default behaviour does not meet your requirements, and override the methods for which you want to supply a custom implementation:

   .. code-block:: python
      
      class MyMetadataAdapter(MetadataProvider):
          """This Remote Metadata Adapter sample class shows a minimal custom
          implementation of the notify_user_message method.
          """
          
          def notify_user_message(self, user, session_id, message):
              """Invoked to forward a message received by a User"""
              print("Message {} arrived for user {} in the session {}"
                    .format(user, session_id, message))
                    
4) Run the adapters, by creating, configuring and starting the server class instances:

   .. code-block:: python
   
      if __name__ == "__main__":
          # The host of the Lightstreamer server, to be changed as required.
          LS_SERVER_HOST = 'localhost'
          
          # Creates a new MetadataProviderServer instance, passing a new
          # MyMetadataAdpater object and the remote address.
          metadata_provider_server = MetadataProviderServer(MyMetadataAdapter(),
                                                            (LS_SERVER_HOST, 8003))
          
          # Starts the server instance.
          metadata_provider_server.start()
          
          # Creates a new DataProviderServer instance, passing a new MyDataAdpater
          # object and the remote address
          data_provider_sever = DataProviderServer(MyDataAdapter(),
                                                   (LS_SERVER_HOST, 8001))
          # Starts the server instance.
          data_provider_sever.start()

5) Ensure that the main thread stays alive. This is needed, since Python 3.9, to allow the SDK library to take advantage of the system's ThreadPoolExecutor class. Here we show a simple way to do so:

   .. code-block:: python
   
      from threading import Event
      
      .....
      
      shutdown_event = Event()
      shutdown_event.wait()

Run
---

From the command line, execute:

.. code-block:: bash

   $ python adapters.py

Connect a Client
----------------

.. code-block:: javascript

    var lsClient = new LightstreamerClient(LS_SERVER_HOST, "PROXY_PYTHON");
    lsClient.connect();
    // To be completed with other client side activities, like registration of subscriptions and handling of 
    // real time updates.
    // ...
    
where ``LS_SERVER_HOST`` is the host of the Lightstreamer Server, and ``"PROXY_PYTHON"`` is the Adapter Set ID as specified in the ``adapters.xml`` file.
    
API Reference
-------------

API Reference is available at `<https://lightstreamer.com/api/ls-python-adapter/1.3.1/>`_.

You can generate it by executing the following command from the ``doc`` folder:

.. code-block:: bash

   $ make html
   
The generated documentation will be available under the ``doc\_build\html`` folder. 


See Also
=================================

- `Adapter Remoting Infrastructure Network Protocol Specification`_
- `Lightstreamer Chat Demo adapter for Python`_

.. _Adapter Remoting Infrastructure Network Protocol Specification: https://lightstreamer.com/api/ls-generic-adapter/latest/ARI%20Protocol.pdf
.. _Lightstreamer Chat Demo adapter for Python: https://github.com/Lightstreamer/Lightstreamer-example-Chat-adapter-python


Lightstreamer Compatibility Notes
=================================

Compatible with Adapter Remoting Infrastructure since Server version 7.4.
- For a version of this library compatible with Adapter Remoting Infrastructure for Server version 7,3, please refer to `this tag`_.
- For a version of this library compatible with Adapter Remoting Infrastructure for Server version 6.0 (corresponding to Adapter Remoting Infrastructure 1.7), please refer to `this older tag`_.

.. _this tag: https://github.com/Lightstreamer/Lightstreamer-lib-python-adapter/tree/version-1.2.3
.. _this older tag: https://github.com/Lightstreamer/Lightstreamer-lib-python-adapter/tree/version-1.0.0post1-27
