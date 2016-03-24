Lightstreamer SDK for Python Adapters 1.0.0a5
=============================================

A Python library to  write Data Adapters and Metadata Adapters for `Lightstreamer Server`_.
The adapters will run in a separate process, communicating with the Server through the Adapter Remoting Infrastructure.

Use
---
Install the package:

.. code-block:: bash

   $ pip install lightstreamer-adapter

Configure Lightstreamer
-----------------------

1) Download and install Lightstreamer
2) Go to the "adapters" folder of your Lightstreamer Server installation. Create a new folder to deploy the remote adapters in, let's call it "PythonAdapter"
3) Create an "adapters.xml" file inside the "PythonAdapter" folder and use the following contents (this is an example configuration, you can modify it to your liking by using the generic template, `DOCS-SDKs/adapter_remoting_infrastructure/doc/adapter_conf_template/adapters.xml` or `DOCS-SDKs/adapter_remoting_infrastructure/doc/adapter_robust_conf_template/adapters.xml`, as a reference):

   .. code-block:: bash

    <?xml version="1.0"?>
    <adapters_conf id="PROXY_NODE">
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
           <param name="notify_port">8002</param>
           <param name="timeout">36000000</param>
        </data_provider>
     </adapters_conf>
    
4) Take note of the ports configured in the adapters.xml file as those are needed to write the remote part of the adapters.

.. _Lightstreamer Server: http://www.lightstreamer.com

Write the Adapters
------------------
Create a new python module, let's call it ``adapters.py``, where we will put  the minimal logic required to write a basic Adapter Set.

1) Import the sever classes needed to setup the connection to the Lightstreamer server, 
and the adapter classes to be extended to write your own Remote Adapters 

   .. code-block: python
   
   from lightstreamer_adapter.server import (DataProviderServer,
                                             MetadataProviderServer)
   from lightstreamer_adapter.interfaces.data import DataProvider
   from lightstreamer_adapter.interfaces.metadata import MetadataProvider
   
2)
    
