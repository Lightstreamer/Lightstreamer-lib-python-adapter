lightstreamer_adapter package
=============================

Subpackages
-----------

.. toctree::

    lightstreamer_adapter.interfaces

Submodules
----------

lightstreamer_adapter.server module
-----------------------------------

.. module:: lightstreamer_adapter.server

.. autoclass:: Server
    :exclude-members: start,on_received_request,on_exception,on_ioexception
    :members:

.. autoclass:: MetadataProviderServer
    :members:
    
    .. automethod:: __init__

.. autoclass:: DataProviderServer
    :exclude-members: close
    :members:
    
    .. automethod:: __init__
    
.. autoclass:: ExceptionHandler
    :members:
