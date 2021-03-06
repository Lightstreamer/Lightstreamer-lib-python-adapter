.. :changelog:

Release History
---------------


[current state]
++++++++++++++++

**Improvements**

Reformulated the compatibility constraint with respect to the Server version.

**Lightstreamer Compatibility Notes**

- Compatible with Adapter Remoting Infrastructure since Server version 7.0.


1.2.0 (2020-01-24)
+++++++++++++++++++

**Improvements**

- Extended DataProviderServer and MetadataProviderServer (through the Server
  superclass) with settings of credentials, to be sent to the Proxy Adapter
  upon each connection. Credential check is an optional configuration of the
  Proxy Adapter; if not leveraged, the credentials will be ignored.

- Modified the handling of the keepalives when connected to a Proxy Adapter
  (i.e. Adapter Remoting Infrastructure) version 1.9 or higher: the preferred
  keepalive interval requested by the Proxy Adapter, when stricter than the
  configured one, is now obeyed (with a safety minimun of 1 second). Moreover,
  in that case, the default interval configuration is now 10 seconds instead of
  1. If an existing installation relies on a very short keepalive interval to
  keep the connection alive due to intermediate nodes, the time should now be
  explicitly configured.

- Added full support for ARI Protocol extensions introduced in Adapter Remoting
  Infrastructure version 1.9.

- Added full support for TLS/SSL encrypted connections the Proxy Adapters.

- Added clarifications in the documentation of the exception handlers and fix
  a few obsolete notes.

- Added clarifications in the documentation of MetadataProviderServer and
  DataProviderServer classes.

- Improved code layout as per pylint/pycodestyle outputs.

- Remove useless "pass" statement from classes of the interfaces package.

- Updated unit tests according to new features

**Bug Fixes**

- Removed useless optional client_principal parameter from the
  MetadataProvider.notify_user method.

- Fixed documentation of the DataProvider class, where "Lightstreamer Kernel"
  was erroneously referred as "Lightstreamer1".

- Fixed broken links in the documentation of the DataProviderServer class.

**Lightstreamer Compatibility Notes**

- Compatible with Adapter Remoting Infrastructure since 1.8.


1.1.4 (2019-03-29)
+++++++++++++++++++

**Bug Fixes**

- Fixed a bug that caused requests sent from Lightstreamer instances running on
  non-Windows platform not to be parsed correctly (see #2).

**Lightstreamer Compatibility Notes**

- Compatible with Adapter Remoting Infrastructure since 1.8.


1.1.3 (2019-03-28)
+++++++++++++++++++

**Bug Fixes**

- Fixed parsing issue when subscribing to more than two items.

**Lightstreamer Compatibility Notes**

- Compatible with Adapter Remoting Infrastructure since 1.8.


1.1.2 (2018-02-22)
+++++++++++++++++++

**Improvements**

- Added clarifications on licensing matters in the docs.

**Bug Fixes**

- Fixed edition note in the documentation of notify_user_with_principal.

**Lightstreamer Compatibility Notes**

- Compatible with Adapter Remoting Infrastructure since 1.8.


1.1.1 (2017-12-22)
+++++++++++++++++++

**Improvements**

- Moved API documentation to `<http://lightstreamer-lib-python-adapter.readthedocs.io/en/latest/>`_.

- Fixed few source code fragments to make them PEP 8 compliant.

**Bug Fixes**

- Fixed Lightstreamer Compatibility Notes in the README file.

**Lightstreamer Compatibility Notes**

- Compatible with Adapter Remoting Infrastructure since 1.8.


1.1.0 (2017-12-19)
+++++++++++++++++++

**Improvements**

- Modified the signature of the notify_mpn_device_access and
  notify_mpn_device_token_change methods of the MetadataProvider class,
  to add a session ID argument.
  Existing Remote Metadata Adapters leveraging notify_mpn_device_access
  and/or notify_mpn_device_token_change have to be ported to the new signature.

- Revised the public constants defined in the MpnPlatformType class.
  The constants referring to the supported platforms have got new names,
  whereas the constants for platforms not yet supported have been removed.
  Existing Remote Metadata Adapters explicitly referring to the constants
  have to be aligned.

- Removed the subclasses of MpnSubscriptionInfo (namely
  MpnApnsSubscriptionInfo and MpnGcmSubscriptionInfo) that were used
  by the SDK library to supply the attributes of the MPN subscriptions
  in notify_mpn_subscription_activation. Now, simple instances of
  MpnSubscriptionInfo will be supplied and attribute information can be
  obtained through the new "notification_format" property.
  See the MPN chapter on the General Concepts document for details on the
  characteristics of the Notification Format.
  Existing Remote Metadata Adapters
  leveraging notify_mpn_subscription_activation and inspecting the supplied
  MpnSubscriptionInfo have to be ported to the new class contract.

- Improved the interface documentation of MPN-related methods.

- Clarified in the docs for notifySessionClose which race conditions with other
  methods can be expected.

- Aligned the documentation to comply with current licensing policies.

**Lightstreamer Compatibility Notes**

- Compatible with Adapter Remoting Infrastructure since 1.8.


1.0.0.post1 (2016-11-22)
++++++++++++++++++++++++

- Finishing touches on the package documentation visible from the PyPi repository


1.0.0 (2016-11-22)
+++++++++++++++++++

**Improvements**

- Updated logging messages.

**Bug Fixes**

- Fixed notification of End Of Snaphsot in case of not availability of the snapshot.

- Fixed docstrings in modules *lightstreamer_adapter/server.py* and *lightstreamer_adapter/subscription.py*.

- Fixed unit tests.

**Lightstreamer Compatibility Notes**

- Compatible with Adapter Remoting Infrastructure since 1.7.


1.0.0b1 (2016-04-15)
+++++++++++++++++++++

**Bug Fixes**

- Fixed docstrings.

- Fixed typo in some Exceptions' message.

- Fixed unit tests.

**Lightstreamer Compatibility Notes**

- Compatible with Adapter Remoting Infrastructure since 1.7.


1.0.0a2 (2016-04-08)
+++++++++++++++++++++

**Bug Fixes**

- Fixed return values in *lightstreamer_adapter.interfaces.metadata.MetadataProvider* class.

- Fixed default handling of I/O related errors.

- Fixed docstrings in modules *lightstreamer_adapter/data_protocol.py* and *lightstreamer_adapter/metadata_protocol.py*.

**Lightstreamer Compatibility Notes**

- Compatible with Adapter Remoting Infrastructure since 1.7.


1.0.0a1 (2016-04-08)
+++++++++++++++++++++

**Initial release**

**Lightstreamer Compatibility Notes**

- Compatible with Adapter Remoting Infrastructure since 1.7.

