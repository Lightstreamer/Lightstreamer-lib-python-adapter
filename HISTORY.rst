.. :changelog:

Release History
---------------


1.3.1 (2023-12-11)
++++++++++++++++++

**Improvements**

- Stored the API docs on a different place.

**Lightstreamer Compatibility Notes**

- Compatible with Adapter Remoting Infrastructure since Server version 7.4.


1.3.0 (2023-07-17)
++++++++++++++++++

**New Features**

- Introduced the support for a single connection instead of two for the
  communication of the Remote Data Adapters.
  In fact, since Server version 7.4, the Proxy Data Adapter can (and should)
  be configured to use a single connection for the communication.
  Hence, the "address" argument of __init__ for the DataProviderServer class,
  which is a tuple, can now have only 2 values (including one port); a tuple
  with 3-values will now be refused.
  As a consequence, if an existing Remote Server based on the previous
  version of this SDK launches a Remote Data Adapter, it cannot be upgraded
  to this new SDK version seamlessly.
  The upgrade will require a change in the code to supply a single
  port for the connection to the Proxy Data Adapter. This, in turn, will
  require the configuration of a single port on the Proxy Data Adapter,
  which is only possible with Lightstreamer Server 7.4 or later.
  However, if a Remote Server only launches Remote Metadata Adapters,
  the compatibility with Server version 7.3 is kept.

- Thoroughly modified the supplied unit tests to implement the single-connection
  behavior and the new compatibility rules.

**Improvements**

- Revised the supplied unit tests to clarifiy dequeueing from the sockets
  and expected messages.

- Clarified the meaning of a None or missing value for a "userMsg" argument
  supplied in a CreditsError: an empty string should be sent to the client.
  Note that, previously, the Server used to send the "null" string as a
  placeholder. Hence, Adapters relying on this behavior should now supply
  "null" explicitly.*

**Lightstreamer Compatibility Notes**

- Compatible with Adapter Remoting Infrastructure since Server version 7.4.


1.2.3 (2023-03-21)
++++++++++++++++++

**Improvements**

- Added handling of runtime exceptions in the internal threads, by submitting
  them to the exception handlers and extended the default handling of runtime
  exceptions by always showing the stack trace.

- Added a unit test on keepalive packets for the notify connection.
  Also improved unit tests log to better identify the current test.

- Added documentation notes regarding the compatibility with Python 3.9 and
  later. See "start" in MetadataProviderServer and DataProviderServer.

**Bug Fixes**

- Fixed a race condition which could have caused the RAC message to be sent
  too late and break the protocol. However, this could only happen when the
  Proxy Adapter had no credential check configured.

- Addressed possible race conditions in the unit tests.

**Lightstreamer Compatibility Notes**

- Compatible with Adapter Remoting Infrastructure since Server version 7.0.


1.2.2 (2021-08-30)
++++++++++++++++++

**Improvements**

- Introduced full support for Server version 7.2. Now the library can log any
  message sent by the Proxy Adapter when forcibly closing the connection.

- Modified the behavior when incomplete credentials are configured: now they
  are sent to the Proxy Adapter, whereas previously they were not sent.
  Note that, if the Proxy Adapter has credentials configured, they cannot be
  incomplete; hence the Proxy Adapter is expected to refuse the connection in
  all cases.

**Bug Fixes**

- Fixed a bug on the handling of keepalives on the notification channel of the
  Data Adapter, which may have caused the Proxy Adapter to close the connection
  due to a keepalive timeout, if configured. This had the highest probability
  to happen in case of a reduced overall data flow, or during the startup phase.

**Lightstreamer Compatibility Notes**

- Compatible with Adapter Remoting Infrastructure since Server version 7.0.


1.2.1 (2021-05-25)
+++++++++++++++++++

**Improvements**

- Reformulated the compatibility constraint with respect to the Server version,
  instead of the Adapter Remoting Infrastructure version.

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
  (i.e. Adapter Remoting Infrastructure) version 1.9
  (corresponding to Server 7.1) or higher: the preferred
  keepalive interval requested by the Proxy Adapter, when stricter than the
  configured one, is now obeyed (with a safety minimun of 1 second). Moreover,
  in that case, the default interval configuration is now 10 seconds instead of
  1. If an existing installation relies on a very short keepalive interval to
  keep the connection alive due to intermediate nodes, the time should now be
  explicitly configured.

- Added full support for ARI Protocol extensions introduced in Adapter Remoting
  Infrastructure version 1.9 (corresponding to Server 7.1).

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

- Compatible with Adapter Remoting Infrastructure since 1.8
  (corresponding to Server 7.0).


1.1.4 (2019-03-29)
+++++++++++++++++++

**Bug Fixes**

- Fixed a bug that caused requests sent from Lightstreamer instances running on
  non-Windows platform not to be parsed correctly (see #2).

**Lightstreamer Compatibility Notes**

- Compatible with Adapter Remoting Infrastructure since 1.8
  (corresponding to Server 7.0).


1.1.3 (2019-03-28)
+++++++++++++++++++

**Bug Fixes**

- Fixed parsing issue when subscribing to more than two items.

**Lightstreamer Compatibility Notes**

- Compatible with Adapter Remoting Infrastructure since 1.8
  (corresponding to Server 7.0).


1.1.2 (2018-02-22)
+++++++++++++++++++

**Improvements**

- Added clarifications on licensing matters in the docs.

**Bug Fixes**

- Fixed edition note in the documentation of notify_user_with_principal.

**Lightstreamer Compatibility Notes**

- Compatible with Adapter Remoting Infrastructure since 1.8
  (corresponding to Server 7.0).


1.1.1 (2017-12-22)
+++++++++++++++++++

**Improvements**

- Moved API documentation to `<http://lightstreamer-lib-python-adapter.readthedocs.io/en/latest/>`_.

- Fixed few source code fragments to make them PEP 8 compliant.

**Bug Fixes**

- Fixed Lightstreamer Compatibility Notes in the README file.

**Lightstreamer Compatibility Notes**

- Compatible with Adapter Remoting Infrastructure since 1.8
  (corresponding to Server 7.0).


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

- Compatible with Adapter Remoting Infrastructure since 1.8
  (corresponding to Server 7.0).


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

- Compatible with Adapter Remoting Infrastructure since 1.7
  (corresponding to Server 6.0).


1.0.0b1 (2016-04-15)
+++++++++++++++++++++

**Bug Fixes**

- Fixed docstrings.

- Fixed typo in some Exceptions' message.

- Fixed unit tests.

**Lightstreamer Compatibility Notes**

- Compatible with Adapter Remoting Infrastructure since 1.7
  (corresponding to Server 6.0).


1.0.0a2 (2016-04-08)
+++++++++++++++++++++

**Bug Fixes**

- Fixed return values in *lightstreamer_adapter.interfaces.metadata.MetadataProvider* class.

- Fixed default handling of I/O related errors.

- Fixed docstrings in modules *lightstreamer_adapter/data_protocol.py* and *lightstreamer_adapter/metadata_protocol.py*.

**Lightstreamer Compatibility Notes**

- Compatible with Adapter Remoting Infrastructure since 1.7
  (corresponding to Server 6.0).


1.0.0a1 (2016-04-08)
+++++++++++++++++++++

**Initial release**

**Lightstreamer Compatibility Notes**

- Compatible with Adapter Remoting Infrastructure since 1.7
  (corresponding to Server 6.0).

