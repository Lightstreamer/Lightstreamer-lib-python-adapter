import logging

__all__ = ['server']

METADATA_PROVIDER_LOGGER = logging.getLogger(("lightstreamer-adapter.server."
                                              "MetaDataProviderServer"))
DATA_PROVIDER_LOGGER = logging.getLogger(("lightstreamer-adapter.server."
                                          "DataProviderServer"))
