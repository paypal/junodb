# frozen_string_literal: true

# Top module for juno client
module Juno
  # Module containing constant default values for Properties
  class DefaultProperties
    RESPONSE_TIMEOUT_MS = 200
    CONNECTION_TIMEOUT_MS = 200
    CONNECTION_POOLSIZE = 1
    CONNECTION_LIFETIME_MS = 30_000
    DEFAULT_LIFETIME_S = 259_200

    # Max for all above property
    MAX_RESPONSE_TIMEOUT_MS = 5000
    MAX_CONNECTION_LIFETIME_MS = 30_000
    MAX_CONNECTION_TIMEOUT_MS = 5000
    MAX_KEY_SIZE_B = 128
    MAX_VALUE_SIZE_B = 204_800
    MAX_NAMESPACE_LENGTH = 64
    MAX_CONNECTION_POOL_SIZE = 3
    MAX_LIFETIME_S = 259_200

    # Required Properties
    HOST = ''
    PORT = 0
    APP_NAME = ''
    RECORD_NAMESPACE = ''

    # optional Properties
    CONFIG_PREFIX = ''
    USE_SSL = true
    RECONNECT_ON_FAIL = false
    USE_PAYLOAD_COMPRESSION = false
    OPERATION_RETRY = false
    BYPASS_LTM = true
  end
end
