# frozen_string_literal: true

# Top module for juno client
module Juno
  # Module for properties key (config file)
  class Properties
    RESPONSE_TIMEOUT = 'juno.response.timeout_msec'
    CONNECTION_TIMEOUT = 'juno.connection.timeout_msec'
    DEFAULT_LIFETIME = 'juno.default_record_lifetime_sec'
    CONNECTION_LIFETIME = 'juno.connection.recycle_duration_msec'
    CONNECTION_POOLSIZE = 'juno.connection.pool_size'
    RECONNECT_ON_FAIL = 'juno.connection.reconnect_on_fail'
    HOST = 'juno.server.host'
    PORT = 'juno.server.port'
    APP_NAME = 'juno.application_name'
    RECORD_NAMESPACE = 'juno.record_namespace'
    USE_SSL = 'juno.useSSL'
    USE_PAYLOAD_COMPRESSION = 'juno.usePayloadCompression'
    ENABLE_RETRY = 'juno.operation.retry'
    BYPASS_LTM = 'juno.connection.byPassLTM'
    CONFIG_PREFIX = 'prefix'

    # Max for each property
    MAX_LIFETIME = 'juno.max_record_lifetime_sec'
    MAX_KEY_SIZE = 'juno.max_key_size'
    MAX_VALUE_SIZE = 'juno.max_value_size'
    MAX_RESPONSE_TIMEOUT = 'juno.response.max_timeout_msec'
    MAX_CONNECTION_TIMEOUT = 'juno.connection.max_timeout_msec'
    MAX_CONNECTION_LIFETIME = 'juno.connection.max_recycle_duration_msec'
    MAX_CONNECTION_POOL_SIZE = 'juno.connection.max_pool_size'
    MAX_NAMESPACE_LENGTH = 'juno.max_record_namespace_length'
  end
end
