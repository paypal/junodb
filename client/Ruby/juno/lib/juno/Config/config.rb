# frozen_string_literal: true

# Top module for juno client
module Juno
  # Juno.configure do |config|
  #   config.record_namespace = "kk"
  #   config.host = '10.138.38.83'
  #   config.port = 5080
  #   config.app_name = "TestApp"
  #   config.file_path = ""
  #   config.url = ""
  # end
  class Config
    attr_reader :response_timeout, :connection_timeout, :connection_pool_size, :connection_lifetime, :default_lifetime,
                :max_response_timeout, :max_connection_timeout, :max_connection_pool_size, :max_connection_lifetime, :max_lifetime, :max_key_size, :max_value_size, :max_namespace_length,
                :host, :port, :app_name, :record_namespace,
                :use_ssl, :use_payload_compression, :operation_retry, :bypass_ltm, :reconnect_on_fail, :config_prefix,
                :log_file, :ssl_cert_file, :ssl_key_file

    REQUIRED_PROPERTIES = %i[host port app_name record_namespace log_file].freeze

    def initialize(config_provider, log_file:, ssl_cert_file: nil, ssl_key_file: nil)
      @PROG_NAME = self.class.name
      @log_file = log_file
      @ssl_cert_file = ssl_cert_file
      @ssl_key_file = ssl_key_file
      @config = config_provider
      read_all
      validate!
    end

    # Function to map all properties read from config file to variables
    def read_all
      @response_timeout = @config.get_property(Juno::Properties::RESPONSE_TIMEOUT,
                                               Juno::DefaultProperties::RESPONSE_TIMEOUT_MS)
      @connection_timeout = @config.get_property(Juno::Properties::CONNECTION_TIMEOUT,
                                                 Juno::DefaultProperties::CONNECTION_TIMEOUT_MS)
      @connection_pool_size = @config.get_property(Juno::Properties::CONNECTION_POOLSIZE,
                                                   Juno::DefaultProperties::CONNECTION_POOLSIZE)
      @connection_lifetime = @config.get_property(Juno::Properties::CONNECTION_LIFETIME,
                                                  Juno::DefaultProperties::CONNECTION_LIFETIME_MS)
      @default_lifetime = @config.get_property(Juno::Properties::DEFAULT_LIFETIME,
                                               Juno::DefaultProperties::DEFAULT_LIFETIME_S)
      @max_response_timeout = @config.get_property(Juno::Properties::MAX_RESPONSE_TIMEOUT,
                                                   Juno::DefaultProperties::MAX_RESPONSE_TIMEOUT_MS)
      @max_connection_timeout = @config.get_property(Juno::Properties::MAX_CONNECTION_TIMEOUT,
                                                     Juno::DefaultProperties::MAX_CONNECTION_TIMEOUT_MS)
      @max_connection_pool_size = @config.get_property(Juno::Properties::MAX_CONNECTION_POOL_SIZE,
                                                       Juno::DefaultProperties::MAX_CONNECTION_POOL_SIZE)
      @max_connection_lifetime = @config.get_property(Juno::Properties::MAX_CONNECTION_LIFETIME,
                                                      Juno::DefaultProperties::MAX_CONNECTION_LIFETIME_MS)
      @max_lifetime = @config.get_property(Juno::Properties::MAX_LIFETIME, Juno::DefaultProperties::MAX_LIFETIME_S)
      @max_key_size = @config.get_property(Juno::Properties::MAX_KEY_SIZE, Juno::DefaultProperties::MAX_KEY_SIZE_B)
      @max_value_size = @config.get_property(Juno::Properties::MAX_VALUE_SIZE,
                                             Juno::DefaultProperties::MAX_VALUE_SIZE_B)
      @max_namespace_length = @config.get_property(Juno::Properties::MAX_NAMESPACE_LENGTH,
                                                   Juno::DefaultProperties::MAX_NAMESPACE_LENGTH)
      @host = @config.get_property(Juno::Properties::HOST, Juno::DefaultProperties::HOST)
      @port = @config.get_property(Juno::Properties::PORT, Juno::DefaultProperties::PORT)
      @app_name = @config.get_property(Juno::Properties::APP_NAME, Juno::DefaultProperties::APP_NAME)
      @record_namespace = @config.get_property(Juno::Properties::RECORD_NAMESPACE,
                                               Juno::DefaultProperties::RECORD_NAMESPACE)
      @use_ssl = @config.get_property(Juno::Properties::USE_SSL, Juno::DefaultProperties::USE_SSL)
      @use_payload_compression = @config.get_property(Juno::Properties::USE_PAYLOAD_COMPRESSION,
                                                      Juno::DefaultProperties::USE_PAYLOAD_COMPRESSION)
      @operation_retry = @config.get_property(Juno::Properties::ENABLE_RETRY, Juno::DefaultProperties::OPERATION_RETRY)
      @bypass_ltm = @config.get_property(Juno::Properties::BYPASS_LTM, Juno::DefaultProperties::BYPASS_LTM)
      @reconnect_on_fail = @config.get_property(Juno::Properties::RECONNECT_ON_FAIL,
                                                Juno::DefaultProperties::RECONNECT_ON_FAIL)
      @config_prefix = @config.get_property(Juno::Properties::CONFIG_PREFIX, Juno::DefaultProperties::CONFIG_PREFIX)
      nil
    end

    def validate!
      missing_properties = []
      REQUIRED_PROPERTIES.each do |property|
        missing_properties.push(property) if send(property).nil?
      end

      if @use_ssl
        %i[ssl_cert_file ssl_key_file].each do |property|
          missing_properties.push(property) if send(property).nil?
        end
      end

      if missing_properties.length.positive?
        raise "Please provide a value for the required property(s) #{missing_properties.join(', ')}."
      end

      if @use_ssl
        raise 'SSL Certificate file not found' unless File.exist?(@ssl_cert_file)
        raise 'SSL Key file not found' unless File.exist?(@ssl_key_file)
      end

      nil
    end

    class Source
      YAML_FILE = 0
      JSON_FILE = 1
      URL = 2
    end
  end

  def self.configure
    if @juno_config.nil?
      config_reader = Juno::ConfigReader.new
      yield config_reader
      if !config_reader.file_path.nil?
        @juno_config = Config.new(ConfigProvider.new(config_reader.file_path, config_reader.source_format, nil),
                                  log_file: config_reader.log_file, ssl_cert_file: config_reader.ssl_cert_file,
                                  ssl_key_file: config_reader.ssl_key_file)
        @LOGGER = Juno::Logger.instance
      elsif !config_reader.url.nil? # URL should URI Object
        # @juno_config = Config.new(ConfigProvider.new(@juno_config.file_path, @juno_config.source_format, nil))
      else
        raise 'No file or url provided'
      end
    else
      Juno::Logger.instance.warn('Juno client cannot be reconfigured')
    end
  end

  def self.juno_config
    raise 'Please configure the properties using Juno.configure' if @juno_config.nil?

    @juno_config
  end
end
