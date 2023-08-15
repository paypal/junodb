# frozen_string_literal: true

# Top module for juno client
module Juno
  # Class to read config from file or url
  class ConfigProvider
    # Constructor
    # @param source_uri [URI] Ruby URI Object for file or URL (required)
    # @param source_format [String] source_format required only for url. Inferred from file extension when using file (optional)
    # @return Propert value. Return default_value if property not found
    # @see Juno::Properties
    def initialize(source_uri, source_format = nil, _http_handler = nil)
      begin
        source_scheme = source_uri&.send(:scheme)
      rescue StandardError => e
        raise "Invalid source_uri object.\n #{e.message}"
      end
      if source_scheme == 'file'
        read_from_file(source_uri)
      elsif source_scheme =~ /^http(s)?$/
        read_from_url(source_uri, source_format, http_handler)
      else
        raise 'Only local file and URL supported'
      end
    end

    # Function to intialize configatron object from config file/URL
    # @param source_uri [URI] Ruby URI Object for file or URL (required)
    # @return [nil]
    def read_from_file(source_uri)
      raise 'Config file not found' unless File.exist?(source_uri.path)

      hsh = if ['.yml', '.yaml'].include?(File.extname(source_uri.path))
              YAML.load_file(source_uri.path)
            elsif ['.json'].inlcude?(File.extname(source_uri.path))
              json_text = File.read(source_uri.path)
              JSON.parse(json_text)
            else
              raise 'Unknown file format'
            end
      configatron.configure_from_hash(hsh)
      nil
    end

    def read_from_url(source_uri, source_format, http_handler); end

    # Function to read propertied in the heirarchy define in Juno::Properties
    # @param property_key [String] String key (required)
    # @param default_value (optional) default value if property not found
    # @return Propert value. Returns default_value if property not found
    # @see Juno::Properties
    def get_property(property_key, default_value = nil)
      return default_value if property_key.to_s.empty?

      value = configatron.to_h
      property_key.to_s.split('.').each do |k|
        return default_value unless value.is_a?(Hash) && value.key?(k.to_sym)

        value = value[k.to_sym]
        # puts "#{k} --- #{value}"
      end

      value.nil? || value.is_a?(Hash) ? default_value : value
    end
  end
end
