# frozen_string_literal: true

# Top module for juno client
module Juno
  # Properties Reader - Properties to be read from the developer using Juno.configure
  # Either file_path or url is required
  # log device can be a filename or IO Object
  class ConfigReader
    attr_accessor :file_path, :url, :source_format, :http_handler, :log_level, :log_device,
                  :max_log_file_bytes, :log_rotation, :log_file, :ssl_cert_file, :ssl_key_file

    def initialize
      # default values
      @log_level = ::Logger::Severity::INFO
      @max_log_file_bytes = 1_048_576 # default for inbuilt logger class
      @log_device = $stdout
      @log_rotation = 'daily' # daily, weekly, monthly
    end
  end
end
