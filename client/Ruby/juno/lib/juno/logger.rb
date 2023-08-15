# frozen_string_literal: true

module Juno
  # DEBUG < INFO < WARN < ERROR < FATAL < UNKNOWN

  class Logger
    @@mutex = Mutex.new

    # Singleton instance
    @@instance = nil

    def self.instance
      return @@instance unless @@instance.nil?

      @@mutex.synchronize do
        if @@instance.nil?
          raise 'log file not configured' if Juno.juno_config.log_file.to_s.empty?

          @@instance = ::Logger.new(Juno.juno_config.log_file, 'daily', progname: 'JunoRubyClient')

          @@instance.level = ::Logger::INFO
        end
      end
      @@instance
    end

    def self.level=(log_level)
      @@instance.level = log_level unless @@instance.nil?
    end

    private_class_method :new
  end
end
