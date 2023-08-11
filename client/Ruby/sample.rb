require 'juno'

Juno.configure do |config|
  config.file_path = URI.join('file:///', '')
  config.log_file = ''
  config.ssl_cert_file = ''
  config.ssl_key_file = ''
end

# Rails cache store
juno_client = Juno::Client::CacheStore.new
juno_client.write('key', 'value')

# Synchronous Client
juno_client = Juno::Client::SyncClient.new
resp = juno_client.create('key', 'value')
resp = juno_client.get('key')
resp = juno_client.update('key', 'newvalue')


# Return Juno::Client::JunoResponse
resp = juno_client.create('15', '99')
resp = juno_client.get('15')
resp = juno_client.update('15', '100')


# Asyn Client
class Callback
  def update(time, value, reason)
    puts time
    if reason.to_s.empty?
      puts value
    else
      puts "failed: #{reason}"
    end
  end
end

juno_client = Juno::Client::ReactClient.new
juno_client.create('mykey', 'myvalue')
juno_client.add_observer(Callback.new)
