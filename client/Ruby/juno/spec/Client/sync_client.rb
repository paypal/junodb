# frozen_string_literal: true

require 'juno'

describe 'Sync client tests' do
  before(:context) do
    Juno.configure do |config|
      config.file_path = URI.join('file:///', File.expand_path('./../../../juno.yml').to_s)
      config.log_file = 'juno.log'
      config.ssl_cert_file = File.expand_path('./../../lib/server.crt')
      config.ssl_key_file = File.expand_path('./../../lib/server.pem')
    end

    @juno_client = Juno::Client::SyncClient.new
    @key = "mykey#{rand(1000)}"
    @value = 'myvalue'
  end

  it 'create test' do
    juno_resp = @juno_client.create(@key, @value, ttl: 12_000)
    expect(juno_resp.status[:code]).to eq(Juno::ServerStatus::SUCCESS[:code])
    expect(juno_resp.record_context.time_to_live_s).to eq(12_000)
  end

  it 'get request test' do
    juno_resp = @juno_client.get(@key)
    expect(juno_resp.status[:code]).to eq(Juno::ServerStatus::SUCCESS[:code])
    expect(juno_resp.value).to eq(@value)
    expect(juno_resp.record_context.version).to eq(1)
  end

  it 'update request test' do
    juno_resp = @juno_client.update(@key, 'newvalue')
    @record_context = juno_resp.record_context
    expect(juno_resp.status[:code]).to eq(Juno::ServerStatus::SUCCESS[:code])
    expect(juno_resp.value).to eq(@value)
    expect(juno_resp.record_context.version).to eq(1)
  end

  it 'compare_and_set request test' do
    juno_resp = @juno_client.compare_and_set(@record_context, 'value99')
    expect(juno_resp.status[:code]).to eq(Juno::ServerStatus::SUCCESS[:code])
    expect(juno_resp.value).to eq(@value)
    expect(juno_resp.record_context.version).to eq(1)
  end
end
