# frozen_string_literal: true

require 'juno'

describe 'Juno::IO::PayloadComponent buffer write' do
  before(:context) do
    @payload = Juno::IO::PayloadComponent.new
    @NAMESPACE = 'testnamespace123'
    @KEY = 'testkey123'
    @VALUE = 'testvalue456'

    @payload.payload_key = @KEY # length: 10
    @payload.namespace = @NAMESPACE # length: 16
    @payload.set_value(@VALUE) # length: 12
    # size = 13 + 10 + 16 + 12 + 5  = 56
    buff = StringIO.new
    @payload.write(buff)

    @bytes_arr = buff.string.bytes
  end

  it 'component size should be 56' do
    expect(@bytes_arr.length).to eq(56)
    size = @bytes_arr.shift(4).pack(Juno::IO::OffsetWidth.UINT8('*')).unpack1(Juno::IO::OffsetWidth.UINT32)
    expect(@payload.component_size).to eq(56)
    expect(size).to eq(56)
  end

  it 'tag_id should be 1' do
    expect(@payload.tag_id).to eq(1)
    expect(@bytes_arr.shift).to eq(1)
  end

  it 'namspace_length should be 16' do
    expect(@payload.namespace_length).to eq(16)
    expect(@bytes_arr.shift).to eq(16)
  end

  it 'key_length should be 10' do
    key_length = @bytes_arr.shift(2).pack(Juno::IO::OffsetWidth.UINT8('*')).unpack1(Juno::IO::OffsetWidth.UINT16)
    expect(@payload.key_length).to eq(10)
    expect(key_length).to eq(10)
  end

  it 'payload_length should be 13' do
    payload_length = @bytes_arr.shift(4).pack(Juno::IO::OffsetWidth.UINT8('*')).unpack1(Juno::IO::OffsetWidth.UINT32)
    expect(@payload.payload_length).to eq(13)
    expect(payload_length).to eq(13)
  end

  it 'namespace as expected' do
    namespace = @bytes_arr.shift(@NAMESPACE.length).pack(Juno::IO::OffsetWidth.UINT8('*'))
    expect(@payload.namespace).to eq(@NAMESPACE)
    expect(namespace).to eq(@NAMESPACE)
  end

  it 'key as expected' do
    key = @bytes_arr.shift(@KEY.length).pack(Juno::IO::OffsetWidth.UINT8('*'))
    expect(@payload.payload_key).to eq(@KEY)
    expect(key).to eq(@KEY)
  end

  it 'payload_type should be Juno::IO::PayloadType::UnCompressed' do
    ptype = @bytes_arr.shift
    expect(@payload.payload_type).to eq(Juno::IO::PayloadType::UnCompressed)
    expect(ptype).to eq(Juno::IO::PayloadType::UnCompressed)
  end

  it 'payload value expected' do
    value = @bytes_arr.shift(@VALUE.length).pack(Juno::IO::OffsetWidth.UINT8('*'))
    expect(@payload.value).to eq(@VALUE)
    expect(value).to eq(@VALUE)
  end

  it 'payload length' do
    padding = @bytes_arr.shift(5).pack(Juno::IO::OffsetWidth.UINT8('*'))
    expect(padding.length).to eq(5)
    expect(@payload.padding.length).to eq(5)
    expect(padding).to eq([0, 0, 0, 0, 0].pack(Juno::IO::OffsetWidth.UINT8('*')))
    expect(@payload.padding).to eq([0, 0, 0, 0, 0].pack(Juno::IO::OffsetWidth.UINT8('*')))
  end
end

describe 'Juno::IO::PayloadComponent buffer read' do
  before(:context) do
    @payload = Juno::IO::PayloadComponent.new
    @NAMESPACE = 'testnamespace123' # length: 16
    @KEY = 'testkey123' # length: 10
    @VALUE = 'testvalue456' # length: 12

    # size = 13 + 10 + 16 + 12 + 5  = 56
    buff = StringIO.new("\x00\x00\x008\x01\x10\x00\n\x00\x00\x00\rtestnamespace123testkey123\x00testvalue456\x00\x00\x00\x00\x00")
    @payload.read(buff)
  end

  it 'component size should be 56' do
    expect(@payload.component_size).to eq(56)
  end

  it 'tag_id should be 1' do
    expect(@payload.tag_id).to eq(1)
  end

  it 'namspace_length should be 16' do
    expect(@payload.namespace_length).to eq(16)
  end

  it 'key_length should be 10' do
    expect(@payload.key_length).to eq(10)
  end

  it 'payload_length should be 13' do
    expect(@payload.payload_length).to eq(13)
  end

  it 'namespace as expected' do
    expect(@payload.namespace).to eq(@NAMESPACE)
  end

  it 'key as expected' do
    expect(@payload.payload_key).to eq(@KEY)
  end

  it 'payload_type should be Juno::IO::PayloadType::UnCompressed' do
    expect(@payload.payload_type).to eq(Juno::IO::PayloadType::UnCompressed)
  end

  it 'payload value expected' do
    expect(@payload.value).to eq(@VALUE)
  end

  it 'payload length' do
    expect(@payload.padding.length).to eq(5)
    expect(@payload.padding).to eq([0, 0, 0, 0, 0].pack(Juno::IO::OffsetWidth.UINT8('*')))
  end
end
