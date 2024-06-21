# frozen_string_literal: true

require 'juno'

describe 'Juno::IO::ProtocolHeader buffer write' do
  before(:context) do
    @header = Juno::IO::ProtocolHeader.new
    @header.message_size = 16
    @header.opaque = 12
    buff = StringIO.new
    @header.write(buff)
    @bytes_arr = buff.string.bytes
  end

  it 'number of bytes should be 16' do
    expect(@bytes_arr.size).to eq(16)
  end

  it 'magic should be 0x5050' do
    magic = @bytes_arr.shift(2).pack(Juno::IO::OffsetWidth.UINT8('*')).unpack1(Juno::IO::OffsetWidth.UINT16)
    expect(magic).to eq(0x5050)
    expect(@header.magic).to eq(0x5050)
  end

  it 'version should be 1' do
    expect(@bytes_arr.shift).to eq(1)
  end

  # by default it is an operation message and RequestType is a TwoWayRequest
  it 'message_type_flag should be 64' do
    expect(@bytes_arr.shift).to eq(64)
  end

  it 'message_size should be 16' do
    message_size = @bytes_arr.shift(4).pack(Juno::IO::OffsetWidth.UINT8('*')).unpack1(Juno::IO::OffsetWidth.UINT32)
    expect(message_size).to eq(16)
    expect(@header.message_size).to eq(16)
  end

  it 'opaque should be 12' do
    opaque = @bytes_arr.shift(4).pack(Juno::IO::OffsetWidth.UINT8('*')).unpack1(Juno::IO::OffsetWidth.UINT32)
    expect(opaque).to eq(12)
    expect(@header.opaque).to eq(12)
  end

  it 'opcode should be 0' do
    expect(@bytes_arr.shift).to eq(Juno::IO::ProtocolHeader::OpCodes::Nop)
    expect(@header.opcode).to eq(Juno::IO::ProtocolHeader::OpCodes::Nop)
  end

  it 'flag should be 0' do
    expect(@bytes_arr.shift).to eq(0)
    expect(@header.flag).to eq(0)
  end

  it 'shard_id should be 0' do
    shard_id = @bytes_arr.shift(4).pack(Juno::IO::OffsetWidth.UINT8('*')).unpack1(Juno::IO::OffsetWidth.UINT16)
    expect(shard_id).to eq(0)
    expect(@header.shard_id).to eq(0)
  end
end

describe 'Juno::IO::ProtocolHeader buffer read' do
  before do
    @header = Juno::IO::ProtocolHeader.new
    buff = StringIO.new("PP\x01@\x00\x00\x00\x10\x00\x00\x00\f\x00\x00\x00\x00")
    @header.read(buff)
  end

  it 'number of bytes should be 16' do
    expect(@header.message_size).to eq(16)
  end

  it 'magic should be 0x5050' do
    expect(@header.magic).to eq(0x5050)
  end

  it 'version should be 1' do
    expect(@header.version).to eq(1)
  end

  # by default it is an operation message and RequestType is a TwoWayRequest
  it 'message_type_flag should be 64' do
    expect(@header.message_type_flag.message_request_type).to eq(Juno::IO::ProtocolHeader::RequestTypes::TwoWayRequest)
    expect(@header.message_type_flag.message_type).to eq(Juno::IO::ProtocolHeader::MessageTypes::OperationalMessage)
  end

  it 'message_size should be 16' do
    expect(@header.message_size).to eq(16)
  end

  it 'opaque should be 12' do
    expect(@header.opaque).to eq(12)
  end

  it 'opcode should be 0' do
    expect(@header.opcode).to eq(Juno::IO::ProtocolHeader::OpCodes::Nop)
  end

  it 'flag should be 0' do
    expect(@header.flag).to eq(Juno::IO::ProtocolHeader::OpCodes::Nop)
  end

  it 'shard_id should be 0' do
    expect(@header.shard_id).to eq(0)
  end
end
