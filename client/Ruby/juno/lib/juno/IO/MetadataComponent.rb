# frozen_string_literal: true

module Juno
  module IO
    #
    # ** MetaData Component **
    # A variable length header followed by a set of meta data fields
    #   Tag/ID: 0x02
    # * Header *
    #
    #     | 0| 1| 2| 3| 4| 5| 6| 7|
    #   0 | size                  | 4 bytes
    # ----+-----------------------+---------
    #   4 | Tag/ID (0x02)         | 1 byte
    # ----+-----------------------+---------
    #   5 | Number of fields      | 1 byte
    # ----+--------------+--------+---------
    #   6 | Field tag    |SizeType| 1 byte
    # ----+--------------+--------+---------
    #     | ...                   |
    # ----+-----------------------+---------
    #     | padding to 4          |
    # ----+-----------------------+---------
    # (Don't think we need a header size. )
    #
    # SizeType:
    #   0		variable length field, for that case,
    #     the first 1 byte of the field MUST be
    #     the size of the field(padding to 4 byte).
    #     The max is 255.
    #   n		Fixed length: 2 ^ (n+1)  bytes
    #
    #
    #
    # * Body *
    # ----+-----------------------+---------
    #     | Field data            | defined by Field tag
    # ----+-----------------------+---------
    #     | ...                   |
    # ----+-----------------------+---------
    #     | padding to 8          |
    # ----+-----------------------+---------
    #
    # * Predefined Field Types *
    #
    # TimeToLive Field
    #   Tag		: 0x01
    #   SizeType	: 0x01
    # Version Field
    #   Tag		: 0x02
    #   SizeType	: 0x01
    # Creation Time Field
    #   Tag		: 0x03
    #   SizeType	: 0x01
    # Expiration Time Field
    #   Tag		: 0x04
    #   SizeType	: 0x01
    # RequestID/UUID Field
    #   Tag		: 0x05
    #   SizeType	: 0x03
    # Source Info Field
    #   Tag		: 0x06
    #   SizeType	: 0
    # Last Modification time (nano second)
    #   Tag		: 0x07
    #   SizeType	: 0x02
    # Originator RequestID Field
    #   Tag		: 0x08
    #   SizeType	: 0x03
    # Correlation ID field
    #   Tag		: 0x09
    #   SizeType	: 0x0
    # Request Handling Time Field
    #   Tag		: 0x0a
    #   SizeType	: 0x01
    #
    # Tag: 0x06
    #
    #   | 0| 1| 2| 3| 4| 5| 6| 7| 0| 1| 2| 3| 4| 5| 6| 7| 0| 1| 2| 3| 4| 5| 6| 7| 0| 1| 2| 3| 4| 5| 6| 7|
    #   |                      0|                      1|                      2|                      3|
    #   +-----------+-----------+--------------------+--+-----------------------+-----------------------+
    #   | size (include padding)| app name length    | T| Port                                          |
    #   +-----------------------+--------------------+--+-----------------------------------------------+
    #   | IPv4 address if T is 0 or IPv6 address if T is 1                                              |
    #   +-----------------------------------------------------------------------------------------------+
    #   | application name, padding to 4-bytes aligned                                                  |
    #   +-----------------------------------------------------------------------------------------------+

    # Wrapper class for MetadataComponentTemplate
    class MetadataComponent
      attr_reader :metadata_field_list, :time_to_live, :version, :creation_time, :expiration_time, :request_uuid,
                  :ip, :app_name, :port, :last_modification, :originator_request_id, :correlation_id, :request_handling_time

      def initialize
        @PROG_NAME = self.class.name
        # @LOGGER = Juno::Logger.instance
        # @metadata_field_list [Array<MetadataField>]
        @metadata_field_list = []
      end

      # @param ttl [Integer] - Record Time to live
      def set_time_to_live(ttl)
        ttl = ttl.to_i
        raise ArgumentError, 'TTL should be > 0' unless ttl.positive?

        @time_to_live = ttl
        ttl = [ttl].pack(OffsetWidth.UINT32)
        add_field(MetadataField.new(0x01, 0x01, ttl))
      end

      # @param version [Integer] - Record version
      def set_version(data)
        @version = data
        version_bytes_string = [data].pack(OffsetWidth.UINT32)
        add_field(MetadataField.new(0x02, 0x01, version_bytes_string))
      end

      # @param creation_time [Integer] - Unix timestamp (required)
      def set_creation_time(data)
        @creation_time = data
        creation_time_bytes_string = [data].pack(OffsetWidth.UINT32)
        add_field(MetadataField.new(0x03, 0x01, creation_time_bytes_string))
      end

      def set_expiration_time(data)
        @expiration_time = data
        expiration_time_bytes_string = [data].pack(OffsetWidth.UINT32)
        add_field(MetadataField.new(0x04, 0x01, expiration_time_bytes_string))
      end

      # @param input_uuid_byte_string [String] - Record Time to live (optional)
      # if not provided, creates a uuid itself
      def set_request_uuid(input_uuid_byte_string = nil)
        @request_uuid = if input_uuid_byte_string.nil?
                          UUIDTools::UUID.random_create
                        else
                          UUIDTools::UUID.parse_raw(input_uuid_byte_string)
                        end
        add_field(MetadataField.new(0x05, 0x03, @request_uuid.raw))
        @request_uuid
      end

      # SourceInfoField
      # @param app_name [String] - Record Time to live (required)
      # @param ip [IPAddr] - ip address for component (required)
      # @param port [Integer]   (required)
      def set_source_info(app_name:, ip:, port:)
        @ip = ip
        @port = port
        @app_name = app_name
        data = MetadataComponentTemplate::SourceInfoField.new
        data.app_name = app_name
        data.ip = ip.hton
        data.port = port
        str_io = StringIO.new
        data.write(str_io)
        add_field(MetadataField.new(0x06, 0x00, str_io.string))
      end

      def set_last_modification(data)
        @last_modification = data
        last_modification_bytes_string = [data].pack(OffsetWidth.UINT64)
        add_field(MetadataField.new(0x07, 0x02, last_modification_bytes_string))
      end

      # @param input_uuid_byte_string [String] (optional)
      # if not provided, creates a uuid itself
      def set_originator_request_id(input_uuid_byte_string = nil)
        @originator_request_id = if input_uuid_byte_string.nil?
                                   UUIDTools::UUID.random_create
                                 else
                                   UUIDTools::UUID.parse_raw(input_uuid_byte_string)
                                 end
        add_field(MetadataField.new(0x08, 0x03, @originator_request_id.raw))
        @originator_request_id
      end

      # @param input_uuid_byte_string [String] (optional)
      # if not provided, creates a uuid itself
      def set_correlation_id(input_uuid_byte_string = nil)
        @correlation_id = if input_uuid_byte_string.nil?
                            UUIDTools::UUID.random_create
                          else
                            UUIDTools::UUID.parse_raw(input_uuid_byte_string)
                          end
        field = MetadataComponentTemplate::CorrelationIDField.new
        field.correlation_id = @correlation_id.raw
        str_io = StringIO.new
        field.write(str_io)
        # puts field
        add_field(MetadataField.new(0x09, 0x0, str_io.string))
      end

      def set_request_handling_time(data)
        @request_handling_time = data
        request_handling_time_bytes_string = [data].pack(OffsetWidth.UINT32)
        add_field(MetadataField.new(0x0A, 0x01, request_handling_time_bytes_string))
      end

      # Function to add feild to the list
      # @param field [MetadataField] (required)
      def add_field(field)
        metadata_field_list.push(field)
      end

      # function to calculate size of metadata component
      def num_bytes
        io = StringIO.new
        write(io)
        io.size
      end

      # Function to serialize Component to buffer
      # @param io [StringIO] (required)
      def write(io)
        buffer = MetadataComponentTemplate.new
        buffer.number_of_fields = metadata_field_list.length
        metadata_field_list.each do |field|
          f = MetadataComponentTemplate::MetadataHeaderField.new
          f.size_type = field.size_type
          f.field_tag = field.tag
          buffer.metadata_fields.push(f)
        end

        body = StringIO.new
        metadata_field_list.each do |field|
          body.write(field.data)
        end
        padding_size = (8 - body.size % 8) % 8
        body.write(Array.new(0, padding_size).pack(OffsetWidth.UINT8('*'))) if padding_size.positive?
        buffer.body = body.string

        buffer.write(io)
      end

      # Function to de-serialize Component to buffer
      # @param io [StringIO] (required)
      def read(io)
        metadata_component = MetadataComponentTemplate.new
        metadata_component.read(io)

        body_buffer = StringIO.new(metadata_component.body)

        metadata_component.metadata_fields.each do |field|
          case field.field_tag
          when TagAndType::TimeToLive[:tag]
            ttl_byte_string = body_buffer.read(1 << (1 + TagAndType::TimeToLive[:size_type]))
            set_time_to_live(ttl_byte_string.unpack1(OffsetWidth.UINT32))

          when TagAndType::Version[:tag]
            version_byte_string = body_buffer.read(1 << (1 + TagAndType::Version[:size_type]))
            set_version(version_byte_string.unpack1(OffsetWidth.UINT32))

          when TagAndType::CreationTime[:tag]
            creation_time_byte_string = body_buffer.read(1 << (1 + TagAndType::CreationTime[:size_type]))
            set_creation_time(creation_time_byte_string.unpack1(OffsetWidth.UINT32))

          when TagAndType::RequestUUID[:tag]
            request_uuid_byte_string = body_buffer.read(1 << (1 + TagAndType::RequestUUID[:size_type]))
            set_request_uuid(request_uuid_byte_string)

          when TagAndType::SourceInfo[:tag]
            source_info = MetadataComponentTemplate::SourceInfoField.new
            source_info.read(body_buffer)
            set_source_info(app_name: source_info.app_name, ip: IPAddr.new_ntoh(source_info.ip), port: source_info.port)

          when TagAndType::LastModification[:tag]
            last_modification_byte_string = body_buffer.read(1 << (1 + TagAndType::LastModification[:size_type]))
            set_last_modification(last_modification_byte_string.unpack1(OffsetWidth.UINT64))

          when TagAndType::ExpirationTime[:tag]
            expiration_time_byte_string = body_buffer.read(1 << (1 + TagAndType::ExpirationTime[:size_type]))
            set_expiration_time(expiration_time_byte_string.unpack1(OffsetWidth.UINT32))

          when TagAndType::OriginatorRequestID[:tag]
            originator_request_id_byte_string = body_buffer.read(1 << (1 + TagAndType::OriginatorRequestID[:size_type]))
            set_originator_request_id(originator_request_id_byte_string)
          # when TagAndType::CorrelationID[:tag]

          when TagAndType::RequestHandlingTime[:tag]
            request_handling_time_byte_string = body_buffer.read(1 << (1 + TagAndType::RequestHandlingTime[:size_type]))
            set_request_handling_time(request_handling_time_byte_string.unpack1(OffsetWidth.UINT32))
          end
        end
      end

      class TagAndType
        TimeToLive = {
          tag: 0x01,
          size_type: 0x01
        }.freeze
        Version = {
          tag: 0x02,
          size_type: 0x01
        }.freeze
        CreationTime = {
          tag: 0x03,
          size_type: 0x01
        }.freeze
        RequestUUID = {
          tag: 0x05,
          size_type: 0x03
        }.freeze
        SourceInfo = {
          tag: 0x06,
          size_type: 0x00
        }.freeze
        ExpirationTime = {
          tag: 0x04,
          size_type: 0x01
        }.freeze
        LastModification = {
          tag: 0x07,
          size_type: 0x02
        }.freeze
        OriginatorRequestID = {
          tag: 0x08,
          size_type: 0x03
        }.freeze
        CorrelationID = {
          tag: 0x09,
          size_type: 0x00
        }.freeze
        RequestHandlingTime = {
          tag: 0x0A,
          size_type: 0x01
        }.freeze
      end

      # DataType for @metadata_field_list
      class MetadataField
        attr_accessor :tag, :size_type, :data

        def initialize(tag, size_type, data)
          @tag = tag
          @size_type = size_type
          @data = data
        end

        def size
          if size_type == SizeType::Variable
            data.length
          else
            1 << (size_type + 1)
          end
        end

        class SizeType
          Variable = 0
        end
      end
    end
  end
end
