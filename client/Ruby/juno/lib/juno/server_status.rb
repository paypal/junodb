# frozen_string_literal: true

module Juno
  class ServerStatus
    SUCCESS = { code: 0, error_msg: 'no error', client_operation_status: Juno::Client::OperationStatus::SUCCESS }.freeze
    BAD_MSG = { code: 1, error_msg: 'bad message',
                client_operation_status: Juno::Client::OperationStatus::INTERNAL_ERROR }.freeze
    NO_KEY = { code: 3, error_msg: 'key not found',
               client_operation_status: Juno::Client::OperationStatus::NO_KEY }.freeze
    DUP_KEY = { code: 4, error_msg: 'dup key',
                client_operation_status: Juno::Client::OperationStatus::UNIQUE_KEY_VIOLATION }.freeze
    BAD_PARAM = { code: 7,  error_msg: 'bad parameter',
                  client_operation_status: Juno::Client::OperationStatus::BAD_PARAM }.freeze
    RECORD_LOCKED = { code: 8, error_msg: 'record locked',
                      client_operation_status: Juno::Client::OperationStatus::RECORD_LOCKED }.freeze
    NO_STORAGE_SERVER = { code: 12, error_msg: 'no active storage server',
                          client_operation_status: Juno::Client::OperationStatus::NO_STORAGE }.freeze
    SERVER_BUSY = { code: 14, error_msg: 'Server busy',
                    client_operation_status: Juno::Client::OperationStatus::INTERNAL_ERROR }.freeze
    VERSION_CONFLICT = { code: 19, error_msg:  'version conflict',
                         client_operation_status: Juno::Client::OperationStatus::CONDITION_VIOLATION }.freeze
    OP_STATUS_SS_READ_TTL_EXTENDERR = { code: 23, error_msg: 'Error extending TTL by SS',
                                        client_operation_status: Juno::Client::OperationStatus::INTERNAL_ERROR }.freeze
    COMMIT_FAILURE = { code: 25, error_msg: 'Commit Failure',
                       client_operation_status: Juno::Client::OperationStatus::INTERNAL_ERROR }.freeze
    INCONSISTENT_STATE = { code: 26, error_msg: 'Inconsistent State',
                           client_operation_status: Juno::Client::OperationStatus::SUCCESS }.freeze
    INTERNAL = { code: 255, error_msg: 'Internal error',
                 client_operation_status: Juno::Client::OperationStatus::INTERNAL_ERROR }.freeze

    @@status_code_map = nil

    def self.initialize_map
      @@status_code_map = {}

      constants.each do |const|
        const_obj = const_get(const)
        @@status_code_map[const_obj[:code].to_i] = const_obj
      end
    end

    def self.get(status_code)
      initialize_map if @@status_code_map.nil?
      return @@status_code_map[status_code] if @@status_code_map.key?(status_code.to_i)

      INTERNAL
    end
  end
end
