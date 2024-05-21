# frozen_string_literal: true

# Top module for juno client
module Juno
  module Client
    # Constant for JunoRequest operation type
    class OperationType
      Nop = {
        code: 0,
        str: 'NOP'
      }.freeze
      Create = {
        code: 1,
        str: 'CREATE'
      }.freeze
      Get = {
        code: 2,
        str: 'GET'
      }.freeze
      Update = {
        code: 3,
        str: 'UPDATE'
      }.freeze
      Set = {
        code: 4,
        str: 'SET'
      }.freeze
      CompareAndSet = {
        code: 5,
        str: 'COMPAREANDSET'
      }.freeze
      Destroy = {
        code: 6,
        str: 'DESTROY'
      }.freeze
    end
  end
end
