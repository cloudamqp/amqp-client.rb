# frozen_string_literal: true

require_relative "../test_helper"

class RPCTest < Minitest::Test
  def test_that_rpc_server_responds_to_rpc_calls
    client = AMQP::Client.new("amqp://localhost").start
    client.rpc_server("rpc-test-method") do |request|
      "foo #{request}"
    end
    result = client.rpc_call("rpc-test-method", "bar")
    assert_equal "foo bar", result
  end

  def test_rpc_client_is_reusable
    client = AMQP::Client.new("amqp://localhost").start
    client.rpc_server("rpc-test-method") do |request|
      "foo #{request}"
    end

    rpc_client = client.rpc_client
    result = rpc_client.call("rpc-test-method", "bar")
    assert_equal "foo bar", result
    result = rpc_client.call("rpc-test-method", "foo")
    assert_equal "foo foo", result
  end
end
