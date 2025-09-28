# frozen_string_literal: true

require_relative "../test_helper"

class RPCTest < Minitest::Test
  def setup
    @client = AMQP::Client.new("amqp://localhost").start
  end

  def teardown
    @client&.stop
  end

  def test_that_rpc_server_responds_to_rpc_calls
    @client.rpc_server(queue: "rpc-test-method", auto_delete: true) do |request|
      "foo #{request}"
    end
    result = @client.rpc_call("bar", queue: "rpc-test-method")

    assert_equal "foo bar", result
  end

  def test_rpc_client_is_reusable
    @client.rpc_server(queue: "rpc-test-method", auto_delete: true) do |request|
      "foo #{request}"
    end

    rpc_client = @client.rpc_client
    result = rpc_client.call("bar", queue: "rpc-test-method")

    assert_equal "foo bar", result
    result = rpc_client.call("foo", queue: "rpc-test-method")

    assert_equal "foo foo", result
  end
end
