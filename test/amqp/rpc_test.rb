# frozen_string_literal: true

require_relative "../test_helper"

class RPCTest < Minitest::Test
  def setup
    @client = AMQP::Client.new("amqp://localhost")
    @client.codec_registry.enable_builtin_codecs
    @client.start
  end

  def teardown
    @client&.stop
  end

  def test_that_rpc_server_responds_to_rpc_calls
    @client.rpc_server("rpc-test-method", auto_delete: true) do |request|
      "foo #{request}"
    end
    result = @client.rpc_call("rpc-test-method", "bar")

    assert_equal "foo bar", result
  end

  def test_rpc_client_is_reusable
    @client.rpc_server("rpc-test-method", auto_delete: true) do |request|
      "foo #{request}"
    end

    rpc_client = @client.rpc_client
    result = rpc_client.call("rpc-test-method", "bar")

    assert_equal "foo bar", result
    result = rpc_client.call("rpc-test-method", "foo")

    assert_equal "foo foo", result
  end

  def test_rpc_call_times_out
    assert_raises(Timeout::Error) do
      @client.rpc_call("rpc-test-method", "bar", timeout: 0.01)
    end
  end

  def test_rpc_client_call_times_out
    rpc_client = @client.rpc_client
    assert_raises(Timeout::Error) do
      rpc_client.call("rpc-test-method", "bar", timeout: 0.01)
    end
  ensure
    rpc_client.close
  end
end
