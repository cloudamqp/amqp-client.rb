# frozen_string_literal: true

require "test_helper"

class AMQPClientTest < Minitest::Test
  def test_that_it_has_a_version_number
    refute_nil ::AMQP::Client::VERSION
  end

  def test_it_can_connect
    client = AMQP::Client.new("amqp://localhost")
    connection = client.connect
    assert connection
  end

  def test_it_can_disconnect
    client = AMQP::Client.new("amqp://localhost")
    connection = client.connect
    connection.close
    assert connection
  end
end
