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

  def test_it_can_open_channel
    client = AMQP::Client.new("amqp://localhost")
    connection = client.connect
    channel = connection.channel
    assert channel
  end

  def test_it_can_close_channel
    client = AMQP::Client.new("amqp://localhost")
    connection = client.connect
    channel = connection.channel
    channel.close
    assert channel
  end
end
