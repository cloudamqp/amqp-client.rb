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

  def test_it_can_publish
    client = AMQP::Client.new("amqp://localhost")
    connection = client.connect
    channel = connection.channel
    channel.basic_publish "", "foo", "bar"
  end

  def test_it_can_declare_queue
    client = AMQP::Client.new("amqp://localhost")
    connection = client.connect
    channel = connection.channel
    resp = channel.queue_declare "foo"
    assert_equal "foo", resp[:queue_name]
  end

  def test_it_can_get_from_empty_queue
    client = AMQP::Client.new("amqp://localhost")
    connection = client.connect
    channel = connection.channel
    channel.queue_declare "e"
    msg = channel.basic_get "e"
    assert_nil msg
  end

  def test_it_can_get_from_queue
    client = AMQP::Client.new("amqp://localhost")
    connection = client.connect
    channel = connection.channel
    channel.queue_declare "foo"
    channel.basic_publish "", "foo", "bar"
    msg = channel.basic_get "foo"
    assert_equal "bar", msg.body
  end
end
