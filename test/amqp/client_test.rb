# frozen_string_literal: true

require_relative "../test_helper"

class AMQPClientTest < Minitest::Test
  def test_that_it_has_a_version_number
    refute_nil ::AMQP::Client::VERSION
  end

  def test_it_can_connect
    client = AMQP::Client.new("amqp://localhost")
    connection = client.connect
    assert connection
  end

  def test_it_raises_on_bad_credentials
    client = AMQP::Client.new("amqp://guest1:guest2@localhost")
    assert_raises(AMQP::Client::Error) do
      client.connect
    end
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

  def test_it_can_delete_queue
    client = AMQP::Client.new("amqp://localhost")
    connection = client.connect
    channel = connection.channel
    channel.queue_declare "foo"
    message_count = channel.queue_delete("foo")
    assert_equal 0, message_count
    assert_raises(AMQP::Client::Error, /exists/) do
      channel.queue_declare "foo", passive: true
    end
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
    channel.basic_publish "bar", "", "foo"
    msg = channel.basic_get "foo"
    assert_equal "bar", msg.body
  end

  def test_it_can_get_from_transiet_queue
    client = AMQP::Client.new("amqp://localhost")
    connection = client.connect
    channel = connection.channel
    q = channel.queue_declare ""
    channel.basic_publish "foobar", "", q[:queue_name]
    msg = channel.basic_get q[:queue_name]
    assert_equal "foobar", msg.body
  end

  def test_it_can_consume
    client = AMQP::Client.new("amqp://localhost")
    connection = client.connect
    channel = connection.channel
    q = channel.queue_declare ""
    channel.basic_publish "foobar", "", q[:queue_name]
    channel.basic_consume(q[:queue_name]) do |msg|
      assert_equal "foobar", msg.body
      channel.basic_cancel msg.consumer_tag
    end
  end
end
