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

  def test_it_can_declare_exchange
    client = AMQP::Client.new("amqp://localhost")
    connection = client.connect
    channel = connection.channel
    channel.exchange_declare "foo", "fanout"
    q = channel.queue_declare ""
    channel.queue_bind q[:queue_name], "foo", ""
    channel.basic_publish "foo", "foo", ""
    msg = channel.basic_get q[:queue_name]
    assert_equal "foo", msg.exchange_name
  end

  def test_it_can_delete_exchange
    client = AMQP::Client.new("amqp://localhost")
    connection = client.connect
    channel = connection.channel
    channel.exchange_declare "foo", "fanout"
    q = channel.queue_declare ""
    channel.queue_bind q[:queue_name], "foo", ""
    channel.exchange_delete "foo"
    channel.basic_publish "foo", "foo", ""
    assert_raises(AMQP::Client::ChannelClosedError) do
      channel.basic_get q[:queue_name]
    end
  end

  def test_it_can_bind_exchanges
    client = AMQP::Client.new("amqp://localhost")
    connection = client.connect
    channel = connection.channel
    channel.exchange_declare "foo", "fanout"
    channel.exchange_declare "bar", "fanout"
    q = channel.queue_declare ""
    channel.queue_bind q[:queue_name], "bar", ""
    channel.exchange_bind "bar", "foo", ""
    channel.basic_publish "foo", "foo", ""
    msg = channel.basic_get q[:queue_name]
    assert_equal "foo", msg.exchange_name
    assert_equal "foo", msg.body
  end

  def test_it_can_unbind_exchanges
    client = AMQP::Client.new("amqp://localhost")
    connection = client.connect
    channel = connection.channel
    channel.exchange_declare "foo", "fanout"
    channel.exchange_declare "bar", "fanout"
    q = channel.queue_declare ""
    channel.queue_bind q[:queue_name], "bar", ""
    channel.exchange_bind "bar", "foo", ""
    channel.exchange_unbind "bar", "foo", ""
    channel.basic_publish "foo", "foo", ""
    msg = channel.basic_get q[:queue_name]
    assert_nil msg
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

  def test_it_can_bind_queue
    client = AMQP::Client.new("amqp://localhost")
    connection = client.connect
    channel = connection.channel
    q = channel.queue_declare ""
    channel.queue_bind q[:queue_name], "amq.direct", "bar"
    channel.basic_publish "foo", "amq.direct", "bar"
    msg = channel.basic_get q[:queue_name]
    assert_equal "amq.direct", msg.exchange_name
    assert_equal "foo", msg.body
  end

  def test_it_can_unbind_queue
    client = AMQP::Client.new("amqp://localhost")
    connection = client.connect
    channel = connection.channel
    q = channel.queue_declare ""
    channel.queue_bind q[:queue_name], "amq.direct", "bar"
    channel.queue_unbind q[:queue_name], "amq.direct", "bar"
    channel.basic_publish "foo", "amq.direct", "bar"
    msg = channel.basic_get q[:queue_name]
    assert_nil msg
  end

  def test_it_can_purge_queue
    client = AMQP::Client.new("amqp://localhost")
    connection = client.connect
    channel = connection.channel
    q = channel.queue_declare ""
    channel.queue_bind q[:queue_name], "amq.direct", "bar"
    channel.basic_publish "foo", "amq.direct", "bar"
    channel.queue_purge q[:queue_name]
    msg = channel.basic_get q[:queue_name]
    assert_nil msg
  end

  def test_it_can_select_confirm
    client = AMQP::Client.new("amqp://localhost")
    connection = client.connect
    channel = connection.channel
    channel.confirm_select
    id = channel.basic_publish "foo", "amq.direct", "bar"
    assert channel.wait_for_confirm id
  end

  def test_it_can_qos
    client = AMQP::Client.new("amqp://localhost")
    connection = client.connect
    channel = connection.channel
    q = channel.queue_declare ""
    channel.basic_qos 0, 1
    channel.basic_publish "foo", "", q[:queue_name]
    channel.basic_publish "bar", "", q[:queue_name]
    i = 0
    channel.basic_consume(q[:queue_name], no_ack: false) do
      i += 1
    end
    sleep(0.1)
    assert_equal 1, i
  end

  def test_it_can_ack
    client = AMQP::Client.new("amqp://localhost")
    connection = client.connect
    channel = connection.channel
    q = channel.queue_declare ""
    channel.basic_qos 0, 1
    channel.basic_publish "foo", "", q[:queue_name]
    channel.basic_publish "bar", "", q[:queue_name]
    i = 0
    channel.basic_consume(q[:queue_name], no_ack: false) do |msg|
      channel.basic_ack msg.delivery_tag
      i += 1
    end
    sleep(0.1)
    assert_equal 2, i
  end

  def test_it_can_nack
    client = AMQP::Client.new("amqp://localhost")
    connection = client.connect
    channel = connection.channel
    q = channel.queue_declare ""
    channel.basic_qos 0, 1
    channel.basic_publish "foo", "", q[:queue_name]
    i = 0
    channel.basic_consume(q[:queue_name], no_ack: false) do |msg|
      if i == 0
        channel.basic_nack msg.delivery_tag, requeue: true
      else
        channel.basic_ack msg.delivery_tag
      end
      i += 1
    end
    sleep(0.1)
    assert_equal 2, i
  end

  def test_it_can_reject
    client = AMQP::Client.new("amqp://localhost")
    connection = client.connect
    channel = connection.channel
    q = channel.queue_declare ""
    channel.basic_qos 0, 1
    channel.basic_publish "foo", "", q[:queue_name]
    i = 0
    channel.basic_consume(q[:queue_name], no_ack: false) do |msg|
      if i == 0
        channel.basic_reject msg.delivery_tag, requeue: true
      else
        channel.basic_ack msg.delivery_tag
      end
      i += 1
    end
    sleep(0.1)
    assert_equal 2, i
  end

  def test_it_can_generate_tables
    client = AMQP::Client.new("amqp://localhost")
    connection = client.connect
    channel = connection.channel
    q = channel.queue_declare ""
    channel.queue_bind q[:queue_name], "amq.headers", "", { a: "b" }
    channel.basic_publish "foo", "amq.headers", "bar", headers: { a: "b" }
    msg = channel.basic_get q[:queue_name]
    assert_equal "foo", msg.body
    assert_equal({ "a" => "b" }, msg.properties[:headers])
  end
end
