# frozen_string_literal: true

require_relative "../test_helper"
require "net/http"
require "json"

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
    assert_raises(AMQP::Client::Error::ChannelClosed) do
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
    resp = channel.queue_declare "foo", exclusive: true
    assert_equal "foo", resp[:queue_name]
    connection.close
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
    q = channel.queue_declare ""
    msg = channel.basic_get q[:queue_name]
    assert_nil msg
    connection.close
  end

  def test_it_can_get_from_queue
    client = AMQP::Client.new("amqp://localhost")
    connection = client.connect
    channel = connection.channel
    channel.queue_declare "foo", exclusive: true
    channel.basic_publish "bar", "", "foo"
    msg = channel.basic_get "foo"
    assert_equal "bar", msg.body
    connection.close
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

  def test_it_can_qos
    client = AMQP::Client.new("amqp://localhost")
    connection = client.connect
    channel = connection.channel
    q = channel.queue_declare ""
    channel.basic_qos(1)
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
    channel.basic_qos 1
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
    channel.basic_qos 1
    channel.basic_publish "foo", "", q[:queue_name]
    i = 0
    channel.basic_consume(q[:queue_name], no_ack: false) do |msg|
      if i.zero?
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
    channel.basic_qos 1
    channel.basic_publish "foo", "", q[:queue_name]
    i = 0
    channel.basic_consume(q[:queue_name], no_ack: false) do |msg|
      if i.zero?
        channel.basic_reject msg.delivery_tag, requeue: true
      else
        channel.basic_ack msg.delivery_tag
      end
      i += 1
    end
    sleep(0.1)
    assert_equal 2, i
  end

  def test_it_can_recover
    client = AMQP::Client.new("amqp://localhost")
    connection = client.connect
    channel = connection.channel
    q = channel.queue_declare ""
    channel.basic_publish "foo", "", q[:queue_name]
    i = 0
    channel.basic_consume(q[:queue_name], no_ack: false) do
      i += 1
    end
    sleep(0.1)
    assert_equal 1, i
    channel.basic_recover
    sleep(0.1)
    assert_equal 2, i
  end

  def test_it_can_return
    client = AMQP::Client.new("amqp://localhost")
    connection = client.connect
    channel = connection.channel
    msgs = Queue.new
    channel.on_return do |msg|
      msgs << msg
    end
    channel.basic_publish "foo", "amq.headers", "bar", mandatory: true
    msg = msgs.pop
    assert_equal "bar", msg.routing_key
  end

  def test_it_can_select_confirm
    client = AMQP::Client.new("amqp://localhost")
    connection = client.connect
    channel = connection.channel
    channel.confirm_select
    channel.basic_publish "foo", "amq.direct", "bar"
    assert channel.wait_for_confirms
  end

  def test_it_can_commit_tx
    client = AMQP::Client.new("amqp://localhost")
    connection = client.connect
    channel = connection.channel
    q = channel.queue_declare "foo", exclusive: true
    channel.tx_select
    channel.basic_publish "foo", "", "foo"
    channel.tx_commit
    msg = channel.basic_get q[:queue_name]
    assert_equal "foo", msg.body
    connection.close
  end

  def test_it_can_rollback_tx
    client = AMQP::Client.new("amqp://localhost")
    connection = client.connect
    channel = connection.channel
    q = channel.queue_declare "foo", exclusive: true
    channel.tx_select
    channel.basic_publish "bar", "", "foo"
    channel.tx_rollback
    msg = channel.basic_get q[:queue_name]
    assert_nil msg
    channel.basic_publish "bar", "", "foo"
    channel.tx_commit
    msg = channel.basic_get q[:queue_name]
    assert_equal "bar", msg.body
    connection.close
  end

  def test_it_can_generate_tables
    client = AMQP::Client.new("amqp://localhost")
    connection = client.connect
    channel = connection.channel
    q = channel.queue_declare ""
    channel.queue_bind q[:queue_name], "amq.headers", "", arguments: { a: "b" }
    channel.basic_publish "foo", "amq.headers", "bar", headers: { a: "b" }
    msg = channel.basic_get q[:queue_name]
    assert_equal "foo", msg.body
    assert_equal({ "a" => "b" }, msg.properties[:headers])
  end

  def test_set_connection_name
    skip "slow, polls HTTP mgmt API"
    client = AMQP::Client.new("amqp://localhost", connection_name: "foobar")
    client.connect

    req = Net::HTTP::Get.new("/api/connections?columns=client_properties")
    req.basic_auth "guest", "guest"
    http = Net::HTTP.new("localhost", 15_672)
    connection_names = []
    100.times do
      sleep 0.1
      res = http.request(req)
      assert_equal Net::HTTPOK, res.class
      connection_names = JSON.parse(res.body).map! { |conn| conn.dig("client_properties", "connection_name") }
      break if connection_names.include? "foobar"
    end
    assert_includes connection_names, "foobar"
  end

  def test_handle_connection_closed_by_server
    conn = AMQP::Client.new("amqp://localhost").connect
    conn.with_channel do |ch|
      assert_raises(AMQP::Client::Error::ChannelClosed, /unknown exchange type/) do
        ch.exchange_declare("foobar", "faulty.exchange.type")
      end
    end
  end

  def test_it_can_consume_multiple_queues_on_one_channel
    msgs1 = Queue.new
    msgs2 = Queue.new
    connection = AMQP::Client.new("amqp://localhost").connect
    channel = connection.channel
    q1 = channel.queue_declare ""
    q2 = channel.queue_declare ""
    channel.basic_consume(q1[:queue_name]) do |msg|
      msgs1 << msg
    end
    channel.basic_consume(q2[:queue_name]) do |msg|
      msgs2 << msg
    end
    channel.basic_publish "foo", "", q1[:queue_name]
    channel.basic_publish "bar", "", q2[:queue_name]

    assert_equal "foo", msgs1.pop.body
    assert_equal "bar", msgs2.pop.body
    connection.close
  end

  def test_it_can_consume_multiple_queues_on_multiple_channel
    msgs1 = Queue.new
    msgs2 = Queue.new
    connection = AMQP::Client.new("amqp://localhost").connect
    ch1 = connection.channel
    ch2 = connection.channel
    q1 = ch1.queue_declare ""
    q2 = ch2.queue_declare ""
    ch1.basic_consume(q1[:queue_name]) do |msg|
      msgs1 << msg
    end
    ch2.basic_consume(q2[:queue_name]) do |msg|
      msgs2 << msg
    end
    ch1.basic_publish "foo", "", q1[:queue_name]
    ch2.basic_publish "bar", "", q2[:queue_name]

    assert_equal "foo", msgs1.pop.body
    assert_equal "bar", msgs2.pop.body
    connection.close
  end

  def test_it_can_ack_a_lot_of_msgs
    msgs1 = Queue.new
    connection = AMQP::Client.new("amqp://localhost").connect
    ch1 = connection.channel
    q = ch1.queue_declare ""
    ch1.basic_qos(200)
    ch1.queue_bind(q[:queue_name], "amq.topic", "foo")
    ch1.basic_consume(q[:queue_name], no_ack: false, worker_threads: 100) do |msg|
      msg.ack
      msgs1 << msg
    end

    ch2 = connection.channel
    ch2.confirm_select
    10_000.times do |i|
      ch2.basic_publish "bar #{i + 1}", "amq.topic", "foo"
    end

    10_000.times do
      assert_equal "foo", msgs1.pop.routing_key
    end
    assert ch2.wait_for_confirms
    connection.close
  end

  def test_it_can_set_channel_max
    connection = AMQP::Client.new("amqp://localhost", channel_max: 1).connect
    assert connection.channel
    assert_raises(AMQP::Client::Error) do
      connection.channel
    end
  ensure
    connection&.close
  end

  def test_it_can_be_blocked
    skip "requires sudo"
    connection = AMQP::Client.new("amqp://localhost", channel_max: 1).connect
    ch = connection.channel
    system("sudo rabbitmqctl set_vm_memory_high_watermark 0.001")
    t = Thread.new do
      ch.basic_publish("body", "", "q")
      sleep 0.01 # server blocks after first publish
      ch.basic_publish("body", "", "q")
    end
    assert_nil t.join(0.1) # make sure the thread is blocked
    system("sudo rabbitmqctl set_vm_memory_high_watermark 0.4")
    assert t.join
    assert_equal false, t.status # status is false when terminated normal
  ensure
    connection&.close
  end
end
