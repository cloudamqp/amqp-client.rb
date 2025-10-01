# frozen_string_literal: true

require_relative "../test_helper"
require "net/http"
require "json"
require "time"

# Specs that require @connection setup/teardown
class AMQPClientLifecycleTest < Minitest::Test
  def setup
    @connection = AMQP::Client.new("amqp://#{TEST_AMQP_HOST}").connect
  end

  def teardown
    @connection&.close
  end

  def test_it_can_connect
    assert @connection
  end

  def test_it_can_disconnect
    @connection.close

    assert @connection
  end

  def test_it_can_open_channel
    channel = @connection.channel

    assert channel
  end

  def test_it_can_close_channel
    channel = @connection.channel
    channel.close

    assert channel
  end

  def test_it_can_publish
    channel = @connection.channel
    channel.basic_publish "", exchange: "foo", routing_key: "bar"
  end

  def test_it_can_declare_exchange
    channel = @connection.channel
    channel.exchange_declare "foo", type: "fanout"
    q = channel.queue_declare ""
    channel.queue_bind q.queue_name, exchange: "foo", binding_key: ""
    channel.basic_publish "foo", exchange: "foo", routing_key: ""
    msg = channel.basic_get q.queue_name

    assert_equal "foo", msg.exchange_name
  ensure
    channel&.exchange_delete "foo"
  end

  def test_it_can_delete_exchange
    channel = @connection.channel
    channel.exchange_declare "foo", type: "fanout"
    q = channel.queue_declare ""
    channel.queue_bind q.queue_name, exchange: "foo", binding_key: ""
    channel.exchange_delete "foo"
    channel.basic_publish "foo", exchange: "foo", routing_key: ""
    assert_raises(AMQP::Client::Error::ChannelClosed) do
      channel.basic_get q.queue_name
    end
  end

  def test_it_can_bind_exchanges
    channel = @connection.channel
    channel.exchange_declare "foo", type: "fanout"
    channel.exchange_declare "bar", type: "fanout"
    q = channel.queue_declare ""
    channel.queue_bind q.queue_name, exchange: "bar", binding_key: ""
    channel.exchange_bind destination: "bar", source: "foo", binding_key: ""
    channel.basic_publish "foo", exchange: "foo", routing_key: ""
    msg = channel.basic_get q.queue_name

    assert_equal "foo", msg.exchange_name
    assert_equal "foo", msg.body
  ensure
    channel&.exchange_delete "foo"
    channel&.exchange_delete "bar"
  end

  def test_it_can_unbind_exchanges
    channel = @connection.channel
    channel.exchange_declare "foo", type: "fanout"
    channel.exchange_declare "bar", type: "fanout"
    q = channel.queue_declare ""
    channel.queue_bind q.queue_name, exchange: "bar", binding_key: ""
    channel.exchange_bind destination: "bar", source: "foo", binding_key: ""
    channel.exchange_unbind destination: "bar", source: "foo", binding_key: ""
    channel.basic_publish "foo", exchange: "foo", routing_key: ""
    msg = channel.basic_get q.queue_name

    assert_nil msg
  ensure
    channel&.exchange_delete "foo"
    channel&.exchange_delete "bar"
  end

  def test_it_can_declare_queue
    channel = @connection.channel
    resp = channel.queue_declare "foo", exclusive: true, auto_delete: true

    assert_equal "foo", resp.queue_name
  end

  def test_it_can_delete_queue
    channel = @connection.channel
    channel.queue_declare "foo"
    message_count = channel.queue_delete("foo")

    assert_equal 0, message_count
    exception = assert_raises(AMQP::Client::Error) do
      channel.queue_declare "foo", passive: true
    end
    assert_match(/404/, exception.message)
  end

  def test_it_can_get_from_empty_queue
    channel = @connection.channel
    q = channel.queue_declare ""
    msg = channel.basic_get q.queue_name

    assert_nil msg
  end

  def test_it_can_get_from_queue
    channel = @connection.channel
    channel.queue_declare "foo", exclusive: true
    channel.basic_publish "bar", exchange: "", routing_key: "foo"
    msg = channel.basic_get "foo"

    assert_equal "bar", msg.body
  end

  def test_it_can_get_from_transient_queue
    channel = @connection.channel
    q = channel.queue_declare ""
    channel.basic_publish "foobar", exchange: "", routing_key: q.queue_name
    msg = channel.basic_get q.queue_name

    assert_equal "foobar", msg.body
  end

  def test_it_can_consume
    channel = @connection.channel
    q = channel.queue_declare ""
    channel.basic_publish "foobar", exchange: "", routing_key: q.queue_name
    channel.basic_consume(q.queue_name, worker_threads: 0) do |msg|
      assert_equal "foobar", msg.body
      channel.basic_cancel msg.consumer_tag
    end
  end

  def test_it_handles_basic_cancel_from_server
    channel = @connection.channel
    q = channel.queue_declare ""
    cancelled = Queue.new

    channel.basic_consume(q.queue_name, on_cancel: ->(tag) { cancelled << tag }) do |msg|
      # noop
    end
    channel.queue_delete q.queue_name # This will send basic.cancel to the client

    refute_nil cancelled.pop(timeout: 1)
  end

  def test_it_can_bind_queue
    channel = @connection.channel
    q = channel.queue_declare ""
    channel.queue_bind q.queue_name, exchange: "amq.direct", binding_key: "bar"
    channel.basic_publish "foo", exchange: "amq.direct", routing_key: "bar"
    msg = channel.basic_get q.queue_name

    assert_equal "amq.direct", msg.exchange_name
    assert_equal "foo", msg.body
  end

  def test_it_can_unbind_queue
    channel = @connection.channel
    q = channel.queue_declare ""
    channel.queue_bind q.queue_name, exchange: "amq.direct", binding_key: "bar"
    channel.queue_unbind q.queue_name, exchange: "amq.direct", binding_key: "bar"
    channel.basic_publish "foo", exchange: "amq.direct", routing_key: "bar"
    msg = channel.basic_get q.queue_name

    assert_nil msg
  end

  def test_it_can_purge_queue
    channel = @connection.channel
    q = channel.queue_declare ""
    channel.queue_bind q.queue_name, exchange: "amq.direct", binding_key: "bar"
    channel.basic_publish "foo", exchange: "amq.direct", routing_key: "bar"
    channel.queue_purge q.queue_name
    msg = channel.basic_get q.queue_name

    assert_nil msg
  end

  def test_it_can_qos
    channel = @connection.channel
    q = channel.queue_declare ""
    channel.basic_qos(1)
    channel.basic_publish "foo", exchange: "", routing_key: q.queue_name
    channel.basic_publish "bar", exchange: "", routing_key: q.queue_name
    i = 0
    channel.basic_consume(q.queue_name, no_ack: false) do
      i += 1
    end
    sleep(0.1)

    assert_equal 1, i
  end

  def test_it_can_ack
    channel = @connection.channel
    q = channel.queue_declare ""
    channel.basic_qos 1
    channel.basic_publish "foo", exchange: "", routing_key: q.queue_name
    channel.basic_publish "bar", exchange: "", routing_key: q.queue_name
    i = 0
    channel.basic_consume(q.queue_name, no_ack: false) do |msg|
      channel.basic_ack msg.delivery_tag
      i += 1
    end
    sleep(0.1)

    assert_equal 2, i
  end

  def test_it_can_nack
    channel = @connection.channel
    q = channel.queue_declare ""
    channel.basic_qos 1
    channel.basic_publish "foo", exchange: "", routing_key: q.queue_name
    i = 0
    channel.basic_consume(q.queue_name, no_ack: false) do |msg|
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
    channel = @connection.channel
    q = channel.queue_declare ""
    channel.basic_qos 1
    channel.basic_publish "foo", exchange: "", routing_key: q.queue_name
    i = 0
    channel.basic_consume(q.queue_name, no_ack: false) do |msg|
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
    channel = @connection.channel
    q = channel.queue_declare ""
    channel.basic_publish "foo", exchange: "", routing_key: q.queue_name
    i = 0
    channel.basic_consume(q.queue_name, no_ack: false) do
      i += 1
    end
    sleep(0.1)

    assert_equal 1, i
    channel.basic_recover
    sleep(0.1)

    assert_equal 2, i
  end

  def test_it_can_return
    channel = @connection.channel
    msgs = Queue.new
    channel.on_return do |msg|
      msgs << msg
    end
    channel.basic_publish "foo", exchange: "amq.headers", routing_key: "bar", mandatory: true
    msg = msgs.pop

    assert_equal "bar", msg.routing_key
  end

  def test_it_can_select_confirm
    channel = @connection.channel
    channel.confirm_select
    channel.basic_publish "foo", exchange: "amq.direct", routing_key: "bar"
    success = channel.wait_for_confirms

    assert success, "Message was not confirmed by the server"
  end

  def test_wait_for_confirms_returns_false_on_nack
    queue_name = "ml-q"
    channel = @connection.channel
    channel.confirm_select
    channel.queue_declare queue_name, arguments: { "x-max-length": 1, "x-overflow": "reject-publish" }
    channel.basic_publish "foo", exchange: "", routing_key: queue_name
    channel.basic_publish "foo", exchange: "", routing_key: queue_name # Will be nack'ed due to max-length=1
    success = channel.wait_for_confirms

    refute_nil success, "wait_for_confirms did not return boolean"
    refute success, "Message was not nack'ed by the server"
  ensure
    channel&.queue_delete(queue_name)
  end

  def test_it_can_commit_tx
    channel = @connection.channel
    q = channel.queue_declare "foo", exclusive: true
    channel.tx_select
    channel.basic_publish "foo", exchange: "", routing_key: "foo"
    channel.tx_commit
    msg = channel.basic_get q.queue_name

    assert_equal "foo", msg.body
  end

  def test_it_can_rollback_tx
    channel = @connection.channel
    q = channel.queue_declare "foo", exclusive: true
    channel.tx_select
    channel.basic_publish "bar", exchange: "", routing_key: "foo"
    channel.tx_rollback
    msg = channel.basic_get q.queue_name

    assert_nil msg
    channel.basic_publish "bar", exchange: "", routing_key: "foo"
    channel.tx_commit
    msg = channel.basic_get q.queue_name

    assert_equal "bar", msg.body
  end

  def test_it_can_generate_tables
    channel = @connection.channel
    q = channel.queue_declare ""
    channel.queue_bind q.queue_name, exchange: "amq.headers", binding_key: "", arguments: { a: "b" }
    channel.basic_publish "foo", exchange: "amq.headers", routing_key: "bar", headers: { a: "b" }
    msg = channel.basic_get q.queue_name

    assert_equal "foo", msg.body
    assert_equal({ "a" => "b" }, msg.properties.headers)
  end

  def test_handle_connection_closed_by_server
    @connection.with_channel do |ch|
      exception = assert_raises(AMQP::Client::Error::ConnectionClosed, AMQP::Client::Error::ChannelClosed) do
        ch.exchange_declare("foobar", type: "faulty.exchange.type")
      end
      assert_match(/unknown exchange type/, exception.message)
    end
  end

  def test_it_can_consume_multiple_queues_on_one_channel
    msgs1 = Queue.new
    msgs2 = Queue.new
    channel = @connection.channel
    q1 = channel.queue_declare ""
    q2 = channel.queue_declare ""
    channel.basic_consume(q1.queue_name) do |msg|
      msgs1 << msg
    end
    channel.basic_consume(q2.queue_name) do |msg|
      msgs2 << msg
    end
    channel.basic_publish "foo", exchange: "", routing_key: q1.queue_name
    channel.basic_publish "bar", exchange: "", routing_key: q2.queue_name

    assert_equal "foo", msgs1.pop.body
    assert_equal "bar", msgs2.pop.body
  end

  def test_it_can_consume_multiple_queues_on_multiple_channel
    msgs1 = Queue.new
    msgs2 = Queue.new
    ch1 = @connection.channel
    ch2 = @connection.channel
    q1 = ch1.queue_declare ""
    q2 = ch2.queue_declare ""
    ch1.basic_consume(q1.queue_name) do |msg|
      msgs1 << msg
    end
    ch2.basic_consume(q2.queue_name) do |msg|
      msgs2 << msg
    end
    ch1.basic_publish "foo", exchange: "", routing_key: q1.queue_name
    ch2.basic_publish "bar", exchange: "", routing_key: q2.queue_name

    assert_equal "foo", msgs1.pop.body
    assert_equal "bar", msgs2.pop.body
  end

  def test_it_can_ack_a_lot_of_msgs
    msgs1 = Queue.new
    ch1 = @connection.channel
    q = ch1.queue_declare ""
    ch1.basic_qos(200)
    ch1.queue_bind(q.queue_name, exchange: "amq.topic", binding_key: "foo")
    ch1.basic_consume(q.queue_name, no_ack: false, worker_threads: 100) do |msg|
      msg.ack
      msgs1 << msg
    end

    ch2 = @connection.channel
    ch2.confirm_select
    10_000.times do |i|
      ch2.basic_publish "bar #{i + 1}", exchange: "amq.topic", routing_key: "foo"
    end
    ch2.wait_for_confirms

    10_000.times do
      assert_equal "foo", msgs1.pop.routing_key
    end
  end

  def test_it_can_be_blocked
    skip_if_no_sudo
    begin
      ch = @connection.channel
      system("sudo rabbitmqctl set_vm_memory_high_watermark 0.001")
      ch.basic_publish("body", exchange: "", routing_key: "q")
      sleep 0.01 # server blocks after first publish

      assert_predicate @connection, :blocked?
      system("sudo rabbitmqctl set_vm_memory_high_watermark 0.4")
      sleep 0.01 # server blocks after first publish

      refute_predicate @connection, :blocked?
    ensure
      system("sudo rabbitmqctl set_vm_memory_high_watermark 0.4")
    end
  end

  def test_it_will_publish_and_consume_properties
    props = AMQP::Client::Properties.new(
      delivery_mode: 2,
      content_type: "text/plain",
      content_encoding: "encoding",
      priority: 240,
      correlation_id: "corrid",
      reply_to: "replyto",
      expiration: "9999",
      message_id: "msgid",
      type: "type",
      user_id: "guest",
      app_id: "appId",
      timestamp: Time.parse("2017-02-03T10:15:30+01:00"),
      headers: {
        "hdr1" => "value1",
        "int" => 1,
        "b" => false,
        "f" => 0.1,
        "array" => [1, "b", true],
        "hash" => { "inner" => "val" },
        "big" => 2**32,
        "nil" => nil
      }
    )

    channel = @connection.channel
    q = channel.queue_declare ""
    channel.basic_publish_confirm "", exchange: "", routing_key: q.queue_name, **props
    msg = channel.basic_get q.queue_name

    assert_equal props, msg.properties
  end

  def test_blocked_handler
    skip_if_no_sudo
    begin
      q = Queue.new
      @connection.on_blocked do |reason|
        q << reason
      end
      @connection.on_unblocked do
        q << nil
      end
      system("sudo rabbitmqctl set_vm_memory_high_watermark 0.001")
      ch = @connection.channel
      ch.basic_publish("", exchange: "", routing_key: "")
      reason = q.pop

      assert_equal "low on memory", reason
      system("sudo rabbitmqctl set_vm_memory_high_watermark 0.4")
      unblocked = q.pop

      assert_nil unblocked
    ensure
      system("sudo rabbitmqctl set_vm_memory_high_watermark 0.4")
    end
  end

  def test_queue_pruge_returns_msg_count
    channel = @connection.channel
    q = channel.queue_declare ""
    3.times do
      channel.basic_publish_confirm "", exchange: "", routing_key: q.queue_name
    end
    msg_count = channel.queue_purge(q.queue_name)

    assert_equal 3, msg_count
  end

  def test_it_can_publish_with_confirm
    channel = @connection.channel
    q = channel.queue_declare ""
    10.times do
      channel.basic_publish_confirm "foo", exchange: "", routing_key: q.queue_name
    end
    q = channel.queue_declare q.queue_name, passive: true

    assert_equal 10, q.message_count
  end

  def test_it_can_update_secret
    @connection.update_secret "secret", reason: "testing"
  end

  def test_open_closing_channels_is_thread_safe
    abort_on_exception_prev = Thread.abort_on_exception
    Thread.abort_on_exception = false
    Array.new(50) do
      Thread.new do
        ch = nil
        40.times do
          ch&.close
          ch = @connection.channel
        end
      end
    end.each(&:join)
  ensure
    Thread.abort_on_exception = abort_on_exception_prev
  end
end
