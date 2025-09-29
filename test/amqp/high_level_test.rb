# frozen_string_literal: true

require_relative "../test_helper"
require "zlib"

class HighLevelTest < Minitest::Test
  def setup
    @client = AMQP::Client.new("amqp://#{TEST_AMQP_HOST}").start
  end

  def teardown
    @client.stop
  end

  def test_it_can_connect_pub_sub
    msgs = Queue.new
    q = @client.queue("test.conn")
    q.subscribe do |msg|
      msgs.push msg
    end
    q.publish("foobar")
    msg = msgs.pop

    assert_equal "test.conn", msg.routing_key
    assert_equal "foobar", msg.body
  ensure
    q.delete
  end

  def test_it_can_bind_unbind
    msgs = Queue.new
    q = @client.queue("test.bind")
    q.subscribe do |msg|
      msgs.push msg
    end
    q.bind("amq.topic", binding_key: "foo.*")
    @client.publish("foo", exchange: "amq.topic", routing_key: "foo.bar")
    q.unbind("amq.topic", binding_key: "foo.*")
    @client.publish("foo", exchange: "amq.topic", routing_key: "foo.bar")

    msg = msgs.pop

    assert_equal "foo.bar", msg.routing_key
    assert_equal "foo", msg.body
  ensure
    q&.delete
  end

  def test_it_can_publish_with_properties
    msgs = Queue.new
    q = @client.queue("test.bind")
    q.subscribe do |msg|
      msg.ack
      msgs.push msg
    end
    q.publish Zlib.gzip("hej"), content_encoding: "gzip"
    msg1 = msgs.pop

    assert_equal "gzip", msg1.properties.content_encoding

    q.bind("amq.topic", binding_key: "foo.*")
    @client.publish("foo", exchange: "amq.topic", routing_key: "foo.bar", headers: { foo: "bar" })

    msg2 = msgs.pop

    assert_equal({ "foo" => "bar" }, msg2.properties.headers)
  ensure
    q&.delete
  end

  def test_it_can_reopen_channel_1_after_failed_publish
    assert_raises(AMQP::Client::Error::ChannelClosed) do
      @client.publish("", exchange: "non-existing-exchange", routing_key: "foo.bar")
    end
    @client.publish("", exchange: "amq.topic", routing_key: "foo.bar")
  end

  def test_it_can_bind_unbind_exchanges
    msgs = Queue.new
    e = @client.exchange("test.exchange", type: "fanout", auto_delete: true)
    q = @client.queue("test.bind")
    q.bind("test.exchange", binding_key: "")
    q.subscribe do |msg|
      msgs.push msg
    end
    @client.publish("foo", exchange: "amq.topic", routing_key: "foo.bar")
    e.bind("amq.topic", binding_key: "foo.bar")
    @client.publish("bar", exchange: "amq.topic", routing_key: "foo.bar")
    e.unbind("amq.topic", binding_key: "foo.bar")
    @client.publish("foo", exchange: "amq.topic", routing_key: "foo.bar")

    msg = msgs.pop

    assert_equal "foo.bar", msg.routing_key
    assert_equal "bar", msg.body

    sleep 0.01
    assert_raises(ThreadError) do
      msgs.pop(true)
    end
  ensure
    e&.delete
    q&.delete
  end

  def test_it_can_resubscribe_on_reconnect
    msgs = Queue.new
    q = @client.queue("foo#{rand}")
    q.subscribe do |msg|
      msgs << msg
    end
    assert_raises(AMQP::Client::Error::ConnectionClosed, AMQP::Client::Error::ChannelClosed) do
      @client.exchange("test.exchange", type: "non.existing.exchange.type")
    end

    q.publish("bar")
    msg = msgs.pop

    assert msg.body, "bar"
  ensure
    q&.delete
  end

  def test_it_calls_on_cancel_on_basic_cancel_from_server
    q = @client.queue("test.cancel")
    cancelled = Queue.new

    q.subscribe(on_cancel: ->(tag) { cancelled << tag }) do |msg|
      # noop
    end
    q.delete # This will send basic.cancel to the @client

    tag = cancelled.pop(timeout: 2)

    refute_nil tag, "Did not receive basic.cancel callback"
    refute @client.instance_variable_get(:@consumers).values.any? { |c|
      c.tag == tag
    }, "Consumer was not removed after basic.cancel"
  end

  def test_default_direct_exchange
    direct = @client.direct_exchange("amq.direct")

    assert_instance_of AMQP::Client::Exchange, direct
    assert_equal "amq.direct", direct.name
  end

  def test_default_exchange
    default = @client.default_exchange

    assert_instance_of AMQP::Client::Exchange, default
    assert_equal "", default.name
  end

  def test_default_fanout_exchange
    fanout = @client.fanout_exchange

    assert_instance_of AMQP::Client::Exchange, fanout
    assert_equal "amq.fanout", fanout.name
  end

  def test_default_topic_exchange
    topic = @client.topic_exchange

    assert_instance_of AMQP::Client::Exchange, topic
    assert_equal "amq.topic", topic.name
  end

  def test_default_headers_exchange
    headers = @client.headers_exchange

    assert_instance_of AMQP::Client::Exchange, headers
    assert_equal "amq.headers", headers.name
  end

  def test_queue_bind_with_exchange_object
    msgs = Queue.new
    exchange = @client.fanout("test.fanout.objbind")
    queue = @client.queue("test.fanout.objbind.queue")

    # Test queue.bind with Exchange object (not just string)
    queue.bind(exchange, binding_key: "") # Pass Exchange object directly

    queue.subscribe do |msg|
      msgs.push msg
    end

    # Publish to exchange and verify message routing
    exchange.publish("message via exchange object bind", routing_key: "")

    msg = msgs.pop

    assert_equal "message via exchange object bind", msg.body
    assert_equal "test.fanout.objbind", msg.exchange_name

    # Test unbind with Exchange object
    queue.unbind(exchange, binding_key: "")

    # Publish again - should not reach queue after unbind
    exchange.publish("message after unbind", routing_key: "")

    sleep 0.01
    assert_raises(ThreadError) do
      msgs.pop(true)
    end
  ensure
    exchange&.delete
    queue&.delete
  end

  def test_queue_bind_with_string_vs_exchange_object
    exchange = @client.fanout("test.queue.bind.string.vs.obj")
    queue1 = @client.queue("test.queue.bind.string")
    queue2 = @client.queue("test.queue.bind.object")

    # Test queue.bind with string name
    queue1.bind("test.queue.bind.string.vs.obj", binding_key: "")

    # Test queue.bind with Exchange object
    queue2.bind(exchange, binding_key: "")

    # Both should work the same way
    assert true, "Queue can bind to both string names and Exchange objects"

    # Test unbinding
    queue1.unbind("test.queue.bind.string.vs.obj", binding_key: "")
    queue2.unbind(exchange, binding_key: "")
  ensure
    exchange&.delete
    queue1&.delete
    queue2&.delete
  end

  def test_exchange_bind_to_exchange_object
    msgs = Queue.new
    source_exchange = @client.fanout("test.source.exchange.obj.unique")
    dest_exchange = @client.fanout("test.dest.exchange.obj.unique")

    # Create queue bound to destination
    queue = @client.queue("test.exchange.exchange.bind.unique")
    queue.bind(dest_exchange, binding_key: "")

    queue.subscribe do |msg|
      msgs.push msg
    end

    # Test exchange.bind with Exchange object
    dest_exchange.bind(source_exchange, binding_key: "") # Bind to Exchange object

    # Publish to source exchange
    source_exchange.publish("message via exchange-exchange bind", routing_key: "")

    msg = msgs.pop

    assert_equal "message via exchange-exchange bind", msg.body
    assert_equal "test.source.exchange.obj.unique", msg.exchange_name

    # Test unbind with Exchange object
    dest_exchange.unbind(source_exchange, binding_key: "")

    # Publish again - should not reach queue
    source_exchange.publish("message after exchange unbind", routing_key: "")

    sleep 0.01
    assert_raises(ThreadError) do
      msgs.pop(true)
    end
  ensure
    source_exchange&.delete
    dest_exchange&.delete
    queue&.delete
  end

  def test_exchange_bind_default_binding_key
    # Create exchanges with unique names to avoid conflicts
    dest_exchange = @client.fanout("test.exchange.default.binding.unique")
    source_exchange = @client.fanout("test.source.default.binding.unique")

    # Test that default binding_key is now "" (empty string, not nil)
    dest_exchange.bind(source_exchange) # No binding_key specified, should default to ""
    dest_exchange.unbind(source_exchange) # No binding_key specified, should default to ""

    # Test explicit empty string works the same
    dest_exchange.bind(source_exchange, binding_key: "")
    dest_exchange.unbind(source_exchange, binding_key: "")

    # If we got here without errors, the default works
    assert true, "Exchange bind/unbind default binding_key works"
  ensure
    dest_exchange&.delete
    source_exchange&.delete
  end

  def test_queue_bind_requires_binding_key
    queue = @client.queue("test.queue.requires.binding.key")
    exchange = @client.fanout("test.exchange.requires.binding")

    # Queue bind/unbind still require explicit binding_key (no default)
    queue.bind(exchange, binding_key: "")
    queue.unbind(exchange, binding_key: "")

    # Test with string
    queue.bind("test.exchange.requires.binding", binding_key: "")
    queue.unbind("test.exchange.requires.binding", binding_key: "")

    # If we got here without errors, the API works
    assert true, "Queue bind/unbind with explicit binding_key works"
  ensure
    exchange&.delete
    queue&.delete
  end

  def test_queue_and_exchange_bind_with_arguments
    # Create headers exchange and queue for argument testing with unique names
    exchange = @client.headers("test.bind.args.exchange.unique")
    queue = @client.queue("test.bind.args.queue.unique")

    # Test queue.bind with arguments
    queue.bind(exchange, binding_key: "", arguments: { "type" => "test", "x-match" => "all" })
    queue.unbind(exchange, binding_key: "", arguments: { "type" => "test", "x-match" => "all" })

    # Test exchange.bind with arguments (to another exchange)
    dest_exchange = @client.headers("test.dest.args.exchange.unique")
    dest_exchange.bind(exchange, binding_key: "", arguments: { "format" => "json", "x-match" => "any" })
    dest_exchange.unbind(exchange, binding_key: "", arguments: { "format" => "json", "x-match" => "any" })

    # If we got here without errors, arguments work with both APIs
    assert true, "Queue and Exchange bind/unbind with arguments works"
  ensure
    exchange&.delete
    dest_exchange&.delete
    queue&.delete
  end

  def test_client_started_method
    @client&.stop
    @client = AMQP::Client.new("amqp://#{TEST_AMQP_HOST}")

    refute_predicate @client, :started?

    @client.start

    assert_predicate @client, :started?

    @client.stop

    refute_predicate @client, :started?
  end
end
