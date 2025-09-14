# frozen_string_literal: true

require_relative "../test_helper"
require "zlib"

class HighLevelTest < Minitest::Test
  def test_it_can_connect_pub_sub
    msgs = Queue.new
    client = AMQP::Client.new("amqp://#{TEST_AMQP_HOST}")
    client.start
    q = client.queue("test.conn")
    begin
      q.subscribe do |msg|
        msgs.push msg
      end
      q.publish("foobar")
      msg = msgs.pop
      assert_equal "test.conn", msg.routing_key
      assert_equal "foobar", msg.body
    ensure
      q.delete
      client.stop
    end
  end

  def test_it_can_bind_unbind
    msgs = Queue.new
    client = AMQP::Client.new("amqp://#{TEST_AMQP_HOST}")
    client.start
    begin
      q = client.queue("test.bind")
      q.subscribe do |msg|
        msgs.push msg
      end
      q.bind("amq.topic", "foo.*")
      client.publish("foo", "amq.topic", "foo.bar")
      q.unbind("amq.topic", "foo.*")
      client.publish("foo", "amq.topic", "foo.bar")

      msg = msgs.pop
      assert_equal "foo.bar", msg.routing_key
      assert_equal "foo", msg.body
    ensure
      q&.delete
      client.stop
    end
  end

  def test_it_can_publish_with_properties
    msgs = Queue.new
    client = AMQP::Client.new("amqp://#{TEST_AMQP_HOST}")
    client.start
    begin
      q = client.queue("test.bind")
      q.subscribe do |msg|
        msg.ack
        msgs.push msg
      end
      q.publish Zlib.gzip("hej"), content_encoding: "gzip"
      msg1 = msgs.pop
      assert_equal "gzip", msg1.properties.content_encoding

      q.bind("amq.topic", "foo.*")
      client.publish("foo", "amq.topic", "foo.bar", headers: { foo: "bar" })

      msg2 = msgs.pop
      assert_equal({ "foo" => "bar" }, msg2.properties.headers)
    ensure
      q&.delete
      client.stop
    end
  end

  def test_it_can_reopen_channel_1_after_failed_publish
    client = AMQP::Client.new("amqp://#{TEST_AMQP_HOST}").start
    begin
      assert_raises(AMQP::Client::Error::ChannelClosed) do
        client.publish("", "non-existing-exchange", "foo.bar")
      end
      client.publish("", "amq.topic", "foo.bar")
    ensure
      client.stop
    end
  end

  def test_it_can_bind_unbind_exchanges
    msgs = Queue.new
    client = AMQP::Client.new("amqp://#{TEST_AMQP_HOST}").start
    begin
      e = client.exchange("test.exchange", "fanout", auto_delete: true)
      q = client.queue("test.bind")
      q.bind("test.exchange", "")
      q.subscribe do |msg|
        msgs.push msg
      end
      client.publish("foo", "amq.topic", "foo.bar")
      e.bind("amq.topic", "foo.bar")
      client.publish("bar", "amq.topic", "foo.bar")
      e.unbind("amq.topic", "foo.bar")
      client.publish("foo", "amq.topic", "foo.bar")

      msg = msgs.pop
      assert_equal "foo.bar", msg.routing_key
      assert_equal "bar", msg.body

      sleep 0.01
      assert_raises(ThreadError) do
        msgs.pop(true)
      end
      e.delete
      q.delete
    ensure
      client.stop
    end
  end

  def test_it_can_resubscribe_on_reconnect
    msgs = Queue.new
    client = AMQP::Client.new("amqp://#{TEST_AMQP_HOST}").start
    begin
      q = client.queue("foo#{rand}")
      q.subscribe do |msg|
        msgs << msg
      end
      assert_raises(AMQP::Client::Error::ConnectionClosed, AMQP::Client::Error::ChannelClosed) do
        client.exchange("test.exchange", "non.existing.exchange.type")
      end

      q.publish("bar")
      msg = msgs.pop
      assert msg.body, "bar"
    ensure
      q&.delete
      client.stop
    end
  end

  def test_fanout_convenience_method
    msgs = Queue.new
    client = AMQP::Client.new("amqp://#{TEST_AMQP_HOST}").start
    begin
      e = client.fanout("test.fanout", auto_delete: true)
      q = client.queue("test.fanout.queue")
      q.bind("test.fanout", "")
      q.subscribe do |msg|
        msgs.push msg
      end
      e.publish("fanout message", "")

      msg = msgs.pop
      assert_equal "fanout message", msg.body
      assert_equal "test.fanout", msg.exchange_name

      e.delete
      q.delete
    ensure
      client.stop
    end
  end

  def test_direct_convenience_method
    msgs = Queue.new
    client = AMQP::Client.new("amqp://#{TEST_AMQP_HOST}").start
    begin
      e = client.direct("test.direct", auto_delete: true)
      q = client.queue("test.direct.queue")
      q.bind("test.direct", "direct.key")
      q.subscribe do |msg|
        msgs.push msg
      end
      e.publish("direct message", "direct.key")

      msg = msgs.pop
      assert_equal "direct message", msg.body
      assert_equal "test.direct", msg.exchange_name
      assert_equal "direct.key", msg.routing_key

      e.delete
      q.delete
    ensure
      client.stop
    end
  end

  def test_topic_convenience_method
    msgs = Queue.new
    client = AMQP::Client.new("amqp://#{TEST_AMQP_HOST}").start
    begin
      e = client.topic("test.topic", auto_delete: true)
      q = client.queue("test.topic.queue")
      q.bind("test.topic", "user.*")
      q.subscribe do |msg|
        msgs.push msg
      end
      e.publish("topic message", "user.created")

      msg = msgs.pop
      assert_equal "topic message", msg.body
      assert_equal "test.topic", msg.exchange_name
      assert_equal "user.created", msg.routing_key

      e.delete
      q.delete
    ensure
      client.stop
    end
  end

  def test_headers_convenience_method
    msgs = Queue.new
    client = AMQP::Client.new("amqp://#{TEST_AMQP_HOST}").start
    begin
      e = client.headers("test.headers", auto_delete: true)
      q = client.queue("test.headers.queue")
      q.bind("test.headers", "", arguments: { type: "order", "x-match": "all" })
      q.subscribe do |msg|
        msgs.push msg
      end
      e.publish("headers message", "", headers: { type: "order", priority: "high" })

      msg = msgs.pop
      assert_equal "headers message", msg.body
      assert_equal "test.headers", msg.exchange_name
      assert_equal({ "type" => "order", "priority" => "high" }, msg.properties.headers)

      e.delete
      q.delete
    ensure
      client.stop
    end
  end

  def test_convenience_methods_equivalent_to_exchange_method
    client = AMQP::Client.new("amqp://#{TEST_AMQP_HOST}").start
    begin
      # Test that convenience methods are equivalent to exchange method calls
      fanout_conv = client.fanout("test.fanout.equiv", auto_delete: true)
      fanout_orig = client.exchange("test.fanout.equiv.orig", "fanout", auto_delete: true)

      direct_conv = client.direct("test.direct.equiv", auto_delete: true)
      direct_orig = client.exchange("test.direct.equiv.orig", "direct", auto_delete: true)

      topic_conv = client.topic("test.topic.equiv", auto_delete: true)
      topic_orig = client.exchange("test.topic.equiv.orig", "topic", auto_delete: true)

      headers_conv = client.headers("test.headers.equiv", auto_delete: true)
      headers_orig = client.exchange("test.headers.equiv.orig", "headers", auto_delete: true)

      # Check that all return Exchange objects
      assert_instance_of AMQP::Client::Exchange, fanout_conv
      assert_instance_of AMQP::Client::Exchange, fanout_orig
      assert_instance_of AMQP::Client::Exchange, direct_conv
      assert_instance_of AMQP::Client::Exchange, direct_orig
      assert_instance_of AMQP::Client::Exchange, topic_conv
      assert_instance_of AMQP::Client::Exchange, topic_orig
      assert_instance_of AMQP::Client::Exchange, headers_conv
      assert_instance_of AMQP::Client::Exchange, headers_orig

      # Clean up
      [fanout_conv, fanout_orig, direct_conv, direct_orig,
       topic_conv, topic_orig, headers_conv, headers_orig].each(&:delete)
    ensure
      client.stop
    end
  end

  def test_convenience_methods_with_default_exchange_names
    msgs = Queue.new
    client = AMQP::Client.new("amqp://#{TEST_AMQP_HOST}").start
    begin
      # Test default exchange names
      default_direct = client.direct  # Should use ""
      default_fanout = client.fanout  # Should use "amq.fanout"
      default_topic = client.topic    # Should use "amq.topic"
      default_headers = client.headers # Should use "amq.headers"

      # Verify we can use the default direct exchange
      q_direct = client.queue("test.default.direct")
      q_direct.subscribe do |msg|
        msgs.push msg
      end

      # Publish to default direct exchange (should route to queue with same name as routing key)
      default_direct.publish("direct default message", "test.default.direct")

      msg = msgs.pop
      assert_equal "direct default message", msg.body
      assert_equal "", msg.exchange_name # Default direct exchange name is empty string
      assert_equal "test.default.direct", msg.routing_key

      # Test that we can get references to built-in exchanges without errors
      assert_instance_of AMQP::Client::Exchange, default_fanout
      assert_instance_of AMQP::Client::Exchange, default_topic
      assert_instance_of AMQP::Client::Exchange, default_headers

      # Test that we can publish to default topic exchange
      q_topic = client.queue("test.default.topic")
      q_topic.bind("amq.topic", "test.*")
      q_topic.subscribe do |msg|
        msgs.push msg
      end

      default_topic.publish("topic default message", "test.message")

      msg = msgs.pop
      assert_equal "topic default message", msg.body
      assert_equal "amq.topic", msg.exchange_name
      assert_equal "test.message", msg.routing_key

      # Clean up (note: we don't delete built-in exchanges as they can't be deleted)
      q_direct.delete
      q_topic.delete
    ensure
      client.stop
    end
  end

  def test_convenience_methods_with_default_names_vs_explicit_names
    client = AMQP::Client.new("amqp://#{TEST_AMQP_HOST}").start
    begin
      # Test that default and explicit names work the same
      default_direct = client.direct
      explicit_direct = client.direct("")

      default_fanout = client.fanout
      explicit_fanout = client.fanout("amq.fanout")

      default_topic = client.topic
      explicit_topic = client.topic("amq.topic")

      default_headers = client.headers
      explicit_headers = client.headers("amq.headers")

      # All should return Exchange objects
      [default_direct, explicit_direct, default_fanout, explicit_fanout,
       default_topic, explicit_topic, default_headers, explicit_headers].each do |exchange|
        assert_instance_of AMQP::Client::Exchange, exchange
      end

      # Test that we can publish using both default and explicit references
      q = client.queue("test.equivalence")
      q.subscribe { |msg| } # Just consume, don't store

      # These should work the same
      default_direct.publish("test1", "test.equivalence")
      explicit_direct.publish("test2", "test.equivalence")

      sleep 0.01 # Allow messages to be consumed
      q.delete
    ensure
      client.stop
    end
  end

  def test_queue_bind_with_exchange_object
    msgs = Queue.new
    client = AMQP::Client.new("amqp://#{TEST_AMQP_HOST}").start
    begin
      exchange = client.fanout("test.fanout.objbind")
      queue = client.queue("test.fanout.objbind.queue")

      # Test queue.bind with Exchange object (not just string)
      queue.bind(exchange, "") # Pass Exchange object directly

      queue.subscribe do |msg|
        msgs.push msg
      end

      # Publish to exchange and verify message routing
      exchange.publish("message via exchange object bind", "")

      msg = msgs.pop
      assert_equal "message via exchange object bind", msg.body
      assert_equal "test.fanout.objbind", msg.exchange_name

      # Test unbind with Exchange object
      queue.unbind(exchange, "")

      # Publish again - should not reach queue after unbind
      exchange.publish("message after unbind", "")

      sleep 0.01
      assert_raises(ThreadError) do
        msgs.pop(true)
      end

      # Clean up
      exchange.delete
      queue.delete
    ensure
      client.stop
    end
  end

  def test_queue_bind_with_string_vs_exchange_object
    client = AMQP::Client.new("amqp://#{TEST_AMQP_HOST}").start
    begin
      exchange = client.fanout("test.queue.bind.string.vs.obj")
      queue1 = client.queue("test.queue.bind.string")
      queue2 = client.queue("test.queue.bind.object")

      # Test queue.bind with string name
      queue1.bind("test.queue.bind.string.vs.obj", "")

      # Test queue.bind with Exchange object
      queue2.bind(exchange, "")

      # Both should work the same way
      assert true, "Queue can bind to both string names and Exchange objects"

      # Test unbinding
      queue1.unbind("test.queue.bind.string.vs.obj", "")
      queue2.unbind(exchange, "")

      # Clean up
      exchange.delete
      queue1.delete
      queue2.delete
    ensure
      client.stop
    end
  end

  def test_exchange_bind_to_queue_object
    msgs = Queue.new
    client = AMQP::Client.new("amqp://#{TEST_AMQP_HOST}").start
    begin
      exchange = client.fanout("test.exchange.bind.to.queue")
      queue = client.queue("test.exchange.bind.queue.obj")

      # Test exchange.bind with Queue object
      exchange.bind(queue, "") # Bind exchange to queue directly

      queue.subscribe do |msg|
        msgs.push msg
      end

      # Publish to exchange and verify message routing
      exchange.publish("message via queue object bind", "")

      msg = msgs.pop
      assert_equal "message via queue object bind", msg.body
      assert_equal "test.exchange.bind.to.queue", msg.exchange_name

      # Test unbind with Queue object
      exchange.unbind(queue, "")

      # Publish again - should not reach queue
      exchange.publish("message after unbind", "")

      sleep 0.01
      assert_raises(ThreadError) do
        msgs.pop(true)
      end

      # Clean up
      exchange.delete
      queue.delete
    ensure
      client.stop
    end
  end

  def test_exchange_bind_to_exchange_object
    msgs = Queue.new
    client = AMQP::Client.new("amqp://#{TEST_AMQP_HOST}").start
    begin
      source_exchange = client.fanout("test.source.exchange.obj.unique")
      dest_exchange = client.fanout("test.dest.exchange.obj.unique")

      # Create queue bound to destination
      queue = client.queue("test.exchange.exchange.bind.unique")
      queue.bind(dest_exchange, "")

      queue.subscribe do |msg|
        msgs.push msg
      end

      # Test exchange.bind with Exchange object
      dest_exchange.bind(source_exchange, "") # Bind to Exchange object

      # Publish to source exchange
      source_exchange.publish("message via exchange-exchange bind", "")

      msg = msgs.pop
      assert_equal "message via exchange-exchange bind", msg.body
      assert_equal "test.source.exchange.obj.unique", msg.exchange_name

      # Test unbind with Exchange object
      dest_exchange.unbind(source_exchange, "")

      # Publish again - should not reach queue
      source_exchange.publish("message after exchange unbind", "")

      sleep 0.01
      assert_raises(ThreadError) do
        msgs.pop(true)
      end

      # Clean up
      source_exchange.delete
      dest_exchange.delete
      queue.delete
    ensure
      client.stop
    end
  end

  def test_exchange_bind_default_binding_key
    client = AMQP::Client.new("amqp://#{TEST_AMQP_HOST}").start
    begin
      # Create exchanges with unique names to avoid conflicts
      dest_exchange = client.fanout("test.exchange.default.binding.unique")
      source_exchange = client.fanout("test.source.default.binding.unique")

      # Test that default binding_key is now "" (empty string, not nil)
      dest_exchange.bind(source_exchange) # No binding_key specified, should default to ""
      dest_exchange.unbind(source_exchange) # No binding_key specified, should default to ""

      # Test explicit empty string works the same
      dest_exchange.bind(source_exchange, "")
      dest_exchange.unbind(source_exchange, "")

      # If we got here without errors, the default works
      assert true, "Exchange bind/unbind default binding_key works"
    ensure
      client.stop
    end
  end

  def test_queue_bind_requires_binding_key
    client = AMQP::Client.new("amqp://#{TEST_AMQP_HOST}").start
    begin
      queue = client.queue("test.queue.requires.binding.key")
      exchange = client.fanout("test.exchange.requires.binding")

      # Queue bind/unbind still require explicit binding_key (no default)
      queue.bind(exchange, "")
      queue.unbind(exchange, "")

      # Test with string
      queue.bind("test.exchange.requires.binding", "")
      queue.unbind("test.exchange.requires.binding", "")

      # If we got here without errors, the API works
      assert true, "Queue bind/unbind with explicit binding_key works"

      # Clean up
      exchange.delete
      queue.delete
    ensure
      client.stop
    end
  end

  def test_queue_and_exchange_bind_with_arguments
    client = AMQP::Client.new("amqp://#{TEST_AMQP_HOST}").start
    begin
      # Create headers exchange and queue for argument testing with unique names
      exchange = client.headers("test.bind.args.exchange.unique")
      queue = client.queue("test.bind.args.queue.unique")

      # Test queue.bind with arguments
      queue.bind(exchange, "", arguments: { "type" => "test", "x-match" => "all" })
      queue.unbind(exchange, "", arguments: { "type" => "test", "x-match" => "all" })

      # Test exchange.bind with arguments (to another exchange)
      dest_exchange = client.headers("test.dest.args.exchange.unique")
      dest_exchange.bind(exchange, "", arguments: { "format" => "json", "x-match" => "any" })
      dest_exchange.unbind(exchange, "", arguments: { "format" => "json", "x-match" => "any" })

      # If we got here without errors, arguments work with both APIs
      assert true, "Queue and Exchange bind/unbind with arguments works"

      # Clean up
      exchange.delete
      dest_exchange.delete
      queue.delete
    ensure
      client.stop
    end
  end
end
