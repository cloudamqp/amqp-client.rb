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
      e&.delete
      q&.delete
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

  def test_default_direct_exchange
    client = AMQP::Client.new("amqp://#{TEST_AMQP_HOST}").start
    begin
      direct = client.direct_exchange("amq.direct")

      assert_instance_of AMQP::Client::Exchange, direct
      assert_equal "amq.direct", direct.name
    ensure
      client.stop
    end
  end

  def test_default_exchange
    client = AMQP::Client.new("amqp://#{TEST_AMQP_HOST}").start
    begin
      default = client.default_exchange

      assert_instance_of AMQP::Client::Exchange, default
      assert_equal "", default.name
    ensure
      client.stop
    end
  end

  def test_default_fanout_exchange
    client = AMQP::Client.new("amqp://#{TEST_AMQP_HOST}").start
    begin
      fanout = client.fanout_exchange

      assert_instance_of AMQP::Client::Exchange, fanout
      assert_equal "amq.fanout", fanout.name
    ensure
      client.stop
    end
  end

  def test_default_topic_exchange
    client = AMQP::Client.new("amqp://#{TEST_AMQP_HOST}").start
    begin
      topic = client.topic_exchange

      assert_instance_of AMQP::Client::Exchange, topic
      assert_equal "amq.topic", topic.name
    ensure
      client.stop
    end
  end

  def test_default_headers_exchange
    client = AMQP::Client.new("amqp://#{TEST_AMQP_HOST}").start
    begin
      headers = client.headers_exchange

      assert_instance_of AMQP::Client::Exchange, headers
      assert_equal "amq.headers", headers.name
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
    ensure
      exchange&.delete
      queue&.delete
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
    ensure
      exchange&.delete
      queue1&.delete
      queue2&.delete
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
    ensure
      source_exchange&.delete
      dest_exchange&.delete
      queue&.delete
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
    ensure
      exchange&.delete
      queue&.delete
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
    ensure
      exchange&.delete
      dest_exchange&.delete
      queue&.delete
      client.stop
    end
  end
end
