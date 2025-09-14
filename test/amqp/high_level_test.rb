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
end
