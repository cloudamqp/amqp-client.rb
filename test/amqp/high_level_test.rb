# frozen_string_literal: true

require_relative "../test_helper"
require "zlib"

class HighLevelTest < Minitest::Test
  def test_it_can_connect_pub_sub
    msgs = Queue.new
    client = AMQP::Client.new("amqp://localhost")
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
    client = AMQP::Client.new("amqp://localhost")
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
    client = AMQP::Client.new("amqp://localhost")
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
    client = AMQP::Client.new("amqp://localhost").start
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
    client = AMQP::Client.new("amqp://localhost").start
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
    client = AMQP::Client.new("amqp://localhost").start
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
end
