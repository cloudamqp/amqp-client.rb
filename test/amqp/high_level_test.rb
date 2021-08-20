# frozen_string_literal: true

require_relative "../test_helper"
require 'zlib'

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
      q.delete
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
      q.delete
      client.stop
    end
  end
end
