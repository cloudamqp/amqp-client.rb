# frozen_string_literal: true

require_relative "../test_helper"

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
      assert_equal msg.routing_key, "test.conn"
      assert_equal msg.body, "foobar"
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
      assert_equal msg.routing_key, "foo.bar"
      assert_equal msg.body, "foo"
    ensure
      q.delete
      client.stop
    end
  end
end
