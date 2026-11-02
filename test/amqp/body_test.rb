# frozen_string_literal: true

require_relative "../test_helper"

class AMQPBodyTest < Minitest::Test
  def setup
    # The smallest frame_max RabbitMQ (4.1+) accepts, it resets connections asking for less
    @connection = AMQP::Client.new("amqp://#{TEST_AMQP_HOST}", frame_max: 8192).connect
  end

  def teardown
    @connection&.close
  end

  # Bodies around and across frame_max boundaries, consumed back to back so that
  # frames end up split across socket reads
  def test_it_can_consume_bodies_of_all_sizes
    ch = @connection.channel
    q = ch.queue_declare ""
    body_max = @connection.frame_max - 8
    sizes = [0, 1, 100, body_max - 1, body_max, body_max + 1, (2 * body_max) + 1, 70_000, 300_000]
    bodies = sizes.map { |size| Array.new(size) { |i| (i % 251).chr }.join.b }
    msgs = Queue.new
    ch.basic_consume(q.queue_name) { |msg| msgs << msg }
    3.times { bodies.each { |body| ch.basic_publish body, exchange: "", routing_key: q.queue_name } }

    3.times do
      bodies.each { |body| assert_equal body, msgs.pop.body }
    end
  end

  # A body is sliced out of the read buffer, which the read loop then reuses for
  # the next frames. Each body must still be a string of its own, that can be
  # kept and modified without affecting the others.
  def test_consumed_bodies_are_not_shared
    ch = @connection.channel
    q = ch.queue_declare ""
    bodies = %w[first second third]
    msgs = Queue.new
    ch.basic_consume(q.queue_name) do |msg|
      msg.body << "!"
      msgs << msg
    end
    bodies.each { |body| ch.basic_publish body, exchange: "", routing_key: q.queue_name }

    received = Array.new(bodies.size) { msgs.pop.body }

    assert_equal bodies.map { |body| "#{body}!" }, received
  end

  # Frames are binary, a body in another encoding still has to go out byte for byte
  def test_it_can_publish_non_ascii_utf8_bodies
    ch = @connection.channel
    q = ch.queue_declare ""
    body = "räksmörgås ✓"
    ch.basic_publish body, exchange: "", routing_key: q.queue_name
    msg = ch.basic_get(q.queue_name)

    assert_equal body.b, msg.body
  end
end
