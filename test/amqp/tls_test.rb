# frozen_string_literal: true

require_relative "../test_helper"

class AMQPSClientTest < Minitest::Test
  def test_it_can_connect_to_tls
    # Default verify_peer (on): SSL_CERT_FILE (set by bin/test-tls) points the
    # client's trust store at the broker's cert, so this exercises real
    # certificate verification, not just the handshake.
    connection = AMQP::Client.new("amqps://#{TEST_AMQP_HOST}:#{TEST_AMQPS_PORT}").connect
    channel = connection.channel
    q = channel.queue_declare ""
    channel.basic_publish "foobar", exchange: "", routing_key: q.queue_name
    # worker_threads: 0 consumes in this thread so basic_cancel completes before
    # the ensure closes the connection. A background worker thread races the
    # close, and on JRuby/TruffleRuby basic_cancel then writes to an already
    # closed connection and raises ConnectionClosed. (Matches test_it_can_consume.)
    channel.basic_consume(q.queue_name, worker_threads: 0) do |msg|
      assert_equal "foobar", msg.body
      channel.basic_cancel msg.consumer_tag
    end
  ensure
    connection&.close
  end

  def test_it_can_ack_a_lot_of_msgs_on_tls
    msgs1 = Queue.new
    # verify_peer: false here covers the unverified-connection path.
    connection = AMQP::Client.new("amqps://#{TEST_AMQP_HOST}:#{TEST_AMQPS_PORT}", verify_peer: false).connect
    ch1 = connection.channel
    q = ch1.queue_declare ""
    ch1.basic_qos(200)
    ch1.queue_bind(q.queue_name, exchange: "amq.topic", binding_key: "foo")
    ch1.basic_consume(q.queue_name, no_ack: false, worker_threads: 100) do |msg|
      msg.ack
      msgs1 << msg
    end

    ch2 = connection.channel
    ch2.confirm_select
    10_000.times do |i|
      ch2.basic_publish "bar #{i + 1}", exchange: "amq.topic", routing_key: "foo"
    end
    ch2.wait_for_confirms

    10_000.times do
      assert_equal "foo", msgs1.pop.routing_key
    end
  ensure
    connection&.close
  end
end
