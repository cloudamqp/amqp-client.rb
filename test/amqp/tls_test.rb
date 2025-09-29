# frozen_string_literal: true

require_relative "../test_helper"

class AMQPSClientTest < Minitest::Test
  def test_it_can_connect_to_tls
    connection = AMQP::Client.new("amqps://#{TEST_AMQP_HOST}", verify_peer: false).connect
    channel = connection.channel
    q = channel.queue_declare ""
    channel.basic_publish "foobar", exchange: "", routing_key: q.queue_name
    channel.basic_consume(q.queue_name) do |msg|
      assert_equal "foobar", msg.body
      channel.basic_cancel msg.consumer_tag
    end
  ensure
    connection&.close
  end

  def test_it_can_ack_a_lot_of_msgs_on_tls
    msgs1 = Queue.new
    connection = AMQP::Client.new("amqps://#{TEST_AMQP_HOST}", verify_peer: false).connect
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
