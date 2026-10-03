# frozen_string_literal: true

require_relative "../test_helper"

class AMQPHeartbeatTest < Minitest::Test
  def test_client_stays_alive_by_heartbeats
    connection = AMQP::Client.new("amqp://#{TEST_AMQP_HOST}", heartbeat: 1).connect
    sleep 1.2

    refute_predicate connection, :closed?, "Client should stay open while receiving heartbeats"
  ensure
    connection&.close
  end

  # The broker only sees what the client sends, so a consumer that never writes
  # (no_ack) still has to send heartbeats while messages keep arriving
  def test_receiving_consumer_sends_heartbeats
    consumer = AMQP::Client.new("amqp://#{TEST_AMQP_HOST}", heartbeat: 1).connect
    ch = consumer.channel
    q = ch.queue_declare ""
    ch.basic_consume(q.queue_name, no_ack: true) { |_msg| nil }
    publisher = AMQP::Client.new("amqp://#{TEST_AMQP_HOST}").connect.channel
    heartbeats_sent = 0
    consumer.stub(:send_heartbeat, -> { heartbeats_sent += 1 }) do
      15.times do
        publisher.basic_publish "x", exchange: "", routing_key: q.queue_name
        sleep 0.1
      end
    end

    assert_operator heartbeats_sent, :>=, 2
  ensure
    publisher&.connection&.close
    consumer&.close
  end

  def test_no_heartbeat_when_disabled
    connection = AMQP::Client.new("amqp://localhost", heartbeat: 0).connect
    heartbeat_sent = false
    connection.stub(:send_heartbeat, -> { heartbeat_sent = true }) do
      sleep 1 # Wait to see if any heartbeats are sent

      refute heartbeat_sent, "No heartbeat frames should be sent when heartbeat is disabled"
    end
  ensure
    connection&.close
  end
end
