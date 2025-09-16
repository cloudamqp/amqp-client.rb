# frozen_string_literal: true

require_relative "../test_helper"

class AMQPHeartbeatTest < Minitest::Test
  def test_client_stays_alive_by_heartbeats
    connection = AMQP::Client.new("amqp://#{TEST_AMQP_HOST}", heartbeat: 1).connect
    sleep 1.2

    refute_predicate connection, :closed?, "Client should stay open while receiving heartbeats"
  end

  def test_no_heartbeat_when_disabled
    connection = AMQP::Client.new("amqp://localhost", heartbeat: 0).connect
    heartbeat_sent = false
    connection.stub(:send_heartbeat, -> { heartbeat_sent = true }) do
      sleep 1 # Wait to see if any heartbeats are sent

      refute heartbeat_sent, "No heartbeat frames should be sent when heartbeat is disabled"
    end
  end
end
