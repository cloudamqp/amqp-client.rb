# frozen_string_literal: true

require_relative "../test_helper"

class AMQPHeartbeatTest < Minitest::Test
  def test_client_stays_alive_by_getting_heartbeats
    connection = AMQP::Client.new("amqp://localhost", heartbeat: 1).connect
    sleep 1.2
    refute connection.closed?, "Client should stay open while receiving heartbeats"
  end
end
