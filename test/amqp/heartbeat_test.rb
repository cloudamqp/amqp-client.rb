# frozen_string_literal: true

require_relative "../test_helper"

class AMQPHeartbeatTest < Minitest::Test
  MAX_MISSED_HEARTBEATS = AMQP::Client::Connection::MAX_MISSED_HEARTBEATS

  def now
    Process.clock_gettime(Process::CLOCK_MONOTONIC)
  end

  def test_client_stays_alive_by_getting_heartbeats
    connection = AMQP::Client.new("amqp://#{TEST_AMQP_HOST}", heartbeat: 1).connect
    sleep 1.2
    refute connection.closed?, "Client should stay open while receiving heartbeats"
  end

  def test_client_closes_on_max_missed_server_heartbeats
    heartbeat = 1
    connection = AMQP::Client.new("amqp://localhost", heartbeat: heartbeat).connect
    # Preventing @last_recv from being updated. i.e. simulate missed heartbeats
    connection.stub(:update_last_recv, -> {}) do
      # Set last_recv to a time in the past to simulate missed heartbeats
      connection.instance_variable_set(:@last_recv, now - heartbeat * MAX_MISSED_HEARTBEATS)
      sleep 0.5 # Wait enough time for the heartbeat check to trigger
      assert connection.closed?, "Client should close connection if server heartbeats are missed"
    end
  end

  def test_client_does_not_close_until_max_missed_server_heartbeats
    heartbeat = 1
    connection = AMQP::Client.new("amqp://localhost", heartbeat: heartbeat).connect
    # Preventing @last_recv from being updated. i.e. simulate missed heartbeats
    connection.stub(:update_last_recv, -> {}) do
      # Set last_recv to a time in the past to simulate missed heartbeats
      connection.instance_variable_set(:@last_recv, now - heartbeat * (MAX_MISSED_HEARTBEATS - 1))
      sleep 0.5 # Wait enough time for the heartbeat check to trigger
      refute connection.closed?, "Client should not close connection until max missed heartbeats"
    end
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
