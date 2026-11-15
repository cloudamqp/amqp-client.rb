# frozen_string_literal: true

require_relative "../test_helper"

class AMQPHeartbeatTest < Minitest::Test
  def test_client_stays_alive_by_heartbeats
    connection = AMQP::Client.new("amqp://#{TEST_AMQP_HOST}", heartbeat: 1).connect
    sleep 3 # longer than the client waits for the broker before closing

    refute_predicate connection, :closed?, "Client should stay open while receiving heartbeats"
  ensure
    connection&.close
  end

  # A broker that's gone, or hung, doesn't even send heartbeats
  def test_connection_closes_when_the_broker_goes_silent
    server, broker = silent_broker
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC) # before connect, as the heartbeats start in it
    connection = AMQP::Client.new("amqp://127.0.0.1:#{server.addr[1]}", heartbeat: 1).connect
    sleep 0.05 until connection.closed? || Process.clock_gettime(Process::CLOCK_MONOTONIC) - started > 10
    closed_after = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
    broker.kill.join # a connection still open then closes for another reason, instead of hanging below

    assert_operator closed_after, :>=, 2, "Closed before two heartbeat timeouts"
    error = assert_raises(AMQP::Client::Error::ConnectionClosed) { connection.channel }
    assert_match "heartbeat timeout", error.message
  ensure
    broker&.kill&.join
    server&.close
  end

  # A publish stuck on a full socket holds the write lock, and the heartbeat
  # thread mustn't wait for it, or it never notices that the broker is gone
  def test_stuck_publish_fails_when_the_broker_goes_silent
    server, broker = silent_broker(channels: 1)
    channel = AMQP::Client.new("amqp://127.0.0.1:#{server.addr[1]}", heartbeat: 1).connect.channel
    publisher = Thread.new do
      channel.basic_publish "x" * 32_000_000, exchange: "", routing_key: "q" # more than the socket buffers hold
    rescue AMQP::Client::Error => e
      e
    end
    publisher.join(10)
    broker.kill.join # a publish still stuck then fails for another reason, instead of hanging below

    assert_kind_of AMQP::Client::Error::ConnectionClosed, publisher.value
    assert_match "heartbeat timeout", publisher.value.message
  ensure
    broker&.kill&.join
    server&.close
  end

  # Closing waits for the broker's close-ok, which a broker that's gone never sends
  def test_close_returns_when_the_broker_goes_silent
    server, broker = silent_broker
    connection = AMQP::Client.new("amqp://127.0.0.1:#{server.addr[1]}", heartbeat: 1).connect
    closer = Thread.new { connection.close }

    refute_nil closer.join(10), "Close didn't return"
  ensure
    broker&.kill&.join # a close still waiting then returns
    closer&.join
    server&.close
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

  private

  # A broker that completes the handshake, with heartbeats, opens channels, and
  # then neither reads nor writes anything
  def silent_broker(channels: 0)
    server = TCPServer.new("127.0.0.1", 0)
    broker = Thread.new do
      socket = server.accept
      socket.read(8) # protocol header
      socket.write method_frame(10, 10, [0, 9, 0, 5, "PLAIN", 5, "en_US"].pack("C C L> L> a* L> a*")) # start
      read_frame(socket) # start-ok
      socket.write method_frame(10, 30, [2047, 131_072, 1].pack("S> L> S>")) # tune
      2.times { read_frame(socket) } # tune-ok, open
      socket.write method_frame(10, 41, "\x00") # open-ok
      channels.times do
        channel_id = 0
        channel_id = read_frame(socket) while channel_id.zero? # skips heartbeats
        socket.write method_frame(20, 11, [0].pack("L>"), channel_id) # channel open-ok
      end
      sleep
    ensure
      socket&.close
    end
    [server, broker]
  end

  def method_frame(class_id, method_id, args, channel_id = 0)
    payload = [class_id, method_id].pack("S> S>") + args
    [1, channel_id, payload.bytesize].pack("C S> L>") + payload + "\xCE".b
  end

  # @return [Integer] The frame's channel
  def read_frame(socket)
    _type, channel_id, size = socket.read(7).unpack("C S> L>")
    socket.read(size + 1)
    channel_id
  end
end
