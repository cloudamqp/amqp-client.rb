# frozen_string_literal: true

require_relative "../test_helper"

# Unit tests for Connection::Channel that drive its threading contract
# directly, without a live broker.
class ChannelTest < Minitest::Test
  # Minimal Connection double. #wait_for_confirms, #closed!, #confirm_select
  # and #basic_publish only ever call #frame_max and #write_bytes on their
  # connection, so a no-op writer is enough to exercise them.
  class FakeConnection
    def frame_max = 131_072
    def write_bytes(*); end
  end

  # Connection double whose every write fails as if the peer just closed.
  class ClosedConnection
    def frame_max = 131_072
    def write_bytes(*) = raise AMQP::Client::Error::ConnectionClosed.new(200, "")
  end

  # Regression test: a consumer worker thread must not crash when the connection
  # (or channel) closes while it is processing a delivery.
  #
  # The subscribe and rpc_server callbacks ack/reject/publish after handling a
  # message; if a shutdown lands first, those writes raise ConnectionClosed
  # (or ChannelClosed) from inside the worker thread. #consume_loop has to
  # swallow that and stop quietly — otherwise the unhandled exception crashes
  # the worker, which is fatal under the suite's Thread.abort_on_exception (and
  # a noisy report_on_exception warning in production). This surfaced on JRuby
  # and truffleruby, whose threads run truly in parallel.
  #
  # Driving #consume_loop with a one-shot queue and a block that writes through
  # an already-closed connection reproduces it deterministically (no broker, no
  # sleeps); #join re-raises here if the worker died instead of stopping.
  def test_consume_loop_stops_quietly_when_a_write_races_connection_close
    channel = AMQP::Client::Connection::Channel.new(ClosedConnection.new, 1)
    deliveries = Queue.new
    deliveries.push(:delivery)
    deliveries.close # so the loop exits after the single delivery

    worker = Thread.new do
      channel.send(:consume_loop, deliveries, "ctag") { channel.basic_ack(1) }
    end

    assert worker.join(5), "consumer worker thread didn't stop after the connection closed"
  end

  # Regression test for a lost-wakeup race in #wait_for_confirms.
  #
  # When the broker closes the channel (e.g. a publish to a missing exchange)
  # the read_loop thread runs #closed!, which broadcasts @unconfirmed_empty.
  # If that broadcast lands *before* the publishing thread starts waiting on
  # the condition variable, the wakeup is lost: @unconfirmed is never emptied,
  # so #wait_for_confirms would block forever. It must instead notice the
  # closed channel and raise.
  #
  # Running #closed! before #wait_for_confirms reproduces the losing
  # interleaving deterministically (no broker, no sleeps). Before the fix this
  # deadlocked (caught here by Timeout); after it, ChannelClosed is raised at
  # once. This race surfaces on truffleruby, whose threads run truly in
  # parallel, far more often than on MRI.
  def test_wait_for_confirms_raises_when_channel_closed_before_waiting
    channel = AMQP::Client::Connection::Channel.new(FakeConnection.new, 1)
    channel.confirm_select(no_wait: true)
    channel.basic_publish("msg", exchange: "missing", routing_key: "rk")

    # Simulate the read_loop handling the broker's channel.close frame: this
    # sets @closed and broadcasts to @unconfirmed_empty with nobody waiting.
    channel.closed!(:channel, 404, "NOT_FOUND - no exchange 'missing'", 60, 40)

    assert_raises(AMQP::Client::Error::ChannelClosed) do
      Timeout.timeout(5, Timeout::Error, "wait_for_confirms blocked: lost wakeup not handled") do
        channel.wait_for_confirms
      end
    end
  end
end
