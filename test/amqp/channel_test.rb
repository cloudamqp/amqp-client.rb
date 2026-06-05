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
