# frozen_string_literal: true

require_relative "../test_helper"

class AMQPLavinMQFlowControlTest < Minitest::Test
  def test_low_disk_blocks_publishing_with_precondition_failed
    with_low_disk_lavinmq do |port|
      conn = AMQP::Client.new("amqp://127.0.0.1:#{port}").connect
      begin
        ch = conn.channel
        ch.confirm_select
        assert_raises(AMQP::Client::Error::PreconditionFailed) do
          ch.basic_publish_confirm("body", exchange: "", routing_key: "low.disk.test")
        end
      ensure
        conn&.close
      end
    end
  end
end
