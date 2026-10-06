# frozen_string_literal: true

require_relative "../test_helper"

class ErrorsTest < Minitest::Test
  # Reply texts as sent by LavinMQ 2.10 and RabbitMQ 4.0, all with reply code 403
  EXCLUSIVE_USE = [
    "ACCESS_REFUSED - Queue 'q' in vhost '/' in exclusive use",
    "ACCESS_REFUSED - queue 'q' in vhost '/' in exclusive use"
  ].freeze
  MISSING_PERMISSIONS = [
    "ACCESS_REFUSED - User 'u' doesn't have permissions to queue 'q'",
    "ACCESS_REFUSED - read access to queue 'q' in vhost '/' refused for user 'u'"
  ].freeze

  def test_access_refused_tells_exclusive_use_from_missing_permissions
    assert(EXCLUSIVE_USE.all? { |reason| access_refused(reason).exclusive_use? })
    assert(MISSING_PERMISSIONS.none? { |reason| access_refused(reason).exclusive_use? })
  end

  private

  def access_refused(reason)
    AMQP::Client::Error::AccessRefused.new(1, 403, reason)
  end
end
