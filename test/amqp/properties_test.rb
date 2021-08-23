# frozen_string_literal: true

require_relative "../test_helper"

class AMQPPropertiesTest < Minitest::Test
  def test_integer_expiration_encode
    expect = "\x01\x00\x041000"
    properties = AMQP::Properties.new(expiration: 1000)

    encoded = properties.encode

    assert_equal expect, encoded
  end

  def test_string_expiration_encode
    expect = "\x01\x00\x041000"
    properties = AMQP::Properties.new(expiration: "1000")

    encoded = properties.encode

    assert_equal expect, encoded
  end
end
