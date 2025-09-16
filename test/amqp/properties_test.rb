# frozen_string_literal: true

require_relative "../test_helper"

class AMQPPropertiesTest < Minitest::Test
  def test_integer_expiration_encode
    expect = "\x01\x00\x041000"
    encoded = AMQP::Client::Properties.encode({ expiration: 1000 })

    assert_equal expect, encoded
  end

  def test_string_expiration_encode
    expect = "\x01\x00\x041000"
    encoded = AMQP::Client::Properties.encode({ expiration: "1000" })

    assert_equal expect, encoded
  end

  def test_encode_decode_everything
    props = {
      delivery_mode: 2,
      content_type: "text/plain",
      content_encoding: "encoding",
      priority: 240,
      correlation_id: "corrid",
      reply_to: "replyto",
      expiration: "99999",
      message_id: "msgid",
      type: "type",
      user_id: "guest",
      app_id: "appId",
      timestamp: Time.at(Time.now.to_i),
      headers: {
        "hdr1" => "value1",
        "int" => 1,
        "bool" => false,
        "float" => 0.1,
        "array" => [1, "b", true],
        "hash" => { "inner" => "val", "time" => Time.at(Time.now.to_i) },
        "big" => 2**32,
        "nil" => nil
      }
    }
    encoded = AMQP::Client::Properties.encode(props)
    decoded = AMQP::Client::Properties.decode(encoded)

    assert_equal props, decoded.to_h
  end
end
