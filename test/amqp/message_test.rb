# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../stubs"
require "stringio"
require "zlib"
require "json"
require_relative "../../lib/amqp/client"
require_relative "../../lib/amqp/client/message"
require_relative "../../lib/amqp/client/properties"
require_relative "../../lib/amqp/client/errors"

class MessageTest < Minitest::Test
  def build_message(body:, content_type: nil, content_encoding: nil, headers: nil, strict: nil)
    msg = AMQP::Client::Message.new(DummyChannel.new(strict), "ctag", 1, "ex", "rk", false)
    msg.body = body
    msg.properties = AMQP::Client::Properties.new(
      content_type:,
      content_encoding:,
      headers:
    )
    msg
  end

  def test_decode_with_gzip
    gzipped_body = StringIO.new
    gz = Zlib::GzipWriter.new(gzipped_body)
    gz.write("Hello, world!")
    gz.close
    message = build_message(body: gzipped_body.string, content_encoding: "gzip")

    assert_equal "Hello, world!", message.decode
  end

  def test_decode_with_deflate
    deflated = Zlib::Deflate.deflate("Hello, world!")
    message = build_message(body: deflated, content_encoding: "deflate")

    assert_equal "Hello, world!", message.decode
  end

  def test_decode_with_no_content_encoding
    message = build_message(body: "No type!")

    assert_equal "No type!", message.decode
  end

  def test_parse_with_json
    json_body = { foo: "bar" }.to_json
    message = build_message(body: json_body, content_type: "application/json")
    parsed = message.parse

    assert_equal({ foo: "bar" }, parsed)
  end

  def test_parse_with_plain_text
    message = build_message(body: "Just text", content_type: "text/plain")
    parsed = message.parse

    assert_equal("Just text", parsed)
  end

  def test_parse_with_no_content_type
    message = build_message(body: "No type!")
    parsed = message.parse

    assert_equal("No type!", parsed)
  end

  def test_decode_with_unsupported_encoding
    message = build_message(body: "data", content_encoding: "unsupported", strict: true)
    assert_raises(AMQP::Client::Error::UnsupportedContentEncoding) { message.decode }
  end

  def test_parse_with_unsupported_content_type
    message = build_message(body: "data", content_type: "application/xml", strict: true)
    assert_raises(AMQP::Client::Error::UnsupportedContentType) { message.parse }
  end
end
