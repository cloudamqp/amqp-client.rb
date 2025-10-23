# frozen_string_literal: true

require_relative "../test_helper"
require_relative "../stubs"
require "json"
require "zlib"
require_relative "../../lib/amqp/client"
require_relative "../../lib/amqp/client/queue"
require_relative "../../lib/amqp/client/exchange"
require_relative "../../lib/amqp/client/properties"
require_relative "../../lib/amqp/client/message"

class HighLevelEncodingTest < Minitest::Test
  def setup
    @client = DummyClient.new
    @client.codec_registry.enable_builtin_codecs
    @queue = AMQP::Client::Queue.new(@client, "q1")
    @exchange = AMQP::Client::Exchange.new(@client, "ex1")
  end

  def test_queue_publish_json_encoding
    @queue.publish({ foo: "bar" }, content_type: "application/json")
    published = @client.published.last

    assert_equal "application/json", published[:properties][:content_type]
    assert_equal({ "foo" => "bar" }, JSON.parse(published[:body]))
  end

  def test_queue_publish_plain_text
    @queue.publish("hello", content_type: "text/plain")
    published = @client.published.last

    assert_equal "text/plain", published[:properties][:content_type]
    assert_equal "hello", published[:body]
  end

  def test_queue_publish_gzip_encoding
    @queue.publish("hello gzip", content_encoding: "gzip")
    published = @client.published.last
    sio = StringIO.new(published[:body])
    gz = Zlib::GzipReader.new(sio)
    decoded = gz.read
    gz.close

    assert_equal "hello gzip", decoded
  end

  def test_exchange_publish_json_encoding
    @exchange.publish({ bar: 42 }, routing_key: "rk1", content_type: "application/json")
    published = @client.published.last

    assert_equal "application/json", published[:properties][:content_type]
    assert_equal({ "bar" => 42 }, JSON.parse(published[:body]))
  end

  def test_exchange_publish_deflate_encoding
    @exchange.publish("deflate me", routing_key: "rk2", content_encoding: "deflate")
    published = @client.published.last
    inflated = Zlib::Inflate.inflate(published[:body])

    assert_equal "deflate me", inflated
  end

  def test_handles_already_deflated_body
    message = "deflate me"
    body = Zlib::Deflate.deflate(message)
    @exchange.publish(body, routing_key: "rk2", content_encoding: "deflate")
    published = @client.published.last
    inflated = Zlib::Inflate.inflate(published[:body])

    assert_equal "deflate me", inflated
  end

  def test_handles_already_gzipped_body
    message = "deflate me"
    body = Zlib.gzip(message)
    @exchange.publish(body, routing_key: "rk2", content_encoding: "gzip")
    published = @client.published.last
    inflated = Zlib.gunzip(published[:body])

    assert_equal "deflate me", inflated
  end

  def test_handles_unsupported_encoded_body
    message = "custom encoding"
    body = message.encode(Encoding::BINARY)
    @exchange.publish(body, routing_key: "rk2", content_encoding: "custom_binary")
    published = @client.published.last
    inflated = published[:body].encode(Encoding::UTF_8)

    assert_equal "custom encoding", inflated
  end

  def test_lenient_mode_passes_through_unknown_encoding
    client = DummyClient.new
    client.strict_coding = false
    queue = AMQP::Client::Queue.new(client, "q1")
    queue.publish("no encoding", content_type: "text/plain", content_encoding: "gzip2")
    published = client.published.last

    assert_equal "no encoding", published[:body]
  end

  def test_strict_mode_raises_unknown_encoding
    client = DummyClient.new
    client.strict_coding = true
    queue = AMQP::Client::Queue.new(client, "q1")
    assert_raises(AMQP::Client::Error::UnsupportedContentEncoding) do
      queue.publish("no encoding", content_type: "text/plain", content_encoding: "gzip2")
    end
  end

  def test_default_content_type_at_class_level
    original = DummyClient.default_content_type
    DummyClient.default_content_type = "application/json"
    client = DummyClient.new
    queue = AMQP::Client::Queue.new(client, "q1")

    queue.publish({ test: "data" })
    published = client.published.last

    # Verify the body was serialized to JSON
    assert_equal({ "test" => "data" }, JSON.parse(published[:body]))
  ensure
    DummyClient.default_content_type = original
  end

  def test_default_content_encoding_at_class_level
    original = DummyClient.default_content_encoding
    DummyClient.default_content_encoding = "gzip"
    client = DummyClient.new
    queue = AMQP::Client::Queue.new(client, "q1")

    queue.publish("test data")
    published = client.published.last

    # Verify the body was gzip encoded
    assert_equal "test data", Zlib.gunzip(published[:body])
  ensure
    DummyClient.default_content_encoding = original
  end

  def test_default_content_type_at_instance_level
    client = DummyClient.new
    client.default_content_type = "application/json"
    queue = AMQP::Client::Queue.new(client, "q1")

    queue.publish({ key: "value" })
    published = client.published.last

    # Verify the body was serialized to JSON
    assert_equal({ "key" => "value" }, JSON.parse(published[:body]))
  end

  def test_properties_override_default_content_type
    client = DummyClient.new
    client.default_content_type = "application/json"
    queue = AMQP::Client::Queue.new(client, "q1")

    queue.publish("plain text", content_type: "text/plain")
    published = client.published.last

    assert_equal "text/plain", published[:properties][:content_type]
    assert_equal "plain text", published[:body]
  end

  def test_properties_override_default_content_encoding
    client = DummyClient.new
    client.default_content_encoding = "gzip"
    queue = AMQP::Client::Queue.new(client, "q1")

    queue.publish("data", content_encoding: "deflate")
    published = client.published.last

    assert_equal "deflate", published[:properties][:content_encoding]
    assert_equal "data", Zlib::Inflate.inflate(published[:body])
  end
end
