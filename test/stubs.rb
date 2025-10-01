# frozen_string_literal: true

require_relative "../lib/amqp/client"

class DummyClient < AMQP::Client
  attr_reader :published

  def initialize
    super
    @published = []
  end

  def publish(body, exchange:, routing_key:, **properties)
    super
    @published << { body: @last_body, exchange:, routing_key:, properties: }
  end

  def serialize_and_encode_body(body, properties)
    @last_body = super
  end

  def with_connection
    yield self
  rescue NoMethodError
    # Ignore errors about missing stubbed methods, allow real client errors to propagate
  end
end

# Ensure built-in codecs are available for these unit tests
DummyClient.codec_registry.enable_builtin_codecs

class DummyChannel
  attr_reader :connection

  def initialize(strict = nil)
    client = DummyClient.new
    client.strict_coding = strict unless strict.nil?
    @connection = DummyConnection.new(client.codec_registry, client.strict_coding)
  end

  def basic_ack(_tag); end
  def basic_reject(_tag, requeue:); end
end

class DummyConnection
  attr_reader :codec_registry, :strict_coding

  def initialize(codec_registry, strict_coding)
    @codec_registry = codec_registry
    @strict_coding = strict_coding
  end
end
