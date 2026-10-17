# frozen_string_literal: true

require_relative "../test_helper"

class AMQPRactorTest < Minitest::Test
  def setup
    skip "Ractors are CRuby only" unless RUBY_ENGINE == "ruby"
    Warning[:experimental] = false
  end

  def test_connection_in_a_ractor
    ractor = Ractor.new("amqp://#{TEST_AMQP_HOST}:#{TEST_AMQP_PORT}") do |url|
      connection = AMQP::Client::Connection.new(url)
      ch = connection.channel
      q = ch.queue_declare("", exclusive: true)
      ch.basic_publish("hello", exchange: "", routing_key: q.queue_name)
      msg = ch.basic_get(q.queue_name)
      connection.close
      msg.body
    end

    assert_equal "hello", ractor_value(ractor)
  end

  def test_builtin_codecs_in_a_ractor
    ractor = Ractor.new do
      json = AMQP::Client::Parsers::JSONParser
      [AMQP::Client::Coders::Gzip, AMQP::Client::Coders::Deflate, AMQP::Client::Coders::DeflateRaw].map do |coder|
        json.parse(coder.decode(coder.encode(json.serialize({ greeting: "hello" }, {}), {}), {}), {})
      end
    end

    assert_equal [{ greeting: "hello" }] * 3, ractor_value(ractor)
  end

  private

  # Ractor#take was replaced by Ractor#value in Ruby 3.5
  def ractor_value(ractor)
    ractor.respond_to?(:value) ? ractor.value : ractor.take
  end
end
