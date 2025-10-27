# frozen_string_literal: true

require_relative "../test_helper"

class AMQPConfigurationTest < Minitest::Test
  def setup
    # Save original configuration
    @original_config = AMQP::Client.config.dup
  end

  def teardown
    # Restore original configuration
    AMQP::Client.instance_variable_set(:@config, @original_config)
  end

  def test_configure_block_pattern
    AMQP::Client.configure do |config|
      config.default_content_type = "application/json"
      config.default_content_encoding = "gzip"
      config.strict_coding = true
    end

    config = AMQP::Client.config

    assert_equal "application/json", config.default_content_type
    assert_equal "gzip", config.default_content_encoding
    assert config.strict_coding
  end

  def test_configure_returns_configuration
    config = AMQP::Client.configure

    assert_instance_of AMQP::Client::Configuration, config
  end

  def test_instance_inherits_class_configuration
    AMQP::Client.configure do |config|
      config.default_content_type = "application/json"
      config.default_content_encoding = "gzip"
      config.strict_coding = true
    end

    client = AMQP::Client.new("amqp://localhost")

    assert_equal "application/json", client.default_content_type
    assert_equal "gzip", client.default_content_encoding
    assert client.strict_coding
  end

  def test_instance_can_override_class_configuration # rubocop:disable Minitest/MultipleAssertions
    AMQP::Client.configure do |config|
      config.default_content_type = "application/json"
      config.default_content_encoding = "gzip"
    end

    client = AMQP::Client.new("amqp://localhost")
    client.default_content_type = "text/plain"
    client.default_content_encoding = "deflate"

    # Instance has overridden values
    assert_equal "text/plain", client.default_content_type
    assert_equal "deflate", client.default_content_encoding

    # Class still has original values
    assert_equal "application/json", AMQP::Client.config.default_content_type
    assert_equal "gzip", AMQP::Client.config.default_content_encoding
  end

  def test_codec_registry_is_duplicated_per_instance
    client1 = AMQP::Client.new("amqp://localhost")
    client2 = AMQP::Client.new("amqp://localhost")

    # Instances have separate codec registries
    refute_same client1.codec_registry, client2.codec_registry
    refute_same client1.codec_registry, AMQP::Client.codec_registry
  end

  def test_subclass_inherits_configuration # rubocop:disable Minitest/MultipleAssertions
    AMQP::Client.configure do |config|
      config.default_content_type = "application/json"
      config.strict_coding = true
    end

    subclass = Class.new(AMQP::Client)

    assert_equal "application/json", subclass.config.default_content_type
    assert subclass.config.strict_coding

    # Subclass can override without affecting parent
    subclass.config.default_content_type = "text/plain"

    assert_equal "text/plain", subclass.config.default_content_type
    assert_equal "application/json", AMQP::Client.config.default_content_type
  end

  def test_configuration_defaults # rubocop:disable Minitest/MultipleAssertions
    registry = AMQP::Client::MessageCodecRegistry.new
    config = AMQP::Client::Configuration.new(registry)

    refute config.strict_coding
    assert_nil config.default_content_type
    assert_nil config.default_content_encoding
    assert_same registry, config.codec_registry
  end

  def test_codec_registry_is_accessible_at_class_level
    assert_instance_of AMQP::Client::MessageCodecRegistry, AMQP::Client.codec_registry
  end

  def test_codec_registry_is_accessible_via_config
    assert_same AMQP::Client.codec_registry, AMQP::Client.config.codec_registry
  end

  def test_enable_builtin_codecs_via_configure
    # Create a fresh config for this test
    test_registry = AMQP::Client::MessageCodecRegistry.new
    config = AMQP::Client::Configuration.new(test_registry)

    config.enable_builtin_codecs

    # Verify codecs were registered
    assert_includes test_registry.list_content_types, "application/json"
    assert_includes test_registry.list_content_encodings, "gzip"
  end

  def test_register_parser_via_configure
    test_registry = AMQP::Client::MessageCodecRegistry.new
    config = AMQP::Client::Configuration.new(test_registry)

    parser = Object.new
    def parser.parse(_data, _properties); end
    def parser.serialize(_obj, _properties); end

    config.register_parser(content_type: "test/type", parser:)

    assert_includes test_registry.list_content_types, "test/type"
  end

  def test_configure_with_codec_operations
    original_types = AMQP::Client.codec_registry.list_content_types.dup

    AMQP::Client.configure do |config|
      config.default_content_type = "application/json"
      config.enable_builtin_codecs
    end

    # Verify both configuration and codec setup worked
    assert_equal "application/json", AMQP::Client.config.default_content_type
    refute_equal original_types, AMQP::Client.codec_registry.list_content_types
  end
end
