# frozen_string_literal: true

module AMQP
  class Client
    # Configuration for AMQP::Client
    # @!attribute strict_coding
    #   @return [Boolean] Whether to raise on unknown codecs
    # @!attribute default_content_type
    #   @return [String, nil] Default content type for published messages
    # @!attribute default_content_encoding
    #   @return [String, nil] Default content encoding for published messages
    class Configuration
      attr_reader :strict_coding, :default_content_type, :default_content_encoding, :codec_registry

      # Initialize a new configuration
      # @param codec_registry [MessageCodecRegistry] The codec registry to use
      # @yield Called on every change to the settings
      def initialize(codec_registry, &on_change)
        @codec_registry = codec_registry
        @strict_coding = false
        @default_content_type = nil
        @default_content_encoding = nil
        @on_change = on_change
      end

      def strict_coding=(value)
        @strict_coding = value
        @on_change&.call
      end

      def default_content_type=(value)
        @default_content_type = value
        @on_change&.call
      end

      def default_content_encoding=(value)
        @default_content_encoding = value
        @on_change&.call
      end

      # A deeply frozen copy, that can be shared with other Ractors
      # @api private
      # @return [Configuration]
      # @raise [Error] if a parser or coder isn't shareable
      def shareable_copy
        copy = Configuration.new(codec_registry.shareable_copy)
        copy.strict_coding = strict_coding
        copy.default_content_type = default_content_type&.dup
        copy.default_content_encoding = default_content_encoding&.dup
        Ractor.make_shareable(copy)
      end

      # Enable all built-in codecs (parsers and coders) for automatic message encoding/decoding
      # This is a convenience method that delegates to the codec registry
      # @return [self]
      def enable_builtin_codecs
        codec_registry.enable_builtin_codecs
        self
      end

      # Enable built-in parsers for common content types
      # @return [self]
      def enable_builtin_parsers
        codec_registry.enable_builtin_parsers
        self
      end

      # Enable built-in coders for common content encodings
      # @return [self]
      def enable_builtin_coders
        codec_registry.enable_builtin_coders
        self
      end

      # Register a custom parser for a content type
      # @param content_type [String] The content_type to match
      # @param parser [Object] The parser object
      # @return [self]
      def register_parser(content_type:, parser:)
        codec_registry.register_parser(content_type:, parser:)
        self
      end

      # Register a custom coder for a content encoding
      # @param content_encoding [String] The content_encoding to match
      # @param coder [Object] The coder object
      # @return [self]
      def register_coder(content_encoding:, coder:)
        codec_registry.register_coder(content_encoding:, coder:)
        self
      end
    end
  end
end
