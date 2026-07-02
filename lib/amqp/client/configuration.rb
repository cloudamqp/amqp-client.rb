# frozen_string_literal: true

module AMQP
  class Client
    # Configuration for AMQP::Client
    # Attribute: <tt>strict_coding</tt>
    # Returns <tt>Boolean</tt> - Whether to raise on unknown codecs
    # Attribute: <tt>default_content_type</tt>
    # Returns <tt>String, nil</tt> - Default content type for published messages
    # Attribute: <tt>default_content_encoding</tt>
    # Returns <tt>String, nil</tt> - Default content encoding for published messages
    class Configuration
      attr_accessor :strict_coding, :default_content_type, :default_content_encoding
      attr_reader :codec_registry

      # Initialize a new configuration
      # * <tt>codec_registry</tt> (<tt>MessageCodecRegistry</tt>) - The codec registry to use
      def initialize(codec_registry)
        @codec_registry = codec_registry
        @strict_coding = false
        @default_content_type = nil
        @default_content_encoding = nil
      end

      # Enable all built-in codecs (parsers and coders) for automatic message encoding/decoding
      # This is a convenience method that delegates to the codec registry
      # Returns <tt>self</tt>.
      def enable_builtin_codecs
        codec_registry.enable_builtin_codecs
        self
      end

      # Enable built-in parsers for common content types
      # Returns <tt>self</tt>.
      def enable_builtin_parsers
        codec_registry.enable_builtin_parsers
        self
      end

      # Enable built-in coders for common content encodings
      # Returns <tt>self</tt>.
      def enable_builtin_coders
        codec_registry.enable_builtin_coders
        self
      end

      # Register a custom parser for a content type
      # * <tt>content_type</tt> (<tt>String</tt>) - The content_type to match
      # * <tt>parser</tt> (<tt>Object</tt>) - The parser object
      # Returns <tt>self</tt>.
      def register_parser(content_type:, parser:)
        codec_registry.register_parser(content_type:, parser:)
        self
      end

      # Register a custom coder for a content encoding
      # * <tt>content_encoding</tt> (<tt>String</tt>) - The content_encoding to match
      # * <tt>coder</tt> (<tt>Object</tt>) - The coder object
      # Returns <tt>self</tt>.
      def register_coder(content_encoding:, coder:)
        codec_registry.register_coder(content_encoding:, coder:)
        self
      end
    end
  end
end
