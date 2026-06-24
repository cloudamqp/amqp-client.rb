# frozen_string_literal: true

module AMQP
  class Client
    # Internal registry that stores content_type parsers and content_encoding coders.
    # Only exact content_type and content_encoding matches are supported.
    class MessageCodecRegistry
      def initialize
        @parsers = {} # content_type => handler
        @coders = {} # content_encoding => handler
      end

      # Register a parser for a content_type
      # * <tt>content_type</tt> (<tt>String</tt>) - The content_type to match
      # * <tt>parser</tt> (<tt>Object</tt>) - The parser object,
      #   must respond to parse(data, properties) and serialize(obj, properties)
      # Returns <tt>self</tt>.
      def register_parser(content_type:, parser:)
        validate_parser!(parser)
        @parsers[content_type] = parser
        self
      end

      # Register a coder for a specific content_encoding
      # * <tt>content_encoding</tt> (<tt>String</tt>) - The content_encoding to match
      # * <tt>coder</tt> (<tt>Object</tt>) - The coder object,
      #   must respond to encode(data, properties) and decode(data, properties)
      # Returns <tt>self</tt>.
      def register_coder(content_encoding:, coder:)
        validate_coder!(coder)
        @coders[content_encoding] = coder
        self
      end

      # Find parser handler based on message properties
      # * <tt>content_type</tt> (<tt>String</tt>) - The content_type to match
      # Returns <tt>Object, nil</tt> - The parser object or nil if not found
      def find_parser(content_type)
        @parsers[content_type]
      end

      # Find coder handler based on content_encoding
      # * <tt>content_encoding</tt> (<tt>String</tt>) - The content_encoding to match
      # Returns <tt>Object, nil</tt> - The coder object or nil if not found
      def find_coder(content_encoding)
        @coders[content_encoding]
      end

      # Introspection helper to list all registered content types
      # Returns <tt>Array<String></tt> - List of registered content types
      def list_content_types
        @parsers.keys
      end

      # Introspection helper to list all registered content encodings
      # Returns <tt>Array<String></tt> - List of registered content encodings
      def list_content_encodings
        @coders.keys
      end

      # Enable built-in parsers for common content types
      # Returns <tt>self</tt>.
      def enable_builtin_parsers
        register_parser(content_type: "text/plain", parser: Parsers::Plain)
        register_parser(content_type: "application/json", parser: Parsers::JSONParser)
        self
      end

      # Enable built-in coders for common content encodings
      # Returns <tt>self</tt>.
      def enable_builtin_coders
        register_coder(content_encoding: "gzip", coder: Coders::Gzip)
        register_coder(content_encoding: "deflate", coder: Coders::Deflate)
        self
      end

      # Enable all built-in codecs (parsers and coders)
      # Returns <tt>self</tt>.
      def enable_builtin_codecs
        enable_builtin_parsers.enable_builtin_coders
      end

      # Lightweight cloning: registry contents duplicated (shallow copy of handler references)
      def dup
        copy = self.class.new
        @parsers.each { |k, v| copy.register_parser(content_type: k, parser: v) }
        @coders.each { |k, v| copy.register_coder(content_encoding: k, coder: v) }
        copy
      end

      private

      def validate_parser!(parser)
        return if parser.respond_to?(:parse) && parser.respond_to?(:serialize)

        raise ArgumentError, "parser must respond to parse(data, properties) and serialize(obj, properties)"
      end

      def validate_coder!(coder)
        return if coder.respond_to?(:encode) && coder.respond_to?(:decode)

        raise ArgumentError, "coder must respond to encode(data, properties) and decode(data, properties)"
      end
    end
  end
end
