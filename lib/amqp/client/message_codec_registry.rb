# frozen_string_literal: true

module AMQP
  class Client
    # Internal registry that stores content_type parsers and content_encoding coders.
    # Only exact content_type and content_encoding matches are supported.
    class MessageCodecRegistry
      # @yield Called on every change to the registry
      def initialize(&on_change)
        @parsers = {} # content_type => handler
        @coders = {} # content_encoding => handler
        @on_change = on_change
      end

      # Register a parser for a content_type
      # @param content_type [String] The content_type to match
      # @param parser [Object] The parser object,
      #   must respond to parse(data, properties) and serialize(obj, properties)
      # @return [self]
      def register_parser(content_type:, parser:)
        validate_parser!(parser)
        @parsers[content_type] = parser
        changed
      end

      # Register a coder for a specific content_encoding
      # @param content_encoding [String] The content_encoding to match
      # @param coder [Object] The coder object,
      #   must respond to encode(data, properties) and decode(data, properties)
      # @return [self]
      def register_coder(content_encoding:, coder:)
        validate_coder!(coder)
        @coders[content_encoding] = coder
        changed
      end

      # Find parser handler based on message properties
      # @param content_type [String] The content_type to match
      # @return [Object, nil] The parser object or nil if not found
      def find_parser(content_type)
        @parsers[content_type]
      end

      # Find coder handler based on content_encoding
      # @param content_encoding [String] The content_encoding to match
      # @return [Object, nil] The coder object or nil if not found
      def find_coder(content_encoding)
        @coders[content_encoding]
      end

      # Introspection helper to list all registered content types
      # @return [Array<String>] List of registered content types
      def list_content_types
        @parsers.keys
      end

      # Introspection helper to list all registered content encodings
      # @return [Array<String>] List of registered content encodings
      def list_content_encodings
        @coders.keys
      end

      # Enable built-in parsers for common content types
      # @return [self]
      def enable_builtin_parsers
        register_parser(content_type: "text/plain", parser: Parsers::Plain)
        register_parser(content_type: "application/json", parser: Parsers::JSONParser)
        self
      end

      # Enable built-in coders for common content encodings
      # @return [self]
      def enable_builtin_coders
        register_coder(content_encoding: "gzip", coder: Coders::Gzip)
        register_coder(content_encoding: "deflate", coder: Coders::Deflate)
        self
      end

      # Enable all built-in codecs (parsers and coders)
      # @return [self]
      def enable_builtin_codecs
        enable_builtin_parsers.enable_builtin_coders
      end

      # Register all parsers and coders of another registry
      # @param other [MessageCodecRegistry]
      # @return [self]
      def register_all(other)
        @parsers.merge!(other.parsers)
        @coders.merge!(other.coders)
        changed
      end

      # Lightweight cloning: registry contents duplicated (shallow copy of handler references)
      def dup
        self.class.new.register_all(self)
      end

      # A deeply frozen copy, that can be shared with other Ractors
      # @api private
      # @return [MessageCodecRegistry]
      # @raise [Error] if a parser or coder isn't shareable
      def shareable_copy
        { "parser for content_type" => @parsers, "coder for content_encoding" => @coders }.each do |kind, codecs|
          codecs.each do |name, codec|
            next if Ractor.shareable?(codec)

            raise Error, "the #{kind} #{name} (#{codec.inspect}) isn't shareable, " \
                         "make it so with Ractor.make_shareable before registering it"
          end
        end
        Ractor.make_shareable(dup)
      end

      protected

      attr_reader :parsers, :coders

      private

      def changed
        @on_change&.call
        self
      end

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
