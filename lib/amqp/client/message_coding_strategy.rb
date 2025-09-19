# frozen_string_literal: true

require "zlib"
require "stringio"
require "json"

module AMQP
  class Client
    # Message coding strategy that handles encoding/decoding of message bodies
    # based on content_type and content_encoding properties.
    #
    # This class can be subclassed to provide custom encoding/decoding logic.
    # @example Custom Base64 strategy
    #   class Base64Strategy < AMQP::Client::MessageCodingStrategy
    #     def encode_body(body, properties)
    #       case properties[:content_encoding]
    #       when "base64"
    #         require "base64"
    #         Base64.encode64(serialize_body(body, properties))
    #       else
    #         super
    #       end
    #     end
    #
    #     def decode_body(body, properties)
    #       case properties.content_encoding
    #       when "base64"
    #         require "base64"
    #         Base64.decode64(body)
    #       else
    #         super
    #       end
    #     end
    #   end
    class MessageCodingStrategy
      # Parse the message body based on content_type and content_encoding
      # @param body [String] The raw message body
      # @param properties [Properties] Message properties containing content_type and content_encoding
      # @raise [Error::UnsupportedContentEncoding] If the content encoding is not supported
      # @raise [Error::UnsupportedContentType] If the content type is not supported
      # @return [Object] The parsed message body
      def parse_body(body, properties)
        decoded_data = decode_body(body, properties)
        case properties.content_type
        when "application/json" then parse_json(decoded_data)
        when "text/plain", "", nil then decoded_data
        else raise Error::UnsupportedContentType, properties.content_type
        end
      end

      def self.validate_strategy!(strategy)
        unless strategy.respond_to?(:encode_body) &&
               strategy.respond_to?(:decode_body) &&
               strategy.respond_to?(:parse_body)
          raise ArgumentError, "strategy must respond to encode_body, decode_body, and parse_body"
        end
      end

      # Decode the message body based on content_encoding
      # @param body [String] The raw message body
      # @param properties [Properties] Message properties containing content_encoding
      # @raise [Error::UnsupportedContentEncoding] If the content encoding is not supported
      # @return [String] The decoded message body
      def decode_body(body, properties)
        case properties.content_encoding
        when "gzip"
          decode_gzip(body)
        when "deflate"
          decode_deflate(body)
        when "", nil
          body
        else
          raise Error::UnsupportedContentEncoding, properties.content_encoding
        end
      end

      # Encode the message body based on content_type and content_encoding
      # This strategy will not encode any data that is already binary.
      # If that is not your desired behavior you should set your own encoding strategy
      # @param body [Object] The message body to encode
      # @param properties [Hash] Message properties hash containing content_type and content_encoding
      # @raise [Error::UnsupportedContentType] If the content type is not supported
      # @raise [Error::UnsupportedContentEncoding] If the content encoding is not supported
      # @return [String] The encoded message body
      def encode_body(body, properties)
        body = serialize_body(body, properties)

        # Return if data is already encoded
        # Actually checking if the data is already in the desired format
        # add too much overhead.
        return body if body.encoding == Encoding::BINARY

        case properties[:content_encoding]
        when "gzip"
          encode_gzip(body)
        when "deflate"
          encode_deflate(body)
        when "", nil
          body
        else
          raise Error::UnsupportedContentEncoding, properties[:content_encoding]
        end
      end

      # Serialize the message body based on content_type
      # @param body [Object] The message body to serialize
      # @param properties [Hash] Message properties hash containing content_type
      # @raise [Error::UnsupportedContentType] If the content type is not supported
      # @return [String] The serialized message body
      def serialize_body(body, properties)
        return body if body.is_a?(String)

        case properties[:content_type]
        when "application/json"
          JSON.dump(body)
        when "text/plain", "", nil
          body.to_s
        else
          raise Error::UnsupportedContentType, properties[:content_type]
        end
      end

      private

      def parse_json(data)
        JSON.parse(data, symbolize_names: true)
      end

      def encode_gzip(data)
        Zlib.gzip(data)
      end

      def decode_gzip(data)
        Zlib.gunzip(data)
      end

      def encode_deflate(data)
        Zlib.deflate(data)
      end

      def decode_deflate(data)
        Zlib.inflate(data)
      end
    end
  end
end
