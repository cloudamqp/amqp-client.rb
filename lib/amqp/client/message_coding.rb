# frozen_string_literal: true

require "zlib"
require "stringio"
require "json"

module AMQP
  class Client
    # @api private
    module MessageCoding
      private

      def parse_body(body, properties)
        decoded_data = decode_body(body, properties)
        case properties.content_type
        when "application/json" then parse_json(decoded_data)
        when "text/plain", "", nil then decoded_data
        else raise Error::UnsupportedContentType, properties.content_type
        end
      end

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

      def encode_body(body, properties)
        body = serialize_body(body, properties)

        # Return if data is already encoded
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
