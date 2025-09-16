# frozen_string_literal: true

require "zlib"
require "stringio"
require "json"

module AMQP
  class Client
    # Methods for encoding/decoding message bodies
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
        sio = StringIO.new
        gz = Zlib::GzipWriter.new(sio)
        gz.write(data)
        gz.close
        sio.string
      end

      def decode_gzip(data)
        StringIO.open(data) do |io|
          gz = Zlib::GzipReader.new(io)
          begin
            return gz.read
          ensure
            gz.close
          end
        end
      end

      def encode_deflate(data)
        Zlib::Deflate.deflate(data)
      end

      def decode_deflate(data)
        inflater = Thread.current[:inflater_raw] ||= Zlib::Inflate.new(-15)
        inflater.inflate(data)
      rescue Zlib::DataError
        inflater = Thread.current[:inflater_zlib] ||= Zlib::Inflate.new
        inflater.inflate(data)
      ensure
        inflater&.reset
      end
    end
  end
end
