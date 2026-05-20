# frozen_string_literal: true

require "json"
require "zlib"

module AMQP
  class Client
    module Parsers
      # Plain text passthrough parser
      Plain = Class.new do
        def parse(data, _properties) = data
        def serialize(obj, _properties) = obj.is_a?(String) ? obj : obj.to_s
      end.new

      JSONParser = Class.new do
        def parse(data, _properties) = ::JSON.parse(data, symbolize_names: true)
        def serialize(obj, _properties) = ::JSON.dump(obj)
      end.new
    end

    module Coders
      Gzip = Class.new do
        def encode(data, _properties)
          return data if data.encoding == Encoding::BINARY

          Zlib.gzip(data)
        end

        def decode(data, _properties) = Zlib.gunzip(data)
      end.new

      Deflate = Class.new do
        def encode(data, _properties)
          return data if data.encoding == Encoding::BINARY

          Zlib.deflate(data)
        end

        def decode(data, _properties) = Zlib.inflate(data)
      end.new

      # Raw DEFLATE (RFC 1951): no zlib header, no Adler-32 checksum.
      # Use when peers send Content-Encoding: deflate the HTTP-ambiguous way
      # (e.g. Node's zlib.deflateRaw). Not registered by default; opt in by
      # registering it under the desired content_encoding.
      DeflateRaw = Class.new do
        def encode(data, _properties)
          return data if data.encoding == Encoding::BINARY

          deflater = Zlib::Deflate.new(Zlib::DEFAULT_COMPRESSION, -Zlib::MAX_WBITS)
          begin
            deflater.deflate(data, Zlib::FINISH)
          ensure
            deflater.close
          end
        end

        def decode(data, _properties)
          inflater = Zlib::Inflate.new(-Zlib::MAX_WBITS)
          begin
            inflater.inflate(data)
          ensure
            inflater.close
          end
        end
      end.new
    end
  end
end
