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
      end.new.freeze

      JSONParser = Class.new do
        def parse(data, _properties) = ::JSON.parse(data, symbolize_names: true)
        # JSON.dump's options, json < 3 reads them from a class instance variable that other Ractors can't read
        def serialize(obj, _properties) = ::JSON.generate(obj, max_nesting: false, allow_nan: true)
      end.new.freeze
    end

    module Coders
      Gzip = Class.new do
        def encode(data, _properties)
          return data if data.encoding == Encoding::BINARY

          Zlib.gzip(data)
        end

        def decode(data, _properties) = Zlib.gunzip(data)
      end.new.freeze

      Deflate = Class.new do
        def encode(data, _properties)
          return data if data.encoding == Encoding::BINARY

          Zlib.deflate(data)
        end

        def decode(data, _properties) = Zlib.inflate(data)
      end.new.freeze

      # Raw DEFLATE coder (RFC 1951 -- no zlib header, no Adler-32 checksum).
      # Not registered by default. Use to interoperate with producers that emit
      # raw DEFLATE under content_encoding "deflate", the same ambiguity HTTP
      # has carried for decades and that Zlib::Deflate.new(level, -MAX_WBITS)
      # exists for.
      #
      # @example Override the built-in deflate coder
      #   AMQP::Client.configure do |config|
      #     config.enable_builtin_codecs
      #     config.register_coder(content_encoding: "deflate",
      #                           coder: AMQP::Client::Coders::DeflateRaw)
      #   end
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
      end.new.freeze
    end
  end
end
