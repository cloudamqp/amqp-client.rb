# frozen_string_literal: true

require "json"
require "zlib"
require "stringio"

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
      # Uses instance-based GzipWriter/GzipReader over StringIO rather than the
      # Zlib.gzip/Zlib.gunzip module helpers. Those helpers share process-global
      # state in Ruby < 3.3 and corrupt each other when called from multiple
      # threads (e.g. concurrent consumers), raising Zlib::DataError.
      Gzip = Class.new do
        def encode(data, _properties)
          return data if data.encoding == Encoding::BINARY

          io = StringIO.new
          io.set_encoding(Encoding::BINARY)
          gz = Zlib::GzipWriter.new(io)
          begin
            gz.write(data)
          ensure
            gz.close
          end
          io.string
        end

        def decode(data, _properties)
          gz = Zlib::GzipReader.new(StringIO.new(data))
          begin
            gz.read
          ensure
            gz.close
          end
        end
      end.new

      # Instance-based for the same thread-safety reason as Gzip: the
      # Zlib.deflate/Zlib.inflate module helpers are not safe to call
      # concurrently on Ruby < 3.3.
      Deflate = Class.new do
        def encode(data, _properties)
          return data if data.encoding == Encoding::BINARY

          deflater = Zlib::Deflate.new
          begin
            deflater.deflate(data, Zlib::FINISH)
          ensure
            deflater.close
          end
        end

        def decode(data, _properties)
          inflater = Zlib::Inflate.new
          begin
            inflater.inflate(data)
          ensure
            inflater.close
          end
        end
      end.new

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
      end.new
    end
  end
end
