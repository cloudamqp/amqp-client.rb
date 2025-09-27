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
    end
  end
end
