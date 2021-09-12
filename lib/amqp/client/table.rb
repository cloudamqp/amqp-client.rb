# frozen_string_literal: true

module AMQP
  class Client
    # Encode and decode an AMQP table to/from hash, only used internally
    # @api private
    module Table
      # Encodes a hash into a byte array
      # @param hash [Hash]
      # @return [String] Byte array
      def self.encode(hash)
        return "" if hash.empty?

        arr = []
        fmt = StringIO.new
        hash.each do |k, value|
          key = k.to_s
          raise ArgumentError, "Header key too long" if key.bytesize > 255

          arr.push(key.bytesize, key)
          fmt << "Ca*"
          encode_field(value, arr, fmt)
        end
        arr.pack(fmt.string)
      end

      # Decodes an AMQP table into a hash
      # @return [Hash<String, Object>]
      def self.decode(bytes)
        hash = {}
        pos = 0
        while pos < bytes.bytesize
          key_len = bytes.getbyte(pos)
          pos += 1
          key = bytes.byteslice(pos, key_len).force_encoding("utf-8")
          pos += key_len
          len, value = decode_field(bytes, pos)
          pos += len + 1
          hash[key] = value
        end
        hash
      end

      # Encoding a single value in a table
      # @return [void]
      # @api private
      def self.encode_field(value, arr, fmt)
        case value
        when Integer
          if value > 2**31
            arr.push("l", value)
            fmt << "a q>"
          else
            arr.push("I", value)
            fmt << "a l>"
          end
        when Float
          arr.push("d", value)
          fmt << "a G"
        when String
          arr.push("S", value.bytesize, value)
          fmt << "a L> a*"
        when Time
          arr.push("T", value.to_i)
          fmt << "a Q>"
        when Array
          value_arr = []
          value_fmt = StringIO.new
          value.each { |e| encode_field(e, value_arr, value_fmt) }
          bytes = value_arr.pack(value_fmt.string)
          arr.push("A", bytes.bytesize, bytes)
          fmt << "a L> a*"
        when Hash
          bytes = Table.encode(value)
          arr.push("F", bytes.bytesize, bytes)
          fmt << "a L> a*"
        when true
          arr.push("t", 1)
          fmt << "a C"
        when false
          arr.push("t", 0)
          fmt << "a C"
        when nil
          arr.push("V")
          fmt << "a"
        else raise ArgumentError, "unsupported table field type: #{value.class}"
        end
      end

      # Decodes a single value
      # @return [Array<Integer, Object>] Bytes read and the parsed value
      # @api private
      def self.decode_field(bytes, pos)
        type = bytes[pos]
        pos += 1
        case type
        when "S"
          len = bytes.byteslice(pos, 4).unpack1("L>")
          pos += 4
          [4 + len, bytes.byteslice(pos, len).force_encoding("utf-8")]
        when "F"
          len = bytes.byteslice(pos, 4).unpack1("L>")
          pos += 4
          [4 + len, decode(bytes.byteslice(pos, len))]
        when "A"
          len = bytes.byteslice(pos, 4).unpack1("L>")
          pos += 4
          array_end = pos + len
          a = []
          while pos < array_end
            length, value = decode_field(bytes, pos)
            pos += length + 1
            a << value
          end
          [4 + len, a]
        when "t"
          [1, bytes.getbyte(pos) == 1]
        when "b"
          [1, bytes.byteslice(pos, 1).unpack1("c")]
        when "B"
          [1, bytes.byteslice(pos, 1).unpack1("C")]
        when "s"
          [2, bytes.byteslice(pos, 2).unpack1("s")]
        when "u"
          [2, bytes.byteslice(pos, 2).unpack1("S")]
        when "I"
          [4, bytes.byteslice(pos, 4).unpack1("l>")]
        when "i"
          [4, bytes.byteslice(pos, 4).unpack1("L>")]
        when "l"
          [8, bytes.byteslice(pos, 8).unpack1("q>")]
        when "f"
          [4, bytes.byteslice(pos, 4).unpack1("g")]
        when "d"
          [8, bytes.byteslice(pos, 8).unpack1("G")]
        when "D"
          scale = bytes.getbyte(pos)
          pos += 1
          value = bytes.byteslice(pos, 4).unpack1("L>")
          d = value / 10**scale
          [5, d]
        when "x"
          len = bytes.byteslice(pos, 4).unpack1("L>")
          [4 + len, bytes.byteslice(pos, len)]
        when "T"
          [8, Time.at(bytes.byteslice(pos, 8).unpack1("Q>"))]
        when "V"
          [0, nil]
        else raise ArgumentError, "unsupported table field type: #{type}"
        end
      end
    end
  end
end
