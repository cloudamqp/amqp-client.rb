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
        return "" if hash.nil? || hash.empty?

        arr = []
        fmt = String.new
        hash.each do |k, value|
          key = k.to_s
          arr.push(key.bytesize, key)
          fmt << "Ca*"
          encode_field(value, arr, fmt)
        end
        arr.pack(fmt)
      end

      # Decodes an AMQP table into a hash
      # @param bytes [String] Byte array the table is part of
      # @param pos [Integer] Position where the table starts
      # @param len [Integer] Length of the table
      # @return [Hash<String, Object>]
      def self.decode(bytes, pos = 0, len = bytes.bytesize - pos)
        hash = {}
        table_end = pos + len
        while pos < table_end
          key_len = bytes.getbyte(pos)
          pos += 1
          key = bytes.byteslice(pos, key_len).force_encoding(Encoding::UTF_8).freeze # Hash won't have to dup it
          pos += key_len
          len, value = decode_field(bytes, pos)
          pos += len + 1
          hash[key] = value
        end
        hash
      end

      # Encoding a single value in a table
      # @return [nil]
      # @api private
      def self.encode_field(value, arr, fmt)
        case value
        when Integer
          if value > 2**31
            arr.push("l", value)
            fmt << "aq>"
          else
            arr.push("I", value)
            fmt << "al>"
          end
        when Float
          arr.push("d", value)
          fmt << "aG"
        when String
          arr.push("S", value.bytesize, value)
          fmt << "aL>a*"
        when Time
          arr.push("T", value.to_i)
          fmt << "aQ>"
        when Array
          value_arr = []
          value_fmt = String.new
          value.each { |e| encode_field(e, value_arr, value_fmt) }
          bytes = value_arr.pack(value_fmt)
          arr.push("A", bytes.bytesize, bytes)
          fmt << "aL>a*"
        when Hash
          bytes = Table.encode(value)
          arr.push("F", bytes.bytesize, bytes)
          fmt << "aL>a*"
        when true
          arr.push("t", 1)
          fmt << "aC"
        when false
          arr.push("t", 0)
          fmt << "aC"
        when nil
          arr << "V"
          fmt << "a"
        else raise ArgumentError, "unsupported table field type: #{value.class}"
        end
        nil
      end

      # Decodes a single value
      # @return [Array<Integer, Object>] Bytes read and the parsed value
      # @api private
      def self.decode_field(bytes, pos)
        type = bytes.getbyte(pos)
        pos += 1
        case type # matched by byte value, avoids allocating a string per field
        when 83 # "S" long string
          len = bytes.unpack1("L>", offset: pos)
          pos += 4
          [4 + len, bytes.byteslice(pos, len).force_encoding(Encoding::UTF_8)]
        when 70 # "F" nested table
          len = bytes.unpack1("L>", offset: pos)
          pos += 4
          [4 + len, decode(bytes, pos, len)]
        when 65 # "A" array
          len = bytes.unpack1("L>", offset: pos)
          pos += 4
          array_end = pos + len
          a = []
          while pos < array_end
            length, value = decode_field(bytes, pos)
            pos += length + 1
            a << value
          end
          [4 + len, a]
        when 116 # "t" boolean
          [1, bytes.getbyte(pos) == 1]
        when 98 # "b" signed 8-bit
          [1, bytes.unpack1("c", offset: pos)]
        when 66 # "B" unsigned 8-bit
          [1, bytes.unpack1("C", offset: pos)]
        when 115 # "s" signed 16-bit
          [2, bytes.unpack1("s>", offset: pos)]
        when 117 # "u" unsigned 16-bit
          [2, bytes.unpack1("S>", offset: pos)]
        when 73 # "I" signed 32-bit
          [4, bytes.unpack1("l>", offset: pos)]
        when 105 # "i" unsigned 32-bit
          [4, bytes.unpack1("L>", offset: pos)]
        when 108 # "l" signed 64-bit
          [8, bytes.unpack1("q>", offset: pos)]
        when 102 # "f" float
          [4, bytes.unpack1("g", offset: pos)]
        when 100 # "d" double
          [8, bytes.unpack1("G", offset: pos)]
        when 68 # "D" decimal
          scale = bytes.getbyte(pos)
          pos += 1
          value = bytes.unpack1("L>", offset: pos)
          d = value / (10**scale)
          [5, d]
        when 120 # "x" byte array
          len = bytes.unpack1("L>", offset: pos)
          [4 + len, bytes.byteslice(pos + 4, len)]
        when 84 # "T" timestamp
          [8, Time.at(bytes.unpack1("Q>", offset: pos))]
        when 86 # "V" void
          [0, nil]
        else raise ArgumentError, "unsupported table field type: #{type&.chr}"
        end
      end
    end
  end
end
