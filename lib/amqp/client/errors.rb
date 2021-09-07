# frozen_string_literal: true

module AMQP
  class Client
    # All errors raised inherit from this class
    class Error < StandardError
      # Raised when a frame that wasn't expected arrives
      class UnexpectedFrame < Error
        def initialize(expected, actual)
          super "Expected frame type '#{expected}' but got '#{actual}'"
        end
      end

      # Raised when a frame doesn't end with 206
      class UnexpectedFrameEnd < Error
        def initialize(actual)
          super "Expected frame end 206 but got '#{actual}'"
        end
      end

      # Should never be raised as we support all official frame types
      class UnsupportedFrameType < Error
        def initialize(type)
          super "Unsupported frame type '#{type}'"
        end
      end

      # Raised if a frame is received but not implemented
      class UnsupportedMethodFrame < Error
        def initialize(class_id, method_id)
          super "Unsupported class/method: #{class_id} #{method_id}"
        end
      end

      # Raised if channel is already closed
      class ChannelClosed < Error
        def initialize(id, code, reason, classid = 0, methodid = 0)
          super "Channel[#{id}] closed (#{code}) #{reason} (#{classid}/#{methodid})"
        end
      end

      # Raised if connection is unexpectedly closed
      class ConnectionClosed < Error
        def initialize(code, reason, classid = 0, methodid = 0)
          super "Connection closed (#{code}) #{reason} (#{classid}/#{methodid})"
        end
      end
    end
  end
end
