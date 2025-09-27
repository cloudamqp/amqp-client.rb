# frozen_string_literal: true

module AMQP
  class Client
    # All errors raised inherit from this class
    class Error < StandardError
      # Raised when a frame that wasn't expected arrives
      class UnexpectedFrameType < Error
        def initialize(expected, actual)
          super("Expected frame type '#{expected}' but got '#{actual}'")
        end
      end

      # Raised when a frame doesn't end with 206
      class UnexpectedFrameEnd < Error
        def initialize(actual)
          super("Expected frame end 206 but got '#{actual}'")
        end
      end

      # Should never be raised as we support all official frame types
      class UnsupportedFrameType < Error
        def initialize(type)
          super("Unsupported frame type '#{type}'")
        end
      end

      # Raised if a frame is received but not implemented
      class UnsupportedMethodFrame < Error
        def initialize(class_id, method_id)
          super("Unsupported class/method: #{class_id} #{method_id}")
        end
      end

      # Depending on close level a ConnectionClosed or ChannelClosed error is returned
      class Closed < Error
        def self.new(id, level, code, reason, classid = 0, methodid = 0)
          case level
          when :connection
            build_connection_error(code, reason, classid, methodid)
          when :channel
            build_channel_error(id, code, reason, classid, methodid)
          else raise ArgumentError, "invalid level '#{level}'"
          end
        end

        private_class_method def self.build_connection_error(code, reason, classid, methodid)
          klass = case code
                  when 320
                    ConnectionForced
                  when 501
                    FrameError
                  when 503
                    CommandInvalid
                  when 504
                    ChannelError
                  when 505
                    UnexpectedFrame
                  when 506
                    ResourceError
                  when 530
                    NotAllowedError
                  when 541
                    InternalError
                  else
                    ConnectionClosed
                  end
          klass.new(code, reason, classid, methodid)
        end

        private_class_method def self.build_channel_error(id, code, reason, classid, methodid)
          klass = case code
                  when 403
                    AccessRefused
                  when 404
                    NotFound
                  when 405
                    ResourceLocked
                  when 406
                    PreconditionFailed
                  else
                    ChannelClosed
                  end
          klass.new(id, code, reason, classid, methodid)
        end
      end

      # Raised if channel is already closed
      class ChannelClosed < Error
        def initialize(id, code, reason, classid = 0, methodid = 0)
          super("Channel[#{id}] closed (#{code}) #{reason} (#{classid}/#{methodid})")
        end
      end

      # Raised if connection is unexpectedly closed
      class ConnectionClosed < Error
        def initialize(code, reason, classid = 0, methodid = 0)
          super("Connection closed (#{code}) #{reason} (#{classid}/#{methodid})")
        end
      end

      class AccessRefused < ChannelClosed; end
      class NotFound < ChannelClosed; end
      class ResourceLocked < ChannelClosed; end
      class PreconditionFailed < ChannelClosed; end

      class ConnectionForced < ConnectionClosed; end
      class FrameError < ConnectionClosed; end
      class CommandInvalid < ConnectionClosed; end
      class ChannelError < ConnectionClosed; end
      class UnexpectedFrame < ConnectionClosed; end
      class ResourceError < ConnectionClosed; end
      class NotAllowedError < ConnectionClosed; end
      class InternalError < ConnectionClosed; end

      # Raised if trying to parse a message with an unsupported content type
      class UnsupportedContentType < Error
        def initialize(content_type)
          super("Unsupported content type #{content_type}")
        end
      end

      # Raised if trying to parse a message with an unsupported content encoding
      class UnsupportedContentEncoding < Error
        def initialize(content_encoding)
          super("Unsupported content encoding #{content_encoding}")
        end
      end
    end
  end
end
