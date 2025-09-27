# frozen_string_literal: true

require_relative "../amqp/client"

AMQP::Client.codec_registry
            .enable_builtin_codecs
