# frozen_string_literal: true

require_relative "lib/amqp/client/version"

Gem::Specification.new do |spec|
  spec.name          = "amqp-client"
  spec.version       = AMQP::Client::VERSION
  spec.authors       = ["CloudAMQP"]
  spec.email         = ["team@cloudamqp.com"]

  spec.summary       = "Modern, fast and dependency-free AMQP 0-9-1 Ruby client for RabbitMQ and LavinMQ"
  spec.description   = "A modern AMQP 0-9-1 Ruby client for RabbitMQ, LavinMQ and any other AMQP 0-9-1 broker. " \
                       "Very fast, fully thread-safe, with blocking operations, straight-forward " \
                       "error handling and no dependencies."
  spec.homepage      = "https://github.com/cloudamqp/amqp-client.rb"
  spec.license       = "MIT"
  spec.required_ruby_version = Gem::Requirement.new(">= 3.3.0")

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = "#{spec.homepage}.git"
  spec.metadata["changelog_uri"] = "https://github.com/cloudamqp/amqp-client.rb/blob/main/CHANGELOG.md"
  spec.metadata["documentation_uri"] = "https://cloudamqp.github.io/amqp-client.rb/"
  spec.metadata["bug_tracker_uri"] = "#{spec.homepage}/issues"
  spec.metadata["rubygems_mfa_required"] = "true"

  spec.files         = Dir["LICENSE.txt", "lib/**/*.rb"]
  spec.require_paths = ["lib"]
end
