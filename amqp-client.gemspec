# frozen_string_literal: true

require_relative "lib/amqp/client/version"

Gem::Specification.new do |spec|
  spec.name          = "amqp-client"
  spec.version       = AMQP::Client::VERSION
  spec.authors       = ["Carl Hörberg"]
  spec.email         = ["carl@84codes.com"]

  spec.summary       = "AMQP 0-9-1 client"
  spec.description   = "Work in progress"
  spec.homepage      = "https://github.com/cloudamqp/amqp-client.rb"
  spec.license       = "MIT"
  spec.required_ruby_version = Gem::Requirement.new(">= 2.4.0")

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = "#{spec.homepage}.git"
  spec.metadata["changelog_uri"] = "https://github.com/cloudamqp/amqp-client.rb/blob/main/CHANGELOG.md"

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  spec.files = Dir.chdir(File.expand_path(__dir__)) do
    `git ls-files -z`.split("\x0").reject { |f| f.match(%r{\A(?:test|spec|features)/}) }
  end
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{\Aexe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  # Uncomment to register a new dependency of your gem
  # spec.add_dependency "example-gem", "~> 1.0"

  # For more information and examples about making a new gem, checkout our
  # guide at: https://bundler.io/guides/creating_gem.html
end
