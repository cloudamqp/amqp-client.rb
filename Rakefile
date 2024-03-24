# frozen_string_literal: true

require "bundler/gem_tasks"
require "rake/testtask"

Rake::TestTask.new(:test) do |t|
  t.description = "Run all but TLS tests"
  t.options = "--exclude=/_tls$/"
  t.pattern = "test/**/*_test.rb"
end

namespace :test do
  Rake::TestTask.new(:all) do |t|
    t.description = "Run all tests"
    t.pattern = "test/**/*_test.rb"
  end
end

require "rubocop/rake_task"

RuboCop::RakeTask.new do |task|
  task.requires << "rubocop-minitest"
end

require "yard"

YARD::Rake::YardocTask.new

task default: [:test, *(:rubocop if RUBY_ENGINE == "ruby")]
