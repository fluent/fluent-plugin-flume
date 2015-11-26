require 'rake'
require 'rake/testtask'
require 'rake/clean'

begin
  require 'jeweler'
  Jeweler::Tasks.new do |gemspec|
    gemspec.name = "fluent-plugin-flume"
    gemspec.summary = "Flume Input/Output plugin for Fluentd event collector"
    gemspec.author = "Muga Nishizawa"
    gemspec.email = "muga.nishizawa@gmail.com"
    gemspec.homepage = "https://github.com/muga/fluent-plugin-flume"
    gemspec.has_rdoc = false
    gemspec.require_paths = ["lib"]
    gemspec.add_development_dependency "test-unit", "~> 3.1"
    gemspec.add_dependency "fluentd", "~> 0.12.0"
    gemspec.add_dependency "thrift", "~> 0.9.0"
    gemspec.test_files = Dir["test/**/*.rb"]
    gemspec.files = Dir["bin/**/*", "lib/**/*", "test/**/*.rb"] +
      %w[example.conf VERSION AUTHORS Rakefile fluent-plugin-flume.gemspec]
    gemspec.executables = ['fluent-flume-remote']
  end
  Jeweler::GemcutterTasks.new
rescue LoadError
  puts "Jeweler not available. Install it with: gem install jeweler"
end

task "thrift_gen" do
  system "mkdir -p tmp"
  system "thrift --gen rb -o tmp lib/fluent/plugin/thrift/flume.thrift"
  system "mv tmp/gen-rb/* lib/fluent/plugin/thrift/"
  system "rm -fR tmp"
end

Rake::TestTask.new(:test) do |t|
  t.test_files = Dir['test/plugin/*.rb']
  t.ruby_opts = ['-rubygems'] if defined? Gem
  t.ruby_opts << '-I.'
end

#VERSION_FILE = "lib/fluent/version.rb"
#
#file VERSION_FILE => ["VERSION"] do |t|
#  version = File.read("VERSION").strip
#  File.open(VERSION_FILE, "w") {|f|
#    f.write <<EOF
#module Fluent
#
#VERSION = '#{version}'
#
#end
#EOF
#  }
#end
#
#task :default => [VERSION_FILE, :build]

task :default => [:build]
