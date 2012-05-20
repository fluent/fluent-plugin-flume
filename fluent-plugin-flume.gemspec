# Generated by jeweler
# DO NOT EDIT THIS FILE DIRECTLY
# Instead, edit Jeweler::Tasks in Rakefile, and run 'rake gemspec'
# -*- encoding: utf-8 -*-

Gem::Specification.new do |s|
  s.name = "fluent-plugin-flume"
  s.version = "0.1.0"

  s.required_rubygems_version = Gem::Requirement.new(">= 0") if s.respond_to? :required_rubygems_version=
  s.authors = ["Muga Nishizawa"]
  s.date = "2012-05-20"
  s.email = "muga.nishizawa@gmail.com"
  s.executables = ["fluent-flume-remote"]
  s.extra_rdoc_files = [
    "ChangeLog",
    "README.rdoc"
  ]
  s.files = [
    "AUTHORS",
    "Rakefile",
    "VERSION",
    "bin/fluent-flume-remote",
    "fluent-plugin-flume.gemspec",
    "lib/fluent/plugin/in_flume.rb",
    "lib/fluent/plugin/out_flume.rb",
    "lib/fluent/plugin/thrift/flume.thrift",
    "lib/fluent/plugin/thrift/flume.thrift.orig",
    "lib/fluent/plugin/thrift/flume_constants.rb",
    "lib/fluent/plugin/thrift/flume_types.rb",
    "lib/fluent/plugin/thrift/thrift_flume_event_server.rb",
    "test/plugin/test_in_flume.rb",
    "test/plugin/test_out_flume.rb"
  ]
  s.homepage = "https://github.com/muga/fluent-plugin-flume"
  s.require_paths = ["lib"]
  s.rubygems_version = "1.8.15"
  s.summary = "Flume Input/Output plugin for Fluentd event collector"
  s.test_files = ["test/plugin/test_in_flume.rb", "test/plugin/test_out_flume.rb"]

  if s.respond_to? :specification_version then
    s.specification_version = 3

    if Gem::Version.new(Gem::VERSION) >= Gem::Version.new('1.2.0') then
      s.add_runtime_dependency(%q<fluentd>, ["~> 0.10.16"])
      s.add_runtime_dependency(%q<thrift>, ["~> 0.8.0"])
    else
      s.add_dependency(%q<fluentd>, ["~> 0.10.16"])
      s.add_dependency(%q<thrift>, ["~> 0.8.0"])
    end
  else
    s.add_dependency(%q<fluentd>, ["~> 0.10.16"])
    s.add_dependency(%q<thrift>, ["~> 0.8.0"])
  end
end

