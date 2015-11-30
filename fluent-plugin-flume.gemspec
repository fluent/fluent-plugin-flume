$:.push File.expand_path('../lib', __FILE__)

Gem::Specification.new do |s|
  s.name          = 'fluent-plugin-flume'
  s.version       = File.read("VERSION").strip
  s.authors       = ["Muga Nishizawa"]
  s.email         = ["muga.nishizawa@gmail.com"]
  s.description   = "Flume Input/Output plugin for Fluentd event collector"
  s.summary       = s.description
  s.homepage      = 'https://github.com/fluent/fluent-plugin-flume'
  s.license       = 'MIT'
  s.has_rdoc    = false

  s.files         = `git ls-files`.split($/)
  s.executables   = s.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  s.test_files    = s.files.grep(%r{^(test|spec|features)/})
  s.require_paths = ['lib']

  s.add_runtime_dependency 'fluentd', '>= 0.10.43'
  s.add_runtime_dependency 'thrift', '~> 0.9.0'

  s.add_development_dependency 'rake', '>= 0.9.2'
  s.add_development_dependency 'test-unit', '~> 3.1.0'
end
