# coding: utf-8
lib = File.expand_path('../lib/', __FILE__)
$:.unshift lib unless $:.include?(lib)
require 'ubiquity/s3/version'

Gem::Specification.new do |spec|
  spec.name          = 'ubiquity-s3'
  spec.version       = Ubiquity::S3::VERSION
  spec.authors       = ['John Whitson']
  spec.email         = ['john.whitson@gmail.com']
  spec.homepage      = 'http://github.com/XPlatform-Consulting/ubiquity-s3'
  spec.summary       = %q{A library to interact with Amazon S3.}
  spec.description   = %q{}

  spec.required_ruby_version     = '>= 1.8.7'
  #spec.required_rubygems_version = '>= 1.3.6'

  spec.files         = `git ls-files -z`.split("\x0")
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})

  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.require_paths = ['lib']

  #spec.add_dependency 'ubiquity'
  spec.add_dependency 'fog', '~> 1.19'
  spec.add_development_dependency 'rspec', '~> 2.99.0.beta1'

end