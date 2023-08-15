# frozen_string_literal: true

lib = File.expand_path('lib', __dir__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'juno/version'

Gem::Specification.new do |spec|
  spec.name          = 'juno'
  spec.version       = Juno::VERSION
  spec.authors       = ['PayPal Inc']
  spec.email         = ['paypal.com']

  spec.summary       = 'Ruby gem for Juno client'
  spec.description   = 'Ruby gem for Juno client'
  spec.homepage      = 'https://github.com/paypal/junodb'
  spec.license       = 'MIT'

  if spec.respond_to?(:metadata)
    spec.metadata['allowed_push_host'] = 'https://github.com/paypal/junodb'

    spec.metadata['homepage_uri'] = spec.homepage
    spec.metadata['source_code_uri'] = 'https://github.com/paypal/junodb'
    spec.metadata['changelog_uri'] = 'https://github.com/paypal/junodb'
  else
    raise 'RubyGems 2.0 or newer is required to protect against ' \
      'public gem pushes.'
  end

  spec.files = Dir.chdir(File.expand_path(__dir__)) do
    `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  end
  spec.bindir        = 'exe'
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ['lib']

  spec.add_development_dependency 'bundler'
  spec.add_development_dependency 'get_process_mem'
  spec.add_development_dependency 'rake'
  spec.add_development_dependency 'rspec'
  spec.add_development_dependency 'rspec-html-formatter'
  spec.add_development_dependency 'rubocop'
  spec.add_development_dependency 'ruby-prof'
  spec.add_development_dependency 'yard'

  spec.add_runtime_dependency 'bindata', '~> 2.4.15'
  spec.add_runtime_dependency 'concurrent-ruby', '~> 1.2.2'
  spec.add_runtime_dependency 'configatron', '~> 4.5.1'
  spec.add_runtime_dependency 'eventmachine', '~> 1.2.7'
  spec.add_runtime_dependency 'json', '~> 2.5.1'
  spec.add_runtime_dependency 'logger', '~> 1.2.7'
  spec.add_runtime_dependency 'openssl', '~> 2.2.0'
  spec.add_runtime_dependency 'snappy', '~> 0.3.0'
  spec.add_runtime_dependency 'uri', '~> 0.12.2'
  spec.add_runtime_dependency 'uuidtools', '~> 2.2.0'
  spec.add_runtime_dependency 'yaml', '~> 0.1.1'
end
