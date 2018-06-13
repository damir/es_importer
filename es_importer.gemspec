lib = File.expand_path("../lib", __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require "es_importer/version"

Gem::Specification.new do |spec|
  spec.name          = "es_importer"
  spec.version       = EsImporter::VERSION
  spec.authors       = ["Damir Roso"]
  spec.email         = ["damir.roso@nih.gov"]

  spec.summary       = %q{Transform and import JSON documents into elastic search.}
  spec.description   = %q{Transform and import JSON documents into elastic search. Configure indices and transformations with ruby hash.}
  spec.homepage      = "https://github.com/damir/es_importer"
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0").reject do |f|
    f.match(%r{^(test|spec|features)/})
  end
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.16"
  spec.add_development_dependency "rake", "~> 10.0"
  spec.add_development_dependency "rspec", "~> 3.0"

  spec.add_dependency 'aws-sdk-dynamodb' # to load AWS credentials from the client
  spec.add_dependency 'elasticsearch'
  spec.add_dependency 'faraday'
  spec.add_dependency 'faraday_middleware'
  spec.add_dependency 'faraday_middleware-aws-sigv4'
  spec.add_dependency 'typhoeus'
end
