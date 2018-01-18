Gem::Specification.new do |s|
  s.name        = 'tablestore-ruby-sdk'
  s.version     = '0.0.6'
  s.date        = '2018-01-18'
  s.summary     = "tablestore"
  s.description = "A simple tablestore gem"
  s.authors     = ["seveninches"]
  s.email       = 'lijinghao333@aliyun.com'
  s.files       = Dir.glob('lib/**') + Dir.glob('lib/**/**')
  s.homepage    ='http://rubygems.org/gems/tablestore-ruby-sdk'
  s.license     = 'MIT'
  s.add_runtime_dependency "rest-client", "~> 2.0"
  s.add_runtime_dependency "ruby_protobuf", "~> 0.4", ">=0.4.11"
  s.add_runtime_dependency "digest-crc", "~> 0.4"
  s.add_runtime_dependency "os", "~> 1.0"
end