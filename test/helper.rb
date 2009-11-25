require 'rubygems'
require 'test/unit'
Bundler.require_env :test

$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
$LOAD_PATH.unshift(File.dirname(__FILE__))
require 'timber'

class Test::Unit::TestCase
  include RR::Adapters::TestUnit
end
