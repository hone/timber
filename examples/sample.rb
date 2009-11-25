require 'vendor/gems/environment'
require 'lib/timber/s3_appender'

log = Logging.logger['my logger']
s3_appender = Timber::S3Appender.new('logger', 'test_logging', 'log')
log.add_appenders(s3_appender)
log.level = :info

AWS::S3::Base.establish_connection!(
  :access_key_id     => ENV['AMAZON_ACCESS_KEY_ID'],
  :secret_access_key => ENV['AMAZON_SECRET_ACCESS_KEY']
)

1000.times do |n|
  log.info("Testing Timber")
end
s3_appender.close
