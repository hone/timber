require 'helper'

class TestS3Appender < Test::Unit::TestCase
  should "push to s3 after 50 writes by default" do
    appender = Timber::S3Appender.new('blah', 'bucket', 'prefix')

    dont_allow(AWS::S3::S3Object).store.with_any_args
    49.times { appender.write('hello') }

    mock(AWS::S3::S3Object).store.with_any_args
    appender.write('hello')
  end
end

