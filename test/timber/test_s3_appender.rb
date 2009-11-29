require 'helper'

class TestS3Appender < Test::Unit::TestCase
  context "S3 key" do
    should "be prefix/uuid/timestamp" do
      t = Time.now
      stub(Time).now { t }
      stub(UUID).generate { 'myuuid' }

      mock(AWS::S3::S3Object).store("web/myuuid/#{(t.to_f * 100_000).to_i.to_s}", 'logit', 'bucket', anything)

      appender = Timber::S3Appender.new('blah', 'bucket', 'web', :event_buffer => 1)
      appender.write('logit')
    end
  end

  context "metadata" do
    should "include hostname and process id by default" do
      hostname = `hostname`.chomp
      mock(AWS::S3::S3Object).store(anything, anything, anything, hash_including("x-amz-meta-hostname" => hostname, "x-amz-meta-pid" => $$))

      appender = Timber::S3Appender.new('blah', 'bucket', 'web', :event_buffer => 1)
      appender.write('logit')
    end
  end

  context "pushing conditions" do
    should "push to s3 after 50 writes by default" do
      appender = Timber::S3Appender.new('blah', 'bucket', 'prefix')

      dont_allow(AWS::S3::S3Object).store.with_any_args
      49.times { appender.write('hello') }

      mock(AWS::S3::S3Object).store.with_any_args
      appender.write('hello')
    end

    should "push to s3 based on size" do
      appender = Timber::S3Appender.new('blah', 'bucket', 'prefix', :size => 1024, :age => 0, :event_buffer => 0)
      
      dont_allow(AWS::S3::S3Object).store.with_any_args
      1023.times { appender.write('h') }

      mock(AWS::S3::S3Object).store.with_any_args
      appender.write('h')
    end

    should "push to s3 based on time" do
      appender = Timber::S3Appender.new('blah', 'bucket', 'prefix', :size => 0, :age => 2, :event_buffer => 0)
      
      t = Time.now

      called = false
      mock(AWS::S3::S3Object).store.with_any_args do 
        elapsed = Time.now - t
        if elapsed > 2 && elapsed < 2.1
          called = true
        end
      end

      250.times { 
        sleep 0.01
        appender.write('h') 
      }
      assert called
    end
  end
end

