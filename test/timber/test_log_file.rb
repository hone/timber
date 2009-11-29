require 'helper'

class TestLogFile < Test::Unit::TestCase
  context 'finding log files' do
    should 'search for objects in a bucket with a prefix' do
    end
    should 'return array of logfiles' do
    end
  end

  context 'metadata' do
    setup do
      @o1 = AWS::S3::S3Object.new
      stub(@o1).value { "hello" }
      @t   = Time.now
      ts  = (@t.to_f * 100_000).to_i
      stub(@o1).key   { "prefix/234a/#{ts}" }
      stub(@o1).size  { @o1.value.size }
      stub(@o1).metadata { {} }
    end

    should 'provide logs start time' do
      l = Timber::LogFile.new([@o1])
      assert_equal l.start_time.to_i, @t.to_i
    end

    should 'provide logs end time' do
      @o2 = AWS::S3::S3Object.new
      t2   = Time.now + 120
      ts  = (t2.to_f * 100_000).to_i
      stub(@o2).value { "hello" }
      stub(@o2).size  { @o2.value.size }
      stub(@o2).key   { "prefix/234a/#{ts}" }

      l = Timber::LogFile.new([@o1, @o2])
      assert_equal l.end_time.to_i, t2.to_i
    end

  end

  context 'reading log files' do
    setup do
      @o1 = AWS::S3::S3Object.new
      stub(@o1).value { "hello" }
      stub(@o1).key   { "prefix/234a/123" }
      stub(@o1).size  { @o1.value.size }
      stub(@o1).metadata { {} }

      @o2 = AWS::S3::S3Object.new
      stub(@o2).value { "goodbye" }
      stub(@o2).key   { "prefix/34sdf/456" }
      stub(@o2).size  { @o2.value.size }
      stub(@o2).metadata { {} }
    end

    context 'single log object' do
      should "return the whole file" do
        l = Timber::LogFile.new([@o1])
        assert_equal "hello", l.read(5)
      end

      should "return partial file" do
        l = Timber::LogFile.new([@o1])
        assert_equal "he", l.read(2)
      end
    end

    context 'multiple log objects' do
      should "read from both files" do
        l = Timber::LogFile.new([@o1, @o2])
        assert_equal "hellogoodbye", l.read(12)
      end

      should "do partial reads" do
        l = Timber::LogFile.new([@o1, @o2])
        assert_equal "he", l.read(2)
        assert_equal "ll", l.read(2)
        assert_equal "ogood", l.read(5)
      end
    end
  end
end

