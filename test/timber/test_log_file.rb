require 'helper'

class TestLogFile < Test::Unit::TestCase
  context 'finding log files' do
    should 'search for objects in a bucket with a prefix' do
    end
    should 'return array of logfiles' do
    end
  end

  context 'reading log files' do
    setup do
      @o1 = AWS::S3::S3Object.new
      stub(@o1).value { "hello" }
      stub(@o1).size  { @o1.value.size }

      @o2 = AWS::S3::S3Object.new
      stub(@o2).value { "goodbye" }
      stub(@o2).size  { @o2.value.size }
    end

    context 'single log object' do
      should "return the whole file" do
        l = Timber::LogFile.new([@o1])
        assert_equal "hello", l.read(5)
      end

      should "return the whole file" do
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

