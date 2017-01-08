# frozen_string_literal: true
require 'ostruct'
require 'http'
require 'yaml'
require 'aws-sdk'

## Scheduled worker regularly check queue, do statistics, and update to newest
class StatisticsWorker
  def initialize(config_file)
    @config = worker_configuration(config_file)
    setup_environment_variables
    @sqs_worker = Aws::SQS::Client.new
    @sqs_stats = Aws::SQS::Client.new
  end

  def call
    do_statistics
  end

  private

  def worker_configuration(config_file)
    puts "CONFIG_FILE: #{config_file}"
    config = OpenStruct.new YAML.load(File.read(config_file))
    puts "AWS_REGION: #{config.AWS_REGION}"
    config
  end

  def setup_environment_variables
    ENV['AWS_REGION'] = @config.AWS_REGION
    ENV['AWS_ACCESS_KEY_ID'] = @config.AWS_ACCESS_KEY_ID
    ENV['AWS_SECRET_ACCESS_KEY'] = @config.AWS_SECRET_ACCESS_KEY
  end

  def do_statistics
    stats_click = {}

    poller = Aws::SQS::QueuePoller.new(find_worker_queue_url)
    poller.poll(wait_time_seconds: nil, idle_timeout: 5) do |msg|
      params = JSON.parse(msg.body)
      video_id   = params['video_id']
      timetag_id = params['timetag_id']
      property   = params['property']
      key = "#{video_id}_#{timetag_id}"

      stats_click[key] = add_one(stats_click[key]) if property == "click"
    end
    puts stats_click

    summary = {}
    summary[:click] = summary_property(stats_click)
    puts summary

    puts "SENDING TO STATISTICS QUEUE"
    begin
      res = @sqs_stats.send_message(queue_url: find_stats_queue_url,
                                   message_body: summary.to_json)
      puts res.inspect
    rescue => e
      puts "SQS ERROR: #{e}"
    end
  end

  def find_worker_queue_url
    @sqs_worker.get_queue_url(queue_name: @config.WORKER_QUEUE).queue_url
  end

  def find_stats_queue_url
    @sqs_stats.get_queue_url(queue_name: @config.STATS_QUEUE).queue_url
  end

  def add_one(hash_value)
    hash_value.nil? ? 1 : hash_value + 1
  end

  def summary_property(stats_property)
    stats_property.sort_by { |key, val| val }.reverse.first(10)
    summary_property = stats_property.map do |item|
      data = item.first.split('_').push(item.last)
      { video_id: data[0], timetag_id: data[1], count: data[2] }
    end
  end
end

begin
  ENV['CONFIG_FILE'] = "config.yml"
  StatisticsWorker.new(ENV['CONFIG_FILE']).call
  puts 'STATUS: SUCCESS'
rescue => e
  puts "STATUS: ERROR (#{e.inspect})"
end
