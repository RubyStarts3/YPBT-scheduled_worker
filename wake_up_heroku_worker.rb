# frozen_string_literal: true
require 'ostruct'
require 'http'
require 'yaml'
require 'aws-sdk'

## Wake up Heroku
class WakeUpHerokuWorker
  def initialize(config_file)
    @config = worker_configuration(config_file)
  end

  def call
    wake_up_heroku
  end

  private

  def worker_configuration(config_file)
    puts "CONFIG_FILE: #{config_file}"
    config = OpenStruct.new YAML.load(File.read(config_file))
    puts "YPBT_APP: #{config.YPBT_APP}"
    config
  end

  def wake_up_heroku
    HTTP.get("#{@config.YPBT_APP}")
  end
end

begin
  WakeUpHerokuWorker.new(ENV['CONFIG_FILE']).call
  puts 'STATUS: SUCCESS'
rescue => e
  puts "STATUS: ERROR (#{e.inspect})"
end
