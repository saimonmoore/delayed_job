require 'rubygems'
require 'rufus/tokyo'

Spec::Runner.configure do |config|
  config.before do
    # Set the storage adapter to TokyoCabinet
    Delayed.clear_storage_adapter
    Delayed.setup_storage_adapter('tokyo_storage')
    Delayed::Job.logger = Logger.new('/tmp/dj.log')    
  end  
end