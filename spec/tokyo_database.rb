require 'rufus/tokyo'
require 'fileutils'

Spec::Runner.configure do |config|
  config.before do
    # Set the storage adapter to TokyoCabinet
    Delayed.clear_storage_adapter
    Delayed.setup_storage_adapter('tokyo_storage')
    
    # Create th db directory where databases will be stored
    db_dir = File.join(File.dirname(__FILE__), 'db')
    FileUtils.mkdir_p(File.join(File.dirname(__FILE__), 'db'))
    
    # You could easily set this to a Rufus::Tokyo::TyrantTable.new('localhost', 45002)
    Delayed::Job.database = Rufus::Tokyo::Table.new(File.join(db_dir, "delayed_job.tdb"))
    Delayed::Job.database.clear #clear out database on each run
    
    # set the logger
    Delayed::Job.logger = Logger.new('/tmp/dj.log')    
  end  
end