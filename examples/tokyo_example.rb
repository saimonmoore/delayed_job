require 'delayed_job'
require 'rufus/tokyo'

Delayed.clear_storage_adapter
Delayed.setup_storage_adapter('tokyo_storage')
Delayed::Job.database = Rufus::Tokyo::Table.new("delayed_job.tdb")
Delayed::Job.database.clear
Delayed::Job.logger = Logger.new('/tmp/dj.log')

class SimpleJob
  def self.runs
    @@runs
  end
  
  def self.runs=(runs)
    @@runs = runs
  end  
  
  self.runs = 0
  
  def perform; @@runs += 1; end
end

Delayed::Job.enqueue SimpleJob.new
Delayed::Job.work_off
# Delayed::Worker.new.start
sleep 2
puts SimpleJob.runs