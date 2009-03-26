require File.dirname(__FILE__) + '/spec_helper'
require File.dirname(__FILE__) + '/tokyo_database'

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

class ErrorJob
  def self.runs
    @@runs
  end
  
  def self.runs=(runs)
    @@runs = runs
  end  
  self.runs = 0
  def perform; raise 'did not work'; end
end             

module M
  class ModuleJob
    def self.runs
      @@runs
    end

    def self.runs=(runs)
      @@runs = runs
    end  
    self.runs = 0
    def perform; @@runs += 1; end    
  end
  
end

describe "Delayed::Job - Tokyo Cabinet experiment" do
  before  do               
    # Delayed::Job.max_priority = nil
    # Delayed::Job.min_priority = nil      
    # 
    # Delayed::Job.delete_all
  end
  
  before(:each) do
    @db = Rufus::Tokyo::Table.new('delayed_job.tdb')
    SimpleJob.runs = 0
  end
  
  it "should be able to generate a unique id" do
    @db.generate_unique_id.should_not be_nil
  end
  
  it "should be able to store the delayed_table job data" do
    # table.integer  :priority, :default => 0
    # table.integer  :attempts, :default => 0
    # table.text     :handler
    # table.string   :last_error
    # table.datetime :run_at
    # table.datetime :locked_at
    # table.string   :locked_by
    # table.datetime :failed_at
    # table.timestamps    
    tomorrow = (Time.now + (60 * 60 * 24))
    id = save_job({
      'priority' => 0.to_s,
      'attempts' => 0.to_s,
      'handler' => SimpleJob.new.to_yaml,
      'last_error' => 'some error',
      'run_at' => tomorrow.to_i.to_s,
      'locked_at' => Time.now.to_i.to_s,
      'locked_by' => 'someone',
      'failed_at' => 0.to_s,
      'created_at' => Time.now.to_i.to_s,
      'updated_at'=> Time.now.to_i.to_s
    })
    id.should_not be_nil
    
    # NextTaskSQL         = '(run_at <= ? AND (locked_at IS NULL OR locked_at < ?) OR (locked_by = ?)) AND failed_at IS NULL'
    # NextTaskOrder       = 'priority DESC, run_at ASC'    
    # sql = NextTaskSQL.dup
    # 
    # conditions = [time_now, time_now - max_run_time, worker_name]
    # 
    # if self.min_priority
    #   sql << ' AND (priority >= ?)'
    #   conditions << min_priority
    # end
    # 
    # if self.max_priority
    #   sql << ' AND (priority <= ?)'
    #   conditions << max_priority
    # end    
    results = @db.query { |q|
      q.add_condition('run_at', :numge, Time.now.to_i.to_s)
      q.add_condition('failed_at', :numequals, 0.to_s)
      q.order_by('priority', :numdesc)
    }.to_a
    results.should_not be_empty
    results.first['handler'].should == SimpleJob.new.to_yaml
  end

end

def save_job(opts = {})
  id = '%064d' % @db.generate_unique_id
  @db[id] = opts
  id
end
