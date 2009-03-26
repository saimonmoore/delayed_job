require File.dirname(__FILE__) + '/spec_helper'
require File.dirname(__FILE__) + '/tokyo_database'
require 'ostruct'

class TokyoStruct < OpenStruct
  @@db ||= Rufus::Tokyo::Table.new("#{self.class.name}.tdb")
  
  def initialize(hash = nil)
    super(hash)
  end
  
  def self.find(args)
    case args
    when String
      instance = self.new(db[args])
      instance.instance_variable_set('@id', args)
      instance
    when Hash
      conditions = args[:conditions]
      query_results = db.query { |q|
        conditions.each do |condition|
          q.pk_only
          q.add_condition(*condition)
        end
      }
      query_results.map {|pk| find(pk) }
    end
  end
  
  def self.create(hash = nil)
    self.new(hash)
  end
  
  def id
    @id ||= '%064d' % db.generate_unique_id
  end
  
  def save
    db[id] = stringify(table)
  end

  def db
    self.class.db
  end
  
  def self.db
    @@db
  end

  protected
  
    def stringify(hash)
      h = {}
      hash.each do |k,v|
        h[k.to_s] = v.to_s
      end
      h
    end
end

describe TokyoStruct do
  before(:each) do
    TokyoStruct.db.clear
  end
  it "should take a hash argument on initialization" do
    lambda {
      TokyoStruct.new({})
    }.should_not raise_error
  end
  
  it "should respond to any keys from supplied hash argument" do
    TokyoStruct.new({'foo' => 'bar'}).should respond_to(:foo)
  end
  
  it "should respond to the :id message" do
    TokyoStruct.new.should respond_to(:id)
  end
  
  it "should generate a unique id on initialization" do
    set = Set.new
    10.times {
      set.add?(TokyoStruct.new.id).should_not be_nil
    }
  end
  
  it "should be able to persist it's data via the save method" do
    struct = TokyoStruct.new({:foo => 'bar'})
    struct.save
    struct.db[struct.id]['foo'].should == 'bar'
  end
  
  it "should be able to find a persisted instance using an id argument to the find class method" do
    original_instance = TokyoStruct.new({:foo => 'bar'})
    original_instance.save
    
    TokyoStruct.find(original_instance.id).foo.should == 'bar'
  end
  
  it "should be able to create a new instance via the create class method" do
    TokyoStruct.create.should be_instance_of(TokyoStruct)
  end
  
  it "should be able to find instances by using a :conditions hash argument to the find class method" do
    ids = [{:foo => 'bar'}, {:foo => 'bar'}, {:baz => 'boo'}].map do |hash|
      instance = TokyoStruct.new(hash)
      instance.save
      instance.id
    end
    
    TokyoStruct.find(:conditions => [
      ['foo', :equals, 'bar']
    ]).map {|i| i.id}.should == [ids[0], ids[1]]
  end
end

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
