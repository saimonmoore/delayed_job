require File.dirname(__FILE__) + '/spec_helper'
require File.dirname(__FILE__) + '/tokyo_database'

describe "TokyoStruct" do
  before(:each) do
    TokyoStruct.db.clear
  end
  it "should take a hash argument on initialization" do
    lambda {
      TokyoStruct.new({})
    }.should_not raise_error
  end
  
  it "should be a new record before being saved" do
    TokyoStruct.new.new_record?.should be_true
  end
  
  it "should not be a new record after being saved" do
    TokyoStruct.create.new_record?.should be_false
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
  
  it "should persist Date values as time since epoch number string" do
    created_at = Date.today
    instance = TokyoStruct.create({:created_at => created_at})
    instance.db[instance.id]['created_at'].should == created_at.to_time.to_i.to_s
  end
  
  it "should persist Time values as time since epoch number string" do
    created_at = Time.now
    instance = TokyoStruct.create({:created_at => created_at})
    instance.db[instance.id]['created_at'].should == created_at.to_i.to_s
  end
  
  it "should persist DateTime values as time since epoch number string" do
    t = Time.now
    created_at = DateTime.new(t.year, t.month, t.day, t.hour, t.min, t.sec)
    instance = TokyoStruct.create({:created_at => created_at})
    instance.db[instance.id]['created_at'].should == created_at.to_time.to_i.to_s
  end
  
  it "should persist symbol values as strings" do
    instance = TokyoStruct.create({:sym => :asymbol})
    instance.db[instance.id]['sym'].should == "asymbol"
  end
  
  it "should persist Numeric values as strings" do
    instance = TokyoStruct.create({:num => 1})
    instance.db[instance.id]['num'].should == "1"
  end
  
  it "should be able to find a persisted instance using an id argument to the find class method" do
    original_instance = TokyoStruct.new({:foo => 'bar'})
    original_instance.save
    
    TokyoStruct.find(original_instance.id).foo.should == 'bar'
  end
  
  it "should have the same id when found as when it was originally instantiated" do
    original_instance = TokyoStruct.new({:foo => 'bar'})
    original_instance.save
    
    TokyoStruct.find(original_instance.id).id.should == original_instance.id
  end
  
  it "should have the same data when found as when it was originally instantiated" do
    hash = {'foo' => 'bar'}
    original_instance = TokyoStruct.new(hash)
    original_instance.save
    
    found_instance = TokyoStruct.find(original_instance.id)
    found_instance.data.should == hash
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
  it "should be equal to the original instance when found via the find class method" do
    original_instance = TokyoStruct.new({:foo => 'bar'})
    original_instance.save
    
    TokyoStruct.find(original_instance.id).should == original_instance    
  end
  
  it "should be able to update all records which match the supplied conditions" do
    ids = [{:foo => 'bar'}, {:foo => 'bar'}, {:baz => 'boo'}].map do |hash|
      instance = TokyoStruct.new(hash)
      instance.save
      instance
    end
    
    TokyoStruct.update_all({:foo => 'baz'}, [
      ['foo', :equals, 'bar']
    ])
    
    [ids[0], ids[1]].map {|i| i.reload}.all? {|i| i.foo == 'baz'}.should be_true
  end
end
