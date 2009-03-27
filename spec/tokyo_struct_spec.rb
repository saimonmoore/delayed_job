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

  it "should be able to remove the persitance entry via the destroy method" do
    instance = TokyoStruct.create({:foo => 'bar'})
    instance.destroy
    TokyoStruct.find(instance.id).should be_nil
  end

  it "should store it's id as an entry in it's data" do
    instance = TokyoStruct.create({:foo => 'bar'})
    TokyoStruct.find(instance.id).db[instance.id]['id'].should == instance.id
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

  it "should persist all other objects as yaml" do
    instance = TokyoStruct.create({:array => [1,2,3]})
    instance.db[instance.id]['array'].should == "--- \n- 1\n- 2\n- 3\n"
  end

  it "should be able to find a persisted instance using an id argument to the find class method" do
    original_instance = TokyoStruct.new({:foo => 'bar'})
    original_instance.save

    TokyoStruct.find(original_instance.id).foo.should == 'bar'
  end

  it "should be able to find a entries when value of key is nil" do
    original_instance = TokyoStruct.new({:foo => nil})
    original_instance.save

    results = TokyoStruct.find(:conditions => [['foo', :equals, '']])
    results.should_not be_empty
  end

  it "should not find entries for instances that have never been persisted" do
    TokyoStruct.find(TokyoStruct.new.id).should be_nil
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

  it "should be able to find all entries by using the :all option to the find class method" do
    ids = [{:foo => 'bar'}, {:foo => 'bar'}, {:baz => 'boo'}].map do |hash|
      instance = TokyoStruct.new(hash)
      instance.save
      instance.id
    end

    TokyoStruct.find(:all).map {|i| i.id}.should == ids
  end

  it "should be able to find all entries by using the :all option and sorted via the :order option to the find class method" do
        ids = [{:foo => 'bar', :priority => 3}, {:foo => 'bar', :priority => 2}, {:baz => 'boo', :priority => 1}].map do |hash|
      instance = TokyoStruct.new(hash)
      instance.save
      instance.id
    end

    TokyoStruct.find(:all, :order => ['priority', :numasc]).map {|i| i.id}.should == ids.reverse
  end

  it "should be able to sort results by using the :order option to the find class method" do
    ids = [{:foo => 'bar', :priority => 3}, {:foo => 'bar', :priority => 2}, {:baz => 'boo', :priority => 1}].map do |hash|
      instance = TokyoStruct.new(hash)
      instance.save
      instance.id
    end

    TokyoStruct.find(:conditions => [
      ['foo', :equals, 'bar']
    ], :order => ['priority', :numasc]).map {|i| i.id}.should == [ids[1], ids[0]]
  end

  it "should be able to limit results by using the :limit option to the find class method" do
    ids = [{:foo => 'bar', :priority => 3}, {:foo => 'bar', :priority => 2}, {:baz => 'boo', :priority => 1}].map do |hash|
      instance = TokyoStruct.new(hash)
      instance.save
      instance.id
    end

    TokyoStruct.find(:conditions => [
      ['foo', :equals, 'bar']
    ], :order => ['priority', :numasc], :limit => 1).map {|i| i.id}.should == [ids[1]]
  end

  it "should be able to offset results which have been limited by using the :offset option to the find class method" do
    ids = [{:foo => 'bar', :priority => 3}, {:foo => 'bar', :priority => 2}, {:baz => 'boo', :priority => 1}].map do |hash|
      instance = TokyoStruct.new(hash)
      instance.save
      instance.id
    end

    TokyoStruct.find(:conditions => [
      ['foo', :equals, 'bar']
    ], :order => ['priority', :numasc], :limit => 1, :offset => 1).map {|i| i.id}.should == [ids[0]]
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

  it "should be able to override property accessors" do
    class TokyoFoo < TokyoStruct
      def run_at
        data['run_at'] ? DateTime.parse(Time.at(data['run_at'].to_i).to_s) : nil
      end
    end
    now = DateTime.parse(Time.now.to_s)
    foo = TokyoFoo.create(:run_at => now)

    fu = TokyoFoo.find(foo.id)
    fu.run_at.should == now
  end

  it "should be able to find instances via their id using a :conditions hash argument to the find class method" do
    ids = [{:foo => 'boo'}, {:foo => 'bar'}, {:baz => 'bif'}].map do |hash|
      instance = TokyoStruct.new(hash)
      instance.save
      instance.id
    end

    TokyoStruct.find(:conditions => [
      ['id', :equals, ids.first]
    ]).map {|i| i.id}.should == [ids[0]]
  end

  it "should be able to find instances via multiple ids using a :conditions hash argument to the find class method" do
    ids = [{:foo => 'boo'}, {:foo => 'bar'}, {:baz => 'bif'}].map do |hash|
      instance = TokyoStruct.new(hash)
      instance.save
      instance.id
    end

    TokyoStruct.find(:conditions => [
      ['id', :matches, "([#{ids.first}|#{ids.last}])$"]
    ]).map {|i| i.id}.should == [ids[0], ids[2]]
  end

  it "should be able to find instances via an array of ids as the first argument to the find class method" do
    ids = [{:foo => 'boo'}, {:foo => 'bar'}, {:baz => 'bif'}].map do |hash|
      instance = TokyoStruct.new(hash)
      instance.save
      instance.id
    end

    TokyoStruct.find([ids.first,ids.last]).map {|i| i.id}.should == [ids[0], ids[2]]
  end
end
