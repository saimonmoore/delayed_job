require 'ostruct'

class DateTime
  
  # Converts self to a Ruby Date object; time portion is discarded
  def to_date
    ::Date.new(year, month, day)
  end

  # Attempts to convert self to a Ruby Time object; returns self if out of range of Ruby Time class
  # If self has an offset other than 0, self will just be returned unaltered, since there's no clean way to map it to a Time
  def to_time
    self.offset == 0 ? ::Time.utc_time(year, month, day, hour, min, sec) : self
  end
end

class TokyoStruct < OpenStruct
  @@db ||= Rufus::Tokyo::Table.new("#{self.name}.tdb")
  
  def initialize(hash = nil)
    super(hash)
  end
  
  def self.find(args)
    case args
    when String
      entry = db[args]
      if entry
        instance = self.new(entry)
        instance.instance_variable_set('@id', args)
        instance        
      else
        nil
      end
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
    instance = self.new(hash)
    instance.save
    instance
  end
  
  def self.update_all(new_data = {}, conditions = [])
    query_results = db.query { |q|
      conditions.each do |condition|
        q.pk_only
        q.add_condition(*condition)
      end
    }.each do |pk|
      instance = db[pk]
      db[pk] = instance.merge(stringify(new_data))
    end    
  end
  
  # removes this entry from the db and freezes this object
  def destroy
    db.delete(id)
    self.freeze
  end
  
  def id
    @id ||= '%064d' % db.generate_unique_id
  end
  
  def save
    db[id] = data
  end
  
  def reload
    self.class.find(id)
  end

  def db
    self.class.db
  end
  
  def self.db
    @@db
  end
  
  def data
    stringify(@table)
  end
  
  def new_record?
    db[id].nil?
  end
  
  # Returns true if the +comparison_object+ is the same object, or is of the same type and has the same id.
  def ==(comparison_object)
    comparison_object.equal?(self) ||
      (comparison_object.instance_of?(self.class) &&
        comparison_object.id == id &&
        !comparison_object.new_record?)
  end

  # Delegates to ==
  def eql?(comparison_object)
    self == (comparison_object)
  end

  # Delegates to id in order to allow two records of the same type and id to work with something like:
  #   [ Person.find(1), Person.find(2), Person.find(3) ] & [ Person.find(1), Person.find(4) ] # => [ Person.find(1) ]
  def hash
    id.hash
  end  

  protected
  
    def self.stringify(hash)
      h = {}
      hash.each do |k,v|
        h[k.to_s] = case v
        when DateTime
          v.to_time.to_i.to_s          
        when Date
          v.to_time.to_i.to_s
        when Time
          v.to_time.to_i.to_s
        else
          v.to_s
        end
      end
      h
    end
    
    def stringify(hash)
      self.class.stringify(hash)
    end
end
