require 'ostruct'
require 'yaml'

class TokyoStruct < OpenStruct
  @@db ||= Rufus::Tokyo::Table.new("#{self.name}.tdb")
  
  def initialize(hash = nil)
    super(hash)
  end
  
  def self.find(*args)
    return nil if args && args.empty?    
    options = args.extract_options!
    first_arg = args.first
    case first_arg
    when String
      entry = db[first_arg]
      if entry
        instance = self.new(entry)
        instance.instance_variable_set('@id', first_arg)
        return instance        
      else
        return nil
      end
    when Symbol
      if first_arg == :all
        return find(options.merge(:conditions => []))
      else
        raise("Unsupported option")
      end
    when Array
      return find(options.merge(:conditions => [['id', :matches, "([#{first_arg.join('|')}])$"]]))
      # return first_arg.map {|id| find(id.to_s)}
    end
    
    conditions = options[:conditions]
    order = options[:order]
    limit = options[:limit]
    offset = options[:offset]
    query_results = db.query { |q|
      q.pk_only        
      conditions.each do |condition|
        q.add_condition(*condition)
      end
      q.order_by(*order) if order        
      q.limit(limit, offset ? offset : -1) if limit
    }
    query_results ? query_results.map {|pk| find(pk)} : []
  end
  
  def self.create(hash = nil)
    instance = self.new(hash)
    instance.save
    instance
  end
  
  def self.delete_all
    db.clear
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
    db[id] = data.merge('id' => id)
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
    stringify(@table).reject {|k,v| k == 'id'}
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
          Time.parse(v.to_s).to_i.to_s
        when Date, Time
          v.to_time.to_i.to_s          
        when String
          v
        when Symbol
          v.to_s
        when Numeric
          v.to_s
        when NilClass
          ''
        else
          v.to_yaml
        end
      end
      h
    end
    
    def stringify(hash)
      self.class.stringify(hash)
    end
end
