require File.dirname(__FILE__) + "/../../tokyo_struct"
require 'logger'
require 'extlib' # sudo gem install extlib

# A job object that is persisted to the database.
# Contains the work object as a YAML field.

# This storage engine uses TokyoCabinet to persist the job

class Delayed::Job < TokyoStruct
  
  include Delayed::Mixins::Base
  include Extlib::Hook
  
  before :save, :set_run_at
  
  # Since we don't have AR, we push the timezone setting to this class (defaults to utc)
  cattr_accessor :default_timezone
  self.default_timezone = :utc
  
  cattr_accessor :logger
  self.logger =  Logger.new(STDOUT)
  
  def initialize(hash = {})
    super(hash)
  end  
  
  def run_at
    unless (data['run_at'].nil? || data['run_at'] == '' || data['run_at'] == '0')
      Time.at(data['run_at'].to_i)
    else
      nil
    end
  end
  
  def locked_at
    unless (data['locked_at'].nil? || data['locked_at'] == '' || data['locked_at'] == '0')
      Time.at(data['locked_at'].to_i)
    else
      nil
    end
  end
  
  def failed_at
    unless (data['failed_at'].nil? || data['failed_at'] == '' || data['failed_at'] == '0')
      Time.at(data['failed_at'].to_i)
    else
      nil
    end
  end
  
  def created_at
    unless (data['created_at'].nil? || data['created_at'] == '' || data['created_at'] == '0')
      Time.at(data['created_at'].to_i)
    else
      nil
    end
  end
  
  def updated_at
    unless (data['updated_at'].nil? || data['updated_at'] == '' || data['updated_at'] == '0')
      Time.at(data['updated_at'].to_i)
    else
      nil
    end
  end
  
  def priority
    data['priority'] ? data['priority'].to_i : nil
  end
  
  def attempts
    data['attempts'] ? data['attempts'].to_i : nil
  end  

  # When a worker is exiting, make sure we don't have any locked jobs.
  def self.clear_locks!
    update_all({:locked_by => '', :locked_at => ''}, [['locked_by', :equals, worker_name]])
  end
  
  def save_job!
    save
  end
  
  def destroy_job
    destroy
  end
  
  def self.create(hash)
    # set priority and attempts as defaults
    # we force failed_at & locked_at to be present so we can search for them later
    instance = self.new(hash.reverse_merge(:failed_at => 0, :locked_at => 0, :priority => 0, :attempts => 0))
    
    # if the payload is supplied trigger the dumping
    payload_object = hash[:payload_object] || hash['payload_object']
    instance.payload_object = payload_object if payload_object
    
    instance.save
    instance
  end
  
  # Find a few candidate jobs to run (in case some immediately get locked by others).
  # Return in random order prevent everyone trying to do same head job at once.
  def self.find_available(limit = 5, max_run_time = MAX_RUN_TIME)

    time_now = db_time_now

    # NextTaskSQL= '(run_at <= ? AND (locked_at IS NULL OR locked_at < ?) OR (locked_by = ?)) AND failed_at IS NULL'
    # NextTaskOrder= 'priority DESC, run_at ASC'

    # sql = NextTaskSQL.dup

    conditions = [
      ['run_at', :numle, time_now.to_i.to_s], # run_at <= time_now
      ['failed_at', :numequals, '0']    #failed_at is null
    ]

    if self.min_priority
      conditions << ['priority', :numge, min_priority.to_s]
    end

    if self.max_priority
      conditions << ['priority', :numle, max_priority.to_s]
    end

    records = find(:conditions => conditions, :order => ['priority', :numdesc])
    # find(:all, :conditions => conditions, :order => NextTaskOrder, :limit => limit)
    
    # We now need to filter the locked records
    records = records.select do |r|
      # (locked_at IS NULL OR locked_at < ?) OR (locked_by = ?))
      ((r.locked_at.nil? || r.locked_at == '') || r.locked_at < (time_now - max_run_time)) || r.locked_by == worker_name
    end
    
    # We need to sort by 'priority DESC, run_at ASC'
    records = records.sort_by {|a| [-a.priority,a.run_at]}
    
    # then limit the results
    records = records[0,5]

    records.sort_by { rand() }
  end

  # Lock this job for this worker.
  # Returns true if we have the lock, false otherwise.
  def lock_exclusively!(max_run_time, worker = worker_name)
    now = self.class.db_time_now
    affected_rows = if locked_by != worker
      # We don't own this job so we will update the locked_by name and the locked_at
      # self.class.update_all(["locked_at = ?, locked_by = ?", now, worker], ["id = ? and (locked_at is null or locked_at < ?)", id, (now - max_run_time.to_i)])
      self.class.update_all({:locked_at => now, :locked_by => worker}, [['id', :equals, id], ['locked_at', :numlt, (now - max_run_time).to_i.to_s]])
    else
      # We already own this job, this may happen if the job queue crashes.
      # Simply resume and update the locked_at
      # self.class.update_all(["locked_at = ?", now], ["id = ? and locked_by = ?", id, worker])
      self.class.update_all({:locked_at => now}, [['id', :equals, id], ['locked_by', :equals, worker]])
    end
    if affected_rows == 1
      self.locked_at    = now
      self.locked_by    = worker
      return true
    else
      return false
    end
  end
  
  # This is a good hook if you need to report job processing errors in additional or different ways
  def log_exception(error)
    logger.error "* [JOB] #{name} failed with #{error.class.name}: #{error.message} - #{attempts} failed attempts"
    logger.error(error)
  end
  
  
  def log_info(msg)
    logger.info msg
  end
  
  def log_warn(msg)
    logger.warn msg
  end

private

  # Constantize the object so that ActiveSupport can attempt
  # its auto loading magic. Will raise LoadError if not successful.
  def attempt_to_load(klass)
     klass.constantize
  end

  # Get the current time (GMT or local depending on DB)
  # Note: This does not ping the DB to get the time, so all your clients
  # must have syncronized clocks.
  def self.db_time_now
    (Delayed::Job.default_timezone == :utc) ? Time.now.utc : Time.now
  end

protected

  # hook that ensures we have run_at set if not set manually
  def set_run_at
    if data['run_at'].nil? || (data['run_at'] == '')
      self.run_at = self.class.db_time_now
    end
  end
  
  def logger
    self.class.logger
  end

end