require File.dirname(__FILE__) + "/tokyo_struct"

# A job object that is persisted to the database.
# Contains the work object as a YAML field.

# What's in here is what needs to be implemented by other storage engines
# Any other storage engine also needs to implement accessors for the main delayed job attributes

# table.integer  :priority, :default => 0
# table.integer  :attempts, :default => 0
# table.text     :handler
# table.string   :last_error
# table.datetime :run_at
# table.datetime :locked_at
# table.string   :locked_by
# table.datetime :failed_at
# table.datetime :created_at
# table.datetime :updated_at
# OpenStruct
class Delayed::Job < TokyoStruct
  
  include Delayed::Mixins::Base
  
  # Since we don't have AR, we push the timezone setting to this class (defaults to utc)
  cattr_accessor :default_timezone
  self.default_timezone = :utc
  
  def run_at
    data['run_at'] ? DateTime.parse(Time.at(data['run_at'].to_i).to_s) : nil
  end
  
  def locked_at
    data['locked_at'] ? DateTime.parse(Time.at(data['locked_at'].to_i).to_s) : nil
  end
  
  def failed_at
    data['failed_at'] ? DateTime.parse(Time.at(data['failed_at'].to_i).to_s) : nil
  end
  
  def created_at
    data['created_at'] ? DateTime.parse(Time.at(data['run_at'].to_i).to_s) : nil
  end
  
  def updated_at
    data['created_at'] ? DateTime.parse(Time.at(data['run_at'].to_i).to_s) : nil
  end
  
  def priority
    data['priority'] ? data['priority'].to_i : nil
  end
  
  def attempts
    data['attempts'] ? data['attempts'].to_i : nil
  end  

  # When a worker is exiting, make sure we don't have any locked jobs.
  def self.clear_locks!
    update_all({:locked_by => nil, :locked_at => nil}, [:locked_by, :equals, worker_name])
  end
  
  def save_job!
    save
  end
  
  def destroy_job
    destroy
  end
  
  # Find a few candidate jobs to run (in case some immediately get locked by others).
  # Return in random order prevent everyone trying to do same head job at once.
  def self.find_available(limit = 5, max_run_time = MAX_RUN_TIME)

    time_now = db_time_now

    # NextTaskSQL= '(run_at <= ? AND (locked_at IS NULL OR locked_at < ?) OR (locked_by = ?)) AND failed_at IS NULL'
    # NextTaskOrder= 'priority DESC, run_at ASC'

    # sql = NextTaskSQL.dup

    conditions = [
      'run_at', :numge, time_now, # run_at >= time_now
      'failed_at', :equals, ''    #failed_at is null
    ]

    if self.min_priority
      conditions << ['priority', :numge, min_priority]
    end

    if self.max_priority
      conditions << ['priority', :numle, max_priority]
    end

    records = find(:conditions => conditions, :order => ['priority', :numdesc])
    # find(:all, :conditions => conditions, :order => NextTaskOrder, :limit => limit)
    
    # We now need to filter the locked records
    records = records.select do |r|
      # (locked_at IS NULL OR locked_at < ?) OR (locked_by = ?))
      (r.locked_at.nil? || r.locked_at == '' || r.locked_at.to_i == (time_now - max_run_time)) || r.locked_by == worker_name
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
      self.class.update_all({:locked_at => now, :locked_by => worker}, [[:id, :equals, id], [:locked_at, :numlt,now - max_run_time.to_i]])
    else
      # We already own this job, this may happen if the job queue crashes.
      # Simply resume and update the locked_at
      # self.class.update_all(["locked_at = ?", now], ["id = ? and locked_by = ?", id, worker])
      self.class.update_all({:locked_at => now}, [[:id, :equals, id], [:locked_by, :equals, worker]])
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

  def before_save
    self.run_at ||= self.class.db_time_now
  end

end