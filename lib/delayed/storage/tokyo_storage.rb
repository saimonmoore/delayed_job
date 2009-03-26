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

  NextTaskSQL         = '(run_at <= ? AND (locked_at IS NULL OR locked_at < ?) OR (locked_by = ?)) AND failed_at IS NULL' unless Delayed::Job.const_defined?(:NextTaskSQL)
  NextTaskOrder       = 'priority DESC, run_at ASC' unless Delayed::Job.const_defined?(:NextTaskOrder)

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

    sql = NextTaskSQL.dup

    conditions = [time_now, time_now - max_run_time, worker_name]

    if self.min_priority
      sql << ' AND (priority >= ?)'
      conditions << min_priority
    end

    if self.max_priority
      sql << ' AND (priority <= ?)'
      conditions << max_priority
    end

    conditions.unshift(sql)

    records = ActiveRecord::Base.silence do
      find(:all, :conditions => conditions, :order => NextTaskOrder, :limit => limit)
    end

    records.sort_by { rand() }
  end

  # Lock this job for this worker.
  # Returns true if we have the lock, false otherwise.
  def lock_exclusively!(max_run_time, worker = worker_name)
    now = self.class.db_time_now
    affected_rows = if locked_by != worker
      # We don't own this job so we will update the locked_by name and the locked_at
      self.class.update_all(["locked_at = ?, locked_by = ?", now, worker], ["id = ? and (locked_at is null or locked_at < ?)", id, (now - max_run_time.to_i)])
    else
      # We already own this job, this may happen if the job queue crashes.
      # Simply resume and update the locked_at
      self.class.update_all(["locked_at = ?", now], ["id = ? and locked_by = ?", id, worker])
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
    (ActiveRecord::Base.default_timezone == :utc) ? Time.now.utc : Time.now
  end

protected

  def before_save
    self.run_at ||= self.class.db_time_now
  end

end