module Delayed
  # Sets up the storage adapter. 
  def self.setup_storage_adapter(adapter_instance='ar_storage')
    require File.dirname(__FILE__) + "/storage/#{adapter_instance}"
  end
  
  def self.clear_storage_adapter
    remove_const(:Job) if Object.const_defined?(:Job)
  end
    
  class DeserializationError < StandardError
  end
  
    module Mixins
      module Base
  
        def self.included(base)
          Delayed::Job.const_set(:MAX_ATTEMPTS, 25) unless Delayed::Job.const_defined?(:MAX_ATTEMPTS)
          # seconds
          Delayed::Job.const_set(:MAX_RUN_TIME, 4*60*60) unless Delayed::Job.const_defined?(:MAX_RUN_TIME)
          Delayed::Job.const_set(:ParseObjectFromYaml, /\!ruby\/\w+\:([^\s]+)/) unless Delayed::Job.const_defined?(:ParseObjectFromYaml)
          
          base.class_eval do 
            
            # By default failed jobs are destroyed after too many attempts.
            # If you want to keep them around (perhaps to inspect the reason
            # for the failure), set this to false.
            cattr_accessor :destroy_failed_jobs
            self.destroy_failed_jobs = true

            # Every worker has a unique name which by default is the pid of the process.
            # There are some advantages to overriding this with something which survives worker retarts:
            # Workers can safely resume working on tasks which are locked by themselves. The worker will assume that it crashed before.
            cattr_accessor :worker_name
            self.worker_name = "host:#{Socket.gethostname} pid:#{Process.pid}" rescue "pid:#{Process.pid}"

            cattr_accessor :min_priority, :max_priority
            self.min_priority = nil
            self.max_priority = nil
            
            include Delayed::Mixins::Base::InstanceMethods
            extend  Delayed::Mixins::Base::ClassMethods
          end # base.class_eval
        end # self.included
  
  
        module ClassMethods
          # storage independant class methods
          # Add a job to the queue
          def enqueue(*args, &block)
            object = block_given? ? EvaledJob.new(&block) : args.shift

            unless object.respond_to?(:perform) || block_given?
              raise ArgumentError, 'Cannot enqueue items which do not respond to perform'
            end

            priority = args.first || 0
            run_at   = args[1]

            # Assumes storage adapter implements #create class method
            Job.create(:payload_object => object, :priority => priority.to_i, :run_at => run_at)
          end
          
          # Run the next job we can get an exclusive lock on.
          # If no jobs are left we return nil
          def reserve_and_run_one_job(max_run_time = Delayed::Job::MAX_RUN_TIME)

            # We get up to 5 jobs from the db. In case we cannot get exclusive access to a job we try the next.
            # this leads to a more even distribution of jobs across the worker processes
            find_available(5, max_run_time).each do |job|
              t = job.run_with_lock(max_run_time, worker_name)
              return t unless t == nil  # return if we did work (good or bad)
            end

            nil # we didn't do any work, all 5 were not lockable
          end         
          
          # Do num jobs and return stats on success/failure.
          # Exit early if interrupted.
          def work_off(num = 100)
            success, failure = 0, 0

            num.times do
              case self.reserve_and_run_one_job
              when true
                  success += 1
              when false
                  failure += 1
              else
                break  # leave if no work could be done
              end
              break if $exit # leave if we're exiting
            end

            return [success, failure]
          end
          
        end 
  
        module InstanceMethods
          # storage independant instance methods
          
          def failed?
            failed_at
          end
          alias_method :failed, :failed?
          
          def payload_object
            @payload_object ||= deserialize(self['handler'])
          end

          def name
            @name ||= begin
              payload = payload_object
              if payload.respond_to?(:display_name)
                payload.display_name
              else
                payload.class.name
              end
            end
          end

          def payload_object=(object)
            self['handler'] = object.to_yaml
          end
          
          # Reschedule the job in the future (when a job fails).
          # Uses an exponential scale depending on the number of failed attempts.
          def reschedule(message, backtrace = [], time = nil)
            if self.attempts < Delayed::Job::MAX_ATTEMPTS
              time ||= Job.db_time_now + (attempts ** 4) + 5

              self.attempts    += 1
              self.run_at       = time
              self.last_error   = message + "\n" + backtrace.join("\n")
              self.unlock
              save_job!
            else
              log_info "* [JOB] PERMANENTLY removing #{self.name} because of #{attempts} consequetive failures."
              destroy_failed_jobs ? destroy : update_attribute(:failed_at, Time.now)
            end
          end          
          
          # Try to run one job. Returns true/false (work done/work failed) or nil if job can't be locked.
          def run_with_lock(max_run_time, worker_name)
            log_info "* [JOB] aquiring lock on #{name}"
            unless lock_exclusively!(max_run_time, worker_name)
              # We did not get the lock, some other worker process must have
              log_warn "* [JOB] failed to aquire exclusive lock for #{name}"
              return nil # no work done
            end

            begin
              runtime =  Benchmark.realtime do
                invoke_job # TODO: raise error if takes longer than max_run_time
                destroy_job
              end
              # TODO: warn if runtime > max_run_time ?
              log_info "* [JOB] #{name} completed after %.4f" % runtime
              return true  # did work
            rescue Exception => e
              reschedule e.message, e.backtrace
              log_exception(e)
              return false  # work failed
            end
          end       
          
          # Unlock this job (note: not saved to DB)
          def unlock
            self.locked_at    = nil
            self.locked_by    = nil
          end
                       
          # Moved into its own method so that new_relic can trace it.
          def invoke_job
            payload_object.perform
          end

          private
          
            def deserialize(source)
              handler = YAML.load(source) rescue nil

              unless handler.respond_to?(:perform)
                if handler.nil? && source =~ Delayed::Job::ParseObjectFromYaml
                  handler_class = $1
                end
                attempt_to_load(handler_class || handler.class)
                handler = YAML.load(source)
              end

              return handler if handler.respond_to?(:perform)

              raise DeserializationError,
                'Job failed to load: Unknown handler. Try to manually require the appropiate file.'
            rescue TypeError, LoadError, NameError => e
              raise DeserializationError,
                "Job failed to load: #{e.message}. Try to manually require the required file."
            end
          
        end # InstanceMethods
  
      end # Storage    
    end # Mixins


  class EvaledJob
    def initialize
      @job = yield
    end

    def perform
      eval(@job)
    end
  end
end
