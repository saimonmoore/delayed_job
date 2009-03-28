Spec::Runner.configure do |config|
  config.before do
    # Set the storage adapter to ActiveRecord
    Delayed.clear_storage_adapter
    Delayed.setup_storage_adapter('ar_storage')
  end  
end