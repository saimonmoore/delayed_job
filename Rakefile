require 'fileutils'

desc "Run specs using the tokyo cabinet adapter"
desc "Run specs using the AR adapter"

task :specs do
end
task :run_ar_specs do
end
task :run_tokyo_specs do
  specs = Dir["**/*_spec.rb"].select {|filename| !%w(tokyo story).any? {|keyword| filename.include?(keyword)}}
  specs.each do |spec_path|
    File.open(spec_path) do |f|
      puts "running tokyo spec for #{File.basename(f.path)}..."
      f.gets #remove spec_helper require
      f.gets #remove ar_database require
      spec_string = <<-EOM
      require File.dirname(__FILE__) + '/spec_helper'
      require File.dirname(__FILE__) + '/tokyo_database'
      EOM
      spec_string << f.read
      # job spec checks on filename
      spec_string.gsub!('job_spec.rb', 'job_tokyo_spec.rb')
      
      new_file_name = spec_path.gsub("_spec", "_tokyo_spec")
      File.open(new_file_name, 'wb') {|file| file.write(spec_string)}
      puts `spec #{new_file_name}`
      FileUtils.rm_rf new_file_name
    end
  end
end