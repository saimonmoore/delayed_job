require File.dirname(__FILE__) + '/spec_helper'
require File.dirname(__FILE__) + '/tokyo_database'

specs = Dir["**/*_spec.rb"].select {|filename| !filename.include?('tokyo')}
specs.each do |spec_path|
  File.open(spec_path) do |f|
    f.gets #remove spec_helper require
    f.gets #remove ar_database require
    eval(f.read, binding, File.basename(f.path))
  end
end