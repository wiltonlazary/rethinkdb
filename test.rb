require 'json'
require './build/drivers/ruby/lib/rethinkdb'
include RethinkDB::Shortcuts

conn = r.connect(:port => 28022)
puts JSON.generate(conn.server())
