#!/usr/bin/env ruby
require 'pathname'

input = ARGV
raise "provide an output directory and number of files to combine as arguments" unless ARGV.length == 2

OUTPUT_DIR = Pathname.new(input[0])
MAX_FILES_TO_COMBINE = input[1].to_i

puts "Writing new .wal files to #{OUTPUT_DIR} with a batch size of #{MAX_FILES_TO_COMBINE}"

wal_file_number = 1
file_name_buffer = []

Dir.glob('*.wal') do |file|
  puts "buf += #{file}"
  file_name_buffer << file
  next if file_name_buffer.length < MAX_FILES_TO_COMBINE

  files_to_join = file_name_buffer.join(' ')
  target_file = OUTPUT_DIR.join("#{sprintf("%016X", wal_file_number)}.wal")
  puts "merge: #{files_to_join} target: #{target_file}"

  `cat #{files_to_join} > #{target_file}`
  file_name_buffer = []
  wal_file_number += 1
end

exit(0) if file_name_buffer.length == 0

files_to_join = file_name_buffer.join(' ')
target_file = OUTPUT_DIR.join("#{sprintf("%016X", wal_file_number)}.wal")
puts "merge: #{files_to_join} target: #{target_file}"

`cat #{files_to_join} > #{target_file}`