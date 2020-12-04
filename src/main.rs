#![feature(str_split_once)]

use std::fs::File;
use std::io::{self, BufRead};
use std::path::Path;
// use log::{debug, error, log_enabled, info, Level};

// use std::io::prelude::*;
// use std::fs;

extern crate log;
extern crate either;
extern crate csv;
extern crate itertools;

mod parser;
mod change_processing;
mod file_writer;

fn main() -> std::io::Result<()> {
    env_logger::init();
    // File hosts must exist in current path before this produces output
    let mut parser = parser::Parser::new(true);
    let mut collector = change_processing::ChangeProcessing::new();
    if let Ok(lines) = read_lines("./data/test_decoding.txt") {
        // Consumes the iterator, returns an (Optional) String
        for line in lines
        {
            if let Ok(ip) = line {
                let parsed_line = parser.parse(&ip);
                match parsed_line {
                    parser::ParsedLine::ContinueParse => {}, // Intentionally left blank, continue parsing
                    _ => { collector.add_change(parsed_line); }
                }
            }
        }
    }
    // collector.print_stats();
    collector.write_files();
    Ok(())
}

// The output is wrapped in a Result to allow matching on errors
// Returns an Iterator to the Reader of the lines of the file.
fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where P: AsRef<Path>, {
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}
