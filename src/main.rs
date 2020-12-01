#![feature(str_split_once)]

use std::fs::File;
use std::io::{self, BufRead};
use std::path::Path;
// use log::{debug, error, log_enabled, info, Level};


extern crate log;

mod parser;

fn main() {
    env_logger::init();
    // File hosts must exist in current path before this produces output
    if let Ok(lines) = read_lines("./data/test_decoding.txt") {
        let mut parser = parser::Parser::new(true);
        // Consumes the iterator, returns an (Optional) String
        for line in lines//.skip(2637).take(3)
        {
            if let Ok(ip) = line {
                let _parsed_line = parser.parse(&ip);
                // println!("{}", parsed_line);
                // println!("{}", ip);
            }
        }
    }
    println!("Hello, world!");
}

// The output is wrapped in a Result to allow matching on errors
// Returns an Iterator to the Reader of the lines of the file.
fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where P: AsRef<Path>, {
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}
