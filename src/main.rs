#![feature(str_split_once)]

use std::fs::File;
use std::io::{self, BufRead};
use std::path::Path;
// use log::{debug, error, log_enabled, info, Level};

// use std::io::prelude::*;
// use std::fs;
use tokio::sync::mpsc;

mod parser;
mod change_processing;
mod file_writer;
mod file_uploader;
mod database_writer;
mod file_uploader_threads;
mod database_writer_threads;

use file_uploader_threads::DEFAULT_CHANNEL_SIZE;

// use std::collections::{ HashSet };

// TEMP

#[tokio::main]
async fn main() {
    env_logger::init();
    let mut parser = parser::Parser::new(true);
    let mut collector = change_processing::ChangeProcessing::new();
    // initialize our channels
    let (mut file_transmitter, file_receiver) = mpsc::channel::<change_processing::ChangeProcessingResult>(DEFAULT_CHANNEL_SIZE);
    let (database_transmitter, database_receiver) = mpsc::channel::<file_uploader_threads::UploaderStageResult>(DEFAULT_CHANNEL_SIZE);
    // initialize our file uploader stream
    let file_uploader_threads_join_handle = file_uploader_threads::FileUploaderThreads::spawn_file_uploader_stream(file_receiver, database_transmitter);
    // initialize our database importer stream
    let database_writer_threads_join_handle = database_writer_threads::DatabaseWriterThreads::spawn_database_writer_stream(database_receiver);

    if let Ok(lines) = read_lines("./data/test_decoding.txt") {
        // Consumes the iterator, returns an (Optional) String
        for line in lines //.take(30)
        {
            if let Ok(ip) = line {
                let parsed_line = parser.parse(&ip);
                match parsed_line {
                    parser::ParsedLine::ContinueParse => {}, // Intentionally left blank, continue parsing
                    _ => {
                        if let Some(change_vec) = collector.add_change(parsed_line) {
                            for change in change_vec {
                                // TODO error handling
                                file_transmitter.send(change).await;
                            }
                        }
                    }
                }
            }
        }
    }
    collector.print_stats();

    let final_changes: Vec<_> = collector.drain_final_changes();
    for change in final_changes {
        // TODO error handling
        file_transmitter.send(change).await;
    }
    collector.print_stats();

    // make sure we close the channel to let things propogate
    drop(file_transmitter);
    // make sure we wait for our uploads to finish
    file_uploader_threads_join_handle.await;

    database_writer_threads_join_handle.await;

}

// The output is wrapped in a Result to allow matching on errors
// Returns an Iterator to the Reader of the lines of the file.
fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where P: AsRef<Path>, {
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}
