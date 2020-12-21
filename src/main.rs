#![feature(str_split_once)]

use std::fs::File;
use std::io::{self, BufRead};
use std::path::Path;
use std::path::PathBuf;
// use log::{debug, error, log_enabled, info, Level};

// use std::io::prelude::*;
// use std::fs;
use tokio::sync::mpsc;

mod change_processing;
mod database_writer;
mod database_writer_threads;
mod file_uploader;
mod file_uploader_threads;
mod file_writer;
mod parser;
mod wal_file_manager;

use file_uploader_threads::DEFAULT_CHANNEL_SIZE;

// use std::collections::{ HashSet };

// TEMP

#[tokio::main]
async fn main() {
    env_logger::init();
    let mut parser = parser::Parser::new(true);
    let mut collector = change_processing::ChangeProcessing::new();
    // initialize our channels
    let (mut file_transmitter, file_receiver) =
        mpsc::channel::<change_processing::ChangeProcessingResult>(DEFAULT_CHANNEL_SIZE);
    let (database_transmitter, database_receiver) =
        mpsc::channel::<file_uploader_threads::UploaderStageResult>(DEFAULT_CHANNEL_SIZE);
    // initialize our file uploader stream
    let file_uploader_threads_join_handle =
        file_uploader_threads::FileUploaderThreads::spawn_file_uploader_stream(
            file_receiver,
            database_transmitter,
        );
    // initialize our database importer stream
    let database_writer_threads_join_handle =
        database_writer_threads::DatabaseWriterThreads::spawn_database_writer_stream(
            database_receiver,
        );
    let mut wal_file_manager = wal_file_manager::WalFileManager::new(
        None,
        PathBuf::from("./data/test_decoding.txt").as_path(),
        PathBuf::from("output_wal").as_path(),
    );

    while let Some(wal_line_result) = wal_file_manager.next_line() {
        match wal_line_result {
            wal_file_manager::WalLineResult::SwapWal => {
                // drain the collector of all it's tables, and send to file transmitter
                drain_collector_and_transmit(&mut collector, &mut file_transmitter).await;
            }
            wal_file_manager::WalLineResult::WalLine(wal_file, line) => {
                let parsed_line = parser.parse(&line);
                match parsed_line {
                    parser::ParsedLine::ContinueParse => {} // Intentionally left blank, continue parsing
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

    drain_collector_and_transmit(&mut collector, &mut file_transmitter).await;

    // make sure we close the channel to let things propogate
    drop(file_transmitter);
    // make sure we wait for our uploads to finish
    file_uploader_threads_join_handle.await;

    database_writer_threads_join_handle.await;
}

async fn drain_collector_and_transmit(
    collector: &mut change_processing::ChangeProcessing,
    transmitter: &mut mpsc::Sender<change_processing::ChangeProcessingResult>,
) {
    let final_changes: Vec<_> = collector.drain_final_changes();
    for change in final_changes {
        // TODO error handling
        transmitter.send(change).await;
    }
}

// The output is wrapped in a Result to allow matching on errors
// Returns an Iterator to the Reader of the lines of the file.
fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where
    P: AsRef<Path>,
{
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}
