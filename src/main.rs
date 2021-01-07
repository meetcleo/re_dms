#![feature(str_split_once)]
#![deny(warnings)]

use lazy_static::lazy_static;
use std::io::{self, BufRead};
use std::path::PathBuf;
// use log::{debug, error, log_enabled, info, Level};

use tokio::sync::mpsc;

mod change_processing;
mod database_writer;
mod database_writer_threads;
mod exponential_backoff;
mod file_uploader;
mod file_uploader_threads;
mod file_writer;
mod parser;
mod wal_file_manager;

use file_uploader_threads::DEFAULT_CHANNEL_SIZE;

lazy_static! {
    static ref OUTPUT_WAL_DIRECTORY: String =
        std::env::var("OUTPUT_WAL_DIRECTORY").expect("OUTPUT_WAL_DIRECTORY env is not set");
}

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
        PathBuf::from(OUTPUT_WAL_DIRECTORY.clone()).as_path(),
    );
    collector.register_wal_file(Some(wal_file_manager.current_wal()));

    for line in io::stdin().lock().lines() {
        if let Ok(ip) = line {
            let parsed_line = parser.parse(&ip);
            match parsed_line {
                parser::ParsedLine::ContinueParse => {} // Intentionally left blank, continue parsing
                _ => {
                    if let Some(change_vec) = collector.add_change(parsed_line) {
                        for change in change_vec {
                            // TODO error handling
                            file_transmitter.send(change).await.expect(
                                "Error writing to file_transmitter channel. Channel dropped.",
                            );
                        }
                    }
                }
            }
            if let wal_file_manager::WalLineResult::SwapWal(wal_file) =
                wal_file_manager.next_line(&ip)
            {
                // drain the collector of all it's tables, and send to file transmitter
                drain_collector_and_transmit(&mut collector, &mut file_transmitter).await;
                collector.register_wal_file(Some(wal_file.clone()));
            }
        }
    }
    collector.print_stats();

    drain_collector_and_transmit(&mut collector, &mut file_transmitter).await;

    // make sure we close the channel to let things propogate
    drop(file_transmitter);
    // make sure we wait for our uploads to finish
    file_uploader_threads_join_handle
        .await
        .expect("Error joining file uploader threads");

    database_writer_threads_join_handle
        .await
        .expect("Error joining database writer threads");

    // remove wal file from collector
    collector.register_wal_file(None);
    // clean up wal file in manager it should be the last one now.
    wal_file_manager.clean_up_final_wal_file();
}

async fn drain_collector_and_transmit(
    collector: &mut change_processing::ChangeProcessing,
    transmitter: &mut mpsc::Sender<change_processing::ChangeProcessingResult>,
) {
    let final_changes: Vec<_> = collector.drain_final_changes();
    for change in final_changes {
        transmitter
            .send(change)
            .await
            .expect("Error draining collector and sending to channel");
    }
}
