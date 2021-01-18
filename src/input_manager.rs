use clap::{App, Arg};
use glob::{glob_with, MatchOptions};
use lazy_static::lazy_static;
use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Command, Stdio};

use either::Either;
use tokio::sync::mpsc;

use crate::change_processing;
use crate::database_writer_threads;
use crate::file_uploader_threads;
use crate::parser;
use crate::wal_file_manager;

use file_uploader_threads::DEFAULT_CHANNEL_SIZE;

#[allow(unused_imports)]
use crate::{function, logger_debug, logger_error, logger_info, logger_panic};

lazy_static! {
    static ref OUTPUT_WAL_DIRECTORY: String =
        std::env::var("OUTPUT_WAL_DIRECTORY").expect("OUTPUT_WAL_DIRECTORY env is not set");
    static ref PG_RECVLOGICAL_PATH: String =
        std::env::var("PG_RECVLOGICAL_PATH").expect("PG_RECVLOGICAL_PATH env is not set");
    static ref REPLICATION_SLOT: String =
        std::env::var("REPLICATION_SLOT").expect("REPLICATION_SLOT env is not set");
    static ref SOURCE_CONNECTION_STRING: String =
        std::env::var("SOURCE_CONNECTION_STRING").expect("SOURCE_CONNECTION_STRING env is not set");
}

pub enum InputType {
    Stdin,
    Wal(String),
    PgRcvlogical,
}

pub struct InputManager {
    pub input_type: InputType,
}

impl InputManager {
    fn new() -> InputManager {
        InputManager {
            input_type: input_type(),
        }
    }

    async fn process_input(&self) {
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
        // for logging
        parser.register_wal_number(wal_file_manager.current_wal().file_number);

        // need to define this at this level so it lives long enough
        let stdin = io::stdin();
        let locked_stdin = stdin.lock();

        let buffered_reader = if let InputType::PgRcvlogical = self.input_type {
            let child_stdout = get_buffered_reader_process();
            Either::Right(child_stdout)
        } else {
            let reader: Box<dyn BufRead> = match &self.input_type {
                InputType::Stdin => Box::new(locked_stdin),

                InputType::Wal(wal_path) => Box::new(BufReader::new(
                    File::open(wal_path)
                        .expect(&format!("Unable to open existing WAL at {}", wal_path)),
                )),
                InputType::PgRcvlogical => {
                    panic!("Should never have gotten here as PgRcvlogical is handled separately")
                }
            };
            Either::Left(reader)
        };

        for line in buffered_reader.lines() {
            if let Ok(ip) = line {
                let wal_file_manager_result = wal_file_manager.next_line(&ip);
                let parsed_line = parser.parse(&ip);
                match parsed_line {
                    parser::ParsedLine::ContinueParse => {} // Intentionally left blank, continue parsing
                    _ => {
                        if let Some(change_vec) = collector.add_change(parsed_line) {
                            for change in change_vec {
                                file_transmitter.send(change).await.expect(
                                    "Error writing to file_transmitter channel. Channel dropped.",
                                );
                            }
                        }
                    }
                }
                if let wal_file_manager::WalLineResult::SwapWal(wal_file) = wal_file_manager_result
                {
                    // drain the collector of all it's tables, and send to file transmitter
                    drain_collector_and_transmit(&mut collector, &mut file_transmitter).await;
                    collector.register_wal_file(Some(wal_file.clone()));
                    parser.register_wal_number(wal_file.file_number);
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

fn get_buffered_reader_process() -> BufReader<std::process::ChildStdout> {
    let child = Command::new(PG_RECVLOGICAL_PATH.clone())
        .args(&[
            "--create-slot",
            "--start",
            "--if-not-exists",
            "--fsync-interval=0",
            "--file=-",
            "--plugin=test_decoding",
            &format!("--slot={}", *REPLICATION_SLOT),
            &format!("--dbname={}", *SOURCE_CONNECTION_STRING),
        ])
        .stdin(Stdio::null())
        .stderr(Stdio::inherit())
        .stdout(Stdio::piped())
        .spawn()
        .expect("Failed to execute pg_recvlogical");
    let stdout = child
        .stdout
        .expect("Failed to get stdout for pg_recvlogical");

    BufReader::new(stdout)
}

fn input_type() -> InputType {
    let arg_matches = App::new("re_dms")
        .version("0.1")
        .author("MeetCleo. <team@meetcleo.com>")
        .about("replication from postgres to redshift")
        .arg(
            Arg::with_name("read_from_stdin")
                .short("-")
                .long("stdin")
                .help("Makes the process read from stdin instead of starting a subprocess"),
        )
        .get_matches();

    if arg_matches.is_present("read_from_stdin") {
        InputType::Stdin
    } else {
        let options = MatchOptions {
            case_sensitive: false,
            ..Default::default()
        };

        let earliest_wal = glob_with(&format!("{}/*.wal", *OUTPUT_WAL_DIRECTORY), options)
            .expect("Unable to check for existing WAL files")
            .map(|file_path| match file_path {
                Ok(path) => path
                    .to_str()
                    .expect("Invalid UTF filename for WAL file")
                    .to_string(),
                Err(_e) => panic!("unreadable path. What did you do?"),
            })
            .min();

        if let Some(path) = earliest_wal {
            InputType::Wal(path)
        } else {
            InputType::PgRcvlogical
        }
    }
}

pub async fn run() {
    loop {
        let input_manager = InputManager::new();
        input_manager.process_input().await;
        match input_manager.input_type {
            InputType::PgRcvlogical => return,
            InputType::Stdin => return,
            InputType::Wal(_) => continue,
        }
    }
}
