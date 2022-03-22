#![deny(warnings)]

use clap::{App, Arg};
use either::Either;
use glob::{glob_with, MatchOptions};
use lazy_static::lazy_static;
use std::convert::TryInto;
use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Command, Stdio};

#[cfg(feature = "with_rollbar")]
#[macro_use]
extern crate rollbar;

use dotenv::dotenv;

use tokio::sync::mpsc;

mod change_processing;
mod database_writer;
mod database_writer_threads;
mod exponential_backoff;
mod file_uploader;
mod file_uploader_threads;
mod file_writer;
mod logger;
mod parser;
mod shutdown_handler;
mod targets_tables_column_names;
mod wal_file_manager;

use file_uploader_threads::DEFAULT_CHANNEL_SIZE;
use shutdown_handler::{RuntimeType, ShutdownHandler};
use wal_file_manager::WalFile;

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

#[derive(Debug, Clone)]
enum InputType {
    Stdin,
    Wal(String),
    PgRcvlogical,
}

fn panic_if_messy_shutdown() ->() {
    if ShutdownHandler::shutting_down_messily() {
        panic!("re_dms had a messy shut down. If running as a service, will attempt to restart automatically a limited number of times.");
    }
}

struct PreprocessingManager {
    continue_parsing_and_processing: bool,
    wal_file: Option<WalFile>
}

impl PreprocessingManager {
    fn new() -> PreprocessingManager {
        PreprocessingManager {
            continue_parsing_and_processing: true,
            wal_file: None
        }
    }

    fn halt_preprocessing_and_register_shutdown(&mut self, wal_file: WalFile, message: &str) {
        let wal_file_number = wal_file.file_number;
        self.continue_parsing_and_processing = false;
        self.wal_file = Some(wal_file);
        logger_error!(
            Some(wal_file_number),
            None,
            message
        );
        logger_info!(
            Some(wal_file_number),
            None,
            "Halting pre-processing"
        );
        ShutdownHandler::register_clean_shutdown();
    }

    fn halt_preprocessing(&mut self) {
        self.continue_parsing_and_processing = false;

        logger_info!(
            None,
            None,
            "Halting pre-processing"
        );
    }

    fn preprocessing_halted(&mut self) -> bool {
        self.continue_parsing_and_processing == false
    }
}

#[tokio::main]
async fn main() {
    ShutdownHandler::register_signal_handlers();
    dotenv().ok();
    env_logger::init();

    #[cfg(feature = "with_rollbar")]
    logger::register_panic_handler();

    let mut targets_tables_column_names =
        targets_tables_column_names::TargetsTablesColumnNames::new();
    let result = targets_tables_column_names.refresh().await;
    match result {
        Ok(_) => logger_info!(
            None,
            None,
            &format!(
                "Fetched column names for {} tables from target DB",
                targets_tables_column_names.len()
            )
        ),
        Err(msg) => logger_panic!(
            None,
            None,
            &format!("Failed to fetch column names from target DB: {:?}", msg)
        ),
    };
    let mut parser = parser::Parser::new(true);
    let mut collector = change_processing::ChangeProcessing::new(targets_tables_column_names);
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

    let mut child_process_guard = ChildGuard(None);
    let mut wal_file_manager;
    let mut previous_input_type = None;
    let mut preprocessing_manager = PreprocessingManager::new();
    loop {
        // need to define this at this level so it lives long enough
        let stdin = io::stdin();
        let locked_stdin = stdin.lock();

        let input_type = input_type(previous_input_type);
        previous_input_type = Some(input_type.clone());
        let buffered_reader = if let InputType::PgRcvlogical = input_type {
            let (process, bufreader) = get_buffered_reader_process();

            let process_id = process
                .id()
                .try_into()
                .expect("pid that's greater than i32::MAX");
            // register it to the childguard, so it gets shutdown in the event of a panic
            child_process_guard.0 = Some(process);
            ShutdownHandler::register_shutdown_handler(RuntimeType::from_pid(process_id));
            // how to term the child process
            Either::Right(bufreader)
        } else {
            let reader: Box<dyn BufRead> = match &input_type {
                InputType::Stdin => {
                    logger_info!(None, None, "Reading from stdin");
                    ShutdownHandler::register_shutdown_handler(RuntimeType::Stdin);
                    Box::new(locked_stdin)
                }

                InputType::Wal(wal_path) => {
                    logger_info!(
                        None,
                        None,
                        &format!("Reading from existing WAL: {}", wal_path)
                    );
                    ShutdownHandler::register_shutdown_handler(RuntimeType::File);
                    Box::new(BufReader::new(
                        File::open(wal_path)
                            .expect(&format!("Unable to open existing WAL at {}", wal_path)),
                    ))
                }
                InputType::PgRcvlogical => {
                    panic!("Should never have gotten here as PgRcvlogical is handled separately")
                }
            };
            Either::Left(reader)
        };

        wal_file_manager = match &input_type {
            InputType::Wal(file_path) => wal_file_manager::WalFileManager::reprocess(
                PathBuf::from(OUTPUT_WAL_DIRECTORY.clone()).as_path(),
                file_path.clone(),
            ),
            _ => wal_file_manager::WalFileManager::new(
                PathBuf::from(OUTPUT_WAL_DIRECTORY.clone()).as_path(),
            ),
        };

        collector.register_wal_file(Some(wal_file_manager.current_wal()));
        // for logging
        parser.register_wal_number(wal_file_manager.current_wal().file_number);

        for line in buffered_reader.lines() {
            if let Ok(ip) = line {
                let wal_file_manager_result = wal_file_manager.next_line(&ip);
                let shutting_down = ShutdownHandler::shutting_down();
                if shutting_down {
                    if ShutdownHandler::should_break_main_loop() {
                        break;
                    }

                    if ShutdownHandler::shutting_down_messily() {
                        preprocessing_manager.halt_preprocessing();
                    }
                }

                if !preprocessing_manager.preprocessing_halted() {
                    let parsed_line_result = parser.parse(&ip);
                    match parsed_line_result {
                        Ok(parsed_line) => {
                            match parsed_line {
                                parser::ParsedLine::ContinueParse => {}, // Intentionally left blank, continue parsing
                                _ => {
                                    let change_vec_result = collector.add_change(parsed_line);
                                    match change_vec_result {
                                        Ok(change_vec) => {
                                            if let Some(change_vec) = change_vec {
                                                for change in change_vec {
                                                    match file_transmitter.send(change).await {
                                                        Err(err) => {
                                                            preprocessing_manager.halt_preprocessing_and_register_shutdown(wal_file_manager.current_wal(), &format!("Error writing to file_transmitter channel. Channel dropped due to: {:?}", err));
                                                         },
                                                        _ => {}
                                                    };
                                                }
                                            }
                                        },
                                        Err(err) => {
                                            preprocessing_manager.halt_preprocessing_and_register_shutdown(wal_file_manager.current_wal(), &format!("Error processing changes. Failed due to: {:?}", err));
                                        }
                                    }
                                }
                            }
                        },
                        Err(err) => {
                            preprocessing_manager.halt_preprocessing_and_register_shutdown(wal_file_manager.current_wal(), &format!("Error parsing changes. Failed due to: {:?}", err));
                        }
                    }
                }
                if !preprocessing_manager.preprocessing_halted() {
                    if let wal_file_manager::WalLineResult::SwapWal(wal_file) = wal_file_manager_result
                    {
                        // drain the collector of all it's tables, and send to file transmitter
                        drain_collector_and_transmit(&mut collector, &mut file_transmitter).await;
                        collector.register_wal_file(Some(wal_file.clone()));
                        parser.register_wal_number(wal_file.file_number);
                    }
                }
            }
        }

        panic_if_messy_shutdown();
        logger_info!(None, None, "exitted_main_loop");

        drain_collector_and_transmit(&mut collector, &mut file_transmitter).await;

        if let InputType::Wal(_) = input_type {
            let shutting_down = ShutdownHandler::shutting_down();
            if shutting_down {
                if ShutdownHandler::should_break_main_loop() {
                    break;
                } else {
                    continue;
                }
            }
        } else {
            break;
        }
    }

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

    ShutdownHandler::log_shutdown_status();

    #[cfg(feature = "with_rollbar")]
    logger::block_on_last_rollbar_thread_handle();

    panic_if_messy_shutdown();
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

fn get_buffered_reader_process() -> (std::process::Child, BufReader<std::process::ChildStdout>) {
    let mut child = Command::new(PG_RECVLOGICAL_PATH.clone())
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
        .take() // take allows us to avoid partially moving the child
        .expect("Failed to get stdout for pg_recvlogical");
    (child, BufReader::new(stdout))
}

// https://stackoverflow.com/questions/30538004/how-do-i-ensure-that-a-spawned-child-process-is-killed-if-my-app-panics

struct ChildGuard(Option<std::process::Child>);

// I'm not sure if this is strictly needed.
// we abort on panic, and if we panic, even without this code we get:
// Command terminated by signal 6
// still, this code feels correct so I'm including it.
impl Drop for ChildGuard {
    fn drop(&mut self) {
        match &mut self.0 {
            Some(process) => match process.kill() {
                Err(e) => match e.kind() {
                    std::io::ErrorKind::InvalidInput => {
                        logger_info!(
                            None,
                            None,
                            &format!(
                                "Child process already killed during child guard dropping: {}",
                                e
                            )
                        )
                    }
                    _unknown_error_kind => {
                        logger_error!(None, None, &format!("Could not kill child process: {}", e))
                    }
                },
                Ok(_) => logger_info!(None, None, "Successfully killed child process"),
            },
            None => {
                logger_info!(
                    None,
                    None,
                    "Child guard dropped with nothing registered to it."
                )
            }
        }
    }
}

fn input_type(previous_input_type: Option<InputType>) -> InputType {
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

        let existing_wals = glob_with(&format!("{}/*.wal", *OUTPUT_WAL_DIRECTORY), options)
            .expect("Unable to check for existing WAL files")
            .filter_map(|file_path| match file_path {
                Ok(path) => {
                    let filename = path
                        .to_str()
                        .expect("Invalid UTF filename for WAL file")
                        .to_string();
                    if let Some(InputType::Wal(previous_wal_file)) = previous_input_type.clone() {
                        if filename > previous_wal_file {
                            Some(filename)
                        } else {
                            None
                        }
                    } else {
                        Some(filename)
                    }
                }
                Err(_e) => panic!("unreadable path. What did you do?"),
            });

        let earliest_wal = existing_wals.min();

        if let Some(path) = earliest_wal {
            InputType::Wal(path)
        } else {
            InputType::PgRcvlogical
        }
    }
}
