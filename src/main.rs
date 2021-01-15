#![feature(str_split_once)]
// #![deny(warnings)]

use clap::{App, Arg};
use either::Either;
use lazy_static::lazy_static;
use std::convert::TryInto;
use std::io::{self, BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Command, Stdio};

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
mod wal_file_manager;

use file_uploader_threads::DEFAULT_CHANNEL_SIZE;
use shutdown_handler::{RuntimeType, ShutdownHandler};

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

#[tokio::main]
async fn main() {
    dotenv().ok();
    env_logger::init();

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

    // use either for match arms returning different types
    // very handy
    let buffered_reader = if !arg_matches.is_present("read_from_stdin") {
        let (process, bufreader) = get_buffered_reader_process();
        // https://stackoverflow.com/questions/49210815/how-do-i-send-a-signal-to-a-child-subprocess

        ShutdownHandler::register_shutdown_handler(RuntimeType::from_pid(
            process
                .id()
                .try_into()
                .expect("pid that's greater than i32::MAX"),
        ));
        // how to term the child process
        Either::Left(bufreader)
    } else {
        ShutdownHandler::register_shutdown_handler(RuntimeType::Stdin);
        Either::Right(locked_stdin)
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
            if let wal_file_manager::WalLineResult::SwapWal(wal_file) = wal_file_manager_result {
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
