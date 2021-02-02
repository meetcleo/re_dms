use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

#[allow(unused_imports)]
use crate::{function, logger_debug, logger_error, logger_info, logger_panic};

use crate::database_writer::DatabaseWriter;
use crate::exponential_backoff::*;
use crate::file_uploader_threads::{
    GenericTableThread, GenericTableThreadSplitter, UploaderStageResult, DEFAULT_CHANNEL_SIZE,
};
use crate::parser::TableName;
use crate::shutdown_handler::ShutdownHandler;

// manages the thread-per-table and the fanout
pub type DatabaseTableThread = GenericTableThread<UploaderStageResult>;

// single thread handle
pub type DatabaseWriterThreads = GenericTableThreadSplitter<DatabaseWriter, UploaderStageResult>;

impl DatabaseWriterThreads {
    pub fn new() -> DatabaseWriterThreads {
        let shared_resource = Arc::new(DatabaseWriter::new());
        let table_streams = HashMap::new();
        DatabaseWriterThreads {
            shared_resource,
            table_streams,
        }
    }

    pub fn spawn_database_writer_stream(
        receiver: mpsc::Receiver<UploaderStageResult>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(DatabaseWriterThreads::database_uploader_stream(receiver))
    }

    pub async fn database_uploader_stream(mut receiver: mpsc::Receiver<UploaderStageResult>) {
        let mut database_uploader_stream = DatabaseWriterThreads::new();
        loop {
            let received = receiver.recv().await;
            if let Some(s3_file) = received {
                let table_name = s3_file.table_name();
                let current_table_name = table_name.clone();
                let sender = database_uploader_stream.get_sender(table_name);
                if let Some(ref mut inner_sender) = sender.sender {
                    let send_result = inner_sender.send(s3_file).await;
                    match send_result {
                        Ok(()) => {}
                        Err(err) => {
                            ShutdownHandler::register_messy_shutdown();
                            panic!(
                                "Sending to database_uploader_stream {:?} failed, channel already closed. err: {:?}",
                                current_table_name,
                                err
                            );
                        }
                    }
                }
            } else {
                logger_info!(None, None, "main_channel_hung_up");
                database_uploader_stream.join_all_table_threads().await;

                logger_info!(None, None, "finished_waiting_on_table_threads");
                break;
            }
        }
    }

    pub fn get_uploader(&self) -> Arc<DatabaseWriter> {
        // create new reference counted pointer
        self.shared_resource.clone()
    }
    // will either get a sender to a async thread
    // for the table_name.
    // if one doesn't exist will spawn one
    pub fn get_sender(&mut self, table_name: TableName) -> &mut DatabaseTableThread {
        let cloned_uploader = self.get_uploader();
        self.table_streams.entry(table_name).or_insert_with(|| {
            let (inner_sender, receiver) =
                mpsc::channel::<UploaderStageResult>(DEFAULT_CHANNEL_SIZE);
            let sender = Some(inner_sender);
            let join_handle = Some(tokio::spawn(Self::spawn_table_thread(
                receiver,
                cloned_uploader,
            )));
            DatabaseTableThread {
                sender,
                join_handle,
            }
        })
    }

    pub async fn spawn_table_thread(
        mut receiver: mpsc::Receiver<UploaderStageResult>,
        uploader: Arc<DatabaseWriter>,
    ) {
        let mut last_table_name = None;
        let mut last_wal_number = None;
        loop {
            if ShutdownHandler::shutting_down_messily() {
                logger_error!(
                    last_wal_number,
                    last_table_name.as_deref(),
                    "shutting_down_database_writer_threads_messily"
                );
                return;
            };
            // need to do things this way rather than a match for the borrow checker
            let received = receiver.recv().await;
            if let Some(ref uploader_stage_result) = received {
                let table_name = uploader_stage_result.table_name();
                last_wal_number = Some(uploader_stage_result.wal_file_number());
                // so we can register an error if we fail
                let mut wal_file = uploader_stage_result.wal_file();
                last_table_name = Some(table_name);
                let backoff_result = (|| async {
                    match uploader_stage_result {
                        UploaderStageResult::S3File(cleo_s3_file) => {
                            // dereference to get the struct, then clone,
                            // so we have a mutable reference to a cloned version of this struct.
                            // we can't hold a mutable reference to something outside of this async closure because then
                            // rust isn't happy because we've got these dangling mutable references around.
                            // (our closure can't be FnOnce, and there's problems if it's FnMut, here we make
                            // it an Fn at the expense of this clone)
                            // it took me a _loooooong_ time to grok all of that.
                            let mut mutable_s3_file = (*cleo_s3_file).clone();
                            uploader.apply_s3_changes(&mut mutable_s3_file).await?;
                        }
                        UploaderStageResult::DdlChange(ddl_change, wal_file) => {
                            // I don't think we need to handle a ddl change being the last
                            // change in a wal file, since it relies on a difference, so there must be a line after it
                            // so we don't need to call maybe_remove_wal_file();
                            uploader
                                .handle_ddl(&ddl_change, wal_file.file_number)
                                .await?;
                        }
                    };
                    Ok(())
                })
                .retry(default_exponential_backoff())
                .await;
                match backoff_result {
                    Ok(..) => {
                        // need to clean up our wal file
                        wal_file.maybe_remove_wal_file();
                    }
                    Err(err) => {
                        wal_file.register_error();
                        logger_error!(
                            last_wal_number,
                            last_table_name.as_deref(),
                            &format!("database_writer_exponential_backoff_failed:{:?}", err)
                        );
                        ShutdownHandler::register_messy_shutdown()
                    }
                }
            } else {
                logger_info!(
                    last_wal_number,
                    last_table_name.as_deref(),
                    "channel_hung_up"
                );
                break;
            }
            // clean up wal file
            // map because it's in an option, but we don't want to
            // borrow it
            received.map(|x| x.consume_and_maybe_remove_wal_file());
        }
    }
}
