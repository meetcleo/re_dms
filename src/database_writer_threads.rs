use backoff::{future::FutureOperation as _, ExponentialBackoff};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

#[allow(unused_imports)]
use log::{debug, error, info, log_enabled, Level};

use crate::database_writer::DatabaseWriter;
use crate::file_uploader_threads::{
    GenericTableThread, GenericTableThreadSplitter, UploaderStageResult, DEFAULT_CHANNEL_SIZE,
};
use crate::parser::TableName;

// manages the thread-per-table and the fanout
pub type DatabaseTableThread = GenericTableThread<UploaderStageResult>;

// single thread handle
pub type DatabaseWriterThreads = GenericTableThreadSplitter<DatabaseWriter, UploaderStageResult>;

const TEN_MINUTES: core::time::Duration = core::time::Duration::from_millis(60_000);

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
                    inner_sender.send(s3_file).await.expect(&format!(
                        "Sending to database_uploader_streame {:?} failed, channel already closed.",
                        current_table_name
                    ));
                }
            } else {
                info!("channel hung up main");
                database_uploader_stream.join_all_table_threads().await;

                info!("finished waiting on threads");
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
        loop {
            // need to do things this way rather than a match for the borrow checker
            let received = receiver.recv().await;
            if let Some(ref uploader_stage_result) = received {
                let table_name = uploader_stage_result.table_name();
                last_table_name = Some(table_name);
                (|| async {
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
                        UploaderStageResult::DdlChange(ddl_change) => {
                            uploader.handle_ddl(&ddl_change).await?;
                        }
                    };
                    Ok(())
                })
                // NOTE: default exponential backoff
                // /// The default initial interval value in milliseconds (0.5 seconds).
                //     pub const INITIAL_INTERVAL_MILLIS: u64 = 500;
                // /// The default randomization factor (0.5 which results in a random period ranging between 50%
                // /// below and 50% above the retry interval).
                // pub const RANDOMIZATION_FACTOR: f64 = 0.5;
                // /// The default multiplier value (1.5 which is 50% increase per back off).
                // pub const MULTIPLIER: f64 = 1.5;
                // /// The default maximum back off time in milliseconds (1 minute).
                // pub const MAX_INTERVAL_MILLIS: u64 = 60_000;
                // /// The default maximum elapsed time in milliseconds (15 minutes).
                // pub const MAX_ELAPSED_TIME_MILLIS: u64 = 900_000;
                // let mut eb = ExponentialBackoff {
                //     current_interval: Duration::from_millis(default::INITIAL_INTERVAL_MILLIS),
                //     initial_interval: Duration::from_millis(default::INITIAL_INTERVAL_MILLIS),
                //     randomization_factor: default::RANDOMIZATION_FACTOR,
                //     multiplier: default::MULTIPLIER,
                //     max_interval: Duration::from_millis(default::MAX_INTERVAL_MILLIS),
                //     max_elapsed_time: Some(Duration::from_millis(default::MAX_ELAPSED_TIME_MILLIS)),
                //     clock: C::default(),
                //     start_time: Instant::now(),
                // };
                .retry(Self::exponential_backoff())
                .await
                .expect(&format!(
                    "Database writing and exponential backoff failed for {:?}",
                    last_table_name,
                ));
            } else {
                info!("channel hung up: {:?}", last_table_name);
                break;
            }
        }
    }

    pub fn exponential_backoff() -> ExponentialBackoff {
        ExponentialBackoff {
            max_elapsed_time: Some(TEN_MINUTES),
            ..ExponentialBackoff::default()
        }
    }
}
