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
                let sender = database_uploader_stream.get_sender(table_name);
                // TODO: handle error
                if let Some(ref mut inner_sender) = sender.sender {
                    inner_sender.send(s3_file).await;
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
            if let Some(uploader_stage_result) = received {
                let table_name = uploader_stage_result.table_name();
                last_table_name = Some(table_name);
                match uploader_stage_result {
                    UploaderStageResult::S3File(cleo_s3_file) => {
                        uploader.import_table(&cleo_s3_file).await;
                    }
                    UploaderStageResult::DdlChange(ddl_change) => {
                        uploader.handle_ddl(&ddl_change).await;
                    }
                }
            } else {
                info!("channel hung up: {:?}", last_table_name);
                break;
            }
        }
    }
}
