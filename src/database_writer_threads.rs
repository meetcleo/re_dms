use tokio::sync::mpsc;
use std::sync::Arc;
use std::collections::{ HashMap };

use crate::file_uploader_threads::{GenericTableThread, GenericTableThreadSplitter, DEFAULT_CHANNEL_SIZE};
use crate::file_uploader::CleoS3File;
use crate::database_writer::DatabaseWriter;
use crate::parser::{TableName};

// manages the thread-per-table and the fanout
pub type DatabaseTableThread = GenericTableThread<CleoS3File>;

// single thread handle
pub type DatabaseWriterThreads = GenericTableThreadSplitter<DatabaseWriter, CleoS3File>;


impl DatabaseWriterThreads {
    pub fn new() -> DatabaseWriterThreads {
        let shared_resource = Arc::new(DatabaseWriter::new());
        let table_streams = HashMap::new();
        DatabaseWriterThreads {shared_resource, table_streams}
    }

    pub fn spawn_database_writer_stream(receiver: mpsc::Receiver<CleoS3File>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(DatabaseWriterThreads::database_uploader_stream(receiver))
    }

    pub async fn database_uploader_stream(mut receiver: mpsc::Receiver<CleoS3File>) {
        let mut database_uploader_stream = DatabaseWriterThreads::new();
        loop {

            let received = receiver.recv().await;
            if let Some(s3_file) = received {
                let table_name = s3_file.table_name.clone();
                let sender = database_uploader_stream.get_sender(table_name);
                // TODO: handle error
                if let Some(ref mut inner_sender) = sender.sender {
                    inner_sender.send(s3_file).await;
                }
            }
            else {
                println!("channel hung up main");
                database_uploader_stream.join_all_table_threads().await;

                println!("finished waiting on threads");
                // TODO: shut down table_streams
                break
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
        self.table_streams
            .entry(table_name)
            .or_insert_with(|| {
                let (inner_sender, receiver) = mpsc::channel::<CleoS3File>(DEFAULT_CHANNEL_SIZE);
                let sender = Some(inner_sender);
                let join_handle = Some(tokio::spawn(Self::spawn_table_thread(receiver, cloned_uploader)));
                DatabaseTableThread { sender, join_handle }
            })
    }

    pub async fn spawn_table_thread(mut receiver: mpsc::Receiver<CleoS3File>, uploader: Arc<DatabaseWriter>) {
        let mut last_table_name = None;
        loop {
            // need to do things this way rather than a match for the borrow checker
            let received = receiver.recv().await;
            if let Some(s3_file) = received {
                let table_name = s3_file.table_name.clone();
                last_table_name = Some(table_name);
                uploader.import_table(&s3_file).await;
            } else {
                println!("channel hung up: {:?}", last_table_name);
                break;
            }
        }
    }
}
