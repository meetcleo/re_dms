use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

#[allow(unused_imports)]
use crate::{function, logger_debug, logger_error, logger_info, logger_panic};

use crate::change_processing;
use crate::file_uploader::{CleoS3File, FileUploader};
use crate::parser::TableName;
use crate::shutdown_handler::ShutdownHandler;
use crate::wal_file_manager::WalFile;

pub const DEFAULT_CHANNEL_SIZE: usize = 1000;

#[derive(Debug)]
pub enum UploaderStageResult {
    S3File(CleoS3File),
    DdlChange(change_processing::DdlChange, WalFile),
}

impl UploaderStageResult {
    // clone, table name is cheap
    pub fn table_name(&self) -> TableName {
        match self {
            Self::S3File(cleo_s3_file) => cleo_s3_file.table_name.clone(),
            Self::DdlChange(ddl_change, ..) => ddl_change.table_name(),
        }
    }

    pub fn wal_file(&self) -> WalFile {
        match self {
            Self::S3File(cleo_s3_file) => cleo_s3_file.wal_file.clone(),
            Self::DdlChange(_, wal_file) => wal_file.clone(),
        }
    }

    pub fn wal_file_number(&self) -> u64 {
        self.wal_file().file_number
    }
}

pub struct GenericTableThreadSplitter<SharedResource, ChannelType> {
    // need to make this public so that type aliases of this type can see it
    // TODO: is there a better way?
    pub shared_resource: Arc<SharedResource>,
    pub table_streams: HashMap<TableName, GenericTableThread<ChannelType>>,
}
// this holds a task, and channel for each table and streams the uploads to them.
pub type FileUploaderThreads =
    GenericTableThreadSplitter<FileUploader, change_processing::ChangeProcessingResult>;

// this holds the task reference and the channel to send to for it.
pub struct GenericTableThread<ChannelType> {
    pub sender: Option<mpsc::Sender<ChannelType>>,
    pub join_handle: Option<tokio::task::JoinHandle<()>>,
}

pub type FileTableThread = GenericTableThread<change_processing::ChangeProcessingResult>;

impl<SharedResource, ChannelType> GenericTableThreadSplitter<SharedResource, ChannelType> {
    pub async fn join_all_table_threads(&mut self) {
        let join_handles = self
            .table_streams
            .values_mut()
            .filter_map(|table_thread| {
                // drop every channel. since we should have already sent everything.
                table_thread.drop_sender_and_return_join_handle()
            })
            .collect::<Vec<_>>();
        logger_info!(
            None,
            None,
            "all_join_handles_collected_file_uploader_waiting"
        );
        futures::future::join_all(join_handles).await;
        logger_info!(None, None, "join_handles_finished_waiting");
    }

    pub fn get_shared_resource(&self) -> Arc<SharedResource> {
        // create new reference counted pointer
        self.shared_resource.clone()
    }
}

impl<ChannelType> GenericTableThread<ChannelType> {
    pub fn drop_sender_and_return_join_handle(&mut self) -> Option<tokio::task::JoinHandle<()>> {
        // once we've let go of the reference, it's dropped automatically at the end of this block
        self.sender = None;
        // get the join handle and remove it from the struct
        std::mem::replace(&mut self.join_handle, None)
    }
}

impl FileUploaderThreads {
    pub fn new() -> FileUploaderThreads {
        let shared_resource = Arc::new(FileUploader::new());
        let table_streams = HashMap::new();
        FileUploaderThreads {
            shared_resource,
            table_streams,
        }
    }

    pub fn spawn_file_uploader_stream(
        receiver: mpsc::Receiver<change_processing::ChangeProcessingResult>,
        result_sender: mpsc::Sender<UploaderStageResult>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(FileUploaderThreads::file_uploader_stream(
            receiver,
            result_sender,
        ))
    }

    // here we fan out to the individual file uploader threads
    pub async fn file_uploader_stream(
        mut receiver: mpsc::Receiver<change_processing::ChangeProcessingResult>,
        result_sender: mpsc::Sender<UploaderStageResult>,
    ) {
        let mut file_uploader_stream = FileUploaderThreads::new();
        loop {
            let received = receiver.recv().await;
            if let Some(file_writer) = received {
                let table_name = file_writer.table_name();
                let sender = file_uploader_stream.get_sender(table_name.clone(), &result_sender);
                // TODO: handle error
                if let Some(ref mut inner_sender) = sender.sender {
                    inner_sender.send(file_writer).await.expect(&format!(
                        "Unable to send from file_uploader_stream main to {}",
                        table_name
                    ));
                }
                sender
                    .sender
                    .as_ref()
                    .map(|inner_sender| async move { inner_sender });
            } else {
                logger_info!(None, None, "main_channel_hung_up");
                file_uploader_stream.join_all_table_threads().await;

                logger_info!(None, None, "finished_waiting_on_table_threads");
                break;
            }
        }
    }

    // will either get a sender to a async thread
    // for the table_name.
    // if one doesn't exist will spawn one
    pub fn get_sender(
        &mut self,
        table_name: TableName,
        result_sender: &mpsc::Sender<UploaderStageResult>,
    ) -> &mut FileTableThread {
        let cloned_uploader = self.get_shared_resource();
        self.table_streams.entry(table_name).or_insert_with(|| {
            let (inner_sender, receiver) =
                mpsc::channel::<change_processing::ChangeProcessingResult>(DEFAULT_CHANNEL_SIZE);
            let sender = Some(inner_sender);
            let cloned_result_sender = result_sender.clone();
            let join_handle = Some(tokio::spawn(Self::spawn_table_thread(
                receiver,
                cloned_uploader,
                cloned_result_sender,
            )));
            FileTableThread {
                sender,
                join_handle,
            }
        })
    }

    pub async fn spawn_table_thread(
        mut receiver: mpsc::Receiver<change_processing::ChangeProcessingResult>,
        uploader: Arc<FileUploader>,
        mut result_sender: mpsc::Sender<UploaderStageResult>,
    ) {
        let mut last_table_name = None;
        let mut last_wal_number = None;
        loop {
            if ShutdownHandler::shutting_down_messily() {
                logger_error!(
                    last_wal_number,
                    last_table_name.as_deref(),
                    "shutting_down_file_uploader_threads_messily"
                );
                return;
            };
            // need to do things this way rather than a match for the borrow checker
            let received = receiver.recv().await;
            if let Some(change) = received {
                let table_name = change.table_name();
                last_wal_number = Some(change.wal_file_number());
                last_table_name = Some(table_name);
                match change {
                    change_processing::ChangeProcessingResult::TableChanges(mut file_writer) => {
                        file_writer.flush_all();
                        let s3_files = uploader.upload_table_to_s3(file_writer).await;
                        for s3_file in s3_files {
                            let result_change = UploaderStageResult::S3File(s3_file);
                            result_sender.send(result_change).await.expect(
                                &format!("Unable to send UploaderStageResult table_changes from file_uploader_stream {:?} to database writer", last_table_name.clone())
                            );
                        }
                    }
                    change_processing::ChangeProcessingResult::DdlChange(ddl_change, wal_file) => {
                        // rewrap into the output enum
                        let result_change = UploaderStageResult::DdlChange(ddl_change, wal_file);
                        result_sender.send(result_change).await.expect(
                            &format!("Unable to send UploaderStageResult ddl_changes from file_uploader_stream {:?} to database writer", last_table_name.clone())
                        );
                    }
                }
            } else {
                logger_info!(
                    last_wal_number,
                    last_table_name.as_deref(),
                    "file_uploader_channel_finished"
                );
                drop(result_sender);
                break;
            }
        }
    }
}
