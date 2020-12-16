use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

use crate::change_processing;
use crate::file_uploader::{CleoS3File, FileUploader};
use crate::parser::TableName;

pub const DEFAULT_CHANNEL_SIZE: usize = 1000;

pub enum UploaderStageResult {
    S3File(CleoS3File),
    DdlChange(change_processing::DdlChange),
}

impl UploaderStageResult {
    // clone, table name is cheap
    pub fn table_name(&self) -> TableName {
        match self {
            Self::S3File(cleo_s3_file) => cleo_s3_file.table_name.clone(),
            Self::DdlChange(ddl_change) => ddl_change.table_name(),
        }
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
        println!("got all join_handles file_uploader waiting");
        futures::future::join_all(join_handles).await;
        println!("finished waiting on all file_uploader join handles");
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

    pub async fn file_uploader_stream(
        mut receiver: mpsc::Receiver<change_processing::ChangeProcessingResult>,
        result_sender: mpsc::Sender<UploaderStageResult>,
    ) {
        let mut file_uploader_stream = FileUploaderThreads::new();
        loop {
            let received = receiver.recv().await;
            if let Some(file_writer) = received {
                let table_name = file_writer.table_name();
                let sender = file_uploader_stream.get_sender(table_name, &result_sender);
                // TODO: handle error
                if let Some(ref mut inner_sender) = sender.sender {
                    inner_sender.send(file_writer).await;
                }
                sender
                    .sender
                    .as_ref()
                    .map(|inner_sender| async move { inner_sender });
            } else {
                println!("channel hung up main");
                file_uploader_stream.join_all_table_threads().await;

                println!("finished waiting on threads");
                // TODO: shut down table_streams
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
        loop {
            // need to do things this way rather than a match for the borrow checker
            let received = receiver.recv().await;
            if let Some(change) = received {
                let table_name = change.table_name();
                last_table_name = Some(table_name);
                match change {
                    change_processing::ChangeProcessingResult::TableChanges(mut file_writer) => {
                        file_writer.flush_all();
                        let s3_files = uploader.upload_table_to_s3(&file_writer).await;
                        for s3_file in s3_files {
                            // TODO handle errors
                            let result_change = UploaderStageResult::S3File(s3_file);
                            result_sender.send(result_change).await;
                        }
                    }
                    change_processing::ChangeProcessingResult::DdlChange(ddl_change) => {
                        // rewrap into the output enum
                        let result_change = UploaderStageResult::DdlChange(ddl_change);
                        result_sender.send(result_change).await;
                    }
                }
            } else {
                // println!("channel hung up: {:?}", last_table_name);
                drop(result_sender);
                break;
            }
        }
    }
}
