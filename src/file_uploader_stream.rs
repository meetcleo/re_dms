use std::sync::mpsc;
use std::sync::Arc;
use crate::parser::{TableName};
use crate::file_writer::{FileWriter};
use crate::file_uploader::FileUploader;

use std::collections::{ HashMap };

// this holds a job for each table and streams the uploads to them with channels.
pub struct FileUploaderStream {
    file_uploader: Arc<FileUploader>,
    table_streams: HashMap<TableName, TableStream>,
}

impl FileUploaderStream {
}

pub struct TableStream {
    sender: Option<mpsc::Sender<FileWriter>>,
    join_handle: Option<tokio::task::JoinHandle<()>>
}

impl FileUploaderStream {
    pub fn new() -> FileUploaderStream {
        let file_uploader = Arc::new(FileUploader::new());
        let table_streams = HashMap::new();
        FileUploaderStream {file_uploader, table_streams}
    }

    pub fn spawn_file_uploader_stream(receiver: mpsc::Receiver<FileWriter>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(FileUploaderStream::file_uploader_stream(receiver))
    }

    pub async fn file_uploader_stream(receiver: mpsc::Receiver<FileWriter>) {
        let mut file_uploader_stream = FileUploaderStream::new();
        loop {

            let received = receiver.recv();
            if let Ok(file_writer) = received {
                let table_name = file_writer.table_name.clone();
                let sender = file_uploader_stream.get_sender(table_name);
                // TODO: handle error
                sender.sender.as_ref().map(|inner_sender| inner_sender.send(file_writer));
            }
            else {
                println!("channel hung up main");
                file_uploader_stream.join_all_table_threads().await;

                println!("finished waiting on threads");
                // TODO: shut down table_streams
                break
            }
        }
    }

    pub fn get_uploader(&self) -> Arc<FileUploader> {
        // create new reference counted pointer
        self.file_uploader.clone()
    }
    // will either get a sender to a async thread
    // for the table_name.
    // if one doesn't exist will spawn one
    pub fn get_sender(&mut self, table_name: TableName) -> &mut TableStream {
        let cloned_uploader = self.get_uploader();
        self.table_streams
            .entry(table_name)
            .or_insert_with(|| {
                let (inner_sender, receiver) = mpsc::channel::<FileWriter>();
                let sender = Some(inner_sender);
                let join_handle = Some(tokio::spawn(Self::spawn_table_thread(receiver, cloned_uploader)));
                TableStream { sender, join_handle }
            })
    }
    pub async fn join_all_table_threads(&mut self) {
        let join_handles = self.table_streams.values_mut()
            .filter_map(
                |x| {
                    // drop every channel. we've should have alredy sent everything.
                    x.sender = None;
                    // get the join handle and remove it from the struct
                    std::mem::replace(&mut x.join_handle, None)
                }
            ).collect::<Vec<_>>();
        println!("got all join_handles waiting");
        futures::future::join_all(join_handles).await;
        println!("finished waiting on all join handles");
    }

    pub async fn spawn_table_thread(receiver: mpsc::Receiver<FileWriter>, uploader: Arc<FileUploader>) {
        let mut last_table_name = None;
        loop {
            // need to do things this way rather than a match for the borrow checker
            let received = receiver.recv();
            if let Ok(mut file_writer) = received {
                let table_name = file_writer.table_name.clone();
                last_table_name = Some(table_name);
                file_writer.flush_all();
                uploader.upload_table_to_s3(&file_writer).await;
                // println!("received: {:?}", file_writer.table_name.as_str());
            } else {
                println!("channel hung up: {:?}", last_table_name);
                break
            }
        }

    }
}
