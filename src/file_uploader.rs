// extern crate futures;

use rusoto_core::{Region, ByteStream};
use rusoto_s3::{S3, S3Client, PutObjectRequest};
use tokio::fs::File;
use tokio_util::codec;
use futures::{TryStreamExt}; // , FutureExt
// // sync
// use std::fs;
// use std::io::Read;


use crate::file_writer::{FileWriter, FileStruct};
use crate::parser::{ChangeKind, ColumnInfo, TableName};

pub struct FileUploader {
    s3_client: S3Client
}

// little bag of data
pub struct CleoS3File {
    pub remote_filename: String,
    pub kind: ChangeKind,
    pub table_name: TableName,
    pub columns: Vec<ColumnInfo>
}
impl CleoS3File {
    pub fn remote_path(&self) -> String {
        "s3://".to_owned() + BUCKET_NAME.as_ref() + "/" + self.remote_filename.as_ref()
    }
}

// just use a constant for now
// use config later
const BUCKET_NAME: &str = "cleo-data-science";

impl FileUploader {

    pub fn new() -> FileUploader {
        FileUploader { s3_client: S3Client::new(Region::UsEast1) }
    }
    // Not actually async yet here
    pub async fn upload_to_s3(&self, file_name: &str, file_struct: &FileStruct) -> CleoS3File {
        // println!("copying file {}", file_name);
        let local_filename =  file_name;
        let remote_filename = "mike-test-2/".to_owned() + file_name;
        // println!("remote key {}", remote_filename);
        // async
        // println!("{}", local_filename);
        let meta = ::std::fs::metadata(local_filename).unwrap();
        let tokio_file_result = File::open(&local_filename).await;
        match tokio_file_result {
            Ok(tokio_file) => {
                // async
                // roughly equivalent to https://stackoverflow.com/questions/59318460/what-is-the-best-way-to-convert-an-asyncread-to-a-trystream-of-bytes
                // but requires ignoring a lot if unimportant stuff.
                // Essentially, we have an async file, and want an immutable bytestream that we can read from it.
                // our s3 library can then stream that to s3. This means we pause the async task on every bit of both read and
                // write IO and are very efficient.
                // map_ok is a future combinator, that will apply the closure to each frame (freeze-ing the frame to make it immutable)
                let byte_stream = codec::FramedRead::new(tokio_file, codec::BytesCodec::new()).map_ok(|frame| frame.freeze() );
                // // sync
                // let mut file = std::fs::File::open(file_name).unwrap();
                // let mut buffer = Vec::new();
                // file.read_to_end(&mut buffer);

                println!("{} {}", meta.len(), file_name);
                let put_request = PutObjectRequest {
                    bucket: BUCKET_NAME.to_owned(),
                    key: remote_filename.clone(),
                    content_length: Some(meta.len() as i64),
                    body: Some(ByteStream::new(byte_stream).into()),
                    // body: Some(buffer.into()),
                    ..Default::default()
                };

                let maybe_uploaded = self.s3_client
                    .put_object(put_request)
                    .await;
                match maybe_uploaded {
                    Ok(_result) => {
                        println!("uploaded file {}", remote_filename);
                    },
                    Err(result) => {
                        panic!("Failed to upload file {} {:?}", remote_filename, result);
                    }
                }
                if let Some(columns) = &file_struct.columns {
                    CleoS3File {
                        remote_filename: remote_filename.clone(),
                        kind: file_struct.kind,
                        table_name: file_struct.table_name.clone(),
                        columns: columns.clone()
                    }
                } else {
                    panic!("columns not initialized on file {}", file_name);
                }
            },
            Err(err) => { panic!("problem with {:?} {:?}", file_name, err); }
        }
    }

    // does all of these concurrently
    pub async fn upload_table_to_s3(&self, file_writer: &FileWriter) -> Vec<CleoS3File> {
        let mut upload_files_vec = vec![];
        let insert_file = &file_writer.insert_file;
        let deletes_file = &file_writer.delete_file;
        let updates_files: Vec<_> = file_writer.update_files.values().collect();
        upload_files_vec.push(insert_file);
        upload_files_vec.push(deletes_file);
        upload_files_vec.extend(updates_files);

        let s3_file_results = upload_files_vec.iter()
            .filter(|x| x.exists())
            .map(
            |file| async move {
                self.upload_to_s3(file.file_name.to_str().unwrap(), &file).await
            }
        ).collect::<Vec<_>>();
        futures::future::join_all(s3_file_results).await
    }
}
