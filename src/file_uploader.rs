// extern crate futures;

use rusoto_core::{Region, ByteStream};
use rusoto_s3::{S3, S3Client, PutObjectRequest};
use tokio::fs::File;
// use std::io::{self, BufReader};
use tokio_util::codec;
// use std::iter::Iterator;
use futures::{TryStreamExt}; // , FutureExt
// use std::io;
// use std::io::prelude::*;
// use std::fs::File;


use crate::file_writer::FileWriter;
use crate::parser::ChangeKind;

pub struct FileUploader {
    s3_client: S3Client
}

// little bag of data
pub struct CleoS3File {
    pub remote_filename: String,
    pub kind: ChangeKind,
    pub table_name: String,
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
    pub async fn upload_to_s3(&self, file_name: &str, kind: ChangeKind, table_name: &str) -> CleoS3File {
        // println!("copying file {}", file_name);
        let local_filename =  file_name;
        let remote_filename = "mike-test/".to_owned() + file_name;
        // println!("remote key {}", remote_filename);
        // async
        println!("{}", local_filename);
        let meta = ::std::fs::metadata(local_filename).unwrap();
        let tokio_file = File::open(&local_filename).await.expect("fuck");
        // async
        let byte_stream = codec::FramedRead::new(tokio_file, codec::BytesCodec::new()).map_ok(|x| x.freeze() );
        // sync
        // let mut file = File::open(file_name).unwrap();
        // let mut buffer = Vec::new();
        // file.read_to_end(&mut buffer);

        println!("{} {}", meta.len(), file_name);
        let put_request = PutObjectRequest {
            bucket: BUCKET_NAME.to_owned(),
            key: remote_filename.clone(),
            content_length: Some(meta.len() as i64),
            body: Some(ByteStream::new(byte_stream).into()),
            ..Default::default()
        };

        self.s3_client
            .put_object(put_request)
            .await
            .expect("Failed to put test object");
        // println!("uploaded file {}", remote_filename);
        CleoS3File { remote_filename: remote_filename.clone(), kind: kind, table_name: table_name.to_owned() }
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
                self.upload_to_s3(file.file_name.to_str().unwrap(), file.kind, &file.table_name).await
            }
        ).collect::<Vec<_>>();
        futures::future::join_all(s3_file_results).await
    }
}
