// extern crate futures;

use rusoto_core::{Region, ByteStream};
use rusoto_s3::{S3, S3Client, PutObjectRequest};
use tokio::fs::File;
// use std::io::{self, BufReader};
use tokio_util::codec;
// use std::iter::Iterator;
use futures::{TryStreamExt}; // , FutureExt

use crate::file_writer::FileWriter;

pub struct FileUploader {
    s3_client: S3Client
}

impl FileUploader {

    pub fn new() -> FileUploader {
        FileUploader { s3_client: S3Client::new(Region::UsEast1) }
    }
    // Not actually async yet here
    pub async fn upload_to_s3(&self, file_name: &str) {
        println!("copying file {}", file_name);
        let local_filename =  file_name;
        let remote_filename = "mike-test/".to_owned() + file_name;
        println!("remote key {}", remote_filename);
        let tokio_file = File::open(&local_filename).await.expect("fuck");
        let meta = ::std::fs::metadata(local_filename).unwrap();
        let byte_stream = codec::FramedRead::new(tokio_file, codec::BytesCodec::new()).map_ok(|x| x.freeze() );
        println!("Bytes being transferred {}", meta.len());
        let put_request = PutObjectRequest {
            bucket: "cleo-data-science".to_owned(),
            key: remote_filename,
            content_length: Some(meta.len() as i64),
            body: Some(ByteStream::new(byte_stream).into()),
            ..Default::default()
        };

        self.s3_client
            .put_object(put_request)
            .await
            .expect("Failed to put test object");
    }

    pub async fn upload_table_to_s3(&self, file_writer: &FileWriter) {
        let insert_file = &file_writer.insert_file;
        if insert_file.exists() {
            self.upload_to_s3(insert_file.file_name.to_str().unwrap()).await;
        }
    }
}
