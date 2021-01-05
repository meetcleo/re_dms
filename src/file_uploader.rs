// extern crate futures;

use futures::TryStreamExt;
use rusoto_core::{ByteStream, Region};
use rusoto_s3::{PutObjectRequest, S3Client, S3};
use tokio::fs::File;
use tokio_util::codec;

#[allow(unused_imports)]
use log::{debug, error, info, log_enabled, Level};

use crate::file_writer::{FileStruct, FileWriter};
use crate::parser::{ChangeKind, ColumnInfo, TableName};
use crate::wal_file_manager;

pub struct FileUploader {
    s3_client: S3Client,
}

// little bag of data
#[derive(Debug)]
pub struct CleoS3File {
    pub remote_filename: String,
    pub kind: ChangeKind,
    pub table_name: TableName,
    pub columns: Vec<ColumnInfo>,
    pub wal_file: wal_file_manager::WalFile,
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
        FileUploader {
            s3_client: S3Client::new(Region::UsEast1),
        }
    }
    // Not actually async yet here
    pub async fn upload_to_s3(
        &self,
        wal_file: wal_file_manager::WalFile,
        file_name: &str,
        file_struct: &FileStruct,
    ) -> CleoS3File {
        // info!("copying file {}", file_name);
        let local_filename = file_name;
        let remote_filename = "mike-test-2/".to_owned() + file_name;
        // info!("remote key {}", remote_filename);
        // async
        // info!("{}", local_filename);
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
                let byte_stream = codec::FramedRead::new(tokio_file, codec::BytesCodec::new())
                    .map_ok(|frame| frame.freeze());
                // // sync
                // let mut file = std::fs::File::open(file_name).unwrap();
                // let mut buffer = Vec::new();
                // file.read_to_end(&mut buffer);

                info!("{} {}", meta.len(), file_name);
                let put_request = PutObjectRequest {
                    bucket: BUCKET_NAME.to_owned(),
                    key: remote_filename.clone(),
                    content_length: Some(meta.len() as i64),
                    body: Some(ByteStream::new(byte_stream).into()),
                    // body: Some(buffer.into()),
                    ..Default::default()
                };

                let maybe_uploaded = self.s3_client.put_object(put_request).await;
                match maybe_uploaded {
                    Ok(_result) => {
                        info!("uploaded file {}", remote_filename);
                    }
                    Err(result) => {
                        panic!("Failed to upload file {} {:?}", remote_filename, result);
                    }
                }
                if let Some(columns) = &file_struct.columns {
                    CleoS3File {
                        remote_filename: remote_filename.clone(),
                        kind: file_struct.kind,
                        table_name: file_struct.table_name.clone(),
                        columns: columns.clone(),
                        wal_file: wal_file,
                    }
                } else {
                    panic!("columns not initialized on file {}", file_name);
                }
            }
            Err(err) => {
                panic!("problem with {:?} {:?}", file_name, err);
            }
        }
    }

    // does all of these concurrently
    // consumes the file_writer
    pub async fn upload_table_to_s3(&self, mut file_writer: FileWriter) -> Vec<CleoS3File> {
        let mut upload_files_vec = vec![];
        let insert_file = &file_writer.insert_file;
        let deletes_file = &file_writer.delete_file;
        let wal_file = &file_writer.wal_file;
        let updates_files: Vec<_> = file_writer
            .update_files
            .values()
            .map(|file| (wal_file.clone(), file))
            .collect();
        upload_files_vec.push((wal_file.clone(), insert_file));
        upload_files_vec.push((wal_file.clone(), deletes_file));
        upload_files_vec.extend(updates_files);

        let s3_file_results = upload_files_vec
            .iter()
            .filter(|(_wal_file, file)| file.exists())
            .map(|(wal_file, file)| async move {
                self.upload_to_s3(wal_file.to_owned(), file.file_name.to_str().unwrap(), &file)
                    .await
            })
            .collect::<Vec<_>>();
        let cleo_s3_files = futures::future::join_all(s3_file_results).await;
        // if we don't have any cleo s3 files... first off, bit weird that we sent a file writer here
        // but secondly, we'd need to clean up the wal file
        file_writer.wal_file.maybe_remove_wal_file();
        cleo_s3_files
    }
}
