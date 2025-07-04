use backoff::Error as BackoffError;
use futures::TryStreamExt;
use lazy_static::lazy_static;
use rusoto_core::{ByteStream, Region, RusotoError};
use rusoto_s3::{PutObjectError, PutObjectRequest, S3Client, S3};
use tokio::fs::File;
use tokio_util::codec;

#[allow(unused_imports)]
use crate::{function, logger_debug, logger_error, logger_info, logger_panic, logger_warning};

use crate::exponential_backoff::*;
use crate::file_writer::{FileStruct, FileWriter};
use crate::parser::{ChangeKind, ColumnInfo, TableName};
use crate::shutdown_handler::ShutdownHandler;
use crate::wal_file_manager;
use crate::wal_file_manager::WalFile;

pub struct FileUploader {
    s3_client: S3Client,
}

// little bag of data
#[derive(Debug, Clone)]
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
lazy_static! {
    static ref BUCKET_NAME: String =
        std::env::var("BUCKET_NAME").expect("BUCKET_NAME env is not set");
    static ref BUCKET_FOLDER: String =
        std::env::var("BUCKET_FOLDER").expect("BUCKET_FOLDER env is not set");
    static ref AWS_REGION: String =
        std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string());
}

impl FileUploader {
    pub fn new() -> FileUploader {
        logger_info!(None, None, &format!("Initializing S3 client with region: {}", AWS_REGION.as_str()));
        
        let region = match AWS_REGION.as_str() {
            "us-east-1" => Region::UsEast1,
            "us-east-2" => Region::UsEast2,
            "us-west-1" => Region::UsWest1,
            "us-west-2" => Region::UsWest2,
            "ca-central-1" => Region::CaCentral1,
            "eu-west-1" => Region::EuWest1,
            "eu-west-2" => Region::EuWest2,
            "eu-west-3" => Region::EuWest3,
            "eu-central-1" => Region::EuCentral1,
            "ap-northeast-1" => Region::ApNortheast1,
            "ap-northeast-2" => Region::ApNortheast2,
            "ap-northeast-3" => Region::ApNortheast3,
            "ap-southeast-1" => Region::ApSoutheast1,
            "ap-southeast-2" => Region::ApSoutheast2,
            "ap-south-1" => Region::ApSouth1,
            "sa-east-1" => Region::SaEast1,
            _ => {
                logger_warning!(None, None, &format!("Invalid AWS region: {}. Defaulting to us-east-1", AWS_REGION.as_str()));
                Region::UsEast1
            }
        };
        
        FileUploader {
            s3_client: S3Client::new(region),
        }
    }
    pub async fn upload_to_s3(
        &self,
        wal_file: &wal_file_manager::WalFile,
        file_name: &str,
        file_struct: &FileStruct,
    ) -> Result<CleoS3File, BackoffError<RusotoError<PutObjectError>>> {
        // info!("copying file {}", file_name);
        let local_filename = file_name;
        let remote_filename = BUCKET_FOLDER.to_owned() + file_name;
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
                // map_ok is a future combinator, that will apply the closure to each frame
                // (freeze-ing the frame to make it immutable since this is needed by the rusoto api)
                let byte_stream = codec::FramedRead::new(tokio_file, codec::BytesCodec::new())
                    .map_ok(|frame| frame.freeze());

                logger_debug!(
                    Some(wal_file.file_number),
                    Some(&file_struct.table_name),
                    &format!("file_length:{} file_name:{}", meta.len(), file_name)
                );
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
                        logger_info!(
                            Some(wal_file.file_number),
                            Some(&file_struct.table_name),
                            &format!("uploaded_file:{}", remote_filename)
                        );
                    }
                    Err(result) => {
                        // Log the specific S3 error details
                        logger_error!(
                            Some(wal_file.file_number),
                            Some(&file_struct.table_name),
                            &format!("S3 upload error: {:?} for file: {}", result, remote_filename)
                        );
                        // treat s3 errors as transient
                        return Err(BackoffError::transient(result));
                    }
                }
                if let Some(columns) = &file_struct.columns {
                    Ok(CleoS3File {
                        remote_filename: remote_filename.clone(),
                        kind: file_struct.kind,
                        table_name: file_struct.table_name.clone(),
                        columns: columns.clone(),
                        wal_file: (*wal_file).clone(),
                    })
                } else {
                    // logic error
                    panic!("columns not initialized on file {}", file_name);
                }
            }
            Err(err) => {
                // bail early for local file disk errors
                // Is this right? should be retry reading from disk?
                panic!("Error reading file from disk {:?} {:?}", file_name, err);
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
        let updates_files: Vec<(WalFile, &FileStruct)> = file_writer
            .update_files
            .values()
            .map(|file| (wal_file.clone(), file))
            .collect();
        upload_files_vec.push((wal_file.clone(), insert_file));
        upload_files_vec.push((wal_file.clone(), deletes_file));
        upload_files_vec.extend(updates_files);

        let s3_file_results = upload_files_vec
            .iter_mut()
            .filter(|(_wal_file, file)| file.exists())
            .map(|(wal_file, file)| async move {
                self.upload_to_s3_with_backoff(wal_file, file.file_name.to_str().unwrap(), &file)
                    .await
            })
            .collect::<Vec<_>>();
        let cleo_s3_files = futures::future::join_all(s3_file_results).await;
        // just make sure we drop any dangling references to wal_files before we try and maybe_remove it
        drop(upload_files_vec);
        // if we don't have any cleo s3 files... first off, bit weird that we sent a file writer here
        // but secondly, we'd need to clean up the wal file
        file_writer.wal_file.maybe_remove_wal_file();
        if cleo_s3_files.iter().any(Result::is_err) {
            vec![]
        } else {
            cleo_s3_files.into_iter().filter_map(Result::ok).collect()
        }
    }

    pub async fn upload_to_s3_with_backoff(
        &self,
        wal_file: &mut wal_file_manager::WalFile,
        file_name: &str,
        file_struct: &FileStruct,
    ) -> Result<CleoS3File, BackoffError<RusotoError<PutObjectError>>> {
        // for simplicity, this
        let result = retry(default_exponential_backoff(), || async { self.upload_to_s3(wal_file, file_name, file_struct).await }).await;
        match result {
            Ok(s3_file) => Ok(s3_file),
            Err(err) => {
                // belt and bracers, this won't get deleted
                wal_file.register_error();
                ShutdownHandler::register_messy_shutdown();
                logger_error!(
                    Some(wal_file.file_number),
                    Some(&file_struct.table_name),
                    &format!("file_upload_failed file:{} error:{}", file_name, err)
                );
                Err(err)?
            }
        }
    }
}
