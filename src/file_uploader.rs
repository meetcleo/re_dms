use backoff::Error as BackoffError;
use lazy_static::lazy_static;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::Error as S3Error;
use aws_sdk_s3::primitives::ByteStream;

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
    static ref AWS_REGION: String = {
        let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_string());
        let valid_regions = [
            "us-east-1", "us-east-2", "us-west-1", "us-west-2",
            "ca-central-1",
            "eu-west-1", "eu-west-2", "eu-west-3", "eu-central-1",
            "ap-northeast-1", "ap-northeast-2", "ap-northeast-3",
            "ap-southeast-1", "ap-southeast-2", "ap-south-1",
            "sa-east-1",
        ];
        if !valid_regions.contains(&region.as_str()) {
            logger_warning!(None, None, &format!("Invalid AWS region: {}. Defaulting to us-east-1", region));
            "us-east-1".to_string()
        } else {
            region
        }
    };
}

impl FileUploader {
    pub async fn new() -> FileUploader {
        logger_info!(None, None, &format!("Initializing S3 client with region: {}", AWS_REGION.as_str()));
        
        let region = aws_config::Region::new(AWS_REGION.to_string());
        let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(region)
            .load()
            .await;
        let s3_client = S3Client::new(&config);
        
        FileUploader {
            s3_client,
        }
    }
    pub async fn upload_to_s3(
        &self,
        wal_file: &wal_file_manager::WalFile,
        file_name: &str,
        file_struct: &FileStruct,
    ) -> Result<CleoS3File, BackoffError<S3Error>> {
        // info!("copying file {}", file_name);
        let local_filename = file_name;
        let remote_filename = BUCKET_FOLDER.to_owned() + file_name;
        // info!("remote key {}", remote_filename);
        // async
        // info!("{}", local_filename);
        let meta = ::std::fs::metadata(local_filename).unwrap();
        let file_path = std::path::Path::new(local_filename);
        let byte_stream_result = ByteStream::from_path(file_path).await;
        match byte_stream_result {
            Ok(byte_stream) => {
                logger_debug!(
                    Some(wal_file.file_number),
                    Some(&file_struct.table_name),
                    &format!("file_length:{} file_name:{}", meta.len(), file_name)
                );
                
                let maybe_uploaded = self.s3_client
                    .put_object()
                    .bucket(BUCKET_NAME.as_str())
                    .key(&remote_filename)
                    .content_length(meta.len() as i64)
                    .body(byte_stream)
                    .send()
                    .await;
                
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
                        return Err(BackoffError::transient(result.into()));
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
    ) -> Result<CleoS3File, BackoffError<S3Error>> {
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
