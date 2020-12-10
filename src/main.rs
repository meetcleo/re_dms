#![feature(str_split_once)]

use std::fs::File;
use std::io::{self, BufRead};
use std::path::Path;
// use log::{debug, error, log_enabled, info, Level};

// use std::io::prelude::*;
// use std::fs;
use std::sync::mpsc;

mod parser;
mod change_processing;
mod file_writer;
mod file_uploader;
mod database_writer;

// use std::collections::{ HashSet };

// TEMP

#[tokio::main]
async fn main() {
    env_logger::init();
    let (file_transmitter, file_receiver) = mpsc::channel();
    let mut parser = parser::Parser::new(true);
    let mut collector = change_processing::ChangeProcessing::new();
    if let Ok(lines) = read_lines("./data/test_decoding.txt") {
        // Consumes the iterator, returns an (Optional) String
        for line in lines
        {
            if let Ok(ip) = line {
                let parsed_line = parser.parse(&ip);
                match parsed_line {
                    parser::ParsedLine::ContinueParse => {}, // Intentionally left blank, continue parsing
                    _ => {
                        if let Some(file) = collector.add_change(parsed_line) {
                            file_transmitter.send(file);
                        }
                    }
                }
            }
        }
    }

    // collector.print_stats();
    // let mut files = collector.write_files_sync_batch();
    // let file_uploader = &file_uploader::FileUploader::new();

    // // TODO: we need to define a proper threading model,
    // // currently we just do things in batches
    // let results: Vec<_> = files.iter_mut().map(
    //     |file| async move {
    //         file.flush_all();
    //         file_uploader.upload_table_to_s3(&file).await
    //     }
    // ).collect();

    // // TODO: select on these futures for some real good async sauce
    // let s3_files: Vec<_> = futures::future::join_all(results).await.into_iter().flatten().collect();

    // // let mut type_set: HashSet<String> = HashSet::new();
    // // for s3_file in s3_files {
    // //     for column in s3_file.columns {
    // //         type_set.insert(column.column_type().to_string());
    // //     }
    // // }
    // // println!("{:?}", type_set);

    // let ref database_writer = database_writer::DatabaseWriter::new();
    // let table_imports = s3_files.iter().map(
    //     |s3_file| async move {
    //         database_writer.import_table(s3_file).await;
    //     }
    // );
    // futures::future::join_all(table_imports).await;
}

// The output is wrapped in a Result to allow matching on errors
// Returns an Iterator to the Reader of the lines of the file.
fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where P: AsRef<Path>, {
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}
