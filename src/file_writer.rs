// use futures::{
//     future::FutureExt, // for `.fuse()`
//     pin_mut,
//     select,
// };
use std::path::Path;
use std::path::PathBuf;
use std::fs;
use crate::parser::{ParsedLine, ChangeKind, ColumnInfo};
use std::collections::{HashMap};//{ HashMap, BTreeMap, HashSet };
use itertools::Itertools;
// use std::io::prelude::*;
use flate2::Compression;
use flate2::write::GzEncoder;

// we have one of these per table,
// it will hold the files to write to and handle the writing
pub struct FileWriter {
    directory: PathBuf,
    pub insert_file: FileStruct,
    // update_file: FileStruct,
    pub update_files: HashMap<String, FileStruct>,
    pub delete_file: FileStruct
}

enum CsvWriter {
    Uninitialized,
    ReadyToWrite(csv::Writer<flate2::write::GzEncoder<fs::File>>),
    Finished
}

impl CsvWriter {
    pub fn is_some(&self) -> bool {
        match self {
            CsvWriter::Uninitialized => false,
            _ => true
        }
    }
    pub fn is_none(&self) -> bool {
        !self.is_some()
    }
    // move
    pub fn flush_and_close(&mut self) {
        if self.is_some() {
            let new_value = CsvWriter::Finished;
            let old_value = std::mem::replace(self, new_value);
            if let CsvWriter::ReadyToWrite(writer) = old_value {
                writer.into_inner().map(|gzip| gzip.finish().unwrap());
            }
        }

    }
}

pub struct FileStruct {
    pub file_name: PathBuf,
    pub table_name: String,
    pub kind: ChangeKind,
    pub columns: Option<Vec<ColumnInfo>>,
    file: CsvWriter,
    written_header: bool
}

impl FileStruct {
    pub fn exists(&self) -> bool {
        self.file.is_some()
    }
}

impl FileStruct {
    fn new(path_name: PathBuf, kind: ChangeKind, table_name: String ) -> FileStruct {
        FileStruct {
            file_name: path_name,
            file: CsvWriter::Uninitialized,
            kind: kind,
            table_name: table_name,
            written_header: false,
            columns: None
        }
    }

    fn create_writer(&mut self) {
        let file = fs::File::create(self.file_name.as_path()).unwrap();
        let writer = GzEncoder::new(file, Compression::default());
        let csv_writer = csv::WriterBuilder::new().flexible(true).from_writer(writer);
        self.file = CsvWriter::ReadyToWrite(csv_writer);
    }

    fn write_header(&mut self, change: &ParsedLine) {
        if !self.written_header {
            if let CsvWriter::ReadyToWrite(_file) = &mut self.file {
                if let ParsedLine::ChangedData{ columns,.. } = change {
                    let strings: Vec<&str> = columns.iter().map(|x| x.column_name()).collect();
                    self.write(&strings);
                    // let result = file.write_record(strings).expect("failed to write csv header");
                }

            }
            self.written_header = true;
        }
    }

    fn write_line(&mut self, change: &ParsedLine) {
        self.write_header(change);
        if let CsvWriter::ReadyToWrite(_file) = &mut self.file {
            if let ParsedLine::ChangedData{ columns,.. } = change {
                // need to own these strings
                let strings: Vec<String> = columns
                    .iter()
                    .filter(|x| x.is_changed_data_column())
                    .map(
                        |x| {
                            if let Some(value) = x.column_value_for_changed_column() {
                                value.to_string()
                            } else {"".to_owned()} // remember blank as nulls
                        }
                    ).collect();
                self.write(&strings);
            }

        }
    }

    fn write<I, T>(&mut self, string: I)
        where
        I: IntoIterator<Item = T>,
        T: AsRef<[u8]>,
    {
        if let CsvWriter::ReadyToWrite(file) = &mut self.file {
            // TODO handle error
            file.write_record(string).expect("failed to write file");
        } else {
            panic!("tried to write to file before creating it");
        }
    }
    fn add_change(&mut self, change: &ParsedLine) {
        if self.file.is_none() {
            self.create_writer();
        }
        self.write_line(change)
    }
}

// TODO: write iterator over files
impl FileWriter {
    pub fn new(table_name: &str) -> FileWriter {
        let directory = Path::new("output");
        // create directory
        let owned_directory = directory.clone().to_owned();
        fs::create_dir_all(owned_directory.as_path()).expect("panic creating directory");
        FileWriter {
            directory: owned_directory,
            insert_file: FileStruct::new(directory.join(table_name.to_owned() + "_inserts.csv.gz"), ChangeKind::Insert, table_name.to_owned()),
            // update_file: FileStruct::new(directory.join(table_name.to_owned() + "_updates.csv")),
            update_files: HashMap::new(),
            delete_file: FileStruct::new(directory.join(table_name.to_owned() + "_deletes.csv.gz"), ChangeKind::Delete, table_name.to_owned())
        }
    }
    pub fn add_change(&mut self, change: &ParsedLine) {
        if let ParsedLine::ChangedData{kind,..} = change {
            match kind {
                ChangeKind::Insert => {
                    self.insert_file.add_change(change);
                },
                ChangeKind::Update => {

                    self.add_change_to_update_file(change);
                },
                ChangeKind::Delete => {
                    self.delete_file.add_change(change);
                },
            }
        }
    }
    pub fn flush_all(&mut self) {
        self.insert_file.file.flush_and_close();
        for x in self.update_files.values_mut() {
            x.file.flush_and_close()
        }
        // self.update_files.values_mut().map(|x| ).collect();
        self.delete_file.file.flush_and_close();
    }
    // update_files is a hash of our column names to our File
    fn add_change_to_update_file(&mut self, change: &ParsedLine) {
        let update_key: String = change.columns_for_changed_data()
            .iter()
            .filter(|x| x.is_changed_data_column())
            .map(|x| x.column_name())
            .sorted()
            .join(",");
        let number_of_updates_that_exist = self.update_files.len();
        let cloned_directory = self.directory.clone();
        if let ParsedLine::ChangedData{table_name, ..} = change {
            self.update_files.entry(update_key)
                .or_insert_with(
                    ||
                        FileStruct::new(
                            cloned_directory.join(table_name.to_owned() + "_" + &number_of_updates_that_exist.to_string() + "_updates.csv.gz"),
                            ChangeKind::Update,
                            table_name.to_owned()
                        )
                )
                .add_change(change);
        } else { panic!("non changed data passed to add_change_to_update_file") }
    }
}
