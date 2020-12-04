// use futures::{
//     future::FutureExt, // for `.fuse()`
//     pin_mut,
//     select,
// };
use std::path::Path;
use std::path::PathBuf;
use std::fs;
use crate::parser::{ParsedLine, ChangeKind};
use std::collections::{HashMap};//{ HashMap, BTreeMap, HashSet };
use itertools::Itertools;
// use std::io::prelude::*;

// we have one of these per table,
// it will hold the files to write to and handle the writing
pub struct FileWriter {
    directory: PathBuf,
    insert_file: FileStruct,
    // update_file: FileStruct,
    update_files: HashMap<String, FileStruct>,
    delete_file: FileStruct
}

pub struct FileStruct {
    file_name: PathBuf,
    file: Option<csv::Writer<fs::File>>,
    written_header: bool
}

impl FileStruct {
    fn new(path_name: PathBuf) -> FileStruct {
        FileStruct { file_name: path_name, file: None, written_header: false}
    }

    fn create_writer(&mut self) {
        // TEMP use flexible csv writer
        let writer = csv::WriterBuilder::new().flexible(true).from_path(self.file_name.as_path()).expect("writer couldn't open file");
        self.file = Some(writer);
    }

    fn write_header(&mut self, change: &ParsedLine) {
        if !self.written_header {
            if let Some(_file) = &mut self.file {
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
        if let Some(_file) = &mut self.file {
            if let ParsedLine::ChangedData{ columns,.. } = change {
                // need to own these strings
                let strings: Vec<String> = columns
                    .iter()
                    .filter(|x| x.is_changed_data_column())
                    .map(
                        |x| {
                            // x.column_name()
                            if let Some(value) = x.column_value_for_changed_column() {
                                value.to_string()
                            } else {"".to_owned()} // remember blank as nulls
                        }
                    ).collect();
                self.write(&strings);
                // let result = file.write_record(strings).expect("failed to write csv header");
            }

        }
    }

    fn write<I, T>(&mut self, string: I)
        where
        I: IntoIterator<Item = T>,
        T: AsRef<[u8]>,
    {
        if let Some(file) = &mut self.file {
            file.write_record(string).expect("failed to write file");
            // file.write_all(string.as_bytes()).expect("write failed");
        } else {
            panic!("tried to write to file before creating it");
        }
    }
    fn add_change(&mut self, change: &ParsedLine) {
        if self.file.is_some() {
            if !self.written_header {
                self.write_header(change)
            }
            self.write_line(change);
        } else {
            self.create_writer();
            // TODO: danger recurse
            self.add_change(change)
        }
    }
}

impl FileWriter {
    pub fn new(table_name: &str) -> FileWriter {
        let directory = Path::new(".").join("output");
        // create directory
        let owned_directory = directory.clone().to_owned();
        fs::create_dir_all(owned_directory.as_path()).expect("panic creating directory");
        FileWriter {
            directory: owned_directory,
            insert_file: FileStruct::new(directory.join(table_name.to_owned() + "_inserts.csv")),
            // update_file: FileStruct::new(directory.join(table_name.to_owned() + "_updates.csv")),
            update_files: HashMap::new(),
            delete_file: FileStruct::new(directory.join(table_name.to_owned() + "_deletes.csv"))
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
                            cloned_directory.join(
                                table_name.to_owned() + "_" + &number_of_updates_that_exist.to_string() + "_updates.csv")
                        )
                )
                .add_change(change);
        } else { panic!("non changed data passed to add_change_to_update_file") }
    }
}
