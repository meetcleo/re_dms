use glob::glob;
use std::fs;
use std::path::Path;
use std::path::PathBuf;

use crate::parser::{ChangeKind, ColumnInfo, ParsedLine, TableName};
use std::collections::HashMap; //{ HashMap, BTreeMap, HashSet };

use itertools::Itertools;

use flate2::write::GzEncoder;
use flate2::Compression;

// we have one of these per table,
// it will hold the files to write to and handle the writing
#[derive(Debug)]
pub struct FileWriter {
    directory: PathBuf,
    pub insert_file: FileStruct,
    pub update_files: HashMap<String, FileStruct>,
    pub delete_file: FileStruct,
    pub table_name: TableName,
}

#[derive(Debug)]
enum CsvWriter {
    Uninitialized,
    ReadyToWrite(csv::Writer<flate2::write::GzEncoder<fs::File>>),
    Finished,
}

impl CsvWriter {
    pub fn is_some(&self) -> bool {
        match self {
            CsvWriter::Uninitialized => false,
            _ => true,
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
                writer
                    .into_inner()
                    .map(|gzip| gzip.finish().unwrap())
                    .unwrap();
            }
        }
    }
}

#[derive(Debug)]
pub struct FileStruct {
    pub file_name: PathBuf,
    pub table_name: TableName,
    pub kind: ChangeKind,
    pub columns: Option<Vec<ColumnInfo>>,
    file: CsvWriter,
    written_header: bool,
}

impl FileStruct {
    pub fn new(directory_name: &Path, kind: ChangeKind, table_name: TableName) -> FileStruct {
        let new_file_name = Self::new_file_name(directory_name, kind, table_name.as_str());
        let file_struct = FileStruct {
            file_name: new_file_name.to_path_buf(),
            file: CsvWriter::Uninitialized,
            kind: kind,
            table_name: table_name.clone(),
            written_header: false,
            columns: None,
        };
        // we touch the file when we create the struct created
        let _file = fs::File::create(new_file_name.as_path()).unwrap();
        file_struct
    }

    // creates a new filename of the sort directory/n_table_name_inserts.csv.gz
    // where n is a number
    // TODO: do we just want to save the number and be passing it in somewhere I'm not super happy with thrashing our directory tree?
    fn new_file_name(directory_name: &Path, kind: ChangeKind, table_name: &str) -> PathBuf {
        let the_file_glob_pattern =
            ["*", table_name, kind.to_string().as_str()].join("_") + ".csv.gz";
        let the_glob_pattern = directory_name.join(the_file_glob_pattern);

        let current_file_number = glob(the_glob_pattern.to_str().unwrap())
            .unwrap()
            .map(|file_path| {
                match file_path {
                    Ok(path) => {
                        let file_name = path.file_name().unwrap();
                        // if it's not UTF-8 it can crash
                        let file_name_str = file_name.to_str().unwrap();
                        // filename is number_stuff.
                        let (file_number_str, _) = file_name_str.split_once('_').unwrap();
                        let file_number: i32 = file_number_str.parse::<i32>().unwrap();
                        file_number
                    }

                    // if the path matched but was unreadable,
                    // thereby preventing its contents from matching
                    Err(_e) => panic!("unreadable path. What did you do?"),
                }
            })
            .max()
            .unwrap_or(0);
        let new_file_number = current_file_number + 1;
        let the_new_file_name = [
            new_file_number.to_string().as_str(),
            table_name,
            kind.to_string().as_str(),
        ]
        .join("_")
            + ".csv.gz";
        let the_new_file_name_and_directory = directory_name.join(the_new_file_name);
        the_new_file_name_and_directory
    }

    // the file only has data in it if we've written the header
    pub fn exists(&self) -> bool {
        self.written_header
    }

    fn create_writer(&mut self) {
        let file = fs::File::create(self.file_name.as_path()).unwrap();
        let writer = GzEncoder::new(file, Compression::default());
        let csv_writer = csv::WriterBuilder::new().from_writer(writer);
        self.file = CsvWriter::ReadyToWrite(csv_writer);
    }

    fn write_header(&mut self, change: &ParsedLine) {
        if !self.written_header {
            if let CsvWriter::ReadyToWrite(_file) = &mut self.file {
                if let ParsedLine::ChangedData { columns, .. } = change {
                    let changed_column_info: Vec<ColumnInfo> = columns
                        .iter()
                        .filter(|x| x.is_changed_data_column())
                        .map(|x| x.column_info().clone())
                        .collect();
                    let strings: Vec<&str> = changed_column_info
                        .iter()
                        .map(|x| x.column_name())
                        .collect();
                    self.write(&strings);
                    self.columns = Some(changed_column_info);
                }
            }
            self.written_header = true;
        }
    }

    fn write_line(&mut self, change: &ParsedLine) {
        self.write_header(change);
        if let CsvWriter::ReadyToWrite(_file) = &mut self.file {
            if let ParsedLine::ChangedData { columns, .. } = change {
                // need to own these strings
                let strings: Vec<String> = columns
                    .iter()
                    .filter(|x| x.is_changed_data_column())
                    .map(|x| {
                        if let Some(value) = x.column_value_for_changed_column() {
                            value.to_string()
                        } else {
                            "".to_owned()
                        } // remember blank as nulls
                    })
                    .collect();
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
    pub fn new(table_name: TableName) -> FileWriter {
        let directory = Path::new("output");
        // create directory
        let owned_directory = directory.clone().to_owned();
        fs::create_dir_all(owned_directory.as_path()).expect("panic creating directory");
        FileWriter {
            directory: owned_directory,
            insert_file: FileStruct::new(directory.clone(), ChangeKind::Insert, table_name.clone()),
            //FileStruct::new(directory.join(table_name.to_owned() + "_inserts.csv.gz"), ChangeKind::Insert, table_name.as_ref()),
            update_files: HashMap::new(),
            delete_file: FileStruct::new(directory.clone(), ChangeKind::Delete, table_name.clone()),
            //FileStruct::new(directory.join(table_name.to_owned() + "_deletes.csv.gz"), ChangeKind::Delete, table_name.as_ref())
            table_name: table_name,
        }
    }
    pub fn add_change(&mut self, change: &ParsedLine) {
        if let ParsedLine::ChangedData { kind, .. } = change {
            match kind {
                ChangeKind::Insert => {
                    self.insert_file.add_change(change);
                }
                ChangeKind::Update => {
                    self.add_change_to_update_file(change);
                }
                ChangeKind::Delete => {
                    self.delete_file.add_change(change);
                }
            }
        }
    }
    pub fn flush_all(&mut self) {
        self.insert_file.file.flush_and_close();
        for x in self.update_files.values_mut() {
            x.file.flush_and_close()
        }
        self.delete_file.file.flush_and_close();
    }

    // update_files is a hash of our column names to our File
    fn add_change_to_update_file(&mut self, change: &ParsedLine) {
        let update_key: String = change
            .columns_for_changed_data()
            .iter()
            .filter(|x| x.is_changed_data_column())
            .map(|x| x.column_name())
            .sorted()
            .join(",");
        // let number_of_updates_that_exist = self.update_files.len();
        let cloned_directory = self.directory.clone();
        if let ParsedLine::ChangedData { table_name, .. } = change {
            self.update_files
                .entry(update_key)
                .or_insert_with(|| {
                    FileStruct::new(
                        cloned_directory.as_path(),
                        ChangeKind::Update,
                        table_name.clone(),
                    )
                })
                .add_change(change);
        } else {
            panic!("non changed data passed to add_change_to_update_file")
        }
    }
}
