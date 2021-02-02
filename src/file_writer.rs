use bigdecimal::BigDecimal;
use glob::glob;
use std::fs;
use std::path::Path;
use std::path::PathBuf;

use crate::parser::{ChangeKind, ColumnInfo, ColumnTypeEnum, ParsedLine, TableName};
use crate::wal_file_manager;
use std::collections::HashMap; //{ HashMap, BTreeMap, HashSet };

use itertools::Itertools;

use flate2::write::GzEncoder;
use flate2::Compression;

use crate::database_writer::{DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE};

use std::str::FromStr;

#[allow(unused_imports)]
use crate::{function, logger_debug, logger_error, logger_info, logger_panic};

// we have one of these per table,
// it will hold the files to write to and handle the writing
#[derive(Debug)]
pub struct FileWriter {
    directory: PathBuf,
    pub insert_file: FileStruct,
    pub update_files: HashMap<String, FileStruct>,
    pub delete_file: FileStruct,
    pub table_name: TableName,
    pub wal_file: wal_file_manager::WalFile,
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
                    .map(|gzip| gzip.finish().expect("Error finishing gzip"))
                    .expect("Error unwrapping gzip encoder from csv writer");
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
        // we touch the file when we create the struct to create the file
        let _file = fs::File::create(new_file_name.as_path()).expect("Error creating file");
        file_struct
    }

    // creates a new filename of the sort directory/n_table_name_inserts.csv.gz
    // where n is a number
    // TODO: do we just want to save the number and be passing it in somewhere
    // I'm not super happy with thrashing our directory tree?
    fn new_file_name(directory_name: &Path, kind: ChangeKind, table_name: &str) -> PathBuf {
        let the_file_glob_pattern =
            ["*", table_name, kind.to_string().as_str()].join("_") + ".csv.gz";
        let the_glob_pattern = directory_name.join(the_file_glob_pattern);

        let current_file_number = glob(the_glob_pattern.to_str().expect(
            "Error turning glob pattern to string. Probably non-UTF8 characters in the directory names?",
        ))
        .expect("Error running glob on directory")
        .map(|file_path| {
            match file_path {
                Ok(path) => {
                    let file_name = path.file_name().expect("Error getting file_name");
                    // if it's not UTF-8 it can crash
                    let file_name_str = file_name.to_str().expect("Error turning file_name to string");
                    // filename is number_stuff.
                    let (file_number_str, _) = file_name_str.split_once('_').expect("Error, no _ in filename so can't parse it");
                    let file_number: i32 = file_number_str.parse::<i32>().expect("Error can't parse file number to i32");
                    file_number
                }

                // if the path matched but was unreadable,
                // thereby preventing its contents from matching
                Err(_e) => panic!("Unreadable filepath. What did you do?"),
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
        let file = fs::File::create(self.file_name.as_path())
            .expect("Unable to create file in file writer");
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
                            if let ColumnTypeEnum::Numeric = x.column_info().column_type_enum() {
                                let big_decimal: BigDecimal =
                                    BigDecimal::from_str(&value.to_string())
                                        .expect("BigDecimal unable to be parsed");
                                // we need to round our internal stuff
                                big_decimal
                                    .with_scale(DEFAULT_NUMERIC_SCALE as i64)
                                    .with_prec(DEFAULT_NUMERIC_PRECISION as u64)
                                    .to_string()
                            } else {
                                value.to_string()
                            }
                        } else {
                            match x.column_info().column_type_enum() {
                                ColumnTypeEnum::Text => "\0".to_owned(),
                                _ => "".to_owned(),
                            }
                        } // remember null byte as nulls
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

    // check whether the internal file has been initialised
    pub fn is_some(&self) -> bool {
        self.file.is_some()
    }
}

// TODO: write iterator over files
impl FileWriter {
    pub fn new(
        table_name: TableName,
        associated_wal_file: wal_file_manager::WalFile,
    ) -> FileWriter {
        let directory = associated_wal_file.path_for_wal_directory();
        let owned_directory = directory.clone().to_owned();
        FileWriter {
            directory: owned_directory,
            insert_file: FileStruct::new(
                directory.as_path(),
                ChangeKind::Insert,
                table_name.clone(),
            ),
            update_files: HashMap::new(),
            delete_file: FileStruct::new(
                directory.as_path(),
                ChangeKind::Delete,
                table_name.clone(),
            ),
            table_name: table_name,
            wal_file: associated_wal_file,
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
        if self.insert_file.is_some() {
            logger_info!(
                Some(self.wal_file.file_number),
                Some(&self.table_name),
                &format!(
                    "finished_writing:{}",
                    self.insert_file
                        .file_name
                        .to_str()
                        .expect("Unprintable file name")
                )
            )
        }
        for x in self.update_files.values_mut() {
            x.file.flush_and_close();
            if x.is_some() {
                logger_info!(
                    Some(self.wal_file.file_number),
                    Some(&self.table_name),
                    &format!(
                        "finished_writing:{}",
                        x.file_name.to_str().expect("Unprintable file name")
                    )
                )
            }
        }
        self.delete_file.file.flush_and_close();
        if self.delete_file.is_some() {
            logger_info!(
                Some(self.wal_file.file_number),
                Some(&self.table_name),
                &format!(
                    "finished_writing:{}",
                    self.delete_file
                        .file_name
                        .to_str()
                        .expect("Unprintable file name")
                )
            )
        }
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
