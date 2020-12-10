use crate::parser::{ ParsedLine, Column, ColumnValue, ChangeKind, TableName };
use std::collections::{ HashMap, BTreeMap, HashSet };

use either::Either;
use crate::file_writer;

#[allow(unused_imports)]
use log::{debug, error, log_enabled, info, Level};

// single threaded f'now
// can have one of these per thread and communicate via
// channels later
pub struct ChangeProcessing {
    table_holder: TableHolder
}

#[derive(Debug)]
struct ChangeSet {
    // TODO
    changes: Vec<ParsedLine>
}

impl ChangeSet {
    fn new() -> ChangeSet {
        ChangeSet { changes: vec![] }
    }
    // batch apply enabled
    fn add_change(&mut self, change: ParsedLine) {
        if self.changes.len() == 0 {
            self.push_change(change);
        } else {
            if let Some(ParsedLine::ChangedData{kind: ChangeKind::Delete,..}) = self.changes.last() {
                // if we have a delete, we can only follow it by an insert
                if let ParsedLine::ChangedData{ kind: ChangeKind::Insert,.. } = change {
                    self.push_change(change)
                } else {
                    panic!("subsequent change after delete")
                }
            } else if let ParsedLine::ChangedData{ kind,.. } = change {
                match kind {
                    ChangeKind::Delete => {
                        self.changes.clear();
                        self.push_change(change);
                    },
                    ChangeKind::Update => {
                        self.handle_update(change)
                    },
                    _ => {
                        panic!("Trying to insert a value twice");
                    }
                }
            }
        }
    }

    fn push_change(&mut self, change: ParsedLine) {
        self.changes.push(change);
    }

    fn handle_update(&mut self, change: ParsedLine) {
        // table name is the same, we keep the same kind as the last thing, just update the columns if they're the column_names have the same keys, otherwise add we add them.
        if let ParsedLine::ChangedData { columns: new_columns, table_name: new_table_name, kind: new_kind} = change {
            if let Some(last_change) = self.changes.last_mut() {
                if let ParsedLine::ChangedData {columns: old_columns, ..} = last_change {
                    // we ignore unchanged toast columns
                    // TODO: handle schema changes
                    let updated_column_names: HashSet<&str> = new_columns.iter().filter_map(
                        |x| if let Column::ChangedColumn{column_info,..} = x {
                            Some(column_info.column_name())
                        } else { None }
                    ).collect();

                    let existing_column_names: HashSet<&str> = old_columns.iter().filter_map(
                        |x| if let Column::ChangedColumn{column_info,..} = x {
                            Some(column_info.column_name())
                        } else { None }
                    ).collect();

                    if updated_column_names == existing_column_names {
                        // use the new columns that are updated
                        // TODO: bug, merge columns
                        *old_columns = new_columns;
                    } else {
                        // recreate the enum
                        self.changes.push(ParsedLine::ChangedData { columns: new_columns, table_name: new_table_name, kind: new_kind })
                    }
                }
            }
        }
    }
}

// BTreeMap, because we want to traverse the indices in order
// when we write them out to files, as this is how it's efficient to load things into redshift.
// id is the sort key
#[derive(Debug)]
enum ChangeSetWithColumnType {
    IntColumnType(BTreeMap<i64, ChangeSet>),
    UuidColumnType(BTreeMap<String, ChangeSet>)
}

impl ChangeSetWithColumnType {
    fn new(value: &ColumnValue) -> ChangeSetWithColumnType {
        match value {
            ColumnValue::Integer(_) => {
                let btree = BTreeMap::<i64, ChangeSet>::new();
                ChangeSetWithColumnType::IntColumnType(btree)
            }
            ColumnValue::Text(_) => {
                let btree = BTreeMap::<String, ChangeSet>::new();
                ChangeSetWithColumnType::UuidColumnType(btree)
            }
            _ => { panic!("unexpected column value used to initialize ChangeSetWithColumnType {:?}", value) }
        }
    }
    fn values(&self) -> impl Iterator<Item = &ChangeSet> {
        match self {
            ChangeSetWithColumnType::IntColumnType(btree) => {
                Either::Left(btree.values())
            }
            ChangeSetWithColumnType::UuidColumnType(btree) => {
                Either::Right(btree.values())
            }
        }
    }

    fn len(&self) -> usize {
        match self {
            ChangeSetWithColumnType::IntColumnType(btree) => {
                btree.len()
            }
            ChangeSetWithColumnType::UuidColumnType(btree) => {
                btree.len()
            }
        }
    }
}

// this holds the existing number of files that have been created for a table
struct FileCounter {
    update_files_count: i32,
    insert_files_count: i32,
    delete_files_count: i32
}

#[derive(Debug)]
struct Table {
    // we want to have a changeset, but need to match on the enum type for the pkey of the column
    changeset: ChangeSetWithColumnType,
}

struct TableHolder {
    tables: HashMap<TableName, Table>
}

impl Table {
    fn new(parsed_line: &ParsedLine) -> Table {
        if let ParsedLine::ChangedData{..} = parsed_line {
            let id_column = parsed_line.find_id_column().column_value_unwrap();
            let changeset = ChangeSetWithColumnType::new(id_column);
            Table { changeset }
        } else {
            panic!("Non changed data used to try and initialize a table {:?}", parsed_line)
        }
    }

    // returns a bool if the table is "full" and ready to be written and uploaded
    fn add_change(&mut self, parsed_line: ParsedLine) -> bool {
        if let ParsedLine::ChangedData{..} = parsed_line {
            let parsed_line_id = parsed_line.find_id_column();
            match parsed_line_id.column_value_unwrap() {
                ColumnValue::Text(string) => {
                    if let ChangeSetWithColumnType::UuidColumnType(ref mut changeset) = self.changeset {
                        let cloned = string.clone();
                        changeset.entry(cloned)
                            .or_insert_with(|| ChangeSet::new())
                            .add_change(parsed_line)
                    }
                }
                ColumnValue::Integer(int) => {
                    if let ChangeSetWithColumnType::IntColumnType(ref mut changeset) = self.changeset {
                        changeset.entry(*int)
                            .or_insert_with(|| ChangeSet::new())
                            .add_change(parsed_line)
                    }
                }
                _ => panic!("foobar")
            };
            self.time_to_swap_tables()
        } else {
            panic!("foobarbaz")
        }
    }

    // TODO make this configurable, just for testing.
    fn time_to_swap_tables(&self) -> bool{
        return self.changeset.len() > 200
    }
    fn get_stats(&self) -> (usize, usize) {
        let number_of_ids = self.changeset.len();
        let number_of_changes = self.changeset.values().fold(0, |acc, value| acc + value.changes.len());
        (number_of_ids, number_of_changes)
    }
}

impl TableHolder {
    fn add_change(&mut self, parsed_line: ParsedLine) -> Option<(TableName, Table)> {
        if let ParsedLine::ChangedData{ref table_name, ..} = parsed_line {
            // these are cheap since this is an interned string
            let cloned = table_name.clone();
            let other_cloned = table_name.clone();
            let should_swap_table = self.tables.entry(cloned)
                .or_insert_with(|| Table::new(&parsed_line))
                .add_change(parsed_line);
            if should_swap_table {
                // unwrap should be safe here, we're single threaded and we just inserted here.
                Some((other_cloned.clone(), self.tables.remove(&other_cloned).unwrap()))
            } else { None }

        } else {
            None
        }
    }
}

impl ChangeProcessing {
    pub fn new() -> ChangeProcessing {
        let hash_map = HashMap::new();
        ChangeProcessing { table_holder: TableHolder { tables: hash_map } }
    }
    pub fn add_change(&mut self, parsed_line: ParsedLine) -> Option<file_writer::FileWriter> {
        match parsed_line {
            ParsedLine::Begin(_) | ParsedLine::Commit(_) | ParsedLine::TruncateTable => { None }, // TODO
            ParsedLine::ContinueParse => { None }, // need to be exhaustive
            ParsedLine::ChangedData{ .. } => {
                self.table_holder.add_change(parsed_line).map(
                    |(table_name, returned_table)| self.write_files_for_table(table_name, returned_table
                    )
                )
            }
        }
    }
    pub fn print_stats(&self) {
        self.table_holder.tables.iter().for_each(
            |(table_name,table)| {
                let (ids, _changes) = table.get_stats();
                println!("{} {}", ids, table_name)
            }
        );
    }

    pub fn write_files_for_table(&self, table_name: TableName, table: Table) -> file_writer::FileWriter {
        let mut file_writer = file_writer::FileWriter::new(table_name.clone());
        table.changeset.values().for_each(
            |record| {
                record.changes.iter().for_each(
                    |change| {
                        file_writer.add_change(change);
                    })
            }

        );
        file_writer
    }
    // pub fn write_files_sync_batch(&self) -> Vec<file_writer::FileWriter> {
    //     self.table_holder.tables.iter().map(
    //         |(table_name, table)| {
    //         let mut file_writer = file_writer::FileWriter::new(table_name);
    //             table.changeset.values().for_each(
    //                 |record| {
    //                     record.changes.iter().for_each(
    //                         |change| {
    //                             file_writer.add_change(change);
    //                         })
    //                 }

    //             );
    //             file_writer
    //         }
    //     ).collect()
    // }
}
