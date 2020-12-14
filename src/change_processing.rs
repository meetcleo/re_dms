use crate::parser::{ ParsedLine, Column, ColumnValue, ChangeKind, TableName, ColumnInfo };
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

// CONFIG HACK:
const CHANGES_PER_TABLE: usize = 100000;

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

    fn clear(&mut self) {
        match self {
            ChangeSetWithColumnType::IntColumnType(btree) => {
                btree.clear();
            }
            ChangeSetWithColumnType::UuidColumnType(btree) => {
                btree.clear();
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
    column_info: HashSet<ColumnInfo>
}

struct TableHolder {
    tables: HashMap<TableName, Table>
}

impl Table {
    fn new(parsed_line: &ParsedLine) -> Table {
        if let ParsedLine::ChangedData{..} = parsed_line {
            let id_column = parsed_line.find_id_column().column_value_unwrap();
            let changeset = ChangeSetWithColumnType::new(id_column);
            let column_info = parsed_line.column_info_set();
            Table { changeset, column_info }
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
    // NOTE: important config hack
    fn time_to_swap_tables(&self) -> bool{
        return self.changeset.len() >= CHANGES_PER_TABLE
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
                let removed_table = self.tables.remove(&other_cloned).unwrap();
                Some((other_cloned, removed_table))
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
                    |(table_name, returned_table)| self.write_files_for_table(table_name, returned_table)
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

    fn write_files_for_table(&self, table_name: TableName, table: Table) -> file_writer::FileWriter {
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

    // this drains every table from the changeset,
    // writes the files, and returns them
    pub fn drain_final_changes(&mut self) -> Vec<file_writer::FileWriter> {
        let resulting_vec = self.table_holder.tables.drain().map(
            |(table_name, table)| {
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
        ).collect();
        println!("DRAINED FINAL CHANGES!!!!! {}", self.table_holder.tables.len());
        resulting_vec
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::*;

    // this is basically an integration test, but that's fine
    #[test]
    fn ddl_change_add_column() {
        let table_name = TableName::new("foobar".to_string());
        let id_column_info = ColumnInfo::new("id", "bigint");
        let new_column_info = ColumnInfo::new("foobar", "bigint");
        let first_changed_columns = vec![
            Column::ChangedColumn {column_info: id_column_info.clone(), value: Some(ColumnValue::Integer(1))}
        ];
        let second_changed_columns = vec![
            Column::ChangedColumn {column_info: id_column_info, value: Some(ColumnValue::Integer(1))}, // id column
            Column::ChangedColumn {column_info: new_column_info, value: Some(ColumnValue::Integer(1))} // new column
        ];
        let first_change = ParsedLine::ChangedData{kind: ChangeKind::Insert, table_name: table_name.clone(), columns: first_changed_columns};
        let second_change = ParsedLine::ChangedData{kind: ChangeKind::Update, table_name: table_name, columns: second_changed_columns};
        let mut change_processing = ChangeProcessing::new();
        let first_result = change_processing.add_change(first_change);
        let second_result = change_processing.add_change(second_change);
        assert!(first_result.is_none());
        // assert!(first_result.is_some())
    }

    #[test]
    fn ddl_change_remove_column() {

    }
}
