use crate::parser::{ ParsedLine, Column, ColumnValue, ChangeKind, TableName, ColumnInfo };
use std::collections::{ HashMap, BTreeMap, HashSet };
use itertools::Itertools;

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

#[derive(Debug,Eq,PartialEq,Hash,Clone)]
pub enum DdlChange {
    AddColumn(ColumnInfo),
    RemoveColumn(ColumnInfo)
}

// CONFIG HACK:
const CHANGES_PER_TABLE: usize = 100000;

#[derive(Debug)]
struct ChangeSet {
    // TODO
    changes: Vec<ParsedLine>
}


#[derive(Debug)]
pub enum ChangeProcessingResult {
    TableChanges(file_writer::FileWriter),
    DdlChange(DdlChange)
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

    // this will return the data, and leave an "emptied" changeset in it's place
    fn empty_and_return(&mut self) -> Self {
        let replacement = self.empty_clone();
        std::mem::replace(self, replacement)
    }

    // will return the same "type" of ChangeSetWithColumnType, but empty
    fn empty_clone(&self) -> Self {
        match self {
            ChangeSetWithColumnType::IntColumnType(..) => {
                let btree = BTreeMap::<i64, ChangeSet>::new();
                ChangeSetWithColumnType::IntColumnType(btree)
            }
            ChangeSetWithColumnType::UuidColumnType(..) => {
                let btree = BTreeMap::<String, ChangeSet>::new();
                ChangeSetWithColumnType::UuidColumnType(btree)
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
    column_info: HashSet<ColumnInfo>,
    table_name: TableName
}

struct TableHolder {
    tables: HashMap<TableName, Table>
}

impl Table {
    fn new(parsed_line: &ParsedLine) -> Table {
        if let ParsedLine::ChangedData{table_name,..} = parsed_line {
            let id_column = parsed_line.find_id_column().column_value_unwrap();
            let changeset = ChangeSetWithColumnType::new(id_column);
            let column_info = parsed_line.column_info_set();
            let table_name = table_name.clone();
            Table { changeset, column_info, table_name }
        } else {
            panic!("Non changed data used to try and initialize a table {:?}", parsed_line)
        }
    }

    fn add_change(&mut self, parsed_line: ParsedLine) -> Option<(Table, Option<Vec<DdlChange>>)> {

        if self.has_ddl_changes(&parsed_line) {
            // if we have ddl changes, send the table data off now, then send the ddl changes, then apply the change
            let ddl_changes = self.ddl_changes(&parsed_line);
            let returned_table = self.reset_and_return_table_data();
            // remember to update to the new column info, as this will be the new schema for after we return our ddl_changes
            self.column_info = parsed_line.column_info_set();

            // time_to_swap_tables is never true immediately after we add the first new change here
            // so we safely don't check it
            self.add_change_to_changeset(parsed_line);

            Some((returned_table, Some(ddl_changes)))
        } else {
            // no ddl changes, add the line as normal
            self.add_change_to_changeset(parsed_line);
            if self.time_to_swap_tables() {
                // we don't actually remove the table
                // we just clear any non-schema
                let returned_table = self.reset_and_return_table_data();
                Some((returned_table, None))
            } else { None }
        }
    }

    fn reset_and_return_table_data(&mut self) -> Table {
        let column_info = self.column_info.clone();
        let table_name = self.table_name.clone();
        let changeset = self.changeset.empty_and_return();
        Table { changeset, column_info, table_name }
    }

    fn add_change_to_changeset(&mut self, parsed_line: ParsedLine) {
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
        } else {
            panic!("foobarbaz")
        }
    }

    fn has_ddl_changes(&self, parsed_line: &ParsedLine) -> bool {
        parsed_line.column_info_set() != self.column_info
    }

    fn ddl_changes(&self, parsed_line: &ParsedLine) -> Vec<DdlChange> {
        let new_column_info = &parsed_line.column_info_set();
        let old_column_info = &self.column_info;
        if !self.has_ddl_changes(parsed_line) {
            vec![]
        } else if
            new_column_info.iter().map(|info| info.name.clone()).collect_vec() ==
            old_column_info.iter().map(|info| info.name.clone()).collect_vec()
        {
            panic!("changes to column type from: {:?} to {:?}", parsed_line.column_info_set(),&self.column_info)
        } else {
            let mut added_ddl = new_column_info.difference(old_column_info).map(|info| DdlChange::AddColumn(info.clone())).collect::<Vec<_>>();
            let removed_ddl = old_column_info.difference(new_column_info).map(|info| DdlChange::RemoveColumn(info.clone())).collect::<Vec<_>>();
            added_ddl.extend(removed_ddl);
            added_ddl
        }
    }

    // TODO make this configurable, just for testing.
    // NOTE: important config hack
    // NOTE: important that this cannot trigger when we only have one line (so we don't process a ddl change, and then swap tables after)
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
    fn add_change(&mut self, parsed_line: ParsedLine) -> Option<(Table, Option<Vec<DdlChange>>)> {
        if let ParsedLine::ChangedData{ref table_name, ..} = parsed_line {
            // these are cheap since this is an interned string
            self.tables.entry(table_name.clone())
                .or_insert_with(|| Table::new(&parsed_line))
                .add_change(parsed_line)
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
    pub fn add_change(&mut self, parsed_line: ParsedLine) -> Option<Vec<ChangeProcessingResult>> {
        match parsed_line {
            ParsedLine::Begin(_) | ParsedLine::Commit(_) | ParsedLine::TruncateTable => { None }, // TODO
            ParsedLine::ContinueParse => { None }, // need to be exhaustive
            ParsedLine::ChangedData{ .. } => {
                // map here unpacks the option
                // NOTE: this means that we must return a table if we want to return a ddl result
                self.table_holder.add_change(parsed_line).map(
                    |(returned_table, maybe_ddl_changes)| {
                        let mut start_vec = vec![ChangeProcessingResult::TableChanges(self.write_files_for_table(returned_table))];
                        if let Some(ddl_changes) = maybe_ddl_changes {
                            for ddl_change in ddl_changes {
                                start_vec.push(ChangeProcessingResult::DdlChange(ddl_change))
                            }
                        }
                        start_vec
                    }
                )
            }
        }
    }
    pub fn get_stats(&self) -> HashMap<&TableName, usize> {
        self.table_holder.tables.iter().map(
            |(table_name,table)| {
                (table_name, table.get_stats().0)
            }
        ).collect()
    }
    pub fn print_stats(&self) {
        self.table_holder.tables.iter().for_each(
            |(table_name,table)| {
                let (ids, _changes) = table.get_stats();
                println!("{} {}", ids, table_name)
            }
        );
    }

    fn write_files_for_table(&self, table: Table) -> file_writer::FileWriter {
        let table_name = table.table_name;
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
    use maplit::{hashmap, hashset};
    use assert_matches::assert_matches;

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
            Column::ChangedColumn {column_info: id_column_info.clone(), value: Some(ColumnValue::Integer(1))}, // id column
            Column::ChangedColumn {column_info: new_column_info.clone(), value: Some(ColumnValue::Integer(1))} // new column
        ];
        let third_changed_columns = vec![
            Column::ChangedColumn {column_info: id_column_info, value: Some(ColumnValue::Integer(2))}, // id column
            Column::ChangedColumn {column_info: new_column_info.clone(), value: Some(ColumnValue::Integer(1))} // new column
        ];
        let first_change = ParsedLine::ChangedData{kind: ChangeKind::Insert, table_name: table_name.clone(), columns: first_changed_columns};
        let second_change = ParsedLine::ChangedData{kind: ChangeKind::Update, table_name: table_name.clone(), columns: second_changed_columns};
        // check we have the new schema and can keep adding changes
        let third_change = ParsedLine::ChangedData{kind: ChangeKind::Update, table_name: table_name.clone(), columns: third_changed_columns};
        let mut change_processing = ChangeProcessing::new();
        let blank_stats_hash = hashmap!();
        assert_eq!(change_processing.get_stats(), blank_stats_hash);
        let first_result = change_processing.add_change(first_change);
        let single_entry_stats_hash = hashmap!(&table_name => 1);
        assert_eq!(change_processing.get_stats(), single_entry_stats_hash);
        let mut second_result = change_processing.add_change(second_change);
        // we popped a record off, and then added another record, so should still have 1 in there
        assert_eq!(change_processing.get_stats(), single_entry_stats_hash);
        let third_result = change_processing.add_change(third_change);
        let double_entry_stats_hash = hashmap!(&table_name => 2);
        assert_eq!(change_processing.get_stats(), double_entry_stats_hash);
        assert!(first_result.is_none());
        assert!(second_result.is_some());
        assert!(third_result.is_none());
        // now lets assert stuff our second result is as we expect it to be
        if let Some(ref mut change_vec) = second_result {
            // table change and ddl change
            assert_eq!(change_vec.len(), 2);

            let table_change = change_vec.remove(0);
            assert_matches!(table_change, ChangeProcessingResult::TableChanges(..));
            let ddl_change = change_vec.remove(0);
            if let ChangeProcessingResult::DdlChange(DdlChange::AddColumn(column_info)) = ddl_change {
                assert_eq!(column_info, new_column_info);
            } else {
                panic!("doesn't match add_column");
            };
        } else {
            panic!("second_result does not contain a table");
        }
    }

    #[test]
    fn ddl_change_remove_column() {
        let table_name = TableName::new("foobar".to_string());
        let id_column_info = ColumnInfo::new("id", "bigint");
        let removed_column_info = ColumnInfo::new("foobar", "bigint");
        let first_changed_columns = vec![
            Column::ChangedColumn {column_info: id_column_info.clone(), value: Some(ColumnValue::Integer(1))},
            Column::ChangedColumn {column_info: removed_column_info.clone(), value: Some(ColumnValue::Integer(1))} // column to be removed
        ];
        let second_changed_columns = vec![
            Column::ChangedColumn {column_info: id_column_info.clone(), value: Some(ColumnValue::Integer(1))}, // id column
        ];
        let third_changed_columns = vec![
            Column::ChangedColumn {column_info: id_column_info, value: Some(ColumnValue::Integer(2))}, // id column
        ];
        let first_change = ParsedLine::ChangedData{kind: ChangeKind::Insert, table_name: table_name.clone(), columns: first_changed_columns};
        let second_change = ParsedLine::ChangedData{kind: ChangeKind::Update, table_name: table_name.clone(), columns: second_changed_columns};
        // check we have the new schema and can keep adding changes
        let third_change = ParsedLine::ChangedData{kind: ChangeKind::Update, table_name: table_name.clone(), columns: third_changed_columns};
        let mut change_processing = ChangeProcessing::new();
        let blank_stats_hash = hashmap!();
        assert_eq!(change_processing.get_stats(), blank_stats_hash);
        let first_result = change_processing.add_change(first_change);
        let single_entry_stats_hash = hashmap!(&table_name => 1);
        assert_eq!(change_processing.get_stats(), single_entry_stats_hash);
        let mut second_result = change_processing.add_change(second_change);
        // we popped a record off, and then added another record, so should still have 1 in there
        assert_eq!(change_processing.get_stats(), single_entry_stats_hash);
        let third_result = change_processing.add_change(third_change);
        let double_entry_stats_hash = hashmap!(&table_name => 2);
        assert_eq!(change_processing.get_stats(), double_entry_stats_hash);
        assert!(first_result.is_none());
        assert!(second_result.is_some());
        assert!(third_result.is_none());
        // now lets assert stuff our second result is as we expect it to be
        if let Some(ref mut change_vec) = second_result {
            // table change and ddl change
            assert_eq!(change_vec.len(), 2);

            let table_change = change_vec.remove(0);
            assert_matches!(table_change, ChangeProcessingResult::TableChanges(..));
            let ddl_change = change_vec.remove(0);

            if let ChangeProcessingResult::DdlChange(DdlChange::RemoveColumn(column_info)) = ddl_change {
                assert_eq!(column_info, removed_column_info);
            } else {
                panic!("doesn't match remove_column");
            }
        } else {
            panic!("second_result does not contain a table");
        }
    }

    #[test]
    fn ddl_changes_add_and_remove_multiple_columns() {
        let table_name = TableName::new("foobar".to_string());
        let id_column_info = ColumnInfo::new("id", "bigint");
        let new_column_info = ColumnInfo::new("foobar", "bigint");
        let new_column_2_info = ColumnInfo::new("baz", "bigint");
        let removed_column_info = ColumnInfo::new("quux", "bigint");
        let removed_column_2_info = ColumnInfo::new("aardvark", "bigint");
        let first_changed_columns = vec![
            Column::ChangedColumn {column_info: id_column_info.clone(), value: Some(ColumnValue::Integer(1))},
            Column::ChangedColumn {column_info: removed_column_info.clone(), value: Some(ColumnValue::Integer(1))}, // removed_column
            Column::ChangedColumn {column_info: removed_column_2_info.clone(), value: Some(ColumnValue::Integer(1))} // removed_column_2
        ];
        let second_changed_columns = vec![
            Column::ChangedColumn {column_info: id_column_info.clone(), value: Some(ColumnValue::Integer(1))}, // id column
            Column::ChangedColumn {column_info: new_column_info.clone(), value: Some(ColumnValue::Integer(1))}, // new column
            Column::ChangedColumn {column_info: new_column_2_info.clone(), value: Some(ColumnValue::Integer(1))} // new column
        ];
        let third_changed_columns = vec![
            Column::ChangedColumn {column_info: id_column_info, value: Some(ColumnValue::Integer(2))}, // id column
            Column::ChangedColumn {column_info: new_column_info.clone(), value: Some(ColumnValue::Integer(1))}, // new column
            Column::ChangedColumn {column_info: new_column_2_info.clone(), value: Some(ColumnValue::Integer(1))} // new column
        ];
        let first_change = ParsedLine::ChangedData{kind: ChangeKind::Insert, table_name: table_name.clone(), columns: first_changed_columns};
        let second_change = ParsedLine::ChangedData{kind: ChangeKind::Update, table_name: table_name.clone(), columns: second_changed_columns};
        // check we have the new schema and can keep adding changes
        let third_change = ParsedLine::ChangedData{kind: ChangeKind::Update, table_name: table_name.clone(), columns: third_changed_columns};
        let mut change_processing = ChangeProcessing::new();
        let blank_stats_hash = hashmap!();
        assert_eq!(change_processing.get_stats(), blank_stats_hash);
        let first_result = change_processing.add_change(first_change);
        let single_entry_stats_hash = hashmap!(&table_name => 1);
        assert_eq!(change_processing.get_stats(), single_entry_stats_hash);
        let mut second_result = change_processing.add_change(second_change);
        // we popped a record off, and then added another record, so should still have 1 in there
        assert_eq!(change_processing.get_stats(), single_entry_stats_hash);
        let third_result = change_processing.add_change(third_change);
        let double_entry_stats_hash = hashmap!(&table_name => 2);
        assert_eq!(change_processing.get_stats(), double_entry_stats_hash);
        assert!(first_result.is_none());
        assert!(second_result.is_some());
        assert!(third_result.is_none());
        // now lets assert stuff our second result is as we expect it to be
        if let Some(ref mut change_vec) = second_result {
            assert_eq!(change_vec.len(), 5);
            // assert!(ddl_changes.is_some());
            let table_change = change_vec.remove(0);
            assert_matches!(table_change, ChangeProcessingResult::TableChanges(..));

            let returned_ddl_set: HashSet<DdlChange> = change_vec.iter().map(|x| {
                if let ChangeProcessingResult::DdlChange(ddl_change) = x { ddl_change.clone() }
                else { panic!("found element that's not ddl change");}
            }).collect();
            let expected_ddl_set = hashset! {
                DdlChange::AddColumn(new_column_info.clone()),
                DdlChange::AddColumn(new_column_2_info.clone()),
                DdlChange::RemoveColumn(removed_column_info.clone()),
                DdlChange::RemoveColumn(removed_column_2_info.clone()),
            };
            assert_eq!(returned_ddl_set, expected_ddl_set);
        } else {
            panic!("second_result does not contain a table");
        }
    }
}
