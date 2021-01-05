use crate::parser::{ChangeKind, Column, ColumnInfo, ColumnValue, ParsedLine, TableName};
use crate::wal_file_manager::WalFile;
use itertools::Itertools;
use std::collections::{BTreeMap, HashMap, HashSet};

use crate::file_writer;
use either::Either;

#[allow(unused_imports)]
use log::{debug, error, info, log_enabled, Level};

#[derive(Debug, Eq, PartialEq, Hash, Clone)]
pub enum DdlChange {
    AddColumn(ColumnInfo, TableName),
    RemoveColumn(ColumnInfo, TableName),
}

impl DdlChange {
    pub fn table_name(&self) -> TableName {
        match self {
            Self::AddColumn(.., table_name) => table_name.clone(),
            Self::RemoveColumn(.., table_name) => table_name.clone(),
        }
    }
}

// CONFIG HACK:
const CHANGES_PER_TABLE: usize = 100000;

#[derive(Debug, Eq, PartialEq)]
struct ChangeSet {
    changes: Option<ParsedLine>,
}

#[derive(Debug)]
pub enum ChangeProcessingResult {
    TableChanges(file_writer::FileWriter),
    DdlChange(DdlChange),
}

impl ChangeProcessingResult {
    // give an owned one, it's just copying a pointer cheap
    pub fn table_name(&self) -> TableName {
        match self {
            Self::TableChanges(file_writer) => file_writer.table_name.clone(),
            Self::DdlChange(ddl_change) => ddl_change.table_name(),
        }
    }
}

impl ChangeSet {
    fn new() -> ChangeSet {
        ChangeSet { changes: None }
    }
    // batch apply enabled
    fn add_change(&mut self, new_change: ParsedLine) {
        self.changes = match self.changes {
            Some(ParsedLine::ChangedData { kind, .. }) => match kind {
                ChangeKind::Insert => self.handle_insert_subsequent(new_change),
                ChangeKind::Update => self.handle_update_subsequent(new_change),
                ChangeKind::Delete => self.handle_delete_subsequent(new_change),
            },
            _ => Some(new_change),
        }
    }

    fn handle_insert_subsequent(&self, new_change: ParsedLine) -> Option<ParsedLine> {
        if let ParsedLine::ChangedData {
            kind,
            columns,
            table_name,
        } = new_change
        {
            match kind {
                ChangeKind::Insert => panic!("attempting to insert a record twice"),
                ChangeKind::Update => {
                    self.untoasted_changes(columns, table_name, ChangeKind::Insert)
                }
                ChangeKind::Delete => None,
            }
        } else {
            panic!("don't know how to handle this type of line here")
        }
    }

    fn handle_update_subsequent(&self, new_change: ParsedLine) -> Option<ParsedLine> {
        if let ParsedLine::ChangedData { kind, .. } = new_change {
            match kind {
                ChangeKind::Insert => panic!("attempting to insert a record twice"),
                ChangeKind::Update => match new_change {
                    ParsedLine::ChangedData {
                        columns,
                        table_name,
                        ..
                    } => self.untoasted_changes(columns, table_name, ChangeKind::Update),
                    _ => panic!("don't know how to handle this type of line here"),
                },
                ChangeKind::Delete => Some(new_change),
            }
        } else {
            panic!("don't know how to handle this type of line here")
        }
    }

    fn handle_delete_subsequent(&self, new_change: ParsedLine) -> Option<ParsedLine> {
        if let ParsedLine::ChangedData {
            kind,
            columns,
            table_name,
        } = new_change
        {
            match kind {
                ChangeKind::Insert => {
                    self.untoasted_changes(columns, table_name, ChangeKind::Update)
                }
                ChangeKind::Update => {
                    panic!("attempting to update a record after it's been deleted")
                }
                ChangeKind::Delete => panic!("attempting to delete a record twice"),
            }
        } else {
            panic!("don't know how to handle this type of line here")
        }
    }

    fn untoasted_changes(
        &self,
        new_columns: Vec<Column>,
        table_name: TableName,
        new_kind: ChangeKind,
    ) -> Option<ParsedLine> {
        if let Some(ParsedLine::ChangedData {
            columns: old_columns,
            ..
        }) = &self.changes
        {
            assert_eq!(new_columns.len(), old_columns.len());
            let untoasted_columns: Vec<Column> = new_columns
                .iter()
                .zip(old_columns.iter())
                .map(|(new_column, old_column)| {
                    debug_assert_eq!(new_column.column_info(), old_column.column_info());
                    if new_column.is_unchanged_toast_column() {
                        old_column
                    } else {
                        new_column
                    }
                    .clone()
                })
                .collect();

            Some(ParsedLine::ChangedData {
                columns: untoasted_columns,
                kind: new_kind,
                table_name: table_name,
            })
        } else {
            panic!("last change was not changed data, no idea how we got here")
        }
    }
}

// BTreeMap, because we want to traverse the indices in order
// when we write them out to files, as this is how it's efficient to load things into redshift.
// id is the sort key
#[derive(Debug, Eq, PartialEq)]
enum ChangeSetWithColumnType {
    IntColumnType(BTreeMap<i64, ChangeSet>),
    UuidColumnType(BTreeMap<String, ChangeSet>),
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
            _ => {
                panic!(
                    "unexpected column value used to initialize ChangeSetWithColumnType {:?}",
                    value
                )
            }
        }
    }
    fn values(&self) -> impl Iterator<Item = &ChangeSet> {
        match self {
            ChangeSetWithColumnType::IntColumnType(btree) => Either::Left(btree.values()),
            ChangeSetWithColumnType::UuidColumnType(btree) => Either::Right(btree.values()),
        }
    }

    fn len(&self) -> usize {
        match self {
            ChangeSetWithColumnType::IntColumnType(btree) => btree.len(),
            ChangeSetWithColumnType::UuidColumnType(btree) => btree.len(),
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

    #[allow(dead_code)]
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

#[derive(Debug, Eq, PartialEq)]
struct Table {
    // we want to have a changeset, but need to match on the enum type for the pkey of the column
    changeset: ChangeSetWithColumnType,
    column_info: Option<HashSet<ColumnInfo>>,
    table_name: TableName,
}

#[derive(Debug, Eq, PartialEq)]
struct TableHolder {
    tables: HashMap<TableName, Table>,
}

impl Table {
    fn new(parsed_line: &ParsedLine) -> Table {
        if let ParsedLine::ChangedData { table_name, .. } = parsed_line {
            let id_column = parsed_line.find_id_column().column_value_unwrap();
            let changeset = ChangeSetWithColumnType::new(id_column);
            let column_info = parsed_line.column_info_set();
            let table_name = table_name.clone();
            Table {
                changeset,
                column_info,
                table_name,
            }
        } else {
            panic!(
                "Non changed data used to try and initialize a table {:?}",
                parsed_line
            )
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
            } else {
                None
            }
        }
    }

    fn reset_and_return_table_data(&mut self) -> Table {
        let column_info = self.column_info.clone();
        let table_name = self.table_name.clone();
        let changeset = self.changeset.empty_and_return();
        Table {
            changeset,
            column_info,
            table_name,
        }
    }

    fn add_change_to_changeset(&mut self, parsed_line: ParsedLine) {
        self.update_column_info_if_unset(&parsed_line);
        if let ParsedLine::ChangedData { .. } = parsed_line {
            let parsed_line_id = parsed_line.find_id_column();
            match parsed_line_id.column_value_unwrap() {
                ColumnValue::Text(string) => {
                    if let ChangeSetWithColumnType::UuidColumnType(ref mut changeset) =
                        self.changeset
                    {
                        let cloned = string.clone();
                        changeset
                            .entry(cloned)
                            .or_insert_with(|| ChangeSet::new())
                            .add_change(parsed_line)
                    }
                }
                ColumnValue::Integer(int) => {
                    if let ChangeSetWithColumnType::IntColumnType(ref mut changeset) =
                        self.changeset
                    {
                        changeset
                            .entry(*int)
                            .or_insert_with(|| ChangeSet::new())
                            .add_change(parsed_line)
                    }
                }
                _ => panic!("foobar"),
            };
        } else {
            panic!("foobarbaz")
        }
    }

    fn update_column_info_if_unset(&mut self, parsed_line: &ParsedLine) {
        if self.column_info.is_some() || parsed_line.column_info_set().is_none() {
            return;
        } else {
            self.column_info = parsed_line.column_info_set();
        }
    }

    fn has_ddl_changes(&self, parsed_line: &ParsedLine) -> bool {
        let incoming_column_info = parsed_line.column_info_set();
        incoming_column_info.is_some()
            && self.column_info.is_some()
            && incoming_column_info != self.column_info
    }

    fn ddl_changes(&self, parsed_line: &ParsedLine) -> Vec<DdlChange> {
        let new_column_info = &parsed_line.column_info_set().unwrap();
        let old_column_info = &self.column_info.clone().unwrap();
        if !self.has_ddl_changes(parsed_line) {
            vec![]
        } else if new_column_info
            .iter()
            .map(|info| info.name.clone())
            .collect_vec()
            == old_column_info
                .iter()
                .map(|info| info.name.clone())
                .collect_vec()
        {
            panic!(
                "changes to column type from: {:?} to {:?}",
                parsed_line.column_info_set(),
                &self.column_info
            )
        } else {
            let mut added_ddl = new_column_info
                .difference(old_column_info)
                .map(|info| DdlChange::AddColumn(info.clone(), self.table_name.clone()))
                .collect::<Vec<_>>();
            let removed_ddl = old_column_info
                .difference(new_column_info)
                .map(|info| DdlChange::RemoveColumn(info.clone(), self.table_name.clone()))
                .collect::<Vec<_>>();
            added_ddl.extend(removed_ddl);
            added_ddl
        }
    }

    // TODO make this configurable, just for testing.
    // NOTE: important config hack
    // NOTE: important that this cannot trigger when we only have one line (so we don't process a ddl change, and then swap tables after)
    fn time_to_swap_tables(&self) -> bool {
        return self.changeset.len() >= CHANGES_PER_TABLE;
    }

    fn get_stats(&self) -> (usize, usize) {
        let number_of_ids = self.changeset.len();
        let number_of_changes = self.changeset.values().fold(0, |acc, value| {
            acc + if value.changes.is_some() { 1 } else { 0 }
        });
        (number_of_ids, number_of_changes)
    }
}

impl TableHolder {
    fn add_change(&mut self, parsed_line: ParsedLine) -> Option<(Table, Option<Vec<DdlChange>>)> {
        if let ParsedLine::ChangedData { ref table_name, .. } = parsed_line {
            // these are cheap since this is an interned string
            self.tables
                .entry(table_name.clone())
                .or_insert_with(|| Table::new(&parsed_line))
                .add_change(parsed_line)
        } else {
            None
        }
    }

    // number of tables
    fn len(&self) -> usize {
        self.tables.len()
    }
}

// single threaded f'now
pub struct ChangeProcessing {
    table_holder: TableHolder,
    associated_wal_file: Option<WalFile>,
}

impl ChangeProcessing {
    pub fn new() -> ChangeProcessing {
        let hash_map = HashMap::new();
        ChangeProcessing {
            table_holder: TableHolder { tables: hash_map },
            associated_wal_file: None,
        }
    }

    // notice this is a move of the wal file
    pub fn register_wal_file(&mut self, associated_wal_file: Option<WalFile>) {
        // if there are no changes,
        // our wal file would be the last one left
        // clean up if so
        self.associated_wal_file
            .as_mut()
            .map(|wal_file| wal_file.maybe_remove_wal_file());

        // it's an error to register a wal file while we have any changes left in our tables
        if self.table_holder.len() != 0 {
            panic!("Tried to register wal file while we have changes in our tables");
        }
        self.associated_wal_file = associated_wal_file;
    }
    pub fn add_change(&mut self, parsed_line: ParsedLine) -> Option<Vec<ChangeProcessingResult>> {
        match parsed_line {
            ParsedLine::Begin(_) | ParsedLine::Commit(_) | ParsedLine::TruncateTable => None, // TODO
            ParsedLine::ContinueParse => None, // need to be exhaustive
            ParsedLine::ChangedData { .. } => {
                // map here maps over the option
                // NOTE: this means that we must return a table if we want to return a ddl result
                self.table_holder.add_change(parsed_line).map(
                    |(returned_table, maybe_ddl_changes)| {
                        let mut start_vec = vec![ChangeProcessingResult::TableChanges(
                            Self::write_files_for_table(
                                returned_table,
                                self.associated_wal_file.clone().unwrap(),
                            ),
                        )];
                        if let Some(ddl_changes) = maybe_ddl_changes {
                            for ddl_change in ddl_changes {
                                start_vec.push(ChangeProcessingResult::DdlChange(ddl_change))
                            }
                        }
                        start_vec
                    },
                )
            }
        }
    }
    pub fn get_stats(&self) -> HashMap<&TableName, usize> {
        self.table_holder
            .tables
            .iter()
            .map(|(table_name, table)| (table_name, table.get_stats().0))
            .collect()
    }
    pub fn print_stats(&self) {
        self.table_holder
            .tables
            .iter()
            .for_each(|(table_name, table)| {
                let (ids, _changes) = table.get_stats();
                info!("{} {}", ids, table_name)
            });
    }

    fn write_files_for_table(
        table: Table,
        associated_wal_file: WalFile,
    ) -> file_writer::FileWriter {
        let table_name = table.table_name;
        let mut file_writer = file_writer::FileWriter::new(table_name.clone(), associated_wal_file);
        table.changeset.values().for_each(|record| {
            if let Some(change) = &record.changes {
                file_writer.add_change(change);
            };
        });
        file_writer
    }

    // this drains every table from the changeset,
    // writes the files, and returns them
    pub fn drain_final_changes(&mut self) -> Vec<ChangeProcessingResult> {
        let maybe_associated_wal_file = self.associated_wal_file.clone();
        // error if associated_wal_file is null
        let resulting_vec = self
            .table_holder
            .tables
            .drain()
            .map(|(_table_name, table)| {
                // need to clone again because this is in a loop
                let file_writer =
                    Self::write_files_for_table(table, maybe_associated_wal_file.clone().unwrap());
                ChangeProcessingResult::TableChanges(file_writer)
            })
            .collect();
        info!("DRAINED FINAL CHANGES!!!!! {}", self.table_holder.len());
        resulting_vec
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::*;
    use maplit::{hashmap, hashset};
    use std::path::PathBuf;

    // NOTE: I think this is actually run globally before all tests. Seems fine to me though.
    #[ctor::ctor]
    fn create_tmp_directory() {
        std::fs::create_dir_all(TESTING_PATH).unwrap();
    }

    // TODO stub filesystem properly
    const TESTING_PATH: &str = "/tmp/wal_change_processing_testing";

    fn new_wal_file() -> WalFile {
        WalFile::new(1, PathBuf::from(TESTING_PATH).as_path())
    }

    // this is basically an integration test, but that's fine
    #[test]
    fn ddl_change_add_column() {
        let table_name = TableName::new("foobar".to_string());
        let id_column_info = ColumnInfo::new("id", "bigint");
        let new_column_info = ColumnInfo::new("foobar", "bigint");
        let first_changed_columns = vec![Column::ChangedColumn {
            column_info: id_column_info.clone(),
            value: Some(ColumnValue::Integer(1)),
        }];
        let second_changed_columns = vec![
            Column::ChangedColumn {
                column_info: id_column_info.clone(),
                value: Some(ColumnValue::Integer(1)),
            }, // id column
            Column::ChangedColumn {
                column_info: new_column_info.clone(),
                value: Some(ColumnValue::Integer(1)),
            }, // new column
        ];
        let third_changed_columns = vec![
            Column::ChangedColumn {
                column_info: id_column_info,
                value: Some(ColumnValue::Integer(2)),
            }, // id column
            Column::ChangedColumn {
                column_info: new_column_info.clone(),
                value: Some(ColumnValue::Integer(1)),
            }, // new column
        ];
        let first_change = ParsedLine::ChangedData {
            kind: ChangeKind::Insert,
            table_name: table_name.clone(),
            columns: first_changed_columns,
        };
        let second_change = ParsedLine::ChangedData {
            kind: ChangeKind::Update,
            table_name: table_name.clone(),
            columns: second_changed_columns,
        };
        // check we have the new schema and can keep adding changes
        let third_change = ParsedLine::ChangedData {
            kind: ChangeKind::Update,
            table_name: table_name.clone(),
            columns: third_changed_columns,
        };
        let mut change_processing = ChangeProcessing::new();
        change_processing.register_wal_file(Some(new_wal_file()));
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
            assert!(matches!(
                table_change,
                ChangeProcessingResult::TableChanges(..)
            ));
            let ddl_change = change_vec.remove(0);
            if let ChangeProcessingResult::DdlChange(DdlChange::AddColumn(column_info, TableName)) =
                ddl_change
            {
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
            Column::ChangedColumn {
                column_info: id_column_info.clone(),
                value: Some(ColumnValue::Integer(1)),
            },
            Column::ChangedColumn {
                column_info: removed_column_info.clone(),
                value: Some(ColumnValue::Integer(1)),
            }, // column to be removed
        ];
        let second_changed_columns = vec![
            Column::ChangedColumn {
                column_info: id_column_info.clone(),
                value: Some(ColumnValue::Integer(1)),
            }, // id column
        ];
        let third_changed_columns = vec![
            Column::ChangedColumn {
                column_info: id_column_info,
                value: Some(ColumnValue::Integer(2)),
            }, // id column
        ];
        let first_change = ParsedLine::ChangedData {
            kind: ChangeKind::Insert,
            table_name: table_name.clone(),
            columns: first_changed_columns,
        };
        let second_change = ParsedLine::ChangedData {
            kind: ChangeKind::Update,
            table_name: table_name.clone(),
            columns: second_changed_columns,
        };
        // check we have the new schema and can keep adding changes
        let third_change = ParsedLine::ChangedData {
            kind: ChangeKind::Update,
            table_name: table_name.clone(),
            columns: third_changed_columns,
        };
        let mut change_processing = ChangeProcessing::new();
        change_processing.register_wal_file(Some(new_wal_file()));
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
            assert!(matches!(
                table_change,
                ChangeProcessingResult::TableChanges(..)
            ));
            let ddl_change = change_vec.remove(0);

            if let ChangeProcessingResult::DdlChange(DdlChange::RemoveColumn(column_info, ..)) =
                ddl_change
            {
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
            Column::ChangedColumn {
                column_info: id_column_info.clone(),
                value: Some(ColumnValue::Integer(1)),
            },
            Column::ChangedColumn {
                column_info: removed_column_info.clone(),
                value: Some(ColumnValue::Integer(1)),
            }, // removed_column
            Column::ChangedColumn {
                column_info: removed_column_2_info.clone(),
                value: Some(ColumnValue::Integer(1)),
            }, // removed_column_2
        ];
        let second_changed_columns = vec![
            Column::ChangedColumn {
                column_info: id_column_info.clone(),
                value: Some(ColumnValue::Integer(1)),
            }, // id column
            Column::ChangedColumn {
                column_info: new_column_info.clone(),
                value: Some(ColumnValue::Integer(1)),
            }, // new column
            Column::ChangedColumn {
                column_info: new_column_2_info.clone(),
                value: Some(ColumnValue::Integer(1)),
            }, // new column
        ];
        let third_changed_columns = vec![
            Column::ChangedColumn {
                column_info: id_column_info,
                value: Some(ColumnValue::Integer(2)),
            }, // id column
            Column::ChangedColumn {
                column_info: new_column_info.clone(),
                value: Some(ColumnValue::Integer(1)),
            }, // new column
            Column::ChangedColumn {
                column_info: new_column_2_info.clone(),
                value: Some(ColumnValue::Integer(1)),
            }, // new column
        ];
        let first_change = ParsedLine::ChangedData {
            kind: ChangeKind::Insert,
            table_name: table_name.clone(),
            columns: first_changed_columns,
        };
        let second_change = ParsedLine::ChangedData {
            kind: ChangeKind::Update,
            table_name: table_name.clone(),
            columns: second_changed_columns,
        };
        // check we have the new schema and can keep adding changes
        let third_change = ParsedLine::ChangedData {
            kind: ChangeKind::Update,
            table_name: table_name.clone(),
            columns: third_changed_columns,
        };
        let mut change_processing = ChangeProcessing::new();
        change_processing.register_wal_file(Some(new_wal_file()));
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
            assert!(matches!(
                table_change,
                ChangeProcessingResult::TableChanges(..)
            ));

            let returned_ddl_set: HashSet<DdlChange> = change_vec
                .iter()
                .map(|x| {
                    if let ChangeProcessingResult::DdlChange(ddl_change) = x {
                        ddl_change.clone()
                    } else {
                        panic!("found element that's not ddl change");
                    }
                })
                .collect();
            let expected_ddl_set = hashset! {
                DdlChange::AddColumn(new_column_info.clone(), table_name.clone()),
                DdlChange::AddColumn(new_column_2_info.clone(), table_name.clone()),
                DdlChange::RemoveColumn(removed_column_info.clone(), table_name.clone()),
                DdlChange::RemoveColumn(removed_column_2_info.clone(), table_name.clone()),
            };
            assert_eq!(returned_ddl_set, expected_ddl_set);
        } else {
            panic!("second_result does not contain a table");
        }
    }

    #[test]
    fn dml_change_insert_update_delete() {
        let table_name = TableName::new("foobar".to_string());
        let id_column_info = ColumnInfo::new("id", "bigint");
        let text_column_info = ColumnInfo::new("foobar", "text");

        // CHANGE 1 - INSERT
        let changed_columns_1 = vec![
            Column::ChangedColumn {
                column_info: id_column_info.clone(),
                value: Some(ColumnValue::Integer(1)),
            },
            Column::ChangedColumn {
                column_info: text_column_info.clone(),
                value: Some(ColumnValue::Text("1".to_string())),
            },
        ];
        let change_1 = ParsedLine::ChangedData {
            kind: ChangeKind::Insert,
            table_name: table_name.clone(),
            columns: changed_columns_1,
        };
        let mut change_processing = ChangeProcessing::new();
        let result_1 = change_processing.add_change(change_1);
        let mut expected_changes_1 = BTreeMap::<i64, ChangeSet>::new();
        expected_changes_1.insert(
            1,
            ChangeSet {
                changes: Some(ParsedLine::ChangedData {
                    columns: vec![
                        Column::ChangedColumn {
                            column_info: id_column_info.clone(),
                            value: Some(ColumnValue::Integer(1)),
                        },
                        Column::ChangedColumn {
                            column_info: text_column_info.clone(),
                            value: Some(ColumnValue::Text("1".to_string())),
                        },
                    ],
                    table_name: table_name.clone(),
                    kind: ChangeKind::Insert,
                }),
            },
        );
        let expected_change_set_1 = ChangeSetWithColumnType::IntColumnType(expected_changes_1);
        let expected_table_holder_1 = TableHolder {
            tables: hashmap!(table_name.clone() => Table {
                table_name: table_name.clone(),
                column_info: Some(hashset!(id_column_info.clone(), text_column_info.clone())),
                changeset: expected_change_set_1 }),
        };
        assert_eq!(change_processing.table_holder, expected_table_holder_1);
        assert!(result_1.is_none());

        // CHANGE 2 - UPDATE
        let changed_columns_2 = vec![
            Column::ChangedColumn {
                column_info: id_column_info.clone(),
                value: Some(ColumnValue::Integer(1)),
            },
            Column::ChangedColumn {
                column_info: text_column_info.clone(),
                value: Some(ColumnValue::Text("2".to_string())),
            },
        ];
        let change_2 = ParsedLine::ChangedData {
            kind: ChangeKind::Update,
            table_name: table_name.clone(),
            columns: changed_columns_2,
        };
        let result_2 = change_processing.add_change(change_2);
        let mut expected_changes_2 = BTreeMap::<i64, ChangeSet>::new();
        expected_changes_2.insert(
            1,
            ChangeSet {
                changes: Some(ParsedLine::ChangedData {
                    columns: vec![
                        Column::ChangedColumn {
                            column_info: id_column_info.clone(),
                            value: Some(ColumnValue::Integer(1)),
                        },
                        Column::ChangedColumn {
                            column_info: text_column_info.clone(),
                            value: Some(ColumnValue::Text("2".to_string())),
                        },
                    ],
                    table_name: table_name.clone(),
                    kind: ChangeKind::Insert,
                }),
            },
        );
        let expected_change_set_2 = ChangeSetWithColumnType::IntColumnType(expected_changes_2);
        let expected_table_holder_2 = TableHolder {
            tables: hashmap!(table_name.clone() => Table {
                table_name: table_name.clone(),
                column_info: Some(hashset!(id_column_info.clone(), text_column_info.clone())),
                changeset: expected_change_set_2 }),
        };
        assert_eq!(change_processing.table_holder, expected_table_holder_2);
        assert!(result_2.is_none());

        // CHANGE 3 - DELETE
        let changed_columns_3 = vec![Column::ChangedColumn {
            column_info: id_column_info.clone(),
            value: Some(ColumnValue::Integer(1)),
        }];
        let change_3 = ParsedLine::ChangedData {
            kind: ChangeKind::Delete,
            table_name: table_name.clone(),
            columns: changed_columns_3,
        };
        let result_3 = change_processing.add_change(change_3);
        let mut expected_changes_3 = BTreeMap::<i64, ChangeSet>::new();
        expected_changes_3.insert(1, ChangeSet { changes: None });
        let expected_change_set_3 = ChangeSetWithColumnType::IntColumnType(expected_changes_3);
        let expected_table_holder_3 = TableHolder {
            tables: hashmap!(table_name.clone() => Table {
                table_name: table_name.clone(),
                column_info: Some(hashset!(id_column_info.clone(), text_column_info.clone())),
                changeset: expected_change_set_3 }),
        };
        assert_eq!(change_processing.table_holder, expected_table_holder_3);
        assert!(result_3.is_none());
    }

    #[test]
    #[should_panic]
    fn dml_change_insert_insert_panics() {
        let table_name = TableName::new("foobar".to_string());
        let id_column_info = ColumnInfo::new("id", "bigint");
        let text_column_info = ColumnInfo::new("foobar", "text");

        let changed_columns_1 = vec![
            Column::ChangedColumn {
                column_info: id_column_info.clone(),
                value: Some(ColumnValue::Integer(1)),
            },
            Column::ChangedColumn {
                column_info: text_column_info.clone(),
                value: Some(ColumnValue::Text("1".to_string())),
            },
        ];
        let change_1 = ParsedLine::ChangedData {
            kind: ChangeKind::Insert,
            table_name: table_name.clone(),
            columns: changed_columns_1,
        };
        let mut change_processing = ChangeProcessing::new();
        change_processing.add_change(change_1.clone());
        change_processing.add_change(change_1.clone());
    }

    #[test]
    #[should_panic]
    fn dml_change_update_insert_panics() {
        let table_name = TableName::new("foobar".to_string());
        let id_column_info = ColumnInfo::new("id", "bigint");
        let text_column_info = ColumnInfo::new("foobar", "text");

        let changed_columns_1 = vec![
            Column::ChangedColumn {
                column_info: id_column_info.clone(),
                value: Some(ColumnValue::Integer(1)),
            },
            Column::ChangedColumn {
                column_info: text_column_info.clone(),
                value: Some(ColumnValue::Text("1".to_string())),
            },
        ];
        let change_1 = ParsedLine::ChangedData {
            kind: ChangeKind::Update,
            table_name: table_name.clone(),
            columns: changed_columns_1.clone(),
        };
        let change_2 = ParsedLine::ChangedData {
            kind: ChangeKind::Insert,
            table_name: table_name.clone(),
            columns: changed_columns_1.clone(),
        };
        let mut change_processing = ChangeProcessing::new();
        change_processing.add_change(change_1.clone());
        change_processing.add_change(change_2.clone());
    }

    #[test]
    #[should_panic]
    fn dml_change_delete_update_panics() {
        let table_name = TableName::new("foobar".to_string());
        let id_column_info = ColumnInfo::new("id", "bigint");
        let text_column_info = ColumnInfo::new("foobar", "text");

        let changed_columns_1 = vec![
            Column::ChangedColumn {
                column_info: id_column_info.clone(),
                value: Some(ColumnValue::Integer(1)),
            },
            Column::ChangedColumn {
                column_info: text_column_info.clone(),
                value: Some(ColumnValue::Text("1".to_string())),
            },
        ];
        let change_1 = ParsedLine::ChangedData {
            kind: ChangeKind::Delete,
            table_name: table_name.clone(),
            columns: changed_columns_1.clone(),
        };
        let change_2 = ParsedLine::ChangedData {
            kind: ChangeKind::Update,
            table_name: table_name.clone(),
            columns: changed_columns_1.clone(),
        };
        let mut change_processing = ChangeProcessing::new();
        change_processing.add_change(change_1.clone());
        change_processing.add_change(change_2.clone());
    }

    #[test]
    fn dml_change_delete_insert_updates() {
        let table_name = TableName::new("foobar".to_string());
        let id_column_info = ColumnInfo::new("id", "bigint");
        let text_column_info = ColumnInfo::new("foobar", "text");

        let changed_columns_1 = vec![
            Column::ChangedColumn {
                column_info: id_column_info.clone(),
                value: Some(ColumnValue::Integer(1)),
            },
            Column::ChangedColumn {
                column_info: text_column_info.clone(),
                value: Some(ColumnValue::Text("1".to_string())),
            },
        ];
        let change_1 = ParsedLine::ChangedData {
            kind: ChangeKind::Delete,
            table_name: table_name.clone(),
            columns: changed_columns_1.clone(),
        };
        let change_2 = ParsedLine::ChangedData {
            kind: ChangeKind::Insert,
            table_name: table_name.clone(),
            columns: changed_columns_1.clone(),
        };
        let mut change_processing = ChangeProcessing::new();
        change_processing.add_change(change_1.clone());
        change_processing.add_change(change_2.clone());

        let mut expected_changes_1 = BTreeMap::<i64, ChangeSet>::new();
        expected_changes_1.insert(
            1,
            ChangeSet {
                changes: Some(ParsedLine::ChangedData {
                    columns: vec![
                        Column::ChangedColumn {
                            column_info: id_column_info.clone(),
                            value: Some(ColumnValue::Integer(1)),
                        },
                        Column::ChangedColumn {
                            column_info: text_column_info.clone(),
                            value: Some(ColumnValue::Text("1".to_string())),
                        },
                    ],
                    table_name: table_name.clone(),
                    kind: ChangeKind::Update,
                }),
            },
        );
        let expected_change_set_1 = ChangeSetWithColumnType::IntColumnType(expected_changes_1);
        let expected_table_holder_1 = TableHolder {
            tables: hashmap!(table_name.clone() => Table {
                table_name: table_name.clone(),
                column_info: Some(hashset!(id_column_info.clone(), text_column_info.clone())),
                changeset: expected_change_set_1 }),
        };
        assert_eq!(change_processing.table_holder, expected_table_holder_1);
    }

    #[test]
    fn dml_change_update_update_delete() {
        let table_name = TableName::new("foobar".to_string());
        let id_column_info = ColumnInfo::new("id", "bigint");
        let text_column_info = ColumnInfo::new("foobar", "text");

        // CHANGE 1 - UPDATE
        let changed_columns_1 = vec![
            Column::ChangedColumn {
                column_info: id_column_info.clone(),
                value: Some(ColumnValue::Integer(1)),
            },
            Column::ChangedColumn {
                column_info: text_column_info.clone(),
                value: Some(ColumnValue::Text("1".to_string())),
            },
        ];
        let change_1 = ParsedLine::ChangedData {
            kind: ChangeKind::Update,
            table_name: table_name.clone(),
            columns: changed_columns_1,
        };
        let mut change_processing = ChangeProcessing::new();
        let result_1 = change_processing.add_change(change_1);
        let mut expected_changes_1 = BTreeMap::<i64, ChangeSet>::new();
        expected_changes_1.insert(
            1,
            ChangeSet {
                changes: Some(ParsedLine::ChangedData {
                    columns: vec![
                        Column::ChangedColumn {
                            column_info: id_column_info.clone(),
                            value: Some(ColumnValue::Integer(1)),
                        },
                        Column::ChangedColumn {
                            column_info: text_column_info.clone(),
                            value: Some(ColumnValue::Text("1".to_string())),
                        },
                    ],
                    table_name: table_name.clone(),
                    kind: ChangeKind::Update,
                }),
            },
        );
        let expected_change_set_1 = ChangeSetWithColumnType::IntColumnType(expected_changes_1);
        let expected_table_holder_1 = TableHolder {
            tables: hashmap!(table_name.clone() => Table {
                table_name: table_name.clone(),
                column_info: Some(hashset!(id_column_info.clone(), text_column_info.clone())),
                changeset: expected_change_set_1 }),
        };
        assert_eq!(change_processing.table_holder, expected_table_holder_1);
        assert!(result_1.is_none());

        // CHANGE 2 - UPDATE
        let changed_columns_2 = vec![
            Column::ChangedColumn {
                column_info: id_column_info.clone(),
                value: Some(ColumnValue::Integer(1)),
            },
            Column::ChangedColumn {
                column_info: text_column_info.clone(),
                value: Some(ColumnValue::Text("2".to_string())),
            },
        ];
        let change_2 = ParsedLine::ChangedData {
            kind: ChangeKind::Update,
            table_name: table_name.clone(),
            columns: changed_columns_2,
        };
        let result_2 = change_processing.add_change(change_2);
        let mut expected_changes_2 = BTreeMap::<i64, ChangeSet>::new();
        expected_changes_2.insert(
            1,
            ChangeSet {
                changes: Some(ParsedLine::ChangedData {
                    columns: vec![
                        Column::ChangedColumn {
                            column_info: id_column_info.clone(),
                            value: Some(ColumnValue::Integer(1)),
                        },
                        Column::ChangedColumn {
                            column_info: text_column_info.clone(),
                            value: Some(ColumnValue::Text("2".to_string())),
                        },
                    ],
                    table_name: table_name.clone(),
                    kind: ChangeKind::Update,
                }),
            },
        );
        let expected_change_set_2 = ChangeSetWithColumnType::IntColumnType(expected_changes_2);
        let expected_table_holder_2 = TableHolder {
            tables: hashmap!(table_name.clone() => Table {
                table_name: table_name.clone(),
                column_info: Some(hashset!(id_column_info.clone(), text_column_info.clone())),
                changeset: expected_change_set_2 }),
        };
        assert_eq!(change_processing.table_holder, expected_table_holder_2);
        assert!(result_2.is_none());

        // CHANGE 3 - DELETE
        let changed_columns_3 = vec![Column::ChangedColumn {
            column_info: id_column_info.clone(),
            value: Some(ColumnValue::Integer(1)),
        }];
        let change_3 = ParsedLine::ChangedData {
            kind: ChangeKind::Delete,
            table_name: table_name.clone(),
            columns: changed_columns_3,
        };
        let result_3 = change_processing.add_change(change_3);
        let mut expected_changes_3 = BTreeMap::<i64, ChangeSet>::new();
        expected_changes_3.insert(
            1,
            ChangeSet {
                changes: Some(ParsedLine::ChangedData {
                    columns: vec![Column::ChangedColumn {
                        column_info: id_column_info.clone(),
                        value: Some(ColumnValue::Integer(1)),
                    }],
                    table_name: table_name.clone(),
                    kind: ChangeKind::Delete,
                }),
            },
        );
        let expected_change_set_3 = ChangeSetWithColumnType::IntColumnType(expected_changes_3);
        let expected_table_holder_3 = TableHolder {
            tables: hashmap!(table_name.clone() => Table {
                table_name: table_name.clone(),
                column_info: Some(hashset!(id_column_info.clone(), text_column_info.clone())),
                changeset: expected_change_set_3 }),
        };
        assert_eq!(change_processing.table_holder, expected_table_holder_3);
        assert!(result_3.is_none());
    }

    #[test]
    #[should_panic]
    fn dml_change_delete_delete_panics() {
        let table_name = TableName::new("foobar".to_string());
        let id_column_info = ColumnInfo::new("id", "bigint");

        let changed_columns = vec![Column::ChangedColumn {
            column_info: id_column_info.clone(),
            value: Some(ColumnValue::Integer(1)),
        }];
        let change = ParsedLine::ChangedData {
            kind: ChangeKind::Delete,
            table_name: table_name.clone(),
            columns: changed_columns,
        };
        let mut change_processing = ChangeProcessing::new();
        change_processing.add_change(change.clone());
        change_processing.add_change(change.clone());
    }

    #[test]
    fn dml_change_unchanged_toast_insert_update() {
        let table_name = TableName::new("foobar".to_string());
        let id_column_info = ColumnInfo::new("id", "bigint");
        let text_column_info = ColumnInfo::new("foobar", "text");

        // CHANGE 1 - INSERT
        let changed_columns_1 = vec![
            Column::ChangedColumn {
                column_info: id_column_info.clone(),
                value: Some(ColumnValue::Integer(1)),
            },
            Column::ChangedColumn {
                column_info: text_column_info.clone(),
                value: Some(ColumnValue::Text("1".to_string())),
            },
        ];
        let change_1 = ParsedLine::ChangedData {
            kind: ChangeKind::Insert,
            table_name: table_name.clone(),
            columns: changed_columns_1,
        };
        let mut change_processing = ChangeProcessing::new();
        let result_1 = change_processing.add_change(change_1);
        let mut expected_changes_1 = BTreeMap::<i64, ChangeSet>::new();
        expected_changes_1.insert(
            1,
            ChangeSet {
                changes: Some(ParsedLine::ChangedData {
                    columns: vec![
                        Column::ChangedColumn {
                            column_info: id_column_info.clone(),
                            value: Some(ColumnValue::Integer(1)),
                        },
                        Column::ChangedColumn {
                            column_info: text_column_info.clone(),
                            value: Some(ColumnValue::Text("1".to_string())),
                        },
                    ],
                    table_name: table_name.clone(),
                    kind: ChangeKind::Insert,
                }),
            },
        );
        let expected_change_set_1 = ChangeSetWithColumnType::IntColumnType(expected_changes_1);
        let expected_table_holder_1 = TableHolder {
            tables: hashmap!(table_name.clone() => Table {
                table_name: table_name.clone(),
                column_info: Some(hashset!(id_column_info.clone(), text_column_info.clone())),
                changeset: expected_change_set_1 }),
        };
        assert_eq!(change_processing.table_holder, expected_table_holder_1);
        assert!(result_1.is_none());

        // CHANGE 2 - UPDATE
        let changed_columns_2 = vec![
            Column::ChangedColumn {
                column_info: id_column_info.clone(),
                value: Some(ColumnValue::Integer(1)),
            },
            Column::UnchangedToastColumn {
                column_info: text_column_info.clone(),
            },
        ];
        let change_2 = ParsedLine::ChangedData {
            kind: ChangeKind::Update,
            table_name: table_name.clone(),
            columns: changed_columns_2,
        };
        let result_2 = change_processing.add_change(change_2);
        let mut expected_changes_2 = BTreeMap::<i64, ChangeSet>::new();
        expected_changes_2.insert(
            1,
            ChangeSet {
                changes: Some(ParsedLine::ChangedData {
                    columns: vec![
                        Column::ChangedColumn {
                            column_info: id_column_info.clone(),
                            value: Some(ColumnValue::Integer(1)),
                        },
                        Column::ChangedColumn {
                            column_info: text_column_info.clone(),
                            value: Some(ColumnValue::Text("1".to_string())),
                        },
                    ],
                    table_name: table_name.clone(),
                    kind: ChangeKind::Insert,
                }),
            },
        );
        let expected_change_set_2 = ChangeSetWithColumnType::IntColumnType(expected_changes_2);
        let expected_table_holder_2 = TableHolder {
            tables: hashmap!(table_name.clone() => Table {
                table_name: table_name.clone(),
                column_info: Some(hashset!(id_column_info.clone(), text_column_info.clone())),
                changeset: expected_change_set_2 }),
        };
        assert_eq!(change_processing.table_holder, expected_table_holder_2);
        assert!(result_2.is_none());
    }

    #[test]
    fn dml_change_unchanged_toast_update_update() {
        let table_name = TableName::new("foobar".to_string());
        let id_column_info = ColumnInfo::new("id", "bigint");
        let text_column_info = ColumnInfo::new("foobar", "text");

        // CHANGE 1 - UPDATE
        let changed_columns_1 = vec![
            Column::ChangedColumn {
                column_info: id_column_info.clone(),
                value: Some(ColumnValue::Integer(1)),
            },
            Column::ChangedColumn {
                column_info: text_column_info.clone(),
                value: Some(ColumnValue::Text("1".to_string())),
            },
        ];
        let change_1 = ParsedLine::ChangedData {
            kind: ChangeKind::Update,
            table_name: table_name.clone(),
            columns: changed_columns_1,
        };
        let mut change_processing = ChangeProcessing::new();
        let result_1 = change_processing.add_change(change_1);
        let mut expected_changes_1 = BTreeMap::<i64, ChangeSet>::new();
        expected_changes_1.insert(
            1,
            ChangeSet {
                changes: Some(ParsedLine::ChangedData {
                    columns: vec![
                        Column::ChangedColumn {
                            column_info: id_column_info.clone(),
                            value: Some(ColumnValue::Integer(1)),
                        },
                        Column::ChangedColumn {
                            column_info: text_column_info.clone(),
                            value: Some(ColumnValue::Text("1".to_string())),
                        },
                    ],
                    table_name: table_name.clone(),
                    kind: ChangeKind::Update,
                }),
            },
        );
        let expected_change_set_1 = ChangeSetWithColumnType::IntColumnType(expected_changes_1);
        let expected_table_holder_1 = TableHolder {
            tables: hashmap!(table_name.clone() => Table {
                table_name: table_name.clone(),
                column_info: Some(hashset!(id_column_info.clone(), text_column_info.clone())),
                changeset: expected_change_set_1 }),
        };
        assert_eq!(change_processing.table_holder, expected_table_holder_1);
        assert!(result_1.is_none());

        // CHANGE 2 - UPDATE
        let changed_columns_2 = vec![
            Column::ChangedColumn {
                column_info: id_column_info.clone(),
                value: Some(ColumnValue::Integer(1)),
            },
            Column::UnchangedToastColumn {
                column_info: text_column_info.clone(),
            },
        ];
        let change_2 = ParsedLine::ChangedData {
            kind: ChangeKind::Update,
            table_name: table_name.clone(),
            columns: changed_columns_2,
        };
        let result_2 = change_processing.add_change(change_2);
        let mut expected_changes_2 = BTreeMap::<i64, ChangeSet>::new();
        expected_changes_2.insert(
            1,
            ChangeSet {
                changes: Some(ParsedLine::ChangedData {
                    columns: vec![
                        Column::ChangedColumn {
                            column_info: id_column_info.clone(),
                            value: Some(ColumnValue::Integer(1)),
                        },
                        Column::ChangedColumn {
                            column_info: text_column_info.clone(),
                            value: Some(ColumnValue::Text("1".to_string())),
                        },
                    ],
                    table_name: table_name.clone(),
                    kind: ChangeKind::Update,
                }),
            },
        );
        let expected_change_set_2 = ChangeSetWithColumnType::IntColumnType(expected_changes_2);
        let expected_table_holder_2 = TableHolder {
            tables: hashmap!(table_name.clone() => Table {
                table_name: table_name.clone(),
                column_info: Some(hashset!(id_column_info.clone(), text_column_info.clone())),
                changeset: expected_change_set_2 }),
        };
        assert_eq!(change_processing.table_holder, expected_table_holder_2);
        assert!(result_2.is_none());
    }

    #[test]
    fn dml_change_unchanged_toast_update_unchanged_toast_update() {
        let table_name = TableName::new("foobar".to_string());
        let id_column_info = ColumnInfo::new("id", "bigint");
        let text_column_info = ColumnInfo::new("foobar", "text");

        // CHANGE 1 - UPDATE
        let changed_columns_1 = vec![
            Column::ChangedColumn {
                column_info: id_column_info.clone(),
                value: Some(ColumnValue::Integer(1)),
            },
            Column::UnchangedToastColumn {
                column_info: text_column_info.clone(),
            },
        ];
        let change_1 = ParsedLine::ChangedData {
            kind: ChangeKind::Update,
            table_name: table_name.clone(),
            columns: changed_columns_1,
        };
        let mut change_processing = ChangeProcessing::new();
        let result_1 = change_processing.add_change(change_1);
        let mut expected_changes_1 = BTreeMap::<i64, ChangeSet>::new();
        expected_changes_1.insert(
            1,
            ChangeSet {
                changes: Some(ParsedLine::ChangedData {
                    columns: vec![
                        Column::ChangedColumn {
                            column_info: id_column_info.clone(),
                            value: Some(ColumnValue::Integer(1)),
                        },
                        Column::UnchangedToastColumn {
                            column_info: text_column_info.clone(),
                        },
                    ],
                    table_name: table_name.clone(),
                    kind: ChangeKind::Update,
                }),
            },
        );
        let expected_change_set_1 = ChangeSetWithColumnType::IntColumnType(expected_changes_1);
        let expected_table_holder_1 = TableHolder {
            tables: hashmap!(table_name.clone() => Table {
                table_name: table_name.clone(),
                column_info: Some(hashset!(id_column_info.clone(), text_column_info.clone())),
                changeset: expected_change_set_1 }),
        };
        assert_eq!(change_processing.table_holder, expected_table_holder_1);
        assert!(result_1.is_none());

        // CHANGE 2 - UPDATE
        let changed_columns_2 = vec![
            Column::ChangedColumn {
                column_info: id_column_info.clone(),
                value: Some(ColumnValue::Integer(1)),
            },
            Column::UnchangedToastColumn {
                column_info: text_column_info.clone(),
            },
        ];
        let change_2 = ParsedLine::ChangedData {
            kind: ChangeKind::Update,
            table_name: table_name.clone(),
            columns: changed_columns_2,
        };
        let result_2 = change_processing.add_change(change_2);
        let mut expected_changes_2 = BTreeMap::<i64, ChangeSet>::new();
        expected_changes_2.insert(
            1,
            ChangeSet {
                changes: Some(ParsedLine::ChangedData {
                    columns: vec![
                        Column::ChangedColumn {
                            column_info: id_column_info.clone(),
                            value: Some(ColumnValue::Integer(1)),
                        },
                        Column::UnchangedToastColumn {
                            column_info: text_column_info.clone(),
                        },
                    ],
                    table_name: table_name.clone(),
                    kind: ChangeKind::Update,
                }),
            },
        );
        let expected_change_set_2 = ChangeSetWithColumnType::IntColumnType(expected_changes_2);
        let expected_table_holder_2 = TableHolder {
            tables: hashmap!(table_name.clone() => Table {
                table_name: table_name.clone(),
                column_info: Some(hashset!(id_column_info.clone(), text_column_info.clone())),
                changeset: expected_change_set_2 }),
        };
        assert_eq!(change_processing.table_holder, expected_table_holder_2);
        assert!(result_2.is_none());
    }
}
