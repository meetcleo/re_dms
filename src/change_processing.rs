use parser::{ ParsedLine, ColumnValue };
use std::collections::{ HashMap, BTreeMap };

#[allow(unused_imports)]
use log::{debug, error, log_enabled, info, Level};

// single threaded f'now
// can have one of these per thread and communicate via
// channels later
pub struct ChangeProcessing {
    table_holder: TableHolder
}
struct ChangeSet {
    // TODO
    changes: Vec<ParsedLine>
}

impl ChangeSet {
    fn new() -> ChangeSet {
        ChangeSet { changes: vec![] }
    }
    fn add_change(&mut self, change: ParsedLine) {
        // if self.changes.len() == 0 {
        // }
        self.changes.push(change);
    }
}

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
}

struct Table {
    // we want to have a changeset, but need to match on the enum type for the pkey of the column
    changeset: ChangeSetWithColumnType
}

struct TableHolder {
    tables: HashMap<String, Table>
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
    fn add_change(&mut self, parsed_line: ParsedLine) {
        if let ParsedLine::ChangedData{..} = parsed_line {
            // i'd rather not be allocating a string here for every search, but
            // the borrow checker is being a PITA for the first time, so this'll have to do.
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
        }
    }
}

impl TableHolder {
    fn add_change(&mut self, parsed_line: ParsedLine) {
        if let ParsedLine::ChangedData{ref table_name, ..} = parsed_line {
            // i'd rather not be allocating a string here for every search, but
            // the borrow checker is being a PITA for the first time, so this'll have to do.
            let cloned = table_name.clone();
            self.tables.entry(cloned)
                .or_insert_with(|| Table::new(&parsed_line))
                .add_change(parsed_line)
        }
    }
}

impl ChangeProcessing {
    pub fn new() -> ChangeProcessing {
        let hash_map = HashMap::<String, Table>::new();
        ChangeProcessing { table_holder: TableHolder { tables: hash_map } }
    }
    pub fn add_change(&mut self, parsed_line: ParsedLine) {
        match parsed_line {
            ParsedLine::Begin(_) | ParsedLine::Commit(_) | ParsedLine::TruncateTable => {}, // TODO
            ParsedLine::ContinueParse => {}, // need to be exhaustive
            ParsedLine::ChangedData{ .. } => { self.table_holder.add_change(parsed_line)}
        }
    }
}
