#[allow(unused_imports)]
use log::{debug, error, info, log_enabled, Level};
use std::fmt;

use internment::ArcIntern;
use lazy_static::lazy_static;
use regex::Regex;
use std::collections::HashSet;

pub type TableName = ArcIntern<String>;
pub type ColumnName = ArcIntern<String>;
pub type ColumnType = ArcIntern<String>;

lazy_static! {
    static ref PARSE_COLUMN_REGEX: regex::Regex = Regex::new(r"(^[^\[^\]]+)\[([^:]+)\]:").unwrap();
    static ref COLUMN_TYPE_REGEX: regex::Regex = Regex::new(r"^.+\[\]$").unwrap();
}

// for tablename
pub trait SchemaAndTable {
    fn schema_and_table_name(&self) -> (&str, &str);
}

// schema.table_name
// we assume a valid table name, so unwrap
impl SchemaAndTable for TableName {
    fn schema_and_table_name(&self) -> (&str, &str) {
        self.split_once('.').unwrap()
    }
}

// define more config later
struct ParserConfig {
    include_xids: bool,
}
struct ParserState {
    currently_parsing: Option<ParsedLine>,
}

// define config later
pub struct Parser {
    config: ParserConfig,
    parse_state: ParserState,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum ColumnValue {
    Boolean(bool),
    Integer(i64),
    Numeric(String),
    Text(String),
    IncompleteText(String),
    UnchangedToast,
}

impl fmt::Display for ColumnValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ColumnValue::UnchangedToast => {
                write!(f, "unchanged-toast-datum")
            }
            ColumnValue::Boolean(x) => {
                write!(f, "{}", x)
            }
            ColumnValue::Integer(x) => {
                write!(f, "{}", x)
            }
            ColumnValue::Numeric(x) => {
                write!(f, "{}", x)
            }
            ColumnValue::Text(x) => {
                write!(f, "{}", x)
            }
            ColumnValue::IncompleteText(x) => {
                write!(f, "{}", x)
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum Column {
    UnchangedToastColumn {
        column_info: ColumnInfo,
    },
    ChangedColumn {
        column_info: ColumnInfo,
        value: Option<ColumnValue>,
    },
    IncompleteColumn {
        column_info: ColumnInfo,
        value: ColumnValue,
    },
}

// happy to clone it, it only holds two pointers
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct ColumnInfo {
    pub name: ColumnName,
    pub column_type: ColumnType,
}

impl ColumnInfo {
    pub fn column_name(&self) -> &str {
        self.name.as_ref()
    }
    pub fn column_type(&self) -> &str {
        self.column_type.as_ref()
    }
    pub fn new<T: ToString>(name: T, column_type: T) -> ColumnInfo {
        ColumnInfo {
            name: ColumnName::new(name.to_string()),
            column_type: ColumnType::new(column_type.to_string()),
        }
    }
    pub fn is_id_column(&self) -> bool {
        self.name.as_ref() == "id"
    }
}

impl Column {
    pub fn column_name(&self) -> &str {
        self.column_info().column_name()
    }
    pub fn column_info(&self) -> &ColumnInfo {
        match self {
            Column::UnchangedToastColumn { column_info, .. } => column_info,
            Column::ChangedColumn { column_info, .. } => column_info,
            Column::IncompleteColumn { column_info, .. } => column_info,
        }
    }
    // for when you _know_ a column has a value, used for id columns
    pub fn column_value_unwrap(&self) -> &ColumnValue {
        match self {
            Column::ChangedColumn { value, .. } => match value {
                Some(inner) => inner,
                None => panic!("panic!"),
            },
            Column::IncompleteColumn { value, .. } => &value,
            Column::UnchangedToastColumn { .. } => {
                panic!("tried to get value for unchanged_toast_column")
            }
        }
    }

    pub fn column_value_for_changed_column(&self) -> Option<&ColumnValue> {
        match self {
            Column::ChangedColumn { value, .. } => value.as_ref(),
            _ => {
                panic!(
                    "column_value_for_changed_column called on non-changed-column {:?}",
                    self
                )
            }
        }
    }
    pub fn is_changed_data_column(&self) -> bool {
        match self {
            Column::ChangedColumn { .. } => true,
            _ => false,
        }
    }
    pub fn is_id_column(&self) -> bool {
        self.column_name() == "id"
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum ChangeKind {
    Insert,
    Update,
    Delete,
}

impl std::string::ToString for ChangeKind {
    fn to_string(&self) -> String {
        match self {
            Self::Insert => "insert".to_string(),
            Self::Update => "update".to_string(),
            Self::Delete => "delete".to_string(),
        }
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub enum ParsedLine {
    // int is xid
    Begin(i64),
    // int is xid
    Commit(i64),
    ChangedData {
        columns: Vec<Column>,
        table_name: TableName,
        kind: ChangeKind,
    },
    TruncateTable, // TODO
    ContinueParse, // this is to signify that we're halfway through parsing a change
}

impl ParsedLine {
    pub fn find_id_column(&self) -> &Column {
        match self {
            ParsedLine::ChangedData { columns, .. } => {
                // unwrap because this is the id column which _must_ be here
                columns.iter().find(|&x| x.is_id_column()).unwrap()
            }
            _ => panic!("tried to find id column of non changed_data"),
        }
    }

    pub fn column_info_set(&self) -> Option<HashSet<ColumnInfo>> {
        match self {
            ParsedLine::ChangedData { columns, kind, .. } => {
                if kind == &ChangeKind::Delete {
                    None
                } else {
                    Some(columns.iter().map(|x| x.column_info().clone()).collect())
                }
            }
            _ => panic!("tried to find id column of non changed_data"),
        }
    }
    // panic if called on something that's not ChangedData
    pub fn columns_for_changed_data(&self) -> &Vec<Column> {
        match self {
            ParsedLine::ChangedData { columns, .. } => columns,
            _ => panic!("changed columns for changed data called on non-changed data"),
        }
    }
}

impl ColumnValue {
    fn parse<'a>(
        string: &'a str,
        column_type: &str,
        continue_parse: bool,
    ) -> (Option<ColumnValue>, &'a str) {
        const NULL_STRING: &str = "null";
        if string.starts_with(NULL_STRING) {
            let (_start, rest) = ColumnValue::split_until_char_or_end(string, ' ');
            (None, rest)
        } else {
            let (column_value, rest_of_string): (ColumnValue, &str) = match column_type {
                "bigint" => ColumnValue::parse_integer(string),
                "smallint" => ColumnValue::parse_integer(string),
                "integer" => ColumnValue::parse_integer(string),
                "character varying" => ColumnValue::parse_text(string, continue_parse),
                "public.citext" => ColumnValue::parse_text(string, continue_parse), // extensions come through as public.
                "text" => ColumnValue::parse_text(string, continue_parse),
                "numeric" => ColumnValue::parse_numeric(string),
                "decimal" => ColumnValue::parse_numeric(string),
                "double precision" => ColumnValue::parse_numeric(string),
                "boolean" => ColumnValue::parse_boolean(string),
                "timestamp without time zone" => ColumnValue::parse_text(string, continue_parse),
                "date" => ColumnValue::parse_text(string, continue_parse),
                "uuid" => ColumnValue::parse_text(string, continue_parse),
                "jsonb" => ColumnValue::parse_text(string, continue_parse),
                "json" => ColumnValue::parse_text(string, continue_parse),
                "public.hstore" => ColumnValue::parse_text(string, continue_parse),
                "interval" => ColumnValue::parse_text(string, continue_parse),
                "array" => ColumnValue::parse_text(string, continue_parse),
                _ => panic!("Unknown column type: {:?}", column_type),
            };
            (Some(column_value), rest_of_string)
        }
    }
    fn parse_integer<'a>(string: &'a str) -> (ColumnValue, &'a str) {
        let (start, rest) = ColumnValue::split_until_char_or_end(string, ' ');
        let integer: i64 = start.parse().unwrap();
        (ColumnValue::Integer(integer), rest)
    }
    fn parse_text<'a>(string: &'a str, continue_parse: bool) -> (ColumnValue, &'a str) {
        if string.starts_with("unchanged-toast-datum") {
            let (start, rest) = ColumnValue::split_until_char_or_end(string, ' ');
            assert_eq!(start, "unchanged-toast-datum");
            return (ColumnValue::UnchangedToast, rest);
        }
        if !continue_parse {
            assert_eq!(&string[0..1], "'");
        }
        let mut total_index: usize = 0;
        let mut complete_text: bool = false;
        let without_first_quote = if continue_parse {
            &string
        } else {
            &string[1..]
        };

        // debug!("total_index {}", total_index);
        while total_index + 1 <= without_first_quote.len() {
            let found_index_option = without_first_quote[total_index..].find("'");
            match found_index_option {
                Some(index) => {
                    total_index += index;
                    let escaped = is_escaped(without_first_quote, total_index);
                    if !escaped {
                        complete_text = true;
                        break;
                    }
                    // move past and keep looking
                    total_index += 1;
                }
                None => {
                    total_index = without_first_quote.len();
                    break;
                }
            };
        }
        let (start, end) = without_first_quote.split_at(total_index);
        // move past final quote if found
        let (column, text) = if complete_text {
            assert_eq!(&end[0..1], "'");
            (ColumnValue::Text(start.to_owned()), &end[1..])
        } else {
            (ColumnValue::IncompleteText(start.to_owned()), end)
        };
        let (_thrown_away_space, adjusted_end) = ColumnValue::split_until_char_or_end(text, ' ');
        (column, adjusted_end)
    }
    fn split_until_char_or_end(string: &str, character: char) -> (&str, &str) {
        match string.split_once(character) {
            Some((start, rest)) => (start, rest),
            None => string.split_at(string.len()),
        }
    }
    fn parse_numeric<'a>(string: &'a str) -> (ColumnValue, &'a str) {
        let (start, rest) = ColumnValue::split_until_char_or_end(string, ' ');
        (ColumnValue::Numeric(start.to_owned()), rest)
    }
    fn parse_boolean<'a>(string: &'a str) -> (ColumnValue, &'a str) {
        let (start, rest) = ColumnValue::split_until_char_or_end(string, ' ');
        let bool_value = match start {
            "true" => true,
            "false" => false,
            _ => panic!("Unknown boolvalue {:?}", start),
        };
        (ColumnValue::Boolean(bool_value), rest)
    }
}

impl Parser {
    pub fn new(include_xids: bool) -> Parser {
        Parser {
            config: ParserConfig { include_xids },
            parse_state: ParserState {
                currently_parsing: None,
            },
        }
    }

    pub fn parse(&mut self, string: &String) -> ParsedLine {
        match string {
            x if { x.starts_with("BEGIN") } => self.parse_begin(x),
            x if { x.starts_with("COMMIT") } => self.parse_commit(x),
            x if { x.starts_with("table") } => self.parse_change(x),
            x if { self.parse_state.currently_parsing.is_some() } => self.continue_parse(x),
            x => {
                panic!("Unknown change kind: {}!", x);
            }
        }
    }

    fn parse_begin(&self, string: &str) -> ParsedLine {
        let parsed_line = if self.config.include_xids {
            const SIZE_OF_BEGIN_TAG: usize = "BEGIN ".len();
            let rest_of_string = &string[SIZE_OF_BEGIN_TAG..string.len()];
            // "BEGIN 1234"
            let xid: i64 = rest_of_string.parse().unwrap();
            debug!("parsed begin {}", xid);
            ParsedLine::Begin(xid)
        } else {
            ParsedLine::Begin(0)
        };
        parsed_line
    }

    fn parse_commit(&self, string: &str) -> ParsedLine {
        let parsed_line = if self.config.include_xids {
            // "COMMIT 1234"
            const SIZE_OF_COMMIT_TAG: usize = "COMMIT ".len();
            let rest_of_string = &string[SIZE_OF_COMMIT_TAG..string.len()];
            // "BEGIN 1234"
            let xid: i64 = rest_of_string.parse().unwrap();
            debug!("parsed commit {}", xid);
            ParsedLine::Commit(xid)
        } else {
            ParsedLine::Commit(0)
        };
        parsed_line
    }

    fn parse_change(&mut self, string: &str) -> ParsedLine {
        const SIZE_OF_TABLE_TAG: usize = "table ".len();
        let string_without_tag = &string[SIZE_OF_TABLE_TAG..string.len()];
        // we assume tables can't have colons in their names
        // fuck you if you put a colon in a table name, you psychopath
        let table_name = slice_until_colon_or_end(string_without_tag);
        // + 2 for colon + space
        assert_eq!(
            &string_without_tag[table_name.len()..table_name.len() + 2],
            ": "
        );
        let string_without_table =
            &string_without_tag[table_name.len() + 2..string_without_tag.len()];
        let kind_string = slice_until_colon_or_end(string_without_table);

        // TODO: split early here for truncate columns

        let kind = self.parse_kind(kind_string);

        debug!("table: {:?} change: {:?}", table_name, kind_string);

        // + 2 for colon + space
        assert_eq!(
            &string_without_table[kind_string.len()..kind_string.len() + 2],
            ": "
        );
        let string_without_kind =
            &string_without_table[kind_string.len() + 2..string_without_table.len()];

        let columns = self.parse_columns(string_without_kind);
        self.handle_parse_changed_data(table_name, kind, columns)
    }

    fn column_is_incomplete(&self, columns: &Vec<Column>) -> bool {
        if let Some(Column::IncompleteColumn { .. }) = columns.last() {
            true
        } else {
            false
        }
    }

    fn parse_kind(&self, string: &str) -> ChangeKind {
        match string {
            "INSERT" => ChangeKind::Insert,
            "UPDATE" => ChangeKind::Update,
            "DELETE" => ChangeKind::Delete,
            _ => panic!("Unknown change kind: {}", string),
        }
    }

    fn parse_columns(&self, string: &str) -> Vec<Column> {
        let mut column_vector = Vec::new();
        let mut remaining_string = string;
        while remaining_string.len() > 0 {
            let (column, rest_of_string) = self.parse_column(remaining_string);
            remaining_string = rest_of_string;
            column_vector.push(column);
        }
        column_vector
    }

    fn parse_column<'a>(&self, string: &'a str) -> (Column, &'a str) {
        let re = &PARSE_COLUMN_REGEX;
        let captures = re.captures(string).unwrap();

        let column_name = captures
            .get(1)
            .expect("couldn't match column_name")
            .as_str();
        let column_type = captures
            .get(2)
            .expect("couldn't match column_type")
            .as_str();
        // For array types, remove the inner type specification - we treat all array types as text
        let column_type = &COLUMN_TYPE_REGEX
            .replace_all(column_type, "array")
            .to_string();
        let string_without_column_type =
            &string[captures.get(0).map_or("", |m| m.as_str()).len() + 0..];

        debug!("column_name: {}", column_name);
        debug!("column_type: {}", column_type);
        debug!("string_without_column_type: {}", string_without_column_type);

        let (column_value, rest) =
            ColumnValue::parse(string_without_column_type, column_type, false);
        let column_info = ColumnInfo::new(column_name, column_type);
        let column = match column_value {
            Some(ColumnValue::UnchangedToast) => Column::UnchangedToastColumn {
                column_info: column_info,
            },
            Some(ColumnValue::IncompleteText(string)) => Column::IncompleteColumn {
                column_info: column_info,
                value: ColumnValue::IncompleteText(string),
            },
            _ => Column::ChangedColumn {
                column_info: column_info,
                value: column_value,
            },
        };
        debug!("parse_column: returned: {:?}", column);
        (column, rest)
    }

    // TODO break this monster up a bit
    fn continue_parse(&mut self, string: &str) -> ParsedLine {
        let incomplete_change = self.parse_state.currently_parsing.take();
        self.parse_state.currently_parsing = None;
        match incomplete_change {
            Some(ParsedLine::ChangedData {
                kind,
                table_name,
                mut columns,
            }) => {
                let incomplete_column = columns.pop().unwrap();
                assert!(matches!(incomplete_column, Column::IncompleteColumn { .. }));
                match incomplete_column {
                    Column::IncompleteColumn { column_info: ColumnInfo{name, column_type}, value: incomplete_value } => {
                        let (continued_column_value, rest) = ColumnValue::parse(string, &column_type, true);
                        let value = match incomplete_value {
                            ColumnValue::IncompleteText(value) => value,
                            _ => panic!("Incomplete value is not ColumnValue::IncompleteText")
                        };

                        let updated_column = match continued_column_value {
                            Some(ColumnValue::Text(string)) => {
                                let column_value = ColumnValue::Text(value + "\n" + &string);
                                Column::ChangedColumn {column_info: ColumnInfo {name, column_type}, value: Some(column_value)}
                            },
                            // another newline, so we're still incomplete
                            Some(ColumnValue::IncompleteText(string)) => {
                                let column_value = ColumnValue::IncompleteText(value + "\n" + &string);
                                Column::IncompleteColumn {column_info: ColumnInfo {name, column_type}, value: column_value}
                            },
                            _ => panic!("Trying to continue to parse a value that's not of type text")
                        };

                        columns.push(updated_column);
                        // because there could be multiple newlines we need to check again
                        if self.column_is_incomplete(&columns) {
                            return self.handle_parse_changed_data(table_name.as_ref(), kind, columns)
                        } else {
                            let mut more_columns = self.parse_columns(rest);
                            // append modifies in place
                            columns.append(&mut more_columns);
                            self.handle_parse_changed_data(table_name.as_ref(), kind, columns)
                        }
                    },
                    _ => panic!("trying to parse an incomplete_column that's not a Column::IncompleteColumn {:?}", incomplete_column)
                }
            }
            _ => panic!(
                "Trying to continue parsing a {:?} rather than a ParsedLine::ChangedData",
                incomplete_change
            ),
        }
    }

    fn handle_parse_changed_data(
        &mut self,
        table_name: &str,
        kind: ChangeKind,
        columns: Vec<Column>,
    ) -> ParsedLine {
        let incomplete_parse = self.column_is_incomplete(&columns);
        let changed_data = ParsedLine::ChangedData {
            table_name: ArcIntern::new(table_name.to_owned()),
            kind: kind,
            columns: columns,
        };

        let result = if incomplete_parse {
            // we save the state to add to next line, and then return ContinueParse
            self.parse_state.currently_parsing = Some(changed_data);
            ParsedLine::ContinueParse
        } else {
            changed_data
        };
        debug!("returning change struct: {:?}", result);
        result
    }
}

fn slice_until_char(string: &str, character: char) -> Option<&str> {
    let found_index = string.find(character);
    found_index.and_then(|x| Some(&string[0..x]))
}

fn slice_until_char_or_end(string: &str, character: char) -> &str {
    let sliced = slice_until_char(string, character);
    match sliced {
        None => string,
        Some(sliced_str) => sliced_str,
    }
}

fn slice_until_colon_or_end(string: &str) -> &str {
    slice_until_char_or_end(string, ':')
}

// fucking escaping
// so this handles when the thing is escaped with a \'
fn is_escaped(string: &str, index: usize) -> bool {
    is_quote_escaped(string, index)
}

fn is_backwards_escaped_by_char(string: &str, index: usize, character: &str) -> bool {
    if index == 0 {
        return false;
    } else if string.get(index - 1..index).unwrap_or("") != character {
        return false;
    } else {
        !is_backwards_escaped_by_char(string, index - 1, character)
    }
}

// things can also be escaped with quotes ''
// since we search for a single quote, the escape is _forwards_
// ffs
// think of escaping things as a pair, since they're both quotes only the last one in a chain can be unescaped
fn is_quote_escaped(string: &str, index: usize) -> bool {
    // next character is a quote
    if index + 1 < string.len()
        && string.get(index..index + 1).unwrap_or("") == r"'"
        && string.get(index + 1..index + 2).unwrap_or("") == r"'"
    {
        return true;
    } else if index == 0 {
        return false;
    } else if index < string.len() {
        is_backwards_escaped_by_char(string, index, "'")
    } else {
        return false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn foobar() {
        let foo = ColumnValue::Text("foo".to_owned());
        assert_eq!(foo.to_string().as_str(), "foo");
    }

    #[test]
    fn quote_escaping_works() {
        env_logger::init();

        let string = "I''m sorry about this damn quote escaping";
        assert!(is_quote_escaped(&string, 1));
        assert!(!is_quote_escaped(&string, 3));
        assert!(!is_quote_escaped(&string, 0));
        assert!(!is_quote_escaped(&string, string.len()));
        assert!(is_quote_escaped(&string, 2));
    }

    #[test]
    fn parses_array_type() {
        let mut parser = Parser::new(true);
        let line = "table public.users: UPDATE: id[bigint]:123 foobar[text]:'foobar string' baz_array[character varying[]]:'{\"foo\", \"bar\", \"baz\"}'";
        let result = parser.parse(&line.to_string());
        assert_eq!(
            result,
            ParsedLine::ChangedData {
                columns: vec![
                    Column::ChangedColumn {
                        column_info: ColumnInfo::new("id".to_string(), "bigint".to_string()),
                        value: Some(ColumnValue::Integer(123))
                    },
                    Column::ChangedColumn {
                        column_info: ColumnInfo::new("foobar".to_string(), "text".to_string()),
                        value: Some(ColumnValue::Text("foobar string".to_string()))
                    },
                    Column::ChangedColumn {
                        column_info: ColumnInfo::new("baz_array".to_string(), "array".to_string()),
                        value: Some(ColumnValue::Text("{\"foo\", \"bar\", \"baz\"}".to_string()))
                    }
                ],
                table_name: ArcIntern::new("public.users".to_string()),
                kind: ChangeKind::Update
            }
        );
    }

    use std::{collections::HashMap, hash::Hash};
    // https://stackoverflow.com/questions/42748277/how-do-i-test-for-the-equality-of-two-unordered-lists
    fn equal_unordered_list<T>(a: &[T], b: &[T]) -> bool
    where
        T: Eq + Hash + std::fmt::Debug,
    {
        fn count<T>(items: &[T]) -> HashMap<&T, usize>
        where
            T: Eq + Hash + std::fmt::Debug,
        {
            let mut cnt = HashMap::new();
            for i in items {
                *cnt.entry(i).or_insert(0) += 1
            }
            cnt
        }

        //println!("a {:?}", a);

        count(a) == count(b)
    }

    #[test]
    fn parsing_works() {
        use std::fs::File;
        use std::io::{self, BufRead};

        let mut parser = Parser::new(true);
        let mut collector = Vec::new();
        let file =
            File::open("./test/parser.txt").expect("couldn't find file containing test data");
        let lines = io::BufReader::new(file).lines();
        for line in lines {
            if let Ok(ip) = line {
                let parsed_line = parser.parse(&ip);
                match parsed_line {
                    ParsedLine::ContinueParse => {}
                    _ => {
                        collector.push(parsed_line);
                    }
                }
            }
        }
        assert!(equal_unordered_list(&collector, &vec![
            ParsedLine::Begin(11989965),
            ParsedLine::ChangedData { columns: vec![
                Column::ChangedColumn { column_info: ColumnInfo::new("id".to_string(), "bigint".to_string()), value: Some(ColumnValue::Integer(376)) },
                Column::ChangedColumn { column_info: ColumnInfo::new("account_id".to_string(), "integer" .to_string()), value: Some(ColumnValue::Integer(1)) },
                Column::ChangedColumn { column_info: ColumnInfo::new("category".to_string(), "character varying".to_string()), value: None },
                Column::ChangedColumn { column_info: ColumnInfo::new("currency_code".to_string(), "character varying".to_string()), value: Some(ColumnValue::Text("USD".to_string())) },
                Column::ChangedColumn { column_info: ColumnInfo::new("amount".to_string(), "numeric".to_string()), value: Some(ColumnValue::Numeric("4.0".to_string())) },
                Column::ChangedColumn { column_info: ColumnInfo::new("description".to_string(), "character varying".to_string()), value: Some(ColumnValue::Text("Salary".to_string())) },
                Column::ChangedColumn { column_info: ColumnInfo::new("made_on".to_string(), "date".to_string()), value: Some(ColumnValue::Text("2020-09-17".to_string())) },
                Column::ChangedColumn { column_info: ColumnInfo::new("duplicated".to_string(), "boolean".to_string()), value: None },
                Column::ChangedColumn { column_info: ColumnInfo::new("mode".to_string(), "character varying".to_string()), value: None },
                Column::ChangedColumn { column_info: ColumnInfo::new("created_at".to_string(), "timestamp without time zone".to_string()), value: Some(ColumnValue::Text("2020-10-09 15:24:40.655714".to_string())) },
                Column::ChangedColumn { column_info: ColumnInfo::new("updated_at".to_string(), "timestamp without time zone".to_string()), value: Some(ColumnValue::Text("2020-11-27 15:31:21.771279".to_string())) },
                Column::ChangedColumn { column_info: ColumnInfo::new("status".to_string(), "character varying".to_string()), value: None },
                Column::ChangedColumn { column_info: ColumnInfo::new("corrected_made_on".to_string(), "date".to_string()), value: Some(ColumnValue::Text("2020-09-17".to_string())) },
                Column::ChangedColumn { column_info: ColumnInfo::new("categorized_by_user".to_string(), "boolean".to_string()), value: None },
                Column::ChangedColumn { column_info: ColumnInfo::new("uuid".to_string(), "uuid".to_string()), value: Some(ColumnValue::Text("a510bcf8-42f1-4ec2-bcbe-04e0e709e014".to_string())) },
                Column::ChangedColumn { column_info: ColumnInfo::new("marked_as_duplicate".to_string(), "boolean".to_string()), value: Some(ColumnValue::Boolean(false)) },
                Column::ChangedColumn { column_info: ColumnInfo::new("transaction_category_id".to_string(), "integer".to_string()), value: Some(ColumnValue::Integer(11)) },
                Column::ChangedColumn { column_info: ColumnInfo::new("bill_id".to_string(), "integer".to_string()), value: None },
                Column::ChangedColumn { column_info: ColumnInfo::new("last_enriched_at".to_string(), "timestamp without time zone".to_string()), value: Some(ColumnValue::Text("2020-10-09 15:24:55.371552".to_string())) },
                Column::ChangedColumn { column_info: ColumnInfo::new("user_id".to_string(), "integer".to_string()), value: Some(ColumnValue::Integer(1)) },
                Column::ChangedColumn { column_info: ColumnInfo::new("external_transaction_id".to_string(), "character varying".to_string()), value: None },
                Column::ChangedColumn { column_info: ColumnInfo::new("login_provider_additional_attributes".to_string(), "jsonb".to_string()), value: None },
                Column::ChangedColumn { column_info: ColumnInfo::new("extra".to_string(), "jsonb".to_string()), value: Some(ColumnValue::Text("{}".to_string())) },
                Column::ChangedColumn { column_info: ColumnInfo::new("recurring_income_id".to_string(), "uuid".to_string()), value: None }
                ],
                table_name: ArcIntern::new("public.transactions".to_string()),
                kind: ChangeKind::Update },
            ParsedLine::Commit(11989965),
            ParsedLine::Begin(4220773504),
            ParsedLine::ChangedData { columns: vec![
                Column::ChangedColumn { column_info: ColumnInfo::new("id".to_string(), "integer".to_string()), value: Some(ColumnValue::Integer(1111111)) },
                Column::ChangedColumn { column_info: ColumnInfo::new("first_name".to_string(), "character varying".to_string()), value: Some(ColumnValue::Text("joshy".to_string())) },
                Column::ChangedColumn { column_info: ColumnInfo::new("last_name".to_string(), "character varying".to_string()), value: Some(ColumnValue::Text("joshy".to_string())) },
                Column::ChangedColumn { column_info: ColumnInfo::new("email".to_string(), "character varying".to_string()), value: Some(ColumnValue::Text("joshy@live.com".to_string())) },
                Column::ChangedColumn { column_info: ColumnInfo::new("created_at".to_string(), "timestamp without time zone".to_string()), value: Some(ColumnValue::Text("2020-11-27 14:57:30.303466".to_string())) },
                Column::ChangedColumn { column_info: ColumnInfo::new("updated_at".to_string(), "timestamp without time zone".to_string()), value: Some(ColumnValue::Text("2020-11-27 15:35:28.542551".to_string())) },
                Column::ChangedColumn { column_info: ColumnInfo::new("saltedge_customer_id".to_string(), "character varying".to_string()), value: None },
                Column::ChangedColumn { column_info: ColumnInfo::new("admin".to_string(), "boolean".to_string()), value: Some(ColumnValue::Boolean(false)) },
                Column::ChangedColumn { column_info: ColumnInfo::new("uuid".to_string(), "uuid".to_string()), value: Some(ColumnValue::Text("ad46edc6-914e-485a-8445-b6a5451d113b".to_string())) },
                Column::ChangedColumn { column_info: ColumnInfo::new("password_hash".to_string(), "character varying".to_string()), value: Some(ColumnValue::Text("$2a$12$q2VDJ4MKnKXM7SiP4OIfseCTXFKDDfJQcuQv0yGQC31bWL/8ytBE.".to_string())) },
                Column::ChangedColumn { column_info: ColumnInfo::new("password_salt".to_string(), "character varying".to_string()), value: Some(ColumnValue::Text("$2a$1x$q2VDJ4MKnKXM7SiP4OIfse".to_string())) },
                Column::ChangedColumn { column_info: ColumnInfo::new("phone_number".to_string(), "character varying".to_string()), value: Some(ColumnValue::Text("6125478788".to_string())) },
                Column::ChangedColumn { column_info: ColumnInfo::new("password_reset_token".to_string(), "character varying".to_string()), value: None },
                Column::ChangedColumn { column_info: ColumnInfo::new("password_reset_sent_at".to_string(), "timestamp without time zone".to_string()), value: None },
                Column::ChangedColumn { column_info: ColumnInfo::new("introduction_text_sent".to_string(), "boolean".to_string()), value: None },
                Column::ChangedColumn { column_info: ColumnInfo::new("fb_cleo_uid".to_string(), "bigint".to_string()), value: None },
                Column::ChangedColumn { column_info: ColumnInfo::new("facebook_photo_url".to_string(), "character varying".to_string()), value: None },
                Column::ChangedColumn { column_info: ColumnInfo::new("fb_timezone".to_string(), "integer".to_string()), value: Some(ColumnValue::Integer(0)) },
                Column::ChangedColumn { column_info: ColumnInfo::new("state".to_string(), "public.hstore".to_string()), value: Some(ColumnValue::Text("\"latest_app_version\"=>\"1.60.0\", \"onboarding_bot_b_group\"=>\"true\", \"is_in_initial_onboarding_flow\"=>\"false\", \"latest_app_version_updated_at\"=>\"2020-11-27T14:59:03+00:00\", \"notification_settings_b_group\"=>\"true\", \"sent_dwolla_customer_created_verified_combo_email\"=>\"true\"".to_string())) },
                Column::ChangedColumn { column_info: ColumnInfo::new("messenger_blocked_date".to_string(), "timestamp without time zone".to_string()), value: None },
                Column::ChangedColumn { column_info: ColumnInfo::new("interactions_count".to_string(), "integer".to_string()), value: Some(ColumnValue::Integer(166)) },
                Column::ChangedColumn { column_info: ColumnInfo::new("last_interaction_at".to_string(), "timestamp without time zone".to_string()), value: Some(ColumnValue::Text("2020-11-27 15:35:28.542551".to_string())) },
                Column::ChangedColumn { column_info: ColumnInfo::new("broadcast_queues_count".to_string(), "integer".to_string()), value: Some(ColumnValue::Integer(0)) },
                Column::ChangedColumn { column_info: ColumnInfo::new("onboarding_state".to_string(), "integer".to_string()), value: Some(ColumnValue::Integer(6)) },
                Column::ChangedColumn { column_info: ColumnInfo::new("date_of_birth".to_string(), "date".to_string()), value: Some(ColumnValue::Text("1966-08-11".to_string())) },
                Column::ChangedColumn { column_info: ColumnInfo::new("nationality".to_string(), "character varying".to_string()), value: None },
                Column::ChangedColumn { column_info: ColumnInfo::new("address".to_string(), "jsonb".to_string()), value: Some(ColumnValue::Text("{\"city\": \"Minneapolis\", \"line_1\": \"929 Portland Ave\", \"postcode\": \"55414\", \"us_state\": \"MN\"}".to_string())) },
                Column::ChangedColumn { column_info: ColumnInfo::new("indexed_settings".to_string(), "jsonb".to_string()), value: None },
                Column::ChangedColumn { column_info: ColumnInfo::new("user_salary_date_estimate".to_string(), "date".to_string()), value: None },
                Column::ChangedColumn { column_info: ColumnInfo::new("referred_from".to_string(), "character varying".to_string()), value: Some(ColumnValue::Text("app".to_string())) },
                Column::ChangedColumn { column_info: ColumnInfo::new("accounts_count".to_string(), "integer".to_string()), value: Some(ColumnValue::Integer(3)) },
                Column::ChangedColumn { column_info: ColumnInfo::new("notification_threshold".to_string(), "integer".to_string()), value: Some(ColumnValue::Integer(1)) },
                Column::ChangedColumn { column_info: ColumnInfo::new("notification_frequency_setting".to_string(), "integer".to_string()), value: Some(ColumnValue::Integer(0)) },
                Column::ChangedColumn { column_info: ColumnInfo::new("gender".to_string(), "character varying".to_string()), value: None },
                Column::ChangedColumn { column_info: ColumnInfo::new("watch_category_id".to_string(), "integer".to_string()), value: None },
                Column::ChangedColumn { column_info: ColumnInfo::new("utm_params".to_string(), "jsonb".to_string()), value: Some(ColumnValue::Text("{}".to_string())) },
                Column::ChangedColumn { column_info: ColumnInfo::new("beta_tester".to_string(), "boolean".to_string()), value: Some(ColumnValue::Boolean(false)) },
                Column::ChangedColumn { column_info: ColumnInfo::new("signup_country_alpha_2".to_string(), "character varying".to_string()), value: Some(ColumnValue::Text("US".to_string())) },
                Column::ChangedColumn { column_info: ColumnInfo::new("fb_locale".to_string(), "character varying".to_string()), value: None },
                Column::ChangedColumn { column_info: ColumnInfo::new("time_zone".to_string(), "character varying".to_string()), value: Some(ColumnValue::Text("Central Time (US & Canada)".to_string())) },
                Column::ChangedColumn { column_info: ColumnInfo::new("invite_code".to_string(), "character varying".to_string()), value: Some(ColumnValue::Text("cleo-12345".to_string())) },
                Column::ChangedColumn { column_info: ColumnInfo::new("pending_deletion".to_string(), "boolean".to_string()), value: None },
                Column::ChangedColumn { column_info: ColumnInfo::new("last_transaction_corrected_made_on".to_string(), "date".to_string()), value: Some(ColumnValue::Text("2020-11-25".to_string())) },
                Column::ChangedColumn { column_info: ColumnInfo::new("profile_photo_file_name".to_string(), "character varying".to_string()), value: None },
                Column::ChangedColumn { column_info: ColumnInfo::new("profile_photo_content_type".to_string(), "character varying".to_string()), value: None },
                Column::ChangedColumn { column_info: ColumnInfo::new("profile_photo_file_size".to_string(), "integer".to_string()), value: None },
                Column::ChangedColumn { column_info: ColumnInfo::new("profile_photo_updated_at".to_string(), "timestamp without time zone".to_string()), value: None },
                Column::ChangedColumn { column_info: ColumnInfo::new("last_transaction_created_at".to_string(), "timestamp without time zone".to_string()), value: Some(ColumnValue::Text("2020-11-27 15:18:13.956393".to_string())) },
                Column::ChangedColumn { column_info: ColumnInfo::new("last_bot_response_at".to_string(), "timestamp without time zone".to_string()), value: Some(ColumnValue::Text("2020-11-27 15:28:27.51497".to_string())) },
                Column::ChangedColumn { column_info: ColumnInfo::new("silhouette_profile_picture".to_string(), "boolean".to_string()), value: None },
                Column::ChangedColumn { column_info: ColumnInfo::new("deleted".to_string(), "boolean".to_string()), value: Some(ColumnValue::Boolean(false)) },
                Column::ChangedColumn { column_info: ColumnInfo::new("deleted_at".to_string(), "timestamp without time zone".to_string()), value: None },
                Column::ChangedColumn { column_info: ColumnInfo::new("chosen_name".to_string(), "character varying".to_string()), value: None },
                Column::ChangedColumn { column_info: ColumnInfo::new("product_country".to_string(), "character varying".to_string()), value: Some(ColumnValue::Text("US".to_string())) },
                Column::ChangedColumn { column_info: ColumnInfo::new("last_bot_request_at".to_string(), "timestamp without time zone".to_string()), value: Some(ColumnValue::Text("2020-11-27 15:28:27.279173".to_string())) },
                Column::ChangedColumn { column_info: ColumnInfo::new("last_messenger_request_at".to_string(), "timestamp without time zone".to_string()), value: None }],
                table_name: ArcIntern::new("public.users".to_string()),
                kind: ChangeKind::Update },
            ParsedLine::Commit(4220773504),
            ParsedLine::Begin(4220773503),
            ParsedLine::ChangedData { columns: vec![
                Column::ChangedColumn { column_info: ColumnInfo::new("id".to_string(), "uuid".to_string()), value: Some(ColumnValue::Text("188101f7-1c30-44c9-88e5-1be3b024470e".to_string())) },
                Column::ChangedColumn { column_info: ColumnInfo::new("user_id".to_string(), "bigint".to_string()), value: Some(ColumnValue::Integer(1111111)) },
                Column::ChangedColumn { column_info: ColumnInfo::new("created_at".to_string(), "timestamp without time zone".to_string()), value: Some(ColumnValue::Text("2020-11-27 15:35:28.540886".to_string())) },
                Column::ChangedColumn { column_info: ColumnInfo::new("updated_at".to_string(), "timestamp without time zone".to_string()), value: Some(ColumnValue::Text("2020-11-27 15:35:28.540886".to_string())) },
                Column::ChangedColumn { column_info: ColumnInfo::new("closed_at".to_string(), "timestamp without time zone".to_string()), value: None }],
                table_name: ArcIntern::new("public.app_sessions".to_string()),
                kind: ChangeKind::Insert },
            ParsedLine::Commit(4220773503),
            ParsedLine::Begin(4220773509),
            ParsedLine::ChangedData { columns: vec![
                Column::ChangedColumn { column_info: ColumnInfo::new("id".to_string(), "bigint".to_string()), value: Some(ColumnValue::Integer(474344529)) },
                Column::ChangedColumn { column_info: ColumnInfo::new("state".to_string(), "integer".to_string()), value: Some(ColumnValue::Integer(0)) },
                Column::ChangedColumn { column_info: ColumnInfo::new("body".to_string(), "jsonb".to_string()), value: Some(ColumnValue::Text("{\"_id\": {\"$oid\": \"5bf5400ac96f865d7af4ce84\"}, \"info\": {\"balance\": {\"amount\": 14113.18, \"currency\": \"USD\"}, \"nickname\": \"Facilitator Fee \", \"bank_code\": \"EBT\", \"document_id\": null, \"name_on_account\": \" \", \"monthly_withdrawals_remaining\": null}, \"type\": \"DEPOSIT-US\", \"_rest\": {\"_id\": \"5bf5400ac96f865d7af4ce84\", \"info\": {\"balance\": {\"amount\": 14113.18, \"currency\": \"USD\"}, \"nickname\": \"Facilitator Fee \", \"bank_code\": \"EBT\", \"document_id\": null, \"name_on_account\": \" \"}, \"type\": \"DEPOSIT-US\", \"extra\": {\"note\": \"Np8W0ePvWl\", \"other\": {}, \"supp_id\": \"\"}, \"client\": {\"id\": \"5be9f21accc480002a5fc952\", \"name\": \"Cleo\"}, \"allowed\": \"CREDIT-AND-DEBIT\", \"user_id\": \"4bc70ef055930d3611c1ca41\", \"timeline\": [{\"date\": 1542799370499, \"note\": \"Node created.\"}], \"is_active\": true}, \"extra\": {\"note\": \"Dp8W0ePvVl\", \"other\": {}, \"supp_id\": \"\"}, \"action\": \"callback\", \"client\": {\"id\": \"5be9f21accc480002a5fc952\", \"name\": \"Cleo\"}, \"allowed\": \"CREDIT-AND-DEBIT\", \"user_id\": \"4bc70ef055930d3611c1ca41\", \"timeline\": [{\"date\": {\"$date\": 1542799370499}, \"note\": \"Node created.\"}], \"is_active\": true, \"controller\": \"webhooks/XXXXXX\", \"XXXXXX\": {\"_id\": {\"$oid\": \"5bf5400ac96f865d7af4ce84\"}, \"info\": {\"balance\": {\"amount\": 14113.18, \"currency\": \"USD\"}, \"nickname\": \"Facilitator Fee \", \"bank_code\": \"EBT\", \"document_id\": null, \"name_on_account\": \" \", \"monthly_withdrawals_remaining\": null}, \"type\": \"DEPOSIT-US\", \"_rest\": {\"_id\": \"5bf5400ac96f865d7af4ce84\", \"info\": {\"balance\": {\"amount\": 14113.18, \"currency\": \"USD\"}, \"nickname\": \"Facilitator Fee \", \"bank_code\": \"EBT\", \"document_id\": null, \"name_on_account\": \" \"}, \"type\": \"DEPOSIT-US\", \"extra\": {\"note\": \"Dp8W0ePvVl\", \"other\": {}, \"supp_id\": \"\"}, \"client\": {\"id\": \"5be9f21accc480002a5fc952\", \"name\": \"Cleo\"}, \"allowed\": \"CREDIT-AND-DEBIT\", \"user_id\": \"4bc70ef055930d3611c1ca41\", \"timeline\": [{\"date\": 1542799370499, \"note\": \"Node created.\"}], \"is_active\": true}, \"extra\": {\"note\": \"Dp8W0ePvVl\", \"other\": {}, \"supp_id\": \"\"}, \"client\": {\"id\": \"5be9f21accc480002a5fc952\", \"name\": \"Cleo\"}, \"allowed\": \"CREDIT-AND-DEBIT\", \"user_id\": \"4bc70ef055930d3611c1ca41\", \"timeline\": [{\"date\": {\"$date\": 1542799370499}, \"note\": \"Node created.\"}], \"is_active\": true, \"webhook_meta\": {\"date\": {\"$date\": 1606491327981}, \"log_id\": \"5fc11cc07a80b2506dd7c491\", \"function\": \"NODE|PATCH\", \"updated_by\": \"SELF\"}}, \"webhook_meta\": {\"date\": {\"$date\": 1606491327981}, \"log_id\": \"5fc11cc07a80b2506dd7c491\", \"function\": \"NODE|PATCH\", \"updated_by\": \"SELF\"}}".to_string())) },
                Column::ChangedColumn { column_info: ColumnInfo::new("controller".to_string(), "character varying".to_string()), value: Some(ColumnValue::Text("webhooks/XXXXXX".to_string())) },
                Column::ChangedColumn { column_info: ColumnInfo::new("action".to_string(), "character varying".to_string()), value: Some(ColumnValue::Text("callback".to_string())) },
                Column::ChangedColumn { column_info: ColumnInfo::new("worker".to_string(), "character varying".to_string()), value: Some(ColumnValue::Text("WebhookWorkers::XXXXXXWorker".to_string())) },
                Column::ChangedColumn { column_info: ColumnInfo::new("object_reference".to_string(), "character varying".to_string()), value: Some(ColumnValue::Text("5bf5400ac96f865d7af4ce84".to_string())) },
                Column::ChangedColumn { column_info: ColumnInfo::new("object_status".to_string(), "character varying".to_string()), value: Some(ColumnValue::Text("CREDIT-AND-DEBIT".to_string())) },
                Column::ChangedColumn { column_info: ColumnInfo::new("processing_started_at".to_string(), "timestamp without time zone".to_string()), value: None },
                Column::ChangedColumn { column_info: ColumnInfo::new("processing_completed_at".to_string(), "timestamp without time zone".to_string()), value: None },
                Column::ChangedColumn { column_info: ColumnInfo::new("created_at".to_string(), "timestamp without time zone".to_string()), value: Some(ColumnValue::Text("2020-11-27 15:35:28.553047".to_string())) },
                Column::ChangedColumn { column_info: ColumnInfo::new("updated_at".to_string(), "timestamp without time zone".to_string()), value: Some(ColumnValue::Text("2020-11-27 15:35:28.553047".to_string())) },
                Column::ChangedColumn { column_info: ColumnInfo::new("processing_failed_at".to_string(), "timestamp without time zone".to_string()), value: None },
                Column::ChangedColumn { column_info: ColumnInfo::new("exception_message".to_string(), "character varying".to_string()), value: None }],
                table_name: ArcIntern::new("public.webhooks_incoming_webhooks".to_string()),
                kind: ChangeKind::Insert },
            ParsedLine::Commit(4220773509),
            ParsedLine::Begin(4220773508),
            ParsedLine::ChangedData { columns: vec![
                Column::ChangedColumn { column_info: ColumnInfo::new("id".to_string(), "integer".to_string()), value: Some(ColumnValue::Integer(508629076)) },
                Column::ChangedColumn { column_info: ColumnInfo::new("category".to_string(), "character varying".to_string()), value: Some(ColumnValue::Text("intercom".to_string())) },
                Column::ChangedColumn { column_info: ColumnInfo::new("user_id".to_string(), "integer".to_string()), value: Some(ColumnValue::Integer(2569262)) },
                Column::ChangedColumn { column_info: ColumnInfo::new("created_at".to_string(), "timestamp without time zone".to_string()), value: Some(ColumnValue::Text("2020-11-27 15:35:28.55155".to_string())) },
                Column::ChangedColumn { column_info: ColumnInfo::new("updated_at".to_string(), "timestamp without time zone".to_string()), value: Some(ColumnValue::Text("2020-11-27 15:35:28.55155".to_string())) },
                Column::ChangedColumn { column_info: ColumnInfo::new("extra".to_string(), "jsonb".to_string()), value: None },
                Column::ChangedColumn { column_info: ColumnInfo::new("visitor_id".to_string(), "uuid".to_string()), value: None }],
                table_name: ArcIntern::new("public.interactions".to_string()),
                kind: ChangeKind::Insert },
            ParsedLine::Commit(4220773508),
            ParsedLine::Begin(4220773511),
            ParsedLine::ChangedData { columns: vec![
                Column::ChangedColumn { column_info: ColumnInfo::new("id".to_string(), "uuid".to_string()), value: Some(ColumnValue::Text("5fe0cb5c-d92b-46ef-84bf-c02018ff19ca".to_string())) },
                Column::ChangedColumn { column_info: ColumnInfo::new("user_id".to_string(), "bigint".to_string()), value: Some(ColumnValue::Integer(3871635)) },
                Column::ChangedColumn { column_info: ColumnInfo::new("flow_root_id".to_string(), "bigint".to_string()), value: Some(ColumnValue::Integer(12741)) },
                Column::ChangedColumn { column_info: ColumnInfo::new("channel".to_string(), "character varying".to_string()), value: Some(ColumnValue::Text("app_notifications_enabled".to_string())) },
                Column::ChangedColumn { column_info: ColumnInfo::new("did_not_send_reason".to_string(), "character varying".to_string()), value: None },
                Column::ChangedColumn { column_info: ColumnInfo::new("kicked_off_at".to_string(), "timestamp without time zone".to_string()), value: Some(ColumnValue::Text("2020-11-27 14:10:44".to_string())) },
                Column::ChangedColumn { column_info: ColumnInfo::new("notification_sent_at".to_string(), "timestamp without time zone".to_string()), value: Some(ColumnValue::Text("2020-11-27 15:35:28.550426".to_string())) },
                Column::ChangedColumn { column_info: ColumnInfo::new("notification_date".to_string(), "date".to_string()), value: Some(ColumnValue::Text("2020-11-27".to_string())) },
                Column::ChangedColumn { column_info: ColumnInfo::new("active_user".to_string(), "boolean".to_string()), value: Some(ColumnValue::Boolean(true)) },
                Column::ChangedColumn { column_info: ColumnInfo::new("disconnected_user".to_string(), "boolean".to_string()), value: Some(ColumnValue::Boolean(false)) },
                Column::ChangedColumn { column_info: ColumnInfo::new("created_at".to_string(), "timestamp without time zone".to_string()), value: Some(ColumnValue::Text("2020-11-27 15:33:03.202097".to_string())) },
                Column::ChangedColumn { column_info: ColumnInfo::new("updated_at".to_string(), "timestamp without time zone".to_string()), value: Some(ColumnValue::Text("2020-11-27 15:35:28.55719".to_string())) }],
                table_name: ArcIntern::new("public.notification_sending_logs".to_string()),
                kind: ChangeKind::Update },
            ParsedLine::Commit(4220773511),
            ]));
    }
}
