#[allow(unused_imports)]
use log::{debug, error, log_enabled, info, Level};
use std::fmt;

// define more config later
struct ParserConfig { include_xids: bool }
struct ParserState {
    currently_parsing: Option<ParsedLine>
}

// define config later
pub struct Parser { config: ParserConfig, parse_state: ParserState }

#[derive(Debug)]
pub enum ColumnValue {
    Boolean(bool),
    Integer(i64),
    Numeric(String),
    Text(String),
    IncompleteText(String),
    UnchangedToast
}

impl fmt::Display for ColumnValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ColumnValue::UnchangedToast => { write!(f, "unchanged-toast-datum") },
            ColumnValue::Boolean(x) => { write!(f,"{}", x)},
            ColumnValue::Integer(x) => { write!(f,"{}", x)},
            ColumnValue::Numeric(x) => { write!(f,"{}", x)},
            ColumnValue::Text(x) => { write!(f,"{}", x)},
            ColumnValue::IncompleteText(x) => { write!(f,"{}", x)},
        }
    }
}

#[derive(Debug)]
pub enum Column {
    UnchangedToastColumn { key: String, column_type: String },
    ChangedColumn { key: String, column_type: String, value: Option<ColumnValue> },
    IncompleteColumn { key: String, column_type: String, value: ColumnValue }
}

impl Column {
    pub fn column_name(&self) -> &str {
        let string = match self {
            Column::UnchangedToastColumn{ key,.. } => key,
            Column::ChangedColumn{ key,.. } => key,
            Column::IncompleteColumn{ key,.. } => key,
        };
        &string
    }
    // for when you _know_ a column has a value, used for id columns
    pub fn column_value_unwrap(&self) -> &ColumnValue {
        match self {
            Column::ChangedColumn{value, ..} => {
                match value {
                    Some(inner) => inner,
                    None => panic!("panic!")
                }
            },
            Column::IncompleteColumn{value, ..} => {
                &value
            },
            Column::UnchangedToastColumn{..} => { panic!("tried to get value for unchanged_toast_column") }
        }
    }

    pub fn column_value_for_changed_column(&self) -> Option<&ColumnValue> {
        match self {
            Column::ChangedColumn{value,..} => {
                value.as_ref()
            },
            _ => { panic!("column_value_for_changed_column called on non-changed-column {:?}", self)}
        }
    }
    pub fn is_changed_data_column(&self) -> bool {
        match self {
            Column::ChangedColumn{..} => true,
            _ => false
        }
    }
    pub fn is_id_column(&self) -> bool {
        self.column_name() == "id"
    }
}

#[derive(Clone,Copy,Debug,PartialEq)]
pub enum ChangeKind {
    Insert,
    Update,
    Delete
}

#[derive(Debug)]
pub enum ParsedLine {
    // int is xid
    Begin(i64),
    // int is xid
    Commit(i64),
    ChangedData { columns: Vec<Column>, table_name: String, kind: ChangeKind },
    TruncateTable, // TODO
    ContinueParse // this is to signify that we're halfway through parsing a change
}

impl ParsedLine {
    pub fn find_id_column(&self) -> &Column {
        match self {
            ParsedLine::ChangedData { columns,.. } => {
                // unwrap because this is the id column which _must_ be here
                columns.iter().find(|&x| x.is_id_column()).unwrap()
            },
            _ => panic!("tried to find id column of non changed_data")
        }
    }
    // panic if called on something that's not ChangedData
    pub fn columns_for_changed_data(&self) -> &Vec<Column> {
        match self {
            ParsedLine::ChangedData {columns, ..} => {
                columns
            },
            _ => panic!("changed columns for changed data called on non-changed data")
        }
    }
}

impl ColumnValue {
    fn parse<'a>(string: &'a str, column_type: &str, continue_parse: bool) -> (Option<ColumnValue>, &'a str) {
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
                _ => panic!("Unknown column type: {:?}", column_type)
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
            return (ColumnValue::UnchangedToast, rest)
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
                        break
                    }
                    // move past and keep looking
                    total_index += 1;
                },
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
            Some((start, rest)) => {
                (start, rest)
            }
            None => {
                string.split_at(string.len())
            }
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
            _ => panic!("Unknown boolvalue {:?}", start)

        };
        (ColumnValue::Boolean(bool_value), rest)
    }
}

impl Parser {
    pub fn new(include_xids: bool) -> Parser {
        Parser {
            config: ParserConfig { include_xids },
            parse_state: ParserState { currently_parsing: None }
        }
    }

    pub fn parse(&mut self, string: &String) -> ParsedLine {
        match string {
            x if { x.starts_with("BEGIN")} => self.parse_begin(x),
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
        assert_eq!(&string_without_tag[table_name.len()..table_name.len() + 2], ": ");
        let string_without_table = &string_without_tag[table_name.len() + 2..string_without_tag.len()];
        let kind_string = slice_until_colon_or_end(string_without_table);

        // TODO: split early here for truncate columns

        let kind = self.parse_kind(kind_string);

        debug!("table: {:?} change: {:?}", table_name, kind_string);

        // + 2 for colon + space
        assert_eq!(&string_without_table[kind_string.len()..kind_string.len() + 2], ": ");
        let string_without_kind = &string_without_table[kind_string.len() + 2..string_without_table.len()];

        let columns = self.parse_columns(string_without_kind);
        self.handle_parse_changed_data(table_name.to_owned(), kind, columns)
    }

    fn column_is_incomplete(&self, columns: &Vec<Column>) -> bool {
        if let Some(Column::IncompleteColumn{..}) = columns.last() {
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
            _ => panic!("Unknown change kind: {}", string)
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
        let column_name = slice_until_char(string, '[').unwrap();
        // + 1 for '['
        assert_eq!(&string[column_name.len()..column_name.len() + 1], "[");
        let string_without_column_name = &string[column_name.len() + 1..];

        debug!("column_name: {}", column_name);

        let column_type = slice_until_char(string_without_column_name, ']').unwrap();

        debug!("column_type: {}", column_type);

        // + 2 for ']:'
        assert_eq!(&string_without_column_name[column_type.len()..column_type.len() + 2], "]:");
        let string_without_column_type = &string_without_column_name[column_type.len() + 2..];

        let (column_value, rest) = ColumnValue::parse(string_without_column_type, column_type, false);
        let column = match column_value {
            Some(ColumnValue::UnchangedToast) => Column::UnchangedToastColumn { key: column_name.to_owned(), column_type: column_type.to_owned()},
            Some(ColumnValue::IncompleteText(string)) => Column::IncompleteColumn { key: column_name.to_owned(), column_type: column_type.to_owned(), value: ColumnValue::IncompleteText(string) },
            _ => Column::ChangedColumn { key: column_name.to_owned(), column_type: column_type.to_owned(), value: column_value }
        };
        debug!("parse_column: returned: {:?}", column);
        (column, rest)
    }

    // TODO break this monster up a bit
    fn continue_parse(&mut self, string: &str) -> ParsedLine {
        let incomplete_change = self.parse_state.currently_parsing.take();
        self.parse_state.currently_parsing = None;
        match incomplete_change {
            Some(ParsedLine::ChangedData {kind, table_name, mut columns}) => {

                let incomplete_column = columns.pop().unwrap();
                assert!(matches!(incomplete_column, Column::IncompleteColumn {..}));
                match incomplete_column {
                    Column::IncompleteColumn { key, column_type, value: incomplete_value } => {
                        let (continued_column_value, rest) = ColumnValue::parse(string, &column_type, true);
                        let value = match incomplete_value {
                            ColumnValue::IncompleteText(value) => value,
                            _ => panic!("Incomplete value is not ColumnValue::IncompleteText")
                        };

                        let updated_column = match continued_column_value {
                            Some(ColumnValue::Text(string)) => {
                                let column_value = ColumnValue::Text(value + "\n" + &string);
                                Column::ChangedColumn {key: key, column_type: column_type, value: Some(column_value)}
                            },
                            // another newline, so we're still incomplete
                            Some(ColumnValue::IncompleteText(string)) => {
                                let column_value = ColumnValue::IncompleteText(value + "\n" + &string);
                                Column::IncompleteColumn {key: key, column_type: column_type, value: column_value}
                            },
                            _ => panic!("Trying to continue to parse a value that's not of type text")
                        };

                        columns.push(updated_column);
                        // because there could be multiple newlines we need to check again
                        if self.column_is_incomplete(&columns) {
                            return self.handle_parse_changed_data(table_name, kind, columns)
                        } else {
                            let mut more_columns = self.parse_columns(rest);
                            // append modifies in place
                            columns.append(&mut more_columns);
                            self.handle_parse_changed_data(table_name, kind, columns)
                        }
                    },
                    _ => panic!("trying to parse an incomplete_column that's not a Column::IncompleteColumn {:?}", incomplete_column)
                }
            },
            _ => panic!("Trying to continue parsing a {:?} rather than a ParsedLine::ChangedData", incomplete_change)
        }
    }

    fn handle_parse_changed_data(&mut self, table_name: String, kind: ChangeKind, columns: Vec<Column>) -> ParsedLine {
        let incomplete_parse = self.column_is_incomplete(&columns);
        let changed_data = ParsedLine::ChangedData {
            table_name: table_name,
            kind: kind,
            columns: columns
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
    found_index.and_then(|x| Some(&string[0..x]) )
}

fn slice_until_char_or_end(string: &str, character: char) -> &str {
    let sliced = slice_until_char(string, character);
    match sliced {
        None => string,
        Some(sliced_str) => sliced_str
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
        return false
    }
    else if string.get(index - 1..index).unwrap_or("") != character {
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
    if index + 1 < string.len() &&
        string.get(index..index+1).unwrap_or("") == r"'" &&
        string.get(index+1..index+2).unwrap_or("") == r"'"
    {
        return true
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
}
