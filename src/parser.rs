use bigdecimal::BigDecimal;
use internment::ArcIntern;
use lazy_static::lazy_static;
use num_bigint::BigInt;
use num_bigint::Sign;
use regex::Regex;
use std::collections::HashSet;
use std::fmt;

use std::env;

use bigdecimal::Signed;

use crate::database_writer::{DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE};

use std::str::FromStr;

#[allow(unused_imports)]
use crate::{function, logger_debug, logger_error, logger_info, logger_panic};

pub type TableName = ArcIntern<String>;
pub type ColumnName = ArcIntern<String>;
pub type ColumnType = ArcIntern<String>;

lazy_static! {
    // leave these as unwrap
    // this regex matches things like `"offset"[integer]:1` giving ("offset", "integer") capture groups
    // and `id[uuid]:"i-am-a-uuid"`, giving ("id", "uuid") capture groups
    // note the first capture group is surrounded by optional quotes to handle the first case above,
    // and the + inside it is made non-greedy to not eat the subsequent optional quote.
    // this is fine as we have a literal `[` to terminate the
    // repetition so it can't end too soon
    static ref PARSE_COLUMN_REGEX: regex::Regex = Regex::new(r#"^"?([^\[^\]]+?)"?\[([^:]+)\]:"#).unwrap();
    // for array types we  actually looks like `my_array[array[string]]:"[\"foobar\"]"`
    // this means the type regex match will be `array[string]`
    // we use this regex to strip off the `[string]` part as all arrays get mapped to a text type in redshift
    static ref COLUMN_TYPE_REGEX: regex::Regex = Regex::new(r"^.+\[\]$").unwrap();
    static ref TABLE_BLACKLIST: Vec<String> = env::var("TABLE_BLACKLIST").unwrap_or("".to_owned()).split(",").map(|x| x.to_owned()).collect();
    static ref TARGET_SCHEMA_NAME: Option<String> = std::env::var("TARGET_SCHEMA_NAME").ok();

    // 99_999_999_999.99999999
    static ref MAX_NUMERIC_VALUE: String = "9".repeat(
        (DEFAULT_NUMERIC_PRECISION - DEFAULT_NUMERIC_SCALE)
            as usize,
    ) + "."
        + "9".repeat(DEFAULT_NUMERIC_SCALE as usize).as_str();
    // https://docs.aws.amazon.com/redshift/latest/dg/r_Numeric_types201.html#r_Numeric_types201-decimal-or-numeric-type
    static ref REDSHIFT_19_PRECISION_MAX_PRECISION_VALUE: BigInt = BigInt::from(9223372036854775807i64);
}

// for tablename
pub trait SchemaAndTable {
    fn schema_and_table_name(&self) -> (&str, &str);
}

// schema.table_name
// we assume a valid table name, so unwrap
impl SchemaAndTable for TableName {
    fn schema_and_table_name(&self) -> (&str, &str) {
        let result = self
            .split_once('.')
            .expect("can't split schema and table name. No `.` character");
        match &*TARGET_SCHEMA_NAME {
            None => result,
            Some(schema_name) => (schema_name, result.1),
        }
    }
}

// define more config later
struct ParserConfig {
    include_xids: bool,
}
struct ParserState {
    currently_parsing: Option<ParsedLine>,
    wal_file_number: Option<u64>,
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
    RoundingNumeric(String),
    Text(String),
    IncompleteText(String),
    UnchangedToast,
}

#[derive(Debug, Eq, PartialEq)]
pub enum ColumnTypeEnum {
    Boolean,
    Integer,
    Numeric,
    RoundingNumeric,
    Text,
    Timestamp,
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

            ColumnValue::RoundingNumeric(x) => {
                let big_decimal: BigDecimal = BigDecimal::from_str(&x.to_string())
                    .expect(&format!("BigDecimal unable to be parsed: {}", x));
                // here we round to our precision and scale
                let rounded_bigdecimal = if big_decimal.round(0).digits() as i32
                    > DEFAULT_NUMERIC_PRECISION - DEFAULT_NUMERIC_SCALE
                {
                    if big_decimal.sign() == Sign::Minus {
                        -BigDecimal::from_str(&MAX_NUMERIC_VALUE)
                            .expect("MAX_NUMERIC_VALUE bigdecimal unable to be parsed.")
                    } else {
                        BigDecimal::from_str(&MAX_NUMERIC_VALUE)
                            .expect("MAX_NUMERIC_VALUE bigdecimal unable to be parsed.")
                    }
                } else {
                    // we need to round our internal stuff
                    big_decimal
                        .with_prec(DEFAULT_NUMERIC_PRECISION as u64) // precision doesn't round
                        .with_scale(DEFAULT_NUMERIC_SCALE as i64)
                };

                // redshift is completely stupid, and stores precision 19 bigdecimals with a 64 bit int for the precision value
                // https://docs.aws.amazon.com/redshift/latest/dg/r_Numeric_types201.html
                // so we need to sort that out.
                let (bigint_precision, _) = rounded_bigdecimal.as_bigint_and_exponent();
                let string = if DEFAULT_NUMERIC_PRECISION == 19
                    && bigint_precision.abs() > *REDSHIFT_19_PRECISION_MAX_PRECISION_VALUE
                {
                    if bigint_precision.sign() == Sign::Minus {
                        BigDecimal::new(
                            -REDSHIFT_19_PRECISION_MAX_PRECISION_VALUE.clone(),
                            DEFAULT_NUMERIC_SCALE as i64,
                        )
                        .to_string()
                    } else {
                        BigDecimal::new(
                            REDSHIFT_19_PRECISION_MAX_PRECISION_VALUE.clone(),
                            DEFAULT_NUMERIC_SCALE as i64,
                        )
                        .to_string()
                    }
                } else {
                    rounded_bigdecimal.to_string()
                };

                write!(f, "{}", string)
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

impl fmt::Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}:{}",
            self.column_info().column_name(),
            self.column_info().column_type()
        )
    }
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

    pub fn column_type_enum(&self) -> ColumnTypeEnum {
        ColumnValue::column_type_for_str(self.column_type())
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
    pub fn is_unchanged_toast_column(&self) -> bool {
        match self {
            Column::UnchangedToastColumn { .. } => true,
            _ => false,
        }
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
    PgRcvlogicalMsg(String),
}

impl ParsedLine {
    pub fn find_id_column(&self) -> &Column {
        match self {
            ParsedLine::ChangedData {
                columns,
                table_name,
                ..
            } => {
                // unwrap because this is the id column which _must_ be here
                columns
                    .iter()
                    .find(|&x| x.is_id_column())
                    .expect(&format!("We have no id column for {}", table_name.as_ref()))
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
            let (column_value, rest_of_string): (ColumnValue, &str) =
                match Self::column_type_for_str(column_type) {
                    ColumnTypeEnum::Integer => ColumnValue::parse_integer(string),
                    ColumnTypeEnum::Boolean => ColumnValue::parse_boolean(string),
                    ColumnTypeEnum::Numeric => ColumnValue::parse_numeric(string),
                    ColumnTypeEnum::RoundingNumeric => ColumnValue::parse_rounding_numeric(string),
                    ColumnTypeEnum::Text => ColumnValue::parse_text(string, continue_parse),
                    ColumnTypeEnum::Timestamp => ColumnValue::parse_text(string, continue_parse),
                };
            (Some(column_value), rest_of_string)
        }
    }

    pub fn column_type_for_str(column_type_str: &str) -> ColumnTypeEnum {
        match column_type_str {
            "bigint" => ColumnTypeEnum::Integer,
            "smallint" => ColumnTypeEnum::Integer,
            "integer" => ColumnTypeEnum::Integer,
            "numeric" => ColumnTypeEnum::RoundingNumeric,
            "decimal" => ColumnTypeEnum::RoundingNumeric,
            "double precision" => ColumnTypeEnum::Numeric,
            "boolean" => ColumnTypeEnum::Boolean,
            "character varying" => ColumnTypeEnum::Text,
            "public.citext" => ColumnTypeEnum::Text, // extensions come through as public.
            "text" => ColumnTypeEnum::Text,
            "timestamp without time zone" => ColumnTypeEnum::Timestamp,
            "date" => ColumnTypeEnum::Timestamp,
            "uuid" => ColumnTypeEnum::Text,
            "jsonb" => ColumnTypeEnum::Text,
            "json" => ColumnTypeEnum::Text,
            "public.hstore" => ColumnTypeEnum::Text,
            "interval" => ColumnTypeEnum::Text,
            "array" => ColumnTypeEnum::Text,
            _ => panic!("Unknown column type: {:?}", column_type_str),
        }
    }
    fn parse_integer<'a>(string: &'a str) -> (ColumnValue, &'a str) {
        let (start, rest) = ColumnValue::split_until_char_or_end(string, ' ');
        let integer: i64 = start
            .parse()
            .expect("Unable to parse integer from integer type");
        (ColumnValue::Integer(integer), rest)
    }
    fn parse_text<'a>(string: &'a str, continue_parse: bool) -> (ColumnValue, &'a str) {
        if string.starts_with("unchanged-toast-datum") {
            let (start, rest) = ColumnValue::split_until_char_or_end(string, ' ');
            assert_eq!(
                start, "unchanged-toast-datum",
                "expected 'unchanged-toast-datum' at start of string, got: `{}` when parsing `{}`",
                start, string
            );
            return (ColumnValue::UnchangedToast, rest);
        }
        if !continue_parse {
            assert_eq!(
                &string[0..1],
                "'",
                "expected ', got `{}` when parsing `{}`",
                &string[0..1],
                string
            );
        }
        let mut total_index: usize = 0;
        let mut complete_text: bool = false;
        let without_first_quote = if continue_parse {
            &string
        } else {
            &string[1..]
        };

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
            assert_eq!(
                &end[0..1],
                "'",
                "expected ', got `{}` when parsing `{}`",
                &end[0..1],
                string
            );
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

    fn parse_rounding_numeric<'a>(string: &'a str) -> (ColumnValue, &'a str) {
        let (start, rest) = ColumnValue::split_until_char_or_end(string, ' ');
        (ColumnValue::RoundingNumeric(start.to_owned()), rest)
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
                wal_file_number: None,
            },
        }
    }

    pub fn parse(&mut self, string: &String) -> ParsedLine {
        match string {
            x if { x.starts_with("BEGIN") } => self.parse_begin(x),
            x if { x.starts_with("COMMIT") } => self.parse_commit(x),
            x if { x.starts_with("table") } => self.parse_change(x),
            x if { self.parse_state.currently_parsing.is_some() } => self.continue_parse(x),
            x if { x.starts_with("pg_recvlogical") } => self.parse_pg_rcvlogical_msg(x),
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
            let xid: i64 = rest_of_string
                .parse()
                .expect("Unable to parse BEGIN xid as i64");
            logger_debug!(
                self.parse_state.wal_file_number,
                None,
                &format!("xid:{}", xid)
            );
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
            let xid: i64 = rest_of_string
                .parse()
                .expect("Unable to parse COMMIT xid as i64");
            logger_debug!(
                self.parse_state.wal_file_number,
                None,
                &format!("xid:{}", xid)
            );
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
        let table_name = TableName::new(slice_until_colon_or_end(string_without_tag).into());
        // + 2 for colon + space
        assert_eq!(
            &string_without_tag[table_name.len()..table_name.len() + 2],
            ": ",
            "expected `: `, got `{}` when parsing `{}`",
            &string_without_tag[table_name.len()..table_name.len() + 2],
            string
        );
        let string_without_table =
            &string_without_tag[table_name.len() + 2..string_without_tag.len()];
        let kind_string = slice_until_colon_or_end(string_without_table);

        // TODO: split early here for truncate columns

        let kind = self.parse_kind(kind_string);

        // + 2 for colon + space
        assert_eq!(
            &string_without_table[kind_string.len()..kind_string.len() + 2],
            ": ",
            "expected `: `, got `{}` when parsing `{}`",
            &string_without_table[kind_string.len()..kind_string.len() + 2],
            string
        );
        let string_without_kind =
            &string_without_table[kind_string.len() + 2..string_without_table.len()];

        let columns = self.parse_columns(string_without_kind, table_name.clone());
        self.handle_parse_changed_data(table_name, kind, columns)
    }

    fn parse_pg_rcvlogical_msg(&self, string: &str) -> ParsedLine {
        // "pg_recvlogical: could not send replication command..."
        const SIZE_OF_TAG: usize = "pg_recvlogical: ".len();
        let rest_of_string = &string[SIZE_OF_TAG..string.len()];
        logger_info!(
            self.parse_state.wal_file_number,
            None,
            &format!("parsed_pg_rcvlogical_message:{}", rest_of_string)
        );
        ParsedLine::PgRcvlogicalMsg(rest_of_string.to_string())
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

    fn parse_columns(&self, string: &str, table_name: TableName) -> Vec<Column> {
        let mut column_vector = Vec::new();
        let mut remaining_string = string;
        while remaining_string.len() > 0 {
            let (column, rest_of_string) = self.parse_column(remaining_string, table_name.clone());
            remaining_string = rest_of_string;
            column_vector.push(column);
        }
        column_vector
    }

    fn parse_column<'a>(&self, string: &'a str, _table_name: TableName) -> (Column, &'a str) {
        let re = &PARSE_COLUMN_REGEX;
        let captures = re
            .captures(string)
            .expect("Unable to match PARSE_COLUMN_REGEX to line");

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

        // logger_debug!(
        //     self.parse_state.wal_file_number,
        //     Some(&table_name),
        //     &format!("column_name:{} column_type:{}", column_name, column_type)
        // );

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
        // logger_debug!(
        //     self.parse_state.wal_file_number,
        //     Some(&table_name),
        //     &format!("column_parsed:{:?}", column)
        // );
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
                let incomplete_column = columns
                    .pop()
                    .expect("error: continue parse called without any columns? wtf?");
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
                            return self.handle_parse_changed_data(table_name, kind, columns)
                        } else {
                            let mut more_columns = self.parse_columns(rest, table_name.clone());
                            // append modifies in place
                            columns.append(&mut more_columns);
                            self.handle_parse_changed_data(table_name, kind, columns)
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
        table_name: TableName,
        kind: ChangeKind,
        columns: Vec<Column>,
    ) -> ParsedLine {
        let incomplete_parse = self.column_is_incomplete(&columns);
        let changed_data = ParsedLine::ChangedData {
            table_name: table_name.clone(),
            kind: kind,
            columns: columns,
        };

        let result = if incomplete_parse {
            // we save the state to add to next line, and then return ContinueParse
            self.parse_state.currently_parsing = Some(changed_data);
            ParsedLine::ContinueParse
        } else {
            if TABLE_BLACKLIST.contains(table_name.as_ref()) {
                logger_info!(
                    self.parse_state.wal_file_number,
                    Some(&table_name),
                    &format!(
                        "table_skipped_due_to_blacklist:{} blacklist:{:?}",
                        table_name, *TABLE_BLACKLIST
                    )
                );
                // we don't save any parse state, since we're skipping this changed_data and dropping it on the ground
                // we still want this to be after the continue parse line so we can
                // handle newlines in our blacklisted tables
                ParsedLine::ContinueParse
            } else {
                changed_data
            }
        };
        logger_debug!(
            self.parse_state.wal_file_number,
            Some(&table_name),
            &format!("returning_change_struct:{:?}", result)
        );
        result
    }

    pub fn register_wal_number(&mut self, wal_file_number: u64) {
        self.parse_state.wal_file_number = Some(wal_file_number);
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

    #[ctor::ctor]
    fn setup_tests() {
        std::env::set_var("TABLE_BLACKLIST", "public.schema_migrations");
    }

    #[test]
    fn table_blacklist_works_as_expected() {
        let mut parser = Parser::new(true);
        let line =
            "table public.schema_migrations: INSERT: version[character varying]:'20210112112814'";
        let parsed_line = parser.parse(&line.to_owned());
        assert_eq!(parsed_line, ParsedLine::ContinueParse);
    }

    #[test]
    fn column_value_text_can_work_as_expected() {
        let foo = ColumnValue::Text("foo".to_owned());
        assert_eq!(foo.to_string().as_str(), "foo");
    }

    #[test]
    fn quote_escaping_works() {
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

    #[test]
    fn parse_numeric_type_as_rounded() {
        let mut parser = Parser::new(true);
        let line = "table public.users: UPDATE: id[bigint]:123 foobar[numeric]:'1.11' baz[double precision]:'3.141'";
        let mut result = parser.parse(&line.to_string());
        assert!(matches!(result, ParsedLine::ChangedData{..}));
        println!("{:?}", result);
        if let ParsedLine::ChangedData {
            ref mut columns, ..
        } = result
        {
            let last = columns.pop();
            assert_eq!(
                last.unwrap().column_info().column_type_enum(),
                ColumnTypeEnum::Numeric
            );
            let second = columns.pop();
            assert_eq!(
                second.unwrap().column_info().column_type_enum(),
                ColumnTypeEnum::RoundingNumeric
            );
        }
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

        count(a) == count(b)
    }

    #[test]
    fn rounding_numeric_works() {
        let big_number = ColumnValue::RoundingNumeric("-10000000000000000000000000".to_string());
        assert_eq!("-92233720368.54775807", big_number.to_string());
        let big_number = ColumnValue::RoundingNumeric("10000000000000000000000000".to_string());
        assert_eq!("92233720368.54775807", big_number.to_string());
        let big_number = ColumnValue::RoundingNumeric("99999999999.99999999".to_string());
        assert_eq!("92233720368.54775807", big_number.to_string());
        let big_number = ColumnValue::RoundingNumeric("91999999999.99999999".to_string());
        assert_eq!("91999999999.99999999", big_number.to_string());
        let big_number = ColumnValue::RoundingNumeric("99999999999.99".to_string());
        assert_eq!("92233720368.54775807", big_number.to_string());
        let big_number = ColumnValue::RoundingNumeric("-99999999999.99".to_string());
        assert_eq!("-92233720368.54775807", big_number.to_string());
        let big_number = ColumnValue::RoundingNumeric("-91999999999.99".to_string());
        assert_eq!("-91999999999.99000000", big_number.to_string());
    }

    #[test]
    fn parse_column_regex_works() {
        let string = "\"offset\"[integer]:0";
        let re = &PARSE_COLUMN_REGEX;
        let captures = re
            .captures(string)
            .expect("Unable to match PARSE_COLUMN_REGEX to line");

        let column_name = captures
            .get(1)
            .expect("couldn't match column_name")
            .as_str();
        assert_eq!(column_name, "offset"); // no quotes
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
                Column::ChangedColumn { column_info: ColumnInfo::new("amount".to_string(), "numeric".to_string()), value: Some(ColumnValue::RoundingNumeric("4.0".to_string())) },
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
