use bigdecimal::BigDecimal;
use internment::ArcIntern;
use lazy_static::lazy_static;
use num_bigint::BigInt;
use num_bigint::Sign;
use regex::Regex;
use std::borrow::Cow;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::{error::Error, fmt};

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
    static ref TABLE_BLACKLIST: Vec<String> = env::var("TABLE_BLACKLIST").unwrap_or("".to_owned()).split(",").map(|x| x.to_owned()).collect();
    static ref SCHEMA_BLACKLIST: Vec<String> = env::var("SCHEMA_BLACKLIST").unwrap_or("".to_owned()).split(",").map(|x| x.to_owned()).collect();
    static ref TARGET_SCHEMA_NAME: Option<String> = env::var("TARGET_SCHEMA_NAME").ok();
    static ref PARTITION_SUFFIX_REGEXP: Option<Regex> = env::var("PARTITION_SUFFIX_REGEXP").map(|s| Regex::new(&s).expect("Failed to parse partition suffix regexp")).ok();
    static ref ARRAY_STRING: String = "array".to_string();

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
    fn original_schema_and_table_name(&self) -> (&str, &str);
}

fn departition_table_name(table_name: &str) -> Cow<str> {
    match &*PARTITION_SUFFIX_REGEXP {
        None => Cow::from(table_name),
        Some(partition_suffix_regexp) => partition_suffix_regexp.replacen(table_name, 1, ""),
    }
}

// schema.table_name
// we assume a valid table name, so unwrap
impl SchemaAndTable for TableName {
    // NOTE: this gives the DESTINATION target schema name.
    // which could be really f-ing confusing if you don't expect that.
    fn schema_and_table_name(&self) -> (&str, &str) {
        let result = self.split_once('.').expect(&format!(
            "can't split schema and table name. No `.` character: {}",
            self
        ));
        match &*TARGET_SCHEMA_NAME {
            None => result,
            Some(schema_name) => (schema_name, result.1),
        }
    }
    fn original_schema_and_table_name(&self) -> (&str, &str) {
        self.split_once('.').expect(&format!(
            "can't split schema and table name. No `.` character: {}",
            self
        ))
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

#[derive(Debug)]
pub struct ParsingError {
    pub line: String,
    pub message: String,
}

impl Error for ParsingError {}

impl fmt::Display for ParsingError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Failed to parse input due to: {}, Offending line: {}",
            self.message, self.line
        )
    }
}

pub type Result<T> = std::result::Result<T, ParsingError>;

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
    Oid,
    StringEnumType,
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
#[derive(Debug, Clone)]
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

impl PartialEq for ColumnInfo {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}
impl Eq for ColumnInfo {}

impl Hash for ColumnInfo {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
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
    ContinueParse, // this is to signify that we're halfway through parsing a change
    PgRcvlogicalMsg(String),
    Truncate,
}

impl ParsedLine {
    pub fn find_id_column(&self) -> Result<&Column> {
        match self {
            ParsedLine::ChangedData {
                columns,
                table_name,
                ..
            } => {
                // unwrap because this is the id column which _must_ be here
                match columns.iter().find(|&x| x.is_id_column()) {
                    Some(column) => Ok(column),
                    None => Err(ParsingError {
                        message: format!("We have no id column for {}", table_name.as_ref()),
                        line: "No line".to_string(),
                    }),
                }
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
    ) -> Result<(Option<ColumnValue>, &'a str)> {
        const NULL_STRING: &str = "null";
        if string.starts_with(NULL_STRING) {
            let (_start, rest) = ColumnValue::split_until_char_or_end(string, ' ');
            Ok((None, rest))
        } else {
            let (column_value, rest_of_string): (ColumnValue, &str) =
                match Self::column_type_for_str(column_type) {
                    ColumnTypeEnum::Integer => ColumnValue::parse_integer(string)?,
                    ColumnTypeEnum::Boolean => ColumnValue::parse_boolean(string)?,
                    ColumnTypeEnum::Numeric => ColumnValue::parse_numeric(string),
                    ColumnTypeEnum::RoundingNumeric => ColumnValue::parse_rounding_numeric(string),
                    ColumnTypeEnum::Text => ColumnValue::parse_text(string, continue_parse)?,
                    ColumnTypeEnum::Timestamp => ColumnValue::parse_text(string, continue_parse)?,
                    ColumnTypeEnum::Oid => ColumnValue::parse_numeric(string),
                    ColumnTypeEnum::StringEnumType => {
                        ColumnValue::parse_text(string, continue_parse)?
                    }
                };
            Ok((Some(column_value), rest_of_string))
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
            "character" => ColumnTypeEnum::Text,
            "character varying" => ColumnTypeEnum::Text,
            "public.citext" => ColumnTypeEnum::Text, // extensions come through as public.
            "text" => ColumnTypeEnum::Text,
            "timestamp without time zone" => ColumnTypeEnum::Timestamp,
            "timestamp with time zone" => ColumnTypeEnum::Timestamp,
            "date" => ColumnTypeEnum::Timestamp,
            "uuid" => ColumnTypeEnum::Text,
            "jsonb" => ColumnTypeEnum::Text,
            "json" => ColumnTypeEnum::Text,
            "public.hstore" => ColumnTypeEnum::Text,
            "interval" => ColumnTypeEnum::Text,
            "array" => ColumnTypeEnum::Text,
            "oid" => ColumnTypeEnum::Oid,
            "sch_repcloud.ty_repack_step" => ColumnTypeEnum::StringEnumType,
            "int4range" => ColumnTypeEnum::Text,
            "int8range" => ColumnTypeEnum::Text,
            "numrange" => ColumnTypeEnum::Text,
            "tsrange" => ColumnTypeEnum::Text,
            "tstzrange" => ColumnTypeEnum::Text,
            "daterange" => ColumnTypeEnum::Text,
            _ => panic!("Unknown column type: {:?}", column_type_str),
        }
    }
    fn parse_integer<'a>(string: &'a str) -> Result<(ColumnValue, &'a str)> {
        let (start, rest) = ColumnValue::split_until_char_or_end(string, ' ');
        match start.parse() {
            Ok(integer) => Ok((ColumnValue::Integer(integer), rest)),
            Err(internal_message) => Err(ParsingError {
                message: format!(
                    "Unable to parse integer from integer type for {}, message: {}",
                    start, internal_message
                ),
                line: string.to_string(),
            }),
        }
    }
    fn parse_text<'a>(string: &'a str, continue_parse: bool) -> Result<(ColumnValue, &'a str)> {
        if string.starts_with("unchanged-toast-datum") {
            let (start, rest) = ColumnValue::split_until_char_or_end(string, ' ');
            fail_parse_if_unequal(
                start,
                "unchanged-toast-datum",
                &format!(
                    "expected 'unchanged-toast-datum' at start of string, got: `{}`",
                    start
                ),
                string,
            )?;
            return Ok((ColumnValue::UnchangedToast, rest));
        }
        if !continue_parse {
            fail_parse_if_unequal(
                &string[0..1],
                "'",
                &format!("expected ', got `{}`", &string[0..1]),
                string,
            )?;
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
            fail_parse_if_unequal(
                &end[0..1],
                "'",
                &format!("expected ', got `{}`", &end[0..1]),
                string,
            )?;
            (ColumnValue::Text(start.to_owned()), &end[1..])
        } else {
            (ColumnValue::IncompleteText(start.to_owned()), end)
        };
        let (_thrown_away_space, adjusted_end) = ColumnValue::split_until_char_or_end(text, ' ');
        Ok((column, adjusted_end))
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

    fn parse_boolean<'a>(string: &'a str) -> Result<(ColumnValue, &'a str)> {
        let (start, rest) = ColumnValue::split_until_char_or_end(string, ' ');
        match start {
            "true" => Ok((ColumnValue::Boolean(true), rest)),
            "false" => Ok((ColumnValue::Boolean(false), rest)),
            _ => Err(ParsingError {
                message: format!("Unknown boolvalue {:?}", start),
                line: string.to_string(),
            }),
        }
    }
}

impl Parser {
    pub fn new(include_xids: bool) -> Parser {
        logger_info!(
            None,
            None,
            &format!("partition_suffix_regexp:{:?}", PARTITION_SUFFIX_REGEXP.clone().ok_or("none"))
        );
        Parser {
            config: ParserConfig { include_xids },
            parse_state: ParserState {
                currently_parsing: None,
                wal_file_number: None,
            },
        }
    }

    pub fn parse(&mut self, string: &String) -> Result<ParsedLine> {
        match string {
            x if { self.parse_state.currently_parsing.is_some() } => self.continue_parse(x),
            x if { x.starts_with("BEGIN") } => self.parse_begin(x),
            x if { x.starts_with("COMMIT") } => self.parse_commit(x),
            x if { x.ends_with("TRUNCATE: (no-flags)") } => self.parse_truncate_msg(x),
            x if { x.starts_with("table") } => self.parse_change(x),
            x if { x.starts_with("pg_recvlogical") } => self.parse_pg_rcvlogical_msg(x),
            x => Err(ParsingError {
                line: string.clone(),
                message: format!("Unknown change kind: {}!", x),
            }),
        }
    }

    fn parse_begin(&self, string: &str) -> Result<ParsedLine> {
        if self.config.include_xids {
            const SIZE_OF_BEGIN_TAG: usize = "BEGIN ".len();
            let rest_of_string = &string[SIZE_OF_BEGIN_TAG..string.len()];
            // "BEGIN 1234"
            match rest_of_string.parse() {
                Ok(xid) => {
                    logger_debug!(
                        self.parse_state.wal_file_number,
                        None,
                        &format!("xid:{}", xid)
                    );
                    Ok(ParsedLine::Begin(xid))
                }
                Err(inner_message) => Err(ParsingError {
                    line: string.to_string(),
                    message: format!("Unable to parse BEGIN xid as i64: {}", inner_message),
                }),
            }
        } else {
            Ok(ParsedLine::Begin(0))
        }
    }

    fn parse_commit(&self, string: &str) -> Result<ParsedLine> {
        if self.config.include_xids {
            // "COMMIT 1234"
            const SIZE_OF_COMMIT_TAG: usize = "COMMIT ".len();
            let rest_of_string = &string[SIZE_OF_COMMIT_TAG..string.len()];
            // "BEGIN 1234"
            match rest_of_string.parse() {
                Ok(xid) => {
                    logger_debug!(
                        self.parse_state.wal_file_number,
                        None,
                        &format!("xid:{}", xid)
                    );
                    Ok(ParsedLine::Commit(xid))
                }
                Err(inner_message) => Err(ParsingError {
                    line: string.to_string(),
                    message: format!("Unable to parse COMMIT xid as i64: {}", inner_message),
                }),
            }
        } else {
            Ok(ParsedLine::Commit(0))
        }
    }

    fn parse_change(&mut self, string: &str) -> Result<ParsedLine> {
        const SIZE_OF_TABLE_TAG: usize = "table ".len();
        let string_without_tag = &string[SIZE_OF_TABLE_TAG..string.len()];
        // we assume tables can't have colons in their names
        // fuck you if you put a colon in a table name, you psychopath
        let table_name = slice_until_colon_or_end(string_without_tag);
        let departitioned_table_name = TableName::new(departition_table_name(table_name).into());
        // + 2 for colon + space
        fail_parse_if_unequal(
            &string_without_tag[table_name.len()..table_name.len() + 2],
            ": ",
            &format!(
                "expected `: `, got `{}`",
                &string_without_tag[table_name.len()..table_name.len() + 2]
            ),
            string,
        )?;
        let string_without_table =
            &string_without_tag[table_name.len() + 2..string_without_tag.len()];
        let kind_string = slice_until_colon_or_end(string_without_table);

        // TODO: split early here for truncate columns

        let kind = self.parse_kind(kind_string);

        // + 2 for colon + space
        fail_parse_if_unequal(
            &string_without_table[kind_string.len()..kind_string.len() + 2],
            ": ",
            &format!(
                "expected `: `, got `{}`",
                &string_without_table[kind_string.len()..kind_string.len() + 2]
            ),
            string,
        )?;
        let string_without_kind =
            &string_without_table[kind_string.len() + 2..string_without_table.len()];

        let columns = self.parse_columns(string_without_kind, departitioned_table_name.clone())?;
        self.handle_parse_changed_data(departitioned_table_name, kind, columns)
    }

    fn parse_pg_rcvlogical_msg(&self, string: &str) -> Result<ParsedLine> {
        // "pg_recvlogical: could not send replication command..."
        const SIZE_OF_TAG: usize = "pg_recvlogical: ".len();
        let rest_of_string = &string[SIZE_OF_TAG..string.len()];
        logger_info!(
            self.parse_state.wal_file_number,
            None,
            &format!("parsed_pg_rcvlogical_message:{}", rest_of_string)
        );
        Ok(ParsedLine::PgRcvlogicalMsg(rest_of_string.to_string()))
    }

    fn parse_truncate_msg(&self, string: &str) -> Result<ParsedLine> {
        // "table public.transaction_enrichment_merchant_matching_logs: TRUNCATE: (no-flags)"
        logger_info!(
            self.parse_state.wal_file_number,
            None,
            &format!("parsed_truncate:{}", string)
        );
        Ok(ParsedLine::Truncate)
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

    fn parse_columns(&self, string: &str, table_name: TableName) -> Result<Vec<Column>> {
        let mut column_vector = Vec::new();
        let mut remaining_string = string;
        while remaining_string.len() > 0 {
            // we get that any time there is a delete if there's no pkey
            // postgres code here: https://github.com/postgres/postgres/blob/master/contrib/test_decoding/test_decoding.c#L663
            if remaining_string == "(no-tuple-data)" {
                remaining_string = "";
                logger_info!(
                    self.parse_state.wal_file_number,
                    None,
                    &format!("Dropping no-tuple-data column:{}", string)
                );
            } else {
                let (column, rest_of_string) =
                    self.parse_column(remaining_string, table_name.clone())?;
                remaining_string = rest_of_string;
                column_vector.push(column);
            }
        }
        Ok(column_vector)
    }

    // this function matches things like `"offset"[integer]:1` giving ("offset", "integer", 17) result (the 17 is the length up to the colon).
    // and `id[uuid]:"i-am-a-uuid"`, giving ("id", "uuid", 8) result
    // NOTE: notice that it removes quotes from offset above.
    // NOTE: it will match `my_column[character varying[]]:` and return ("my_column", "array", 30) (note that it calls all arrays type "array")
    fn parse_column_name_and_type<'a>(&self, string: &'a str) -> Result<(&'a str, &'a str, usize)> {
        let string_find_index = string.find('[').ok_or_else(|| ParsingError {
            message: "Unable to match bracket while searching for column name".to_string(),
            line: string.to_string(),
        })?;
        let column_name = &string[0..string_find_index];
        let original_column_name_size = column_name.len();

        // +1 to skip the open square bracket
        let string_without_column_name = &string[column_name.len() + 1..];
        // we then rewrite our column name to strip the quotes from it.
        let column_name = if column_name.starts_with('"') && column_name.ends_with('"') {
            &column_name[1..column_name.len() - 1]
        } else {
            column_name
        };
        let mut initial_string = string_without_column_name;

        // we've had a single open `[` so far
        let mut column_type = "";
        let mut bracket_stack = 1;
        let mut current_index = 0;
        while bracket_stack > 0 {
            let matching_bracket =
                initial_string
                    .find(&['[', ']'])
                    .ok_or_else(|| ParsingError {
                        message: "couldn't match column_type brackets".to_string(),
                        line: string.to_string(),
                    })?;
            if &string_without_column_name[matching_bracket..matching_bracket + 1] == "[" {
                bracket_stack += 1
            } else {
                bracket_stack -= 1
            }
            current_index += matching_bracket + 1;
            // advance
            initial_string = &initial_string[matching_bracket + 1..];
            // we don't include the final `]`
            column_type = &string_without_column_name[0..current_index - 1];
        }
        // column name, open square bracket, column type, close square bracket
        let column_string_size = original_column_name_size + 1 + column_type.len() + 1;
        // // For array types, remove the inner type specification - we treat all array types as text
        let column_type = if column_type.ends_with("[]") {
            &ARRAY_STRING.as_str()
        } else {
            column_type
        };
        Ok((column_name, column_type, column_string_size))
    }

    fn parse_column<'a>(
        &self,
        string: &'a str,
        _table_name: TableName,
    ) -> Result<(Column, &'a str)> {
        let (column_name, column_type, capture_size) = self.parse_column_name_and_type(string)?;
        // add 1 for the `:`
        let string_without_column_type = &string[capture_size + 1..];

        // logger_debug!(
        //     self.parse_state.wal_file_number,
        //     Some(&table_name),
        //     &format!("column_name:{} column_type:{}", column_name, column_type)
        // );

        let (column_value, rest) =
            ColumnValue::parse(string_without_column_type, column_type, false)?;
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
        Ok((column, rest))
    }

    // TODO break this monster up a bit
    fn continue_parse(&mut self, string: &str) -> Result<ParsedLine> {
        let incomplete_change = self.parse_state.currently_parsing.take();
        self.parse_state.currently_parsing = None;
        match incomplete_change {
            Some(ParsedLine::ChangedData {
                kind,
                table_name,
                mut columns,
            }) => {
                let incomplete_column = match columns.pop() {
                    Some(result) => result,
                    None => {
                        return Err(ParsingError {
                            message: "error: continue parse called without any columns? wtf?"
                                .to_string(),
                            line: string.to_string(),
                        })
                    }
                };
                assert!(matches!(incomplete_column, Column::IncompleteColumn { .. }));
                match incomplete_column {
                    Column::IncompleteColumn { column_info: ColumnInfo{name, column_type}, value: incomplete_value } => {
                        let (continued_column_value, rest) = ColumnValue::parse(string, &column_type, true)?;
                        let value = match incomplete_value {
                            ColumnValue::IncompleteText(value) => value,
                            _ => return Err(ParsingError{ message: "Incomplete value is not ColumnValue::IncompleteText".to_string(), line: string.to_string() })
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
                            _ => return Err(ParsingError{ message: "Trying to continue to parse a value that's not of type text".to_string(), line: string.to_string() })
                        };

                        columns.push(updated_column);
                        // because there could be multiple newlines we need to check again
                        if self.column_is_incomplete(&columns) {
                            return self.handle_parse_changed_data(table_name, kind, columns)
                        } else {
                            let mut more_columns = self.parse_columns(rest, table_name.clone())?;
                            // append modifies in place
                            columns.append(&mut more_columns);
                            self.handle_parse_changed_data(table_name, kind, columns)
                        }
                    },
                    _ => return Err(ParsingError{ message: format!("trying to parse an incomplete_column that's not a Column::IncompleteColumn {:?}", incomplete_column), line: string.to_string() })
                }
            }
            _ => {
                return Err(ParsingError {
                    message: format!(
                        "Trying to continue parsing a {:?} rather than a ParsedLine::ChangedData",
                        incomplete_change
                    ),
                    line: string.to_string(),
                })
            }
        }
    }

    fn handle_parse_changed_data(
        &mut self,
        table_name: TableName,
        kind: ChangeKind,
        columns: Vec<Column>,
    ) -> Result<ParsedLine> {
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
            let schema_name: String = table_name.original_schema_and_table_name().0.to_string();
            if TABLE_BLACKLIST.contains(table_name.as_ref()) {
                logger_debug!(
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
            } else if SCHEMA_BLACKLIST.contains(&schema_name) {
                logger_debug!(
                    self.parse_state.wal_file_number,
                    Some(&table_name),
                    &format!(
                        "schema_skipped_due_to_blacklist:{} blacklist:{:?}",
                        table_name, *SCHEMA_BLACKLIST
                    )
                );
                // we don't save any parse state, since we're skipping this changed_data and dropping it on the ground
                // we still want this to be after the continue parse line so we can
                // handle newlines in our blacklisted schemas
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
        Ok(result)
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

fn fail_parse_if_unequal(
    left: &str,
    right: &str,
    message: &str,
    line: &str,
) -> std::result::Result<(), ParsingError> {
    if left != right {
        Err(ParsingError {
            line: line.to_string(),
            message: message.to_string(),
        })
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[ctor::ctor]
    fn setup_tests() {
        std::env::set_var("TABLE_BLACKLIST", "public.schema_migrations");
        std::env::set_var(
            "SCHEMA_BLACKLIST",
            "partman,data_science,sch_repcloud,sch_repdrop,sch_repnew,private",
        );
        std::env::set_var("PARTITION_SUFFIX_REGEXP", r"_p\d{4}w\d{1,2}\z");
    }

    #[test]
    fn table_departition_works_as_expected() {
        let mut parser = Parser::new(true);
        let line = "table public.webhooks_incoming_webhooks_p2024w30: INSERT: id[bigint]:123";
        let result = parser.parse(&line.to_string()).expect("failed parsing");
        let table_name = match result {
            ParsedLine::ChangedData { table_name, .. } => table_name,
            _ => panic!("tried to find table name of non changed_data"),
        };
        let actual = table_name.schema_and_table_name().1.to_string();
        assert_eq!(actual, "webhooks_incoming_webhooks".to_string());
    }

    #[test]
    fn table_blacklist_works_as_expected() {
        let mut parser = Parser::new(true);
        let line =
            "table public.schema_migrations: INSERT: version[character varying]:'20210112112814'";
        let parsed_line = parser.parse(&line.to_owned()).expect("failed parsing");
        assert_eq!(parsed_line, ParsedLine::ContinueParse);
    }

    #[test]
    fn schema_blacklist_works_as_expected() {
        let mut parser = Parser::new(true);
        let line = "table private.anythings: INSERT: version[character varying]:'20210112112814'";
        let parsed_line = parser.parse(&line.to_owned()).expect("failed parsing");
        assert_eq!(parsed_line, ParsedLine::ContinueParse);
    }

    #[test]
    fn schema_blacklist_still_works_as_expected() {
        let mut parser = Parser::new(true);
        let line = "table partman.anythings: INSERT: version[character varying]:'20210112112814'";
        let parsed_line = parser.parse(&line.to_owned()).expect("failed parsing");
        assert_eq!(parsed_line, ParsedLine::ContinueParse);
    }

    #[test]
    fn schema_blacklist_still2_works_as_expected() {
        let mut parser = Parser::new(true);
        let line = "table sch_repcloud.t_table_repack: UPDATE: i_id_table[bigint]:1 oid_old_table[oid]:16745 oid_new_table[oid]:459073292 v_old_table_name[character varying]:'admins' v_new_table_name[character varying]:'admins_16745' v_log_table_name[character varying]:'log_16745' v_schema_name[character varying]:'public' t_tab_pk[text[]]:null en_repack_step[sch_repcloud.ty_repack_step]:null v_status[character varying]:null b_ready_for_swap[boolean]:false i_size_start[bigint]:null i_size_end[bigint]:null xid_copy_start[bigint]:null xid_sync_end[bigint]:null ts_repack_start[timestamp without time zone]:null ts_repack_end[timestamp without time zone]:null";
        let parsed_line = parser.parse(&line.to_owned()).expect("failed parsing");
        assert_eq!(parsed_line, ParsedLine::ContinueParse);
    }

    #[test]
    fn schema_blacklist_still3_works_as_expected() {
        let mut parser = Parser::new(true);
        let line = "table sch_repcloud.t_idx_repack: INSERT: i_id_index[bigint]:1 i_id_table[bigint]:1 v_table_name[character varying]:'admins_16745' v_schema_name[character varying]:'public' b_indisunique[boolean]:true b_idx_constraint[boolean]:true v_contype[character]:'p' t_index_name[text]:'admins_pkey' t_index_def[text]:' btree (id)' t_constraint_def[text]:'PRIMARY KEY (id)'";
        let parsed_line = parser.parse(&line.to_owned()).expect("failed parsing");
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
    fn column_info_compares_on_name() {
        assert!(
            ColumnInfo::new("baz_array".to_string(), "array".to_string())
                == ColumnInfo::new("baz_array".to_string(), "integer".to_string())
        );
        assert!(
            ColumnInfo::new("baz_array".to_string(), "array".to_string())
                != ColumnInfo::new("not_baz_array".to_string(), "array".to_string())
        );
    }

    #[test]
    fn parses_array_type() {
        let mut parser = Parser::new(true);
        let line = "table public.users: UPDATE: id[bigint]:123 foobar[text]:'foobar string' baz_array[character varying[]]:'{\"foo\", \"bar\", \"baz\"}'";
        let result = parser.parse(&line.to_string()).expect("failed parsing");
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
    fn parses_int8range_type() {
        let mut parser = Parser::new(true);
        let line = "table public.users: UPDATE: id[bigint]:123 foobar[text]:'foobar string' baz_int8range[int8range]:'[1743532200,1743553800)'";
        let result = parser.parse(&line.to_string()).expect("failed parsing");
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
                        column_info: ColumnInfo::new("baz_int8range".to_string(), "int8range".to_string()),
                        value: Some(ColumnValue::Text("[1743532200,1743553800)".to_string()))
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
        let mut result = parser
            .parse(&line.to_string())
            .expect(&format!("failed to parse: {}", line));
        assert!(matches!(result, ParsedLine::ChangedData { .. }));
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
        let parser = Parser::new(true);

        let string = "\"offset\"[integer]:0";
        let (column_name, column_type, capture_size) =
            parser.parse_column_name_and_type(string).unwrap();
        assert_eq!(column_name, "offset");
        assert_eq!(column_type, "integer");
        assert_eq!(capture_size, 17);
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
                let parsed_line = parser
                    .parse(&ip)
                    .expect(&format!("failed to parse: {}", &ip));
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
            ParsedLine::Begin(4220773599),
            ParsedLine::ChangedData { columns: vec![],
                table_name: ArcIntern::new("public.smart_insight_admin_conditions".to_string()),
                kind: ChangeKind::Delete },
            ParsedLine::Commit(4220773599),
            ParsedLine::Begin(4220773600),
            ParsedLine::Truncate,
            ParsedLine::Commit(4220773600),
            ]));
    }

    #[test]
    fn parsing_with_commit_line_works() {
        use std::fs::File;
        use std::io::{self, BufRead};

        let mut parser = Parser::new(true);
        let mut collector = Vec::new();
        let file = File::open("./test/parser_commit_bug.txt")
            .expect("couldn't find file containing test data");
        let lines = io::BufReader::new(file).lines();
        for line in lines {
            if let Ok(ip) = line {
                let parsed_line = parser
                    .parse(&ip)
                    .expect(&format!("failed to parse: {}", &ip));
                match parsed_line {
                    ParsedLine::ContinueParse => {}
                    _ => {
                        collector.push(parsed_line);
                    }
                }
            }
        }

        assert!(equal_unordered_list(
            &collector,
            &vec![
                ParsedLine::Begin(3970124255),
                ParsedLine::ChangedData {
                    columns: vec![Column::ChangedColumn {
                        column_info: ColumnInfo::new("c_ddlqry".to_string(), "text".to_string()),
                        value: Some(ColumnValue::Text("BEGIN;\nSELECT 1;\nCOMMIT;".to_string()))
                    }],
                    table_name: ArcIntern::new("public.foobar".to_string()),
                    kind: ChangeKind::Insert
                },
                ParsedLine::Commit(3970124255)
            ]
        ))
    }
}
