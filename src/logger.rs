use crate::parser::TableName;
use log::{debug, error, info};

pub struct Logger {}

// lets structure our logger so that we can grep our logs more easily
impl Logger {
    pub fn info(wal_number: Option<u64>, table_name: Option<TableName>, tag: &str, message: &str) {
        info!(
            "{}",
            Self::structured_format(wal_number, table_name, tag, message)
        );
    }

    #[allow(dead_code)]
    pub fn error(wal_number: Option<u64>, table_name: Option<TableName>, tag: &str, message: &str) {
        error!(
            "{}",
            Self::structured_format(wal_number, table_name, tag, message)
        );
    }

    #[allow(dead_code)]
    pub fn debug(wal_number: Option<u64>, table_name: Option<TableName>, tag: &str, message: &str) {
        debug!(
            "{}",
            Self::structured_format(wal_number, table_name, tag, message)
        );
    }

    #[allow(dead_code)]
    pub fn structured_panic(
        wal_number: Option<u64>,
        table_name: Option<TableName>,
        tag: &str,
        message: &str,
    ) {
        panic!(Self::structured_format(
            wal_number, table_name, tag, message
        ));
    }

    pub fn structured_format(
        wal_number: Option<u64>,
        table_name: Option<TableName>,
        tag: &str,
        message: &str,
    ) -> String {
        format!(
            "wal:{} table:{} tag:{} message:{}",
            wal_number
                .map(|x| format!("{:0>16X}", x))
                .unwrap_or("none".to_owned()),
            table_name
                .map(|x| x.to_string())
                .unwrap_or("none".to_owned()),
            tag,
            message
        )
    }
}
