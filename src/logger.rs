use crate::parser::TableName;
use log::{debug, error, info};

pub struct Logger {}

// could deduplicate with a macro, but not gonna bother f'now
// lets structure our logger so that we can grep our logs more easily
impl Logger {
    pub fn info(wal_number: Option<u64>, table_name: Option<TableName>, tag: &str, message: &str) {
        info!(
            "wal:{} table:{} tag:{} message:{}",
            wal_number
                .map(|x| format!("{:0>16X}", x))
                .unwrap_or("none".to_owned()),
            table_name
                .map(|x| x.to_string())
                .unwrap_or("none".to_owned()),
            tag,
            message
        );
    }

    #[allow(dead_code)]
    pub fn error(wal_number: Option<u64>, table_name: Option<TableName>, tag: &str, message: &str) {
        error!(
            "wal:{} table:{} tag:{} message:{}",
            wal_number
                .map(|x| format!("{:0>16X}", x))
                .unwrap_or("none".to_owned()),
            table_name
                .map(|x| x.to_string())
                .unwrap_or("none".to_owned()),
            tag,
            message
        );
    }

    #[allow(dead_code)]
    pub fn debug(wal_number: Option<u64>, table_name: Option<TableName>, tag: &str, message: &str) {
        debug!(
            "wal:{} table:{} tag:{} message:{}",
            wal_number
                .map(|x| format!("{:0>16X}", x))
                .unwrap_or("none".to_owned()),
            table_name
                .map(|x| x.to_string())
                .unwrap_or("none".to_owned()),
            tag,
            message
        );
    }

    #[allow(dead_code)]
    pub fn structured_panic(
        wal_number: Option<u64>,
        table_name: Option<TableName>,
        tag: &str,
        message: &str,
    ) {
        panic!(
            "wal:{} table:{} tag:{} message:{}",
            wal_number
                .map(|x| format!("{:0>16X}", x))
                .unwrap_or("none".to_owned()),
            table_name
                .map(|x| x.to_string())
                .unwrap_or("none".to_owned()),
            tag,
            message
        );
    }
}
