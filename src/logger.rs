struct Logger {}

// lets structure our logger so that we can
impl Logger {
    pub fn info(
        wal_number: Option<usize>,
        table_name: Option<TableName>,
        tag: &str,
        message: &str,
    ) {
        info!(
            "wal:{wal_number} table:{TableName} tag:{tag} message:{message}",
            wal_number: wal_number,
            table: table_name,
            tag: tag,
            message: message
        );
    }

    pub fn error(
        wal_number: Option<usize>,
        table_name: Option<TableName>,
        tag: &str,
        message: &str,
    ) {
        error!(
            "wal:{wal_number} table:{TableName} tag:{tag} message:{message}",
            wal_number: wal_number,
            table: table_name,
            tag: tag,
            message: message
        );
    }

    pub fn debug(
        wal_number: Option<usize>,
        table_name: Option<TableName>,
        tag: &str,
        message: &str,
    ) {
        debug!(
            "wal:{wal_number} table:{TableName} tag:{tag} message:{message}",
            wal_number: wal_number,
            table: table_name,
            tag: tag,
            message: message
        );
    }

    pub fn structured_panic(
        wal_number: Option<usize>,
        table_name: Option<TableName>,
        tag: &str,
        message: &str,
    ) {
        panic!(
            "wal:{wal_number} table:{TableName} tag:{tag} message:{message}",
            wal_number: wal_number,
            table: table_name,
            tag: tag,
            message: message
        );
    }
}
