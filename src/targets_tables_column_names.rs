use deadpool_postgres::{Client, ManagerConfig, Pool, RecyclingMethod, Runtime};
use lazy_static::lazy_static;
use openssl::ssl::{SslConnector, SslMethod};
use postgres_openssl::MakeTlsConnector;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fmt;

use itertools::Itertools;

#[allow(unused_imports)]
use crate::{function, logger_debug, logger_error, logger_info, logger_panic};

use crate::parser::{ColumnName, SchemaAndTable, TableName};

lazy_static! {
    static ref TARGET_SCHEMA_NAME: Option<String> = std::env::var("TARGET_SCHEMA_NAME").ok();
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct ColumnInfo {
    pub name: ColumnName,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Table {
    pub column_info: HashSet<ColumnInfo>,
    pub name: TableName,
}

#[derive(Debug, Eq, PartialEq)]
struct TableHolder {
    tables: HashMap<TableName, Table>,
}

pub struct TargetsTablesColumnNames {
    connection_pool: Option<Pool>,
    table_holder: TableHolder,
}

#[derive(Debug, Deserialize)]
struct Config {
    pg: deadpool_postgres::Config,
}

#[derive(Debug)]
pub enum TargetsTablesColumnNamesError {
    PoolError(deadpool_postgres::PoolError),
    TokioError(tokio_postgres::Error),
}

impl fmt::Display for TargetsTablesColumnNamesError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TargetsTablesColumnNamesError::PoolError(err) => {
                write!(f, "Pool error: {}", err)
            }
            TargetsTablesColumnNamesError::TokioError(err) => {
                write!(f, "Tokio postgres error: {}", err)
            }
        }
    }
}

impl Error for TargetsTablesColumnNamesError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            TargetsTablesColumnNamesError::PoolError(err) => Some(err),
            TargetsTablesColumnNamesError::TokioError(err) => Some(err),
        }
    }
}

impl Config {
    pub fn from_env() -> Result<Self, ::config::ConfigError> {
        ::config::Config::builder()
            .add_source(::config::Environment::default().separator("__"))
            .build()?
            .try_deserialize()
    }
}

impl TargetsTablesColumnNames {
    pub fn new() -> TargetsTablesColumnNames {
        let hash_map = HashMap::new();
        TargetsTablesColumnNames {
            connection_pool: None,
            table_holder: TableHolder { tables: hash_map },
        }
    }

    #[cfg(test)]
    pub fn from_map(
        tables_column_names: HashMap<TableName, HashSet<ColumnName>>,
    ) -> TargetsTablesColumnNames {
        let mut tables = HashMap::new();
        for (table_name, table_rows) in tables_column_names {
            let column_info: HashSet<ColumnInfo> = table_rows
                .into_iter()
                .map(|name| ColumnInfo { name })
                .collect();
            tables.insert(
                table_name.clone(),
                Table {
                    name: table_name.clone(),
                    column_info,
                },
            );
        }

        TargetsTablesColumnNames {
            connection_pool: None,
            table_holder: TableHolder { tables },
        }
    }

    pub fn get_by_name(&self, table_name_with_schema: &TableName) -> Option<Table> {
        // Strip the schema name in order to match, as it's possible schemas will differ between source and target
        let (_, table_name) = table_name_with_schema.schema_and_table_name();
        match self
            .table_holder
            .tables
            .get(&TableName::new(table_name.to_string()))
        {
            None => None,
            Some(table) => Some(table.clone()),
        }
    }

    pub fn len(&self) -> usize {
        self.table_holder.tables.len()
    }

    fn create_connection_pool() -> Pool {
        // fail fast
        let mut cfg = Config::from_env().expect("Unable to build config from environment");
        cfg.pg.manager = Some(ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        });
        // only needs 1 connection to do its business
        let mut pool_cfg = cfg.pg.get_pool_config();
        pool_cfg.max_size = 1;
        cfg.pg.pool = Some(pool_cfg);

        let builder = SslConnector::builder(SslMethod::tls())
            .expect("Unable to build ssl connector. Are ssl libraries configured correctly?");
        let connector = MakeTlsConnector::new(builder.build());
        cfg.pg
            .create_pool(Some(Runtime::Tokio1), connector)
            .expect("Unable to build database connection pool")
    }

    async fn get_connection_from_pool(&self) -> Result<Client, TargetsTablesColumnNamesError> {
        let client = self
            .connection_pool
            .as_ref()
            .expect("No connection pool set")
            .get()
            .await;
        match client {
            Ok(ok) => Ok(ok),
            Err(err) => {
                logger_error!(None, None, &format!("error_getting_connection:{:?}", err));
                Err(err).map_err(TargetsTablesColumnNamesError::PoolError)
            }
        }
    }

    pub async fn refresh(
        &mut self,
    ) -> Result<&mut TargetsTablesColumnNames, TargetsTablesColumnNamesError> {
        self.connection_pool = Some(TargetsTablesColumnNames::create_connection_pool());
        let client = self.get_connection_from_pool().await?;

        let schema_filter = match &*TARGET_SCHEMA_NAME {
            Some(_) => "table_schema = $1",
            None => "$1 = $1",
        };

        let schema_name = match &*TARGET_SCHEMA_NAME {
            Some(schema_name) => schema_name,
            None => "none",
        };

        let query = format!(
            "SELECT table_name, column_name
             FROM information_schema.columns
             WHERE {}
             ORDER BY table_name, ordinal_position;",
            schema_filter
        );

        let rows = client
            .query(&*query, &[&schema_name])
            .await
            .map_err(TargetsTablesColumnNamesError::TokioError)?;

        let tables_rows = &rows
            .into_iter()
            .map(|row| {
                (
                    row.get::<_, &str>(0).to_string(),
                    row.get::<_, &str>(1).to_string(),
                )
            })
            .group_by(|(table_name, _)| table_name.to_string());

        let mut tables = HashMap::new();
        for (table_name, table_rows) in tables_rows {
            let column_info: HashSet<ColumnInfo> = table_rows
                .map(|(_, column_name)| ColumnInfo {
                    name: ColumnName::new(column_name),
                })
                .collect();
            tables.insert(
                TableName::new(table_name.clone()),
                Table {
                    name: TableName::new(table_name.clone()),
                    column_info,
                },
            );
        }

        self.table_holder = TableHolder { tables };
        self.connection_pool = None;

        Ok(self)
    }
}
