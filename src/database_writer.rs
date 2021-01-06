use deadpool_postgres::{Client, ManagerConfig, Pool, RecyclingMethod};
use openssl::ssl::{SslConnector, SslMethod};
use postgres_openssl::MakeTlsConnector;
use serde::Deserialize;
// use config;
use dotenv::dotenv;
use std::env;

#[allow(unused_imports)]
use log::{debug, error, info, log_enabled, Level};

use crate::change_processing::DdlChange;
use crate::file_uploader::CleoS3File;
use crate::parser::{ChangeKind, ColumnInfo, SchemaAndTable, TableName};

pub struct DatabaseWriter {
    connection_pool: Pool,
}

#[derive(Debug, Deserialize)]
struct Config {
    pg: deadpool_postgres::Config,
}

#[derive(Debug)]
pub enum DatabaseWriterError {
    PoolError(deadpool_postgres::PoolError),
    TokioError(tokio_postgres::Error),
}

impl Config {
    pub fn from_env() -> Result<Self, ::config::ConfigError> {
        let mut cfg = ::config::Config::new();
        cfg.merge(::config::Environment::new().separator("__"))?;
        cfg.try_into()
    }
}

impl DatabaseWriter {
    pub fn new() -> DatabaseWriter {
        DatabaseWriter {
            connection_pool: DatabaseWriter::create_connection_pool(),
        }
    }

    fn create_connection_pool() -> Pool {
        dotenv().ok();
        // fail fast
        let mut cfg = Config::from_env().expect("Unable to build config from environment");
        cfg.pg.manager = Some(ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        });
        let builder = SslConnector::builder(SslMethod::tls())
            .expect("Unable to build ssl connector. Are ssl libraries configured correctly?");
        let connector = MakeTlsConnector::new(builder.build());
        cfg.pg
            .create_pool(connector)
            .expect("Unable to build database connection pool")
    }

    pub async fn handle_ddl(&self, ddl_change: &DdlChange) -> Result<(), DatabaseWriterError> {
        let alter_table_statement = match ddl_change {
            DdlChange::AddColumn(column_info, table_name) => {
                self.add_column_statement(column_info, table_name)
            }
            DdlChange::RemoveColumn(column_info, table_name) => {
                self.remove_column_statement(column_info, table_name)
            }
        };
        info!("alter table statement: {}", alter_table_statement.as_str());
        let client = self.connection_pool.get().await.unwrap();

        client
            .execute(alter_table_statement.as_str(), &[])
            .await
            .map_err(DatabaseWriterError::TokioError)?;

        info!("alter table finished: {}", alter_table_statement.as_str());
        Ok(())
    }

    fn add_column_statement(&self, column_info: &ColumnInfo, table_name: &TableName) -> String {
        let (schema_name, just_table_name) = table_name.schema_and_table_name();
        let column_name_and_type = self.column_and_type_for_column(column_info);
        format!(
            "alter table \"{schema_name}\".\"{just_table_name}\" add column {column_name_and_type}",
            schema_name = &schema_name,
            just_table_name = &just_table_name,
            column_name_and_type = &column_name_and_type
        )
    }

    fn remove_column_statement(&self, column_info: &ColumnInfo, table_name: &TableName) -> String {
        let (schema_name, just_table_name) = table_name.schema_and_table_name();
        // TODO: foreign keys
        format!(
            "alter table \"{schema_name}\".\"{just_table_name}\" drop column \"{column_name}\"",
            schema_name = &schema_name,
            just_table_name = &just_table_name,
            column_name = &column_info.name
        )
    }

    pub async fn apply_s3_changes(
        &self,
        s3_file: &mut CleoS3File,
    ) -> Result<(), DatabaseWriterError> {
        let kind = &s3_file.kind;
        let table_name = &s3_file.table_name;
        if [
            "public.transaction_descriptions",
            "public.user_relationships_timestamps",
        ]
        .contains(&table_name.as_str())
        {
            return Ok(());
        }
        // temp tables are present in the session, so we still need to drop it at the end of the transaction
        info!("BEGIN INSERT {}", table_name);
        let client = self
            .connection_pool
            .get()
            .await
            .map_err(DatabaseWriterError::PoolError)?;
        // let transaction = client.transaction().await.unwrap();
        let transaction = client;
        info!("GOT CONNECTION {}", table_name);
        let (schema_name, just_table_name) = table_name.schema_and_table_name();
        assert!(!table_name.contains('"'));
        let staging_name = self.staging_name(s3_file);
        let return_early = self
            .create_table_if_not_exists(s3_file, &transaction)
            .await?;
        if return_early {
            return Ok(());
        }
        let create_staging_table = self.query_for_create_staging_table(
            kind,
            &s3_file.columns,
            &staging_name,
            &schema_name,
            &just_table_name,
        );
        info!("{}", create_staging_table);

        let remote_filepath = s3_file.remote_path();
        let access_key_id = env::var("AWS_ACCESS_KEY_ID").unwrap();
        let secret_access_key = env::var("AWS_SECRET_ACCESS_KEY").unwrap();
        let credentials_string = format!(
            "aws_access_key_id={aws_access_key_id};aws_secret_access_key={secret_access_key}",
            aws_access_key_id = access_key_id,
            secret_access_key = secret_access_key
        );
        // no gzip
        let copy_to_staging_table = format!(
            "
            copy \"{staging_name}\" from '{remote_filepath}'
            CREDENTIALS '{credentials_string}'
            GZIP
            CSV
            TRUNCATECOLUMNS
            IGNOREHEADER 1
            DELIMITER ','
            emptyasnull
            blanksasnull
            compupdate off
            statupdate off
",
            staging_name = &staging_name,
            remote_filepath = &remote_filepath,
            credentials_string = &credentials_string
        );

        let data_migration_query_string = self.query_for_change_kind(
            kind,
            staging_name.as_ref(),
            just_table_name.as_ref(),
            schema_name.as_ref(),
            &s3_file.columns,
        );
        let drop_staging_table = format!("drop table if exists {}", &staging_name);
        // let insert_query = format!();
        let result = transaction
            .execute(create_staging_table.as_str(), &[])
            .await;
        if let Err(err) = result {
            error!(
                "Received error: {} {} {} {:?}",
                table_name,
                remote_filepath,
                create_staging_table.as_str(),
                err
            );
        }
        info!("CREATED STAGING TABLE {}", table_name);
        transaction
            .execute(copy_to_staging_table.as_str(), &[])
            .await
            .map_err(DatabaseWriterError::TokioError)?;
        info!("COPIED TO STAGING TABLE {}", table_name);
        transaction
            .execute(data_migration_query_string.as_str(), &[])
            .await
            .map_err(DatabaseWriterError::TokioError)?;
        info!("INSERTED FROM STAGING TABLE {}", table_name);
        transaction
            .execute(drop_staging_table.as_str(), &[])
            .await
            .map_err(DatabaseWriterError::TokioError)?;
        info!("DROPPED STAGING TABLE {}", table_name);
        // TEMP
        // serialiseable isolation error. might be to do with dms.
        // transaction.commit().await.unwrap();
        // info!("COMMITTED TX {}", table_name);

        info!("INSERTED {} {}", &remote_filepath, table_name);

        s3_file.wal_file.maybe_remove_wal_file();

        Ok(())
    }

    // bool is whether we return early. Only necessary for delete where the table
    // does not exist
    async fn create_table_if_not_exists(
        &self,
        s3_file: &CleoS3File,
        database_client: &Client,
    ) -> Result<bool, DatabaseWriterError> {
        let (schema_name, just_table_name) = s3_file.table_name.schema_and_table_name();
        let query = "
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE  table_schema = $1
                    AND    table_name   = $2
            );";
        let row = database_client
            .query_one(query, &[&schema_name, &just_table_name])
            .await
            .map_err(DatabaseWriterError::TokioError)?;
        let result: bool = row.get(0);
        if !result {
            // check this isn't a delete command, because if it is,
            // we've got no table so job done (good thing because there's no schema)
            if s3_file.kind == ChangeKind::Delete {
                error!("Delete when there's no table {:?}!", s3_file.table_name);
                return Ok(true);
            } else if s3_file.kind == ChangeKind::Update {
                panic!("update when there's no table {:?}!", s3_file.table_name);
            }
            info!("table {} does not exist creating...", s3_file.table_name);
            // TODO: distkey
            let create_table_query = format!(
                "create table \"{schema_name}\".\"{just_table_name}\" ({columns}) SORTKEY(id)",
                schema_name = schema_name,
                just_table_name = just_table_name,
                columns = self.values_description_for_table(&s3_file.columns)
            );
            database_client
                .execute(create_table_query.as_str(), &[])
                .await
                .unwrap();
            info!("finished creating table {} ", s3_file.table_name);
        } // else the table exists and do nothing
        Ok(false)
    }

    fn staging_name<'a>(&self, s3_file: &'a CleoS3File) -> String {
        // s3://bucket/path/schema.table_name_insert.tar.gz -> table_name_insert_staging
        //                         ^^^^^^^^^^^^^^^^^
        let remote_filename = &s3_file.remote_filename;
        let last_slash = &remote_filename[remote_filename.rfind('/').unwrap() + 1..];
        let dot_after_last_slash = &last_slash[last_slash.find('.').unwrap() + 1..];
        let dot_until_dot = &dot_after_last_slash[..dot_after_last_slash.find('.').unwrap()];
        format!("{}_staging", dot_until_dot)
    }

    fn query_for_create_staging_table(
        &self,
        kind: &ChangeKind,
        columns: &Vec<ColumnInfo>,
        staging_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> String {
        match kind {
            ChangeKind::Insert => {
                format!(
                    "create temp table \"{}\" (like \"{}\".\"{}\")",
                    &staging_name, &schema_name, &table_name
                )
            }
            ChangeKind::Delete => {
                format!(
                    "create temp table \"{}\" ({})",
                    &staging_name,
                    self.values_description_for_table(columns)
                )
            }
            ChangeKind::Update => {
                format!(
                    "create temp table \"{}\" ({})",
                    &staging_name,
                    self.values_description_for_table(columns)
                )
            }
        }
    }

    fn values_description_for_table(&self, columns: &Vec<ColumnInfo>) -> String {
        columns
            .iter()
            .map(|x| self.column_and_type_for_column(x))
            .collect::<Vec<_>>()
            .join(",")
    }

    // NOTE: if you have a column named "tag" it needs to be surrounded by quotes
    fn column_and_type_for_column(&self, column_info: &ColumnInfo) -> String {
        format!(
            "\"{column_name}\" {column_type}",
            column_name = column_info.column_name(),
            column_type = self.column_type_mapping(column_info.column_type()).as_str()
        )
    }

    fn query_for_change_kind(
        &self,
        kind: &ChangeKind,
        staging_name: &str,
        table_name: &str,
        schema_name: &str,
        columns: &Vec<ColumnInfo>,
    ) -> String {
        match kind {
            ChangeKind::Insert => {
                format!(
                    "insert into \"{schema_name}\".\"{table_name}\"
                    select s.* from \"{staging_name}\" s left join \"{schema_name}\".\"{table_name}\" t
                    on s.id = t.id
                    where t.id is NULL",
                    schema_name=&schema_name,
                    table_name=&table_name,
                    staging_name=&staging_name
                )
            }
            ChangeKind::Delete => {
                format!(
                    "delete from \"{schema_name}\".\"{table_name}\" where id in (select id from \"{staging_name}\")",
                    schema_name=&schema_name,
                    table_name=&table_name,
                    staging_name=&staging_name
                )
            }
            ChangeKind::Update => {
                // Don't update the id column
                format!(
                    "
                    update \"{schema_name}\".\"{table_name}\" t
                    set {columns_to_update} from \"{staging_name}\" s
                    where t.id = s.id
                    ",
                    schema_name = &schema_name,
                    table_name = &table_name,
                    columns_to_update = columns
                        .iter()
                        .filter(|x| !x.is_id_column())
                        .map(|x| x.column_name())
                        .map(|x| format!("\"{}\" = s.\"{}\"", x, x))
                        .collect::<Vec<_>>()
                        .join(","),
                    staging_name = &staging_name
                )
            }
        }
    }

    fn column_type_mapping(&self, column_type: &str) -> String {
        // TODO: for money if we care
        // const DEFAULT_PRECISION: i32 = 19; // 99_999_999_999_999_999.99
        // const DEFAULT_SCALE: i32 = 2;
        // {"boolean", "double precision", "integer", "interval", "numeric", "public.hstore", "timestamp without time zone", "text", "character varying", "json", "bigint", "public.citext", "date", "uuid", "jsonb"}

        let return_type = match column_type {
            "text" => "CHARACTER VARYING(65535)",
            "json" => "CHARACTER VARYING(65535)",
            "jsonb" => "CHARACTER VARYING(65535)",
            "bytea" => "CHARACTER VARYING(65535)",
            "oid" => "CHARACTER VARYING(65535)",
            "ARRAY" => "CHARACTER VARYING(65535)",
            "USER-DEFINED" => "CHARACTER VARYING(65535)",
            "public.citext" => "CHARACTER VARYING(65535)",
            "public.hstore" => "CHARACTER VARYING(65535)",
            "uuid" => "CHARACTER VARYING(36)",
            "interval" => "CHARACTER VARYING(65535)",
            _ => column_type,
        };
        return_type.to_string()
    }

    // pub async fn run_query_with_no_args(&self, query_string: &str) {
    // }

    // pub async fn test(&self) {
    //     let ref pool = &self.connection_pool;
    //     let foo: Vec<_> = (1..10).map(|i| async move {
    //         let client = pool.get().await.unwrap();
    //         // let stmt = client.prepare("SELECT 1 + $1;SELECT 1").await.unwrap();
    //         let rows = client.query("select 1 + $1", &[&i]).await.unwrap();
    //         let value: i32 = rows[0].get(0);
    //         assert_eq!(value, i + 1);
    //         info!("{}", value);
    //     }).collect();
    //     futures::future::join_all(foo).await;
    // }
}
