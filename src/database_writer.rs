use deadpool_postgres::{Client, ManagerConfig, Pool, RecyclingMethod};
use openssl::ssl::{SslConnector, SslMethod};
use postgres_openssl::MakeTlsConnector;
use serde::Deserialize;
// use config;
use std::env;

#[allow(unused_imports)]
use crate::{function, logger_debug, logger_error, logger_info, logger_panic};

use crate::change_processing::DdlChange;
use crate::file_uploader::CleoS3File;
use crate::parser::{ChangeKind, ColumnInfo, SchemaAndTable, TableName};

pub const DEFAULT_NUMERIC_PRECISION: i32 = 19; // 99_999_999_999.99999999
pub const DEFAULT_NUMERIC_SCALE: i32 = 8;

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

    pub async fn handle_ddl(
        &self,
        ddl_change: &DdlChange,
        wal_file_number: u64,
    ) -> Result<(), DatabaseWriterError> {
        let table_name = ddl_change.table_name();
        let alter_table_statement = match ddl_change {
            DdlChange::AddColumn(column_info, table_name) => {
                self.add_column_statement(column_info, table_name)
            }
            DdlChange::RemoveColumn(column_info, table_name) => {
                self.remove_column_statement(column_info, table_name)
            }
        };
        let client = self
            .get_connection_from_pool(wal_file_number, &table_name)
            .await?;

        self.execute_single_query(
            &client,
            alter_table_statement.as_str(),
            &format!("alter_table_statement:{:?}", ddl_change),
            table_name.clone(),
            wal_file_number,
        )
        .await?;

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

    async fn get_connection_from_pool(
        &self,
        wal_file_number: u64,
        table_name: &TableName,
    ) -> Result<Client, DatabaseWriterError> {
        let client = self.connection_pool.get().await;
        match client {
            Ok(ok) => Ok(ok),
            Err(err) => {
                logger_error!(
                    Some(wal_file_number),
                    Some(table_name),
                    &format!("error_getting_connection:{:?}", err)
                );
                Err(err).map_err(DatabaseWriterError::PoolError)
            }
        }
    }

    pub async fn apply_s3_changes(
        &self,
        s3_file: &mut CleoS3File,
    ) -> Result<(), DatabaseWriterError> {
        let kind = &s3_file.kind;
        let table_name = &s3_file.table_name;
        let wal_file_number = s3_file.wal_file.file_number;
        // temp tables are present in the session, so we still need to drop it at the end of the transaction
        let remote_filepath = s3_file.remote_path();
        logger_info!(
            Some(wal_file_number),
            Some(&table_name),
            &format!("begin_import:{}", remote_filepath)
        );

        let client = self
            .get_connection_from_pool(wal_file_number, table_name)
            .await?;

        // let transaction = client.transaction().await.unwrap();
        let transaction = client;
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

        let access_key_id =
            env::var("AWS_ACCESS_KEY_ID").expect("Unable to find AWS_ACCESS_KEY_ID");
        let secret_access_key =
            env::var("AWS_SECRET_ACCESS_KEY").expect("Unable to find AWS_SECRET_ACCESS_KEY");
        let credentials_string = format!(
            "aws_access_key_id={aws_access_key_id};aws_secret_access_key={secret_access_key}",
            aws_access_key_id = access_key_id,
            secret_access_key = secret_access_key
        );
        let column_list = self.column_name_list(&s3_file.columns);
        // no gzip
        let copy_to_staging_table = format!(
            "copy \"{staging_name}\" ({column_list}) from '{remote_filepath}' CREDENTIALS '{credentials_string}' GZIP CSV TRUNCATECOLUMNS IGNOREHEADER 1 DELIMITER ',' NULL as '\\0' compupdate off statupdate off",
            staging_name = &staging_name,
            column_list = &column_list,
            remote_filepath = &remote_filepath,
            credentials_string = &credentials_string,
        );

        let data_migration_query_string = self.query_for_change_kind(
            kind,
            staging_name.as_ref(),
            just_table_name.as_ref(),
            schema_name.as_ref(),
            &s3_file.columns,
        );
        let drop_staging_table = format!("drop table if exists {}", &staging_name);

        self.execute_single_query(
            &transaction,
            create_staging_table.as_str(),
            &format!("create_staging_table:{}", &remote_filepath),
            table_name.clone(),
            wal_file_number,
        )
        .await?;

        let result = self
            .execute_single_query(
                &transaction,
                copy_to_staging_table.as_str(),
                &format!("copy_to_staging_table:{}", &remote_filepath),
                table_name.clone(),
                wal_file_number,
            )
            .await;
        match result {
            Ok(..) => {}
            Err(err) => {
                if let DatabaseWriterError::TokioError(tokio_error) = err {
                    // https://github.com/sfackler/rust-postgres/blob/master/tokio-postgres/src/error/mod.rs
                    // I can't find a better way to determine if something is a Kind::Db. since kind is private.
                    let error_string = format!("{}", tokio_error);
                    // we bail early if we have a db error here, as something is wrong.
                    if error_string.starts_with("db error") {
                        s3_file.wal_file.register_error();
                        logger_panic!(
                            Some(wal_file_number),
                            Some(&table_name),
                            &format!("copy_to_staging_table_got_error:{:?}", tokio_error)
                        );
                    } else {
                        // we throw back up to kick in the retry mechanism
                        // need to recreate it because it's partially moved
                        // by our match
                        Err(DatabaseWriterError::TokioError(tokio_error))?
                    }
                } else {
                    logger_panic!(
                        Some(wal_file_number),
                        Some(&table_name),
                        "non_tokio_error_from_execute_single_query"
                    )
                }
            }
        }
        self.execute_single_query(
            &transaction,
            data_migration_query_string.as_str(),
            &format!("apply_changes_to_real_table:{}", &remote_filepath),
            table_name.clone(),
            wal_file_number,
        )
        .await?;

        self.execute_single_query(
            &transaction,
            drop_staging_table.as_str(),
            &format!("drop_staging_table:{}", &remote_filepath),
            table_name.clone(),
            wal_file_number,
        )
        .await?;

        // TEMP
        // serialiseable isolation error. might be to do with dms.
        // transaction.commit().await.unwrap();
        // info!("COMMITTED TX {}", table_name);

        logger_info!(
            Some(wal_file_number),
            Some(&table_name),
            &format!("finished_importing:{}", &remote_filepath)
        );

        s3_file.wal_file.maybe_remove_wal_file();

        Ok(())
    }

    async fn execute_single_query(
        &self,
        client: &Client,
        query_to_execute: &str,
        log_tag: &str,
        table_name: TableName,
        wal_file_number: u64,
    ) -> Result<(), DatabaseWriterError> {
        logger_info!(
            Some(wal_file_number),
            Some(&table_name),
            &format!("about_to_execute:{}", log_tag)
        );
        logger_debug!(
            Some(wal_file_number),
            Some(&table_name),
            &format!(
                "about_to_execute:{} full_query:{}",
                log_tag, query_to_execute
            )
        );
        let result = client.execute(query_to_execute, &[]).await;
        match result {
            Ok(..) => {
                logger_info!(
                    Some(wal_file_number),
                    Some(&table_name),
                    &format!("successfully_executed:{}", log_tag)
                );
                Ok(())
            }
            Err(err) => {
                logger_error!(
                    Some(wal_file_number),
                    Some(&table_name),
                    &format!("error_executing:{} err:{:?}", log_tag, err)
                );
                Err(err).map_err(DatabaseWriterError::TokioError)
            }
        }
    }

    // bool is whether we return early. Only necessary for delete where the table
    // does not exist
    async fn create_table_if_not_exists(
        &self,
        s3_file: &CleoS3File,
        database_client: &Client,
    ) -> Result<bool, DatabaseWriterError> {
        let (schema_name, just_table_name) = s3_file.table_name.schema_and_table_name();
        let table_name = s3_file.table_name.clone();
        let wal_file_number = s3_file.wal_file.file_number;
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
                logger_error!(
                    Some(wal_file_number),
                    Some(&table_name),
                    "delete_when_theres_no_table"
                );
                return Ok(true);
            } else if s3_file.kind == ChangeKind::Update {
                logger_panic!(
                    Some(wal_file_number),
                    Some(&table_name),
                    "update_when_theres_no_table"
                );
            }

            logger_info!(
                Some(wal_file_number),
                Some(&table_name),
                "creating_table_that_doesnt_exist"
            );

            // TODO: distkey
            let create_table_query = format!(
                "create table \"{schema_name}\".\"{just_table_name}\" ({columns}) SORTKEY(id)",
                schema_name = schema_name,
                just_table_name = just_table_name,
                columns = self.values_description_for_table(&s3_file.columns)
            );

            self.execute_single_query(
                &database_client,
                create_table_query.as_str(),
                "create_table_statement",
                table_name.clone(),
                wal_file_number,
            )
            .await?;
        } // else the table exists and do nothing
        Ok(false)
    }

    fn staging_name<'a>(&self, s3_file: &'a CleoS3File) -> String {
        // s3://bucket/path/schema.table_name_insert.tar.gz -> table_name_insert_staging
        //                         ^^^^^^^^^^^^^^^^^
        // unwrap, because if this isn't true, it's a logic error
        let remote_filename = &s3_file.remote_filename;
        let last_slash = &remote_filename[remote_filename
            .rfind('/')
            .expect("Unable to find / in s3 filename")
            + 1..];
        let dot_after_last_slash = &last_slash[last_slash
            .find('.')
            .expect("Unable to find dot after schema in s3 filename")
            + 1..];
        let dot_until_dot = &dot_after_last_slash[..dot_after_last_slash
            .find('.')
            .expect("Unable to find file extension . in s3 file name")];
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
    // NOTE: you also need to remove quotes from the column name
    fn column_and_type_for_column(&self, column_info: &ColumnInfo) -> String {
        format!(
            "\"{column_name}\" {column_type}",
            column_name = column_info.column_name().replace("\"", ""),
            column_type = self.column_type_mapping(column_info.column_type()).as_str()
        )
    }

    fn column_name_list(&self, columns: &Vec<ColumnInfo>) -> String {
        columns
            .iter()
            .map(|x| format!("\"{}\"", x.column_name().replace("\"", ""))) // replace any quotes and wrap in quotes
            .collect::<Vec<_>>()
            .join(",")
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
                        .map(|x| x.column_name().replace("\"", ""))
                        .map(|x| format!("\"{}\" = s.\"{}\"", x, x))
                        .collect::<Vec<_>>()
                        .join(","),
                    staging_name = &staging_name
                )
            }
        }
    }

    fn column_type_mapping(&self, column_type: &str) -> String {
        // Postgres and Redshift have different default precision and scale for numerics. This is a workaround that prevents us from losing the information to the right of the decimal point during replication.
        let numeric_type = &format!(
            "NUMERIC({},{})",
            DEFAULT_NUMERIC_PRECISION, DEFAULT_NUMERIC_SCALE
        );
        // {"boolean", "double precision", "integer", "interval", "numeric", "public.hstore", "timestamp without time zone", "text", "character varying", "json", "bigint", "public.citext", "date", "uuid", "jsonb"}
        let return_type = match column_type {
            "text" => "CHARACTER VARYING(65535)",
            "json" => "CHARACTER VARYING(65535)",
            "jsonb" => "CHARACTER VARYING(65535)",
            "bytea" => "CHARACTER VARYING(65535)",
            "oid" => "CHARACTER VARYING(65535)",
            "ARRAY" => "CHARACTER VARYING(65535)",
            "array" => "CHARACTER VARYING(65535)",
            "USER-DEFINED" => "CHARACTER VARYING(65535)",
            "public.citext" => "CHARACTER VARYING(65535)",
            "public.hstore" => "CHARACTER VARYING(65535)",
            "uuid" => "CHARACTER VARYING(36)",
            "interval" => "CHARACTER VARYING(65535)",
            "numeric" => numeric_type,
            _ => column_type,
        };
        return_type.to_string()
    }
}
