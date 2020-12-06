use deadpool_postgres::{ManagerConfig, Pool, RecyclingMethod };
use openssl::ssl::{SslConnector, SslMethod};
use postgres_openssl::MakeTlsConnector;
use serde::Deserialize;
// use config;
use dotenv::dotenv;
use std::env;

#[allow(unused_imports)]
use log::{debug, error, log_enabled, info, Level};

use crate::file_uploader::CleoS3File;
use crate::parser::ChangeKind;


pub struct DatabaseWriter {
    connection_pool: Pool
}

#[derive(Debug, Deserialize)]
struct Config {
    pg: deadpool_postgres::Config
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
            connection_pool: DatabaseWriter::create_connection_pool()
        }
    }

    fn create_connection_pool() -> Pool {
        dotenv().ok();
        let mut cfg = Config::from_env().unwrap();
        // cfg.dbname = Some("cleo_development".to_string());
        cfg.pg.manager = Some(ManagerConfig { recycling_method: RecyclingMethod::Fast });
        let builder = SslConnector::builder(SslMethod::tls()).expect("fuck");
        let connector = MakeTlsConnector::new(builder.build());
        cfg.pg.create_pool(connector).unwrap()
    }

    pub async fn import_table(&self, s3_file: &CleoS3File) {
        let CleoS3File{ kind, table_name,.. } = s3_file;
        if kind != &ChangeKind::Insert  {
            return
        }
        if ["public.transaction_descriptions", "public.user_relationships_timestamps", "transactions"].contains(&table_name.as_str()) {
            return
        }
        // temp tables are present in the session, so we still need to drop it at the end of the transaction
        info!("BEGIN INSERT {}", table_name);
        let client = self.connection_pool.get().await.unwrap();
        // let transaction = client.transaction().await.unwrap();
        let transaction = client;
        info!("GOT CONNECTION {}", table_name);
        let (schema_name, just_table_name) = table_name.split_once('.').unwrap();
        assert!(!table_name.contains('"'));
        // TODO: escaping, check the table name is well formed
        let staging_name = format!("{}_staging", just_table_name);
        let create_staging_table = format!("create temp table \"{}\" (like \"{}\".\"{}\")", &staging_name, &schema_name, &just_table_name);
        // make sure we drop it
        // TODO: escaping, check the table name is well formed
        let remote_filepath = s3_file.remote_path();
        let access_key_id = env::var("AWS_ACCESS_KEY_ID").unwrap();
        let secret_access_key = env::var("AWS_SECRET_ACCESS_KEY").unwrap();
        let credentials_string = format!("aws_access_key_id={aws_access_key_id};aws_secret_access_key={secret_access_key}", aws_access_key_id=access_key_id, secret_access_key=secret_access_key);
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
            staging_name=&staging_name,
            remote_filepath=&remote_filepath,
            credentials_string=&credentials_string);

        let data_migration_query_string = self.query_for_change_kind(kind, staging_name.as_ref(), just_table_name.as_ref(), schema_name.as_ref());
        let drop_staging_table = format!("drop table if exists {}_staging", &table_name);
        // let insert_query = format!();
        transaction.execute(create_staging_table.as_str(), &[]).await.unwrap();
        info!("CREATED STAGING TABLE {}", table_name);
        transaction.execute(copy_to_staging_table.as_str(), &[]).await.unwrap();
        info!("COPIED TO STAGING TABLE {}", table_name);
        transaction.execute(data_migration_query_string.as_str(), &[]).await.unwrap();
        info!("INSERTED FROM STAGING TABLE {}", table_name);
        transaction.execute(drop_staging_table.as_str(), &[]).await.unwrap();
        info!("DROPPED STAGING TABLE {}", table_name);
        // TEMP
        // serialiseable isolation error. might be to do with dms.
        // transaction.commit().await.unwrap();
        info!("COMMITTED TX {}", table_name);

        info!("INSERTED {} {}", &remote_filepath, table_name);
    }

    fn query_for_change_kind(&self, kind: &ChangeKind, staging_name: &str, table_name: &str, schema_name: &str ) -> String {
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
            },
            ChangeKind::Delete => {
                format!(
                    "delete from \"{schema_name}\"\"{table_name}\" where id in (select id from \"{staging_name}\")",
                    schema_name=&schema_name,
                    table_name=&table_name,
                    staging_name=&staging_name
                )
            }
            _ => { unreachable!()}
        }
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
