use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod };
use openssl::ssl::{SslConnector, SslMethod};
use postgres_openssl::MakeTlsConnector;
use serde::Deserialize;
use config;


use dotenv::dotenv;
use std::env;

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

    pub async fn test(&self) {
        let ref pool = &self.connection_pool;
        let foo: Vec<_> = (1..2).map(|i| async move {
            let client = pool.get().await.unwrap();
            let stmt = client.prepare("SELECT count(*) from users").await.unwrap();
            let rows = client.query(&stmt, &[]).await.unwrap();
            let value: i32 = rows[0].get(0);
            assert_eq!(value, i + 1);
            println!("{}", value);
        }).collect();
        futures::future::join_all(foo).await;
    }
}
