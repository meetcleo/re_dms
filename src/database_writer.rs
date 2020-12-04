use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod };
use openssl::ssl::{SslConnector, SslMethod};
use postgres_openssl::MakeTlsConnector;
use serde::Deserialize;
use config;


use dotenv::dotenv;
use std::env;

pub struct DatabaseWriter {

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
        DatabaseWriter {}
    }
    pub async fn test(&self) {
        dotenv().ok();
        let mut cfg = Config::from_env().unwrap();
        // cfg.dbname = Some("cleo_development".to_string());
        cfg.pg.manager = Some(ManagerConfig { recycling_method: RecyclingMethod::Fast });
        let builder = SslConnector::builder(SslMethod::tls()).expect("fuck");
        // builder.set_ca_file("database_cert.pem")?;
        let connector = MakeTlsConnector::new(builder.build());
        let ref pool = cfg.pg.create_pool(connector).unwrap();

        let foo: Vec<_> = (1..10).map(|i| async move {
            let client = pool.get().await.unwrap();
            let stmt = client.prepare("SELECT 1 + $1").await.unwrap();
            let rows = client.query(&stmt, &[&i]).await.unwrap();
            let value: i32 = rows[0].get(0);
            assert_eq!(value, i + 1);
            println!("{}", value);
        }).collect();
        futures::future::join_all(foo).await;
    }
}
