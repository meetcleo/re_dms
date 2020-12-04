use deadpool_postgres::{Config, Manager, ManagerConfig, Pool, RecyclingMethod };
use openssl::ssl::{SslConnector, SslMethod};
use postgres_openssl::MakeTlsConnector;

use dotenv::dotenv;
use std::env;

pub struct DatabaseWriter {

}

impl DatabaseWriter {
    pub fn new() -> DatabaseWriter {
        DatabaseWriter {}
    }
    pub async fn test(&self) {
        let mut cfg = Config::new();
        cfg.dbname = Some("cleo_development".to_string());
        cfg.manager = Some(ManagerConfig { recycling_method: RecyclingMethod::Fast });
        let mut builder = SslConnector::builder(SslMethod::tls()).expect("fuck");
        // builder.set_ca_file("database_cert.pem")?;
        let connector = MakeTlsConnector::new(builder.build());
        let pool = cfg.create_pool(connector).unwrap();
        for i in 1..10 {
            let mut client = pool.get().await.unwrap();
            let stmt = client.prepare("SELECT 1 + $1").await.unwrap();
            let rows = client.query(&stmt, &[&i]).await.unwrap();
            let value: i32 = rows[0].get(0);
            assert_eq!(value, i + 1);
            println!("{}", value);
        }
    }
}
