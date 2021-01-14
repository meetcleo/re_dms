#![feature(str_split_once)]
#![deny(warnings)]

use dotenv::dotenv;

mod change_processing;
mod database_writer;
mod database_writer_threads;
mod exponential_backoff;
mod file_uploader;
mod file_uploader_threads;
mod file_writer;
mod input_manager;
mod logger;
mod parser;
mod wal_file_manager;

#[tokio::main]
async fn main() {
    dotenv().ok();
    env_logger::init();

    let manager = input_manager::InputManager::new();
    manager.run().await
}
