# Postgres to Redshift v2
* aims to provide a client to stream replication from postgres to redshift.

## Outline
* reads input data from a `test_decoding` replication slot
* will process these changes batch applies them
* then create a bunch of csv files to upload to s3
* it will then upload all of this data to s3
* process them loading them into redshift.

## Structure
* files are parsed into structures by `parser.rs`
* files are then collected into data structures in `change_processing.rs`
* files are written via `file_writer.rs`
* structs representing these files are passed on to the `file_uploader_threads`.
* This reads from a single channel, and starts a new task for each distinct table (unless the task has already been started otherwise it uses the existing channel) giving it a channel. The new task will receive tables passed to the channel and sequentially upload files to s3 (via `file_uploader`), then posting the resulting CleoS3File to an output channel.
* this output channel leads to a `database_writer_threads`.
* similar to the `file_uploader_threads` this will read from the channel, and start a new task for each distinct table name (unless a task has already been started, otherwise it will reuse the channel). It will then send the `CleoS3File` to this task, which will process each `CleoS3File` and import it into the database via the `database_writer`.
* `main.rs` does exactly what it says on the tin and runs the input loop, sending the results onwards through the pipeline. Initial files are written synchronously (`file_writer`).

NOTE: this isn't actually threading, it's only based on async tasks and a few event loops. I use the term thread throughout since it's conceptually simpler.

## Architecture diagram
https://drive.google.com/file/d/1L2Hd8hW8nhLKLGqcS1TkBWd1czcEc49x/view?usp=sharing

## Running locally

### Prereqs

Install Rust:

`$ brew install rustup-init`

`$ rustup install nightly`

`$ source "$HOME/.cargo/env"`

Ensure your local postgres has wal replication enabled, on OSX:

`wal_level = logical` in ` /usr/local/var/postgres/postgresql.conf` (requires DB restart)

Ensure the tests pass:

* NOTE: currently the tests modify some state in a testing directory (in tmp). Because of this, to have stable test runs you need to set `RUST_TEST_THREADS=1` or `cargo test -- --test-threads=1`

`$ cargo test -- --test-threads=1`

### Build and run

Build re_dms:

`$ cargo build --release`

Start logical replication, piping it into re_dms:

`$ pg_recvlogical --create-slot --start --if-not-exists --fsync-interval=0 --file=- --plugin=test_decoding --slot=re_dms -d postgresql://<you>@localhost/cleo_development | ./target/release/re_dms`

Docs on `pg_recvlogical` [here](https://www.postgresql.org/docs/10/app-pgrecvlogical.html)

### Errors
* any errors sending to a channel are logic errors, so panic.
