# Postgres to Redshift v2
* aims to provide a client to stream replication from postgres to redshift.

## Outline
* reads input data from a `test_decoding` replication slot
* will process these changes batch applies them
* then create a bunch of csv files to upload to s3
* it will then upload all of this data to s3
* process them loading them into redshift.
* NOTE: any text based columns that have a single null byte as the value of the text will come through as null values (we could fix this, but _come on!_).

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

Starts re_dms, which will start logical replication using `pg_recvlogical`:

`$ ./target/release/re_dms`

Docs on `pg_recvlogical` [here](https://www.postgresql.org/docs/10/app-pgrecvlogical.html)

### Errors
* any errors sending to a channel are logic errors, so panic.

## Deploying to EC2

### Pre-requisites

1. Have ansible installed locally
1. Have Docker running locally
1. Have a target instance with the following:
    1. Debian or Ubuntu (based on `Buster`) installed
    1. Writable directory (ideally with persistent storage) for keeping WAL files
    1. Ability to communicate with source and target DB
1. SSH config for target instance, name of connection specified in `hosts` file (copy from `hosts.example`)
1. `roles/re_dms/files/re_dms.conf.example` copied to `roles/re_dms/files/re_dms.conf`, including the following:
    1. Write creds for Redshift
    1. S3 bucket for storing changes to be applied
    1. AWS creds for writing to S3 bucket
    1. Connection string to source DB, source DB needs to have logical replication enabled - user needs to either be a superuser or have replication privileges
    1. Name of the replication slot to be used (one will be created if it does not already exists)

### Commands

Build the executable for Linux:

`make build`

_We use a Docker container to build an executable that can run on Linux to avoid cross-compiling. Rust has decent cross-compilation support, but dependencies like SSL libraries are harder to support._

Deploy using ansible:

`make deploy`

Clean any build artefacts:

`make clean`

## Runbook

Managing the status|start|stop|restart of the re_dms service:

`$ sudo systemctl status|start|stop|restart re_dms`

Tailing the logs of the re_dms service:

`$ sudo journalctl -f -u re_dms`


## configuring cloudwatch (optional)
```
$ ansible-galaxy install christiangda.amazon_cloudwatch_agent
$ ansible-playbook -i hosts re_dms.yml --tags cloudwatch
```
