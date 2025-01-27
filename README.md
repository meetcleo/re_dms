# re_dms (Postgres to redshift streaming replication)
re_dms (DMS stands for database migration system) is a project that provides a client that will use [postgresql's logical replication](https://www.postgresql.org/docs/current/logical-replication.html) to stream data to [amazon redshift](https://aws.amazon.com/redshift/).

At [Cleo](https://web.meetcleo.com/) we use redshift for our analytics database, and postgres for our production database. In order to run our business analytics we replicate a lot of our production data to our analytics database.
We used to use [postgres_to_redshift](https://github.com/toothrot/postgres_to_redshift) to do this replication until the data needed to replicate became too large for this. We changed this to [perform incrementally](https://github.com/meetcleo/postgres_to_redshift) for a while but the volume of data still grew to be too large.
We started evulauting other tools like [Amazon's DMS](https://aws.amazon.com/dms/) and some others. Ultimately, the way DMS fails to batch changes meant it wasn't performant enough to handle our throughput, so we built this tool to solve our problem.

This project provides
* The client itself.
* a systemd service to handle running the client
* a Makefile and docker based build system (targetting ubuntu)
* (Optional) integration with an error reporting service ([Sentry](https://sentry.io))
* an ansible script to allow you to deploy this service.
* cloudwatch configuration and metrics integration for the service.

## Client features
* Will use logical replication to stream postgres data to redshift (duh)
* Will create new tables on the target redshift database when new tables are created (as soon as data is written into them).
* Will add new columns to the target redshift database when new columns are added to a table on the source database.
* Will also drop columns on the target redshift database when columns are removed from a table in the source database.
* Handles [some idiosynchrasies](https://docs.aws.amazon.com/redshift/latest/dg/r_Numeric_types201.html) to do with the redshift numeric type by saturating it to the maximum value allowed by the type. (redshift happens to store values with 19 precision as a 64 bit int.)
* Handles some type conversions. [see here](https://github.com/meetcleo/re_dms/blob/master/src/database_writer.rs#L712-L735).
* Truncates values (e.g. text fields) so that they will fit into the destination column size.

## Limitations
* The client assumes, and requires that all tables that are being replicated have a unique column called `id` as the primary key. This column can either be a UUID or integer type.
* The default `NUMERIC` type is hardcoded to `NUMERIC(19,8)` (this could easily be changed).
* Column types that are not specified in the mapping linked above, and are not common to both postgres and redshift will not work.
* Truncates values (e.g. text fields) so that they will fit into the destination column size.
* Will not apply changes to redshift until the next data is received after the configured timelimit (or bytelimit) for the wal file switchover (or when it is shutdown).

## Running locally

### Prereqs

Install Rust:

`$ brew install rustup-init`

`$ rustup-init` (selecting the `nightly` build) or else `$ rustup install nightly`

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

Or, pass your own stream into re_dms:

`$ cat data/test_decoding.txt | ./target/release/re_dms --stdin`

Docs on `pg_recvlogical` [here](https://www.postgresql.org/docs/10/app-pgrecvlogical.html)

### Errors
* any errors sending to a channel are logic errors, so panic.

## Deploying to EC2

### Pre-requisites

1. Have ansible installed locally, including the following collections (`ansible-galaxy collection install ...`):
    - `community.general`
    - `ansible.posix`
1. Have Docker running locally
1. Have a target instance with the following:
    1. Debian or Ubuntu (based on `Noble`) installed
    1. Writable directory (ideally with persistent storage) for keeping WAL files
    1. Ability to communicate with source and target DB
1. SSH config for target instance, name of connection specified in `hosts` file (copy from `hosts.example`)
1. `roles/re_dms/files/re_dms.conf.example` copied to `roles/re_dms/files/re_dms.conf.[staging|production]`, including the following:
    1. Write creds for Redshift
    1. S3 bucket for storing changes to be applied
    1. AWS creds for writing to S3 bucket
    1. Connection string to source DB, source DB needs to have logical replication enabled - user needs to either be a superuser or have replication privileges
    1. Name of the replication slot to be used (one will be created if it does not already exists)

### Commands

Build the executable for Linux:

`make build`

_We use a Docker container to build an executable that can run on Linux to avoid cross-compiling. Rust has decent cross-compilation support, but dependencies like SSL libraries are harder to support._
_NOTE: the rollbar feature is enabled by default in the docker build called by the makefile._

Deploy using ansible:

`make deploy`

Clean any build artefacts:

`make clean`

## Runbook

Managing the status|start|stop|restart of the re_dms service:

`$ sudo systemctl status|start|stop|restart re_dms`

Tailing the logs of the re_dms service:

`$ sudo journalctl -f -u re_dms`

## Monitoring

### configuring rollbar (optional)
to build with rollbar error reporting you need to build with:
```
cargo build --features with_sentry
```
and when running you need to specify the `ROLLBAR_ACCESS_TOKEN` environment variable.

## configuring cloudwatch (optional)
for details on the metrics and file format see [metrics](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/metrics-collected-by-CloudWatch-agent.html#linux-metrics-enabled-by-CloudWatch-agent) and [config file format](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/CloudWatch-Agent-Configuration-File-Details.html)
```
$ ansible-galaxy install christiangda.amazon_cloudwatch_agent
$ pip install boto # needed for creating the log group with community.aws.cloudwatchlogs_log_group
$ ansible-galaxy collection install community.aws
$ aws sso login
$ ansible-playbook -i hosts re_dms.yml --tags cloudwatch -e "env=[staging|production]" -e "ansible_python_interpreter=python3" -e "cloudwatch_aws_access_key_id=SOME_ACCESS_KEY_ID" -e "cloudwatch_aws_access_key_secret=SOME_SECRET" # or however you want to provide these variables
```
_I needed to tell ansible to use my system python3 intepreter in order to find the additional libraries it needed_

## How it works
* reads input data from a `test_decoding` logical replication slot.
* It saves this data as soon as it comes in into a "WAL" file. (this allows picking up and restarting).
* will process these changes and batches any changes together (There will only be 1 change per row, so a `create` followed by an `update` gets aggregated into a single change e.t.c.)
* then will create a bunch of gzipped csv files containing the inserts/updates/deletes for each table.
* concurrently for all tables it will:
  * upload all of this csv files to s3.
  * process them loading them into redshift.
* NOTE: any text based columns that have a single null byte as the value of the text will come through as null values (we could fix this, but _come on!_).


## Code structure
* the `wal_file_manager.rs` handles writing the wal file, and then splitting it into multiple sections. (when the wal file splits, either by a configurable timeperiod elapsing, or the wal file reaching a configurable byte limit, the batched changes will be written to redshift)
* files are parsed into structures by `parser.rs`
* files are then collected into data structures in `change_processing.rs`
* files are written via `file_writer.rs`
* structs representing these files are passed on to the `file_uploader_threads`.
* This reads from a single channel, and starts a new task for each distinct table (unless the task has already been started otherwise it uses the existing channel) giving it a channel. The new task will receive tables passed to the channel and sequentially upload files to s3 (via `file_uploader`), then posting the resulting CleoS3File to an output channel.
* this output channel leads to a `database_writer_threads`.
* similar to the `file_uploader_threads` this will read from the channel, and start a new task for each distinct table name (unless a task has already been started, otherwise it will reuse the channel). It will then send the `CleoS3File` to this task, which will process each `CleoS3File` and import it into the database via the `database_writer`.
* `main.rs` does exactly what it says on the tin and runs the input loop, sending the results onwards through the pipeline. Initial files are written synchronously (`file_writer`).

NOTE: this isn't actually threading, it's only based on async tasks and a few event loops. I use the term thread throughout since it's conceptually simpler.

### Implementation note about TOAST-ed columns.
* The design of re_dms has been influenced by how postgres treats [TOAST](https://www.postgresql.org/docs/current/storage-toast.html)-ed columns, and how they show up (or rather don't) through logical replication.
* As a reminder, TOAST stands for "The Oversized attribute storage technique". When a single column has a value that is greater than a certain number of bytes, postgres moves this data to a separate area and stores this data there.
* When an update is made to a row that has a TOASTed column, if the column itself is updated to have new data, then there is no problem, and the new data appears in the logical replication stream.
* However, if an update is mode to a row that has a TOASTed column, that does not update the data within the TOASTed column, then the value of the data in the toasted column is _not_ provided in the logical replication stream.
* This means for every table that has toasted columns, we may need to be able to update the rows both where the column has changed, and where it hasn't changed. This means for a single toasted column, we need to be able to generate 2 different update files, and in the general case, we need to be able to handle updates for any subset of columns.
* For this tool, we also need to be able to distinguish this case from the case where a column has been dropped (since we keep the schema of the postgresql source, and the redshift target in sync.)
* For this reason, we use the `test_decoding` plugin for postgres, as this exposes the data of whether the absense of data is due to an unchanged toast column, or because a column doesn't exist.

## Architecture diagram
https://drive.google.com/file/d/1L2Hd8hW8nhLKLGqcS1TkBWd1czcEc49x/view?usp=sharing

## Contributing
* feel free to open an issue or PR with any problems you run into, or suggestions for improvements.

## License
MIT license.
