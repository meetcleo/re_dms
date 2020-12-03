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
* `main.rs` does exactly what it says on the tin.
