use chrono::{DateTime, Duration, NaiveDateTime, Utc};
use lazy_static::lazy_static;
use std::env;

#[allow(unused_imports)]
use crate::{function, logger_debug, logger_error, logger_info, logger_panic};

lazy_static! {
    /// Threshold in seconds for filtering old transactions
    /// Transactions with updated_at older than (wal_creation_time - threshold) will be ignored
    static ref TRANSACTION_AGE_THRESHOLD_SECONDS: i64 = env::var("TRANSACTION_AGE_THRESHOLD_SECONDS")
        .unwrap_or("3600".to_string()) // Default: 1 hour
        .parse::<i64>()
        .expect("TRANSACTION_AGE_THRESHOLD_SECONDS must be a valid integer");
}

/// Result of filtering a transaction
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum FilterResult {
    /// Process the transaction normally
    Process,
    /// Skip the transaction due to age
    Skip(String), // reason for skipping
}

/// Check if a transaction should be filtered based on its updated_at timestamp
/// Returns Some(FilterResult) if the transaction has an updated_at column, None otherwise
///
/// # Arguments
/// * `wal_creation_time` - When the current WAL file was created
/// * `parsed_line` - The parsed transaction line to check
///
/// # Returns
/// * `Some(FilterResult::Process)` if the transaction should be processed
/// * `Some(FilterResult::Skip(reason))` if the transaction should be skipped
/// * `None` if no updated_at column found (don't filter)
pub fn should_filter_transaction(wal_creation_time: DateTime<Utc>, parsed_line: &crate::parser::ParsedLine) -> Option<FilterResult> {
    use crate::parser::{Column, ColumnValue, ParsedLine};

    if let ParsedLine::ChangedData { columns, .. } = parsed_line {
        // Look for an updated_at column in the transaction
        for column in columns {
            if column.column_name() == "updated_at" {
                // Found updated_at column, check if it has a value
                match column {
                    Column::ChangedColumn { value: Some(ColumnValue::Text(timestamp_str)), .. } => {
                        return Some(filter_transaction_by_timestamp(wal_creation_time, timestamp_str));
                    }
                    Column::ChangedColumn { value: None, .. } => {
                        // updated_at is NULL, process the transaction
                        return Some(FilterResult::Process);
                    }
                    Column::UnchangedToastColumn { .. } => {
                        // Toast column, we can't check the timestamp, so process it
                        return Some(FilterResult::Process);
                    }
                    _ => {
                        // Other column value types for updated_at (unexpected), process it
                        return Some(FilterResult::Process);
                    }
                }
            }
        }
    }
    // No updated_at column found, don't filter
    None
}

/// Filter a transaction based on its updated_at timestamp relative to WAL creation time
///
/// # Arguments
/// * `wal_creation_time` - When the current WAL file was created
/// * `updated_at_str` - The updated_at timestamp as a string (postgres format)
///
/// # Returns
/// * `FilterResult::Process` if the transaction should be processed
/// * `FilterResult::Skip(reason)` if the transaction should be skipped
pub fn filter_transaction_by_timestamp(wal_creation_time: DateTime<Utc>, updated_at_str: &str) -> FilterResult {
    let updated_at = match parse_timestamp(updated_at_str) {
        Ok(dt) => dt,
        Err(e) => {
            logger_debug!(
                None,
                None,
                &format!("Failed to parse updated_at timestamp '{}': {}", updated_at_str, e)
            );
            // If we can't parse the timestamp, process the transaction to be safe
            return FilterResult::Process;
        }
    };

    let threshold_duration = Duration::seconds(*TRANSACTION_AGE_THRESHOLD_SECONDS);
    let cutoff_time = wal_creation_time - threshold_duration;

    if updated_at < cutoff_time {
        let age_seconds = (wal_creation_time - updated_at).num_seconds();
        let reason = format!(
            "Transaction updated_at {} is {} seconds older than WAL age threshold ({}s)",
            updated_at_str,
            age_seconds,
            *TRANSACTION_AGE_THRESHOLD_SECONDS
        );

        logger_debug!(
            None,
            None,
            &format!("Filtering old transaction: {}", reason)
        );

        FilterResult::Skip(reason)
    } else {
        FilterResult::Process
    }
}

/// Parse a PostgreSQL timestamp string to UTC DateTime
/// Handles PostgreSQL "timestamp without time zone" format: "2020-11-27 15:31:21.771279"
fn parse_timestamp(timestamp_str: &str) -> Result<DateTime<Utc>, String> {
    // Try parsing with microseconds first (our standard format)
    if let Ok(naive_dt) = NaiveDateTime::parse_from_str(timestamp_str, "%Y-%m-%d %H:%M:%S%.f") {
        return Ok(DateTime::from_utc(naive_dt, Utc));
    }

    // Fallback: try parsing without microseconds
    if let Ok(naive_dt) = NaiveDateTime::parse_from_str(timestamp_str, "%Y-%m-%d %H:%M:%S") {
        return Ok(DateTime::from_utc(naive_dt, Utc));
    }

    Err(format!("Unable to parse timestamp: {}", timestamp_str))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Datelike, TimeZone, Utc};

    #[test]
    fn test_filter_recent_transaction() {
        let wal_time = Utc.ymd(2023, 1, 1).and_hms(12, 0, 0);

        // Transaction from 30 minutes ago (within 1 hour threshold)
        let recent_transaction = "2023-01-01 11:30:00.000000";
        assert_eq!(filter_transaction_by_timestamp(wal_time, recent_transaction), FilterResult::Process);
    }

    #[test]
    fn test_filter_old_transaction() {
        let wal_time = Utc.ymd(2023, 1, 1).and_hms(12, 0, 0);

        // Transaction from 2 hours ago (beyond 1 hour threshold)
        let old_transaction = "2023-01-01 10:00:00.000000";
        if let FilterResult::Skip(reason) = filter_transaction_by_timestamp(wal_time, old_transaction) {
            assert!(reason.contains("7200 seconds older"));
        } else {
            panic!("Expected transaction to be filtered out");
        }
    }

    #[test]
    fn test_filter_boundary_transaction() {
        let wal_time = Utc.ymd(2023, 1, 1).and_hms(12, 0, 0);

        // Transaction exactly at the threshold (1 hour ago)
        let boundary_transaction = "2023-01-01 11:00:00.000000";
        assert_eq!(filter_transaction_by_timestamp(wal_time, boundary_transaction), FilterResult::Process);
    }

    #[test]
    fn test_filter_just_beyond_threshold() {
        let wal_time = Utc.ymd(2023, 1, 1).and_hms(12, 0, 0);

        // Transaction 1 second beyond threshold
        let beyond_threshold = "2023-01-01 10:59:59.000000";
        if let FilterResult::Skip(_) = filter_transaction_by_timestamp(wal_time, beyond_threshold) {
            // Expected
        } else {
            panic!("Expected transaction to be filtered out");
        }
    }

    #[test]
    fn test_parse_postgresql_timestamp_formats() {
        // PostgreSQL timestamp with microseconds (our standard format)
        assert!(parse_timestamp("2020-11-27 15:31:21.771279").is_ok());

        // PostgreSQL timestamp without microseconds (fallback)
        assert!(parse_timestamp("2020-11-27 15:31:21").is_ok());

        // Invalid formats should fail
        assert!(parse_timestamp("invalid-timestamp").is_err());
        assert!(parse_timestamp("2020-11-27T15:31:21.771279+00:00").is_err()); // ISO format not supported
        assert!(parse_timestamp("").is_err());
    }

    #[test]
    fn test_parse_actual_test_data_format() {
        // Test the exact formats found in test/parser.txt
        let test_timestamps = vec![
            "2020-11-27 15:31:21.771279", // from transactions table
            "2020-11-27 15:35:28.542551", // from users table
            "2020-11-27 15:35:28.540886", // from app_sessions table
        ];

        for timestamp in test_timestamps {
            let result = parse_timestamp(timestamp);
            assert!(result.is_ok(), "Failed to parse timestamp: {}", timestamp);

            // Verify the parsed timestamp is reasonable
            let parsed = result.unwrap();
            assert_eq!(parsed.year(), 2020);
            assert_eq!(parsed.month(), 11);
            assert_eq!(parsed.day(), 27);
        }
    }

    #[test]
    fn test_invalid_timestamp_processing() {
        let wal_time = Utc.ymd(2023, 1, 1).and_hms(12, 0, 0);

        // Invalid timestamps should be processed (fail safe)
        assert_eq!(filter_transaction_by_timestamp(wal_time, "invalid-timestamp"), FilterResult::Process);
        assert_eq!(filter_transaction_by_timestamp(wal_time, ""), FilterResult::Process);
        assert_eq!(filter_transaction_by_timestamp(wal_time, "2023-13-45 25:70:99"), FilterResult::Process);
    }

    #[test]
    fn test_future_transactions() {
        let wal_time = Utc.ymd(2023, 1, 1).and_hms(12, 0, 0);

        // Future transaction (should be processed)
        let future_transaction = "2023-01-01 13:00:00.000000";
        assert_eq!(filter_transaction_by_timestamp(wal_time, future_transaction), FilterResult::Process);
    }

    #[test]
    fn test_microsecond_precision() {
        let wal_time = Utc.ymd(2023, 1, 1).and_hms_micro(12, 0, 0, 0);

        // Test microsecond precision in filtering
        let precise_transaction = "2023-01-01 11:00:00.000001"; // Just over threshold
        assert_eq!(filter_transaction_by_timestamp(wal_time, precise_transaction), FilterResult::Process);

        let precise_old_transaction = "2023-01-01 10:59:59.999999"; // Just under threshold
        assert!(matches!(filter_transaction_by_timestamp(wal_time, precise_old_transaction), FilterResult::Skip(_)));
    }


    #[test]
    fn test_filter_result_equality() {
        assert_eq!(FilterResult::Process, FilterResult::Process);
        assert_eq!(FilterResult::Skip("test".to_string()), FilterResult::Skip("test".to_string()));
        assert_ne!(FilterResult::Process, FilterResult::Skip("test".to_string()));
    }

    #[test]
    fn test_edge_case_timestamps() {
        // Test edge cases
        let edge_cases = vec![
            "2000-01-01 00:00:00.000000", // Y2K
            "2038-01-19 03:14:07.000000", // Unix timestamp limit (32-bit)
            "1970-01-01 00:00:00.000000", // Unix epoch
            "2023-12-31 23:59:59.999999", // End of year
            "2024-02-29 12:00:00.000000", // Leap year
        ];

        for timestamp in edge_cases {
            assert!(parse_timestamp(timestamp).is_ok(), "Failed to parse: {}", timestamp);
        }
    }

    #[test]
    fn test_very_old_transactions() {
        let wal_time = Utc.ymd(2023, 1, 1).and_hms(12, 0, 0);

        // Very old transaction (days old)
        let very_old = "2022-12-01 12:00:00.000000";
        if let FilterResult::Skip(reason) = filter_transaction_by_timestamp(wal_time, very_old) {
            assert!(reason.contains("seconds older"));
        } else {
            panic!("Expected very old transaction to be filtered");
        }
    }

    // Performance test for high-volume filtering
    #[test]
    fn test_filtering_performance() {
        let wal_time = Utc.ymd(2023, 1, 1).and_hms(12, 0, 0);

        let start = std::time::Instant::now();

        for i in 0..10000 {
            let timestamp = format!("2023-01-01 11:{:02}:{:02}.000000",
                                  (i / 60) % 60, i % 60);
            filter_transaction_by_timestamp(wal_time, &timestamp);
        }

        let duration = start.elapsed();
        // Should complete 10k filters in reasonable time (< 1 second)
        assert!(duration.as_secs() < 1, "Filtering took too long: {:?}", duration);
    }
}
