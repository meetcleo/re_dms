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

/// Filter a transaction based on its updated_at timestamp relative to WAL creation time
///
/// # Arguments
/// * `wal_creation_time` - When the current WAL file was created
/// * `updated_at_str` - The updated_at timestamp as a string (postgres format)
///
/// # Returns
/// * `FilterResult::Process` if the transaction should be processed
/// * `FilterResult::Skip(reason)` if the transaction should be skipped
pub fn filter_transaction(wal_creation_time: DateTime<Utc>, updated_at_str: &str) -> FilterResult {
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
/// Handles various PostgreSQL timestamp formats:
/// - "2020-11-27 15:31:21.771279" (timestamp without timezone)
/// - "2020-11-27 15:31:21.771279+00:00" (timestamp with timezone)
fn parse_timestamp(timestamp_str: &str) -> Result<DateTime<Utc>, String> {
    // Try parsing with timezone first
    if let Ok(dt) = DateTime::parse_from_str(timestamp_str, "%Y-%m-%d %H:%M:%S%.f%z") {
        return Ok(dt.with_timezone(&Utc));
    }

    // Try parsing with 'T' separator and timezone
    if let Ok(dt) = DateTime::parse_from_str(timestamp_str, "%Y-%m-%dT%H:%M:%S%.f%z") {
        return Ok(dt.with_timezone(&Utc));
    }

    // Try parsing without timezone (assume UTC)
    if let Ok(naive_dt) = NaiveDateTime::parse_from_str(timestamp_str, "%Y-%m-%d %H:%M:%S%.f") {
        return Ok(DateTime::from_utc(naive_dt, Utc));
    }

    // Try parsing without microseconds
    if let Ok(naive_dt) = NaiveDateTime::parse_from_str(timestamp_str, "%Y-%m-%d %H:%M:%S") {
        return Ok(DateTime::from_utc(naive_dt, Utc));
    }

    Err(format!("Unable to parse timestamp: {}", timestamp_str))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    #[test]
    fn test_filter_recent_transaction() {
        let wal_time = Utc.ymd(2023, 1, 1).and_hms(12, 0, 0);

        // Transaction from 30 minutes ago (within 1 hour threshold)
        let recent_transaction = "2023-01-01 11:30:00.000000";
        assert_eq!(filter_transaction(wal_time, recent_transaction), FilterResult::Process);
    }

    #[test]
    fn test_filter_old_transaction() {
        let wal_time = Utc.ymd(2023, 1, 1).and_hms(12, 0, 0);

        // Transaction from 2 hours ago (beyond 1 hour threshold)
        let old_transaction = "2023-01-01 10:00:00.000000";
        if let FilterResult::Skip(reason) = filter_transaction(wal_time, old_transaction) {
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
        assert_eq!(filter_transaction(wal_time, boundary_transaction), FilterResult::Process);
    }

    #[test]
    fn test_filter_just_beyond_threshold() {
        let wal_time = Utc.ymd(2023, 1, 1).and_hms(12, 0, 0);

        // Transaction 1 second beyond threshold
        let beyond_threshold = "2023-01-01 10:59:59.000000";
        if let FilterResult::Skip(_) = filter_transaction(wal_time, beyond_threshold) {
            // Expected
        } else {
            panic!("Expected transaction to be filtered out");
        }
    }

    #[test]
    fn test_parse_various_timestamp_formats() {
        // PostgreSQL timestamp without timezone
        assert!(parse_timestamp("2020-11-27 15:31:21.771279").is_ok());

        // PostgreSQL timestamp without microseconds
        assert!(parse_timestamp("2020-11-27 15:31:21").is_ok());

        // ISO format with timezone
        assert!(parse_timestamp("2020-11-27T15:31:21.771279+00:00").is_ok());

        // Invalid format
        assert!(parse_timestamp("invalid-timestamp").is_err());
    }

    #[test]
    fn test_invalid_timestamp_processing() {
        let wal_time = Utc.ymd(2023, 1, 1).and_hms(12, 0, 0);

        // Invalid timestamps should be processed (fail safe)
        assert_eq!(filter_transaction(wal_time, "invalid-timestamp"), FilterResult::Process);
        assert_eq!(filter_transaction(wal_time, ""), FilterResult::Process);
        assert_eq!(filter_transaction(wal_time, "2023-13-45 25:70:99"), FilterResult::Process);
    }

    #[test]
    fn test_future_transactions() {
        let wal_time = Utc.ymd(2023, 1, 1).and_hms(12, 0, 0);

        // Future transaction (should be processed)
        let future_transaction = "2023-01-01 13:00:00.000000";
        assert_eq!(filter_transaction(wal_time, future_transaction), FilterResult::Process);
    }

    #[test]
    fn test_microsecond_precision() {
        let wal_time = Utc.ymd(2023, 1, 1).and_hms_micro(12, 0, 0, 0);

        // Test microsecond precision in filtering
        let precise_transaction = "2023-01-01 11:00:00.000001"; // Just over threshold
        assert_eq!(filter_transaction(wal_time, precise_transaction), FilterResult::Process);

        let precise_old_transaction = "2023-01-01 10:59:59.999999"; // Just under threshold
        assert!(matches!(filter_transaction(wal_time, precise_old_transaction), FilterResult::Skip(_)));
    }

    #[test]
    fn test_timezone_handling() {
        let wal_time = Utc.ymd(2023, 1, 1).and_hms(12, 0, 0);

        // Test timezone conversion (assuming UTC for simplicity in test)
        let tz_transaction = "2023-01-01T11:00:00.000000+00:00";
        assert_eq!(filter_transaction(wal_time, tz_transaction), FilterResult::Process);
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
        if let FilterResult::Skip(reason) = filter_transaction(wal_time, very_old) {
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
            filter_transaction(wal_time, &timestamp);
        }

        let duration = start.elapsed();
        // Should complete 10k filters in reasonable time (< 1 second)
        assert!(duration.as_secs() < 1, "Filtering took too long: {:?}", duration);
    }
}