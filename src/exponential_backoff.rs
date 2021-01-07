// re-export for simplicity
// first part provides the `.retry` function, second part provides the config struct
pub use backoff::{future::FutureOperation as _, ExponentialBackoff};

use lazy_static::lazy_static;
lazy_static! {
    static ref SECONDS_UNTIL_END_OF_EXPONENTIAL_BACKOFF: core::time::Duration =
        core::time::Duration::from_secs(
            std::env::var("SECONDS_UNTIL_END_OF_EXPONENTIAL_BACKOFF")
                .expect("SECONDS_UNTIL_END_OF_EXPONENTIAL_BACKOFF env is not set")
                .parse::<u64>()
                .expect("SECONDS_UNTIL_END_OF_EXPONENTIAL_BACKOFF is not a valid integer")
        );
}

// NOTE: default exponential backoff
// /// The default initial interval value in milliseconds (0.5 seconds).
//     pub const INITIAL_INTERVAL_MILLIS: u64 = 500;
// /// The default randomization factor (0.5 which results in a random period ranging between 50%
// /// below and 50% above the retry interval).
// pub const RANDOMIZATION_FACTOR: f64 = 0.5;
// /// The default multiplier value (1.5 which is 50% increase per back off).
// pub const MULTIPLIER: f64 = 1.5;
// /// The default maximum back off time in milliseconds (1 minute).
// pub const MAX_INTERVAL_MILLIS: u64 = 60_000;
// /// The default maximum elapsed time in milliseconds (15 minutes).
// pub const MAX_ELAPSED_TIME_MILLIS: u64 = 900_000;
pub fn default_exponential_backoff() -> ExponentialBackoff {
    ExponentialBackoff {
        max_elapsed_time: Some(*SECONDS_UNTIL_END_OF_EXPONENTIAL_BACKOFF),
        ..ExponentialBackoff::default()
    }
}
