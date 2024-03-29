#[cfg(feature = "with_sentry")]
use lazy_static::lazy_static;

use log::{debug, error, info, warn};

#[cfg(feature = "with_sentry")]
use sentry;
#[cfg(feature = "with_sentry")]
use sentry::Level;

#[cfg(feature = "with_sentry")]
lazy_static! {
    static ref SENTRY_DSN: String =
        std::env::var("SENTRY_DSN").expect("SENTRY_DSN env is not set");
}

#[cfg(feature = "with_sentry")]
pub fn init_sentry() -> sentry::ClientInitGuard {
    let guard = sentry::init((&**SENTRY_DSN, sentry::ClientOptions{
        release: sentry::release_name!(),
        attach_stacktrace: true,
        ..Default::default()
    }));
    return guard;
}

pub struct Logger {}

// get name of function, with struct e.t.c.
// e.g. re_dms::wal_file_manager::WalFileManager::should_swap_wal
#[macro_export]
macro_rules! function {
    () => {{
        fn f() {}
        fn type_name_of<T>(_: T) -> &'static str {
            std::any::type_name::<T>()
        }
        let name = type_name_of(f);
        &name[..name.len() - 3]
    }};
}

// macros get exported to the root of the crate
#[macro_export]
macro_rules! logger_debug {
    ($wal_number:expr, $table_name:expr, $message:expr) => {
        crate::logger::Logger::debug($wal_number, $table_name, function!(), $message)
    };
}

#[macro_export]
macro_rules! logger_info {
    ($wal_number:expr, $table_name:expr, $message:expr) => {
        crate::logger::Logger::info($wal_number, $table_name, function!(), $message)
    };
}

#[macro_export]
macro_rules! logger_warning {
    ($wal_number:expr, $table_name:expr, $message:expr) => {
        crate::logger::Logger::warning($wal_number, $table_name, function!(), $message)
    };
}

#[macro_export]
macro_rules! logger_error {
    ($wal_number:expr, $table_name:expr, $message:expr) => {
        crate::logger::Logger::error($wal_number, $table_name, function!(), $message)
    };
}

#[macro_export]
macro_rules! logger_panic {
    ($wal_number:expr, $table_name:expr, $message:expr) => {
        crate::logger::Logger::structured_panic($wal_number, $table_name, function!(), $message)
    };
}

// lets structure our logger so that we can grep our logs more easily
impl Logger {
    pub fn info(wal_number: Option<u64>, table_name: Option<&String>, tag: &str, message: &str) {
        info!(
            "{}",
            Self::structured_format(wal_number, table_name, tag, message)
        );
    }

    pub fn error(wal_number: Option<u64>, table_name: Option<&String>, tag: &str, message: &str) {
        let message = Self::structured_format(wal_number, table_name, tag, message);

        #[cfg(feature = "with_sentry")]
        {
            sentry::capture_message(&*message, Level::Error);
        }
        error!("{}", message);
    }

    pub fn warning(wal_number: Option<u64>, table_name: Option<&String>, tag: &str, message: &str) {
        let message = Self::structured_format(wal_number, table_name, tag, message);

        #[cfg(feature = "with_sentry")]
        {
            sentry::capture_message(&*message, Level::Warning);
        }

        warn!("{}", message);
    }

    pub fn debug(wal_number: Option<u64>, table_name: Option<&String>, tag: &str, message: &str) {
        debug!(
            "{}",
            Self::structured_format(wal_number, table_name, tag, message)
        );
    }

    pub fn structured_panic(
        wal_number: Option<u64>,
        table_name: Option<&String>,
        tag: &str,
        message: &str,
    ) {
        panic!("{}", Self::structured_format(
            wal_number, table_name, tag, message
        ));
    }

    pub fn structured_format(
        wal_number: Option<u64>,
        table_name: Option<&String>,
        tag: &str,
        message: &str,
    ) -> String {
        format!(
            "wal:{} table:{} tag:{} message:{}",
            wal_number
                .map(|x| format!("{:0>16X}", x))
                .unwrap_or("none".to_owned()),
            table_name
                .map(|x| x.to_string())
                .unwrap_or("none".to_owned()),
            tag,
            message
        )
    }
}
