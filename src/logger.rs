#[cfg(feature = "with_rollbar")]
use lazy_static::lazy_static;

use log::{debug, error, info};

#[cfg(feature = "with_rollbar")]
use backtrace;
#[cfg(feature = "with_rollbar")]
use rollbar;

#[cfg(feature = "with_rollbar")]
lazy_static! {
    static ref ROLLBAR_ACCESS_TOKEN: String =
        std::env::var("ROLLBAR_ACCESS_TOKEN").expect("ROLLBAR_ACCESS_TOKEN env is not set");
    static ref ROLLBAR_CLIENT: rollbar::Client =
        rollbar::Client::new((*ROLLBAR_ACCESS_TOKEN).clone(), "development".to_owned());
}

#[cfg(feature = "with_rollbar")]
pub fn register_panic_handler() {
    std::panic::set_hook(Box::new(move |panic_info| {
        let backtrace = backtrace::Backtrace::new();
        // bare print for backtraces
        println!("{:?}", backtrace);
        // send to rollbar
        let result = ROLLBAR_CLIENT
            .build_report()
            .from_panic(panic_info)
            .with_backtrace(&backtrace)
            .send()
            .join();
        // explicitly we don't unwrap to not panic in a panic handler
        result.is_err();
        // NOTE: on join see here https://github.com/RoxasShadow/rollbar-rs/issues/16
    }))
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
        crate::logger::Logger::debug($wal_number, $table_name, function!(), $message);
    };
}

#[macro_export]
macro_rules! logger_info {
    ($wal_number:expr, $table_name:expr, $message:expr) => {
        crate::logger::Logger::info($wal_number, $table_name, function!(), $message);
    };
}

#[macro_export]
macro_rules! logger_error {
    ($wal_number:expr, $table_name:expr, $message:expr) => {
        crate::logger::Logger::error($wal_number, $table_name, function!(), $message);
    };
}

#[macro_export]
macro_rules! logger_panic {
    ($wal_number:expr, $table_name:expr, $message:expr) => {
        crate::logger::Logger::structured_panic($wal_number, $table_name, function!(), $message);
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

        #[cfg(feature = "with_rollbar")]
        report_error_message!(ROLLBAR_CLIENT, message);
        error!("{}", message);
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
        panic!(Self::structured_format(
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
