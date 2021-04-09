#[cfg(feature = "with_rollbar")]
use lazy_static::lazy_static;

use log::{debug, error, info, warn};

#[cfg(feature = "with_rollbar")]
use backtrace;
#[cfg(feature = "with_rollbar")]
use rollbar::{self, ResponseStatus};

#[cfg(feature = "with_rollbar")]
lazy_static! {
    static ref ROLLBAR_ACCESS_TOKEN: String =
        std::env::var("ROLLBAR_ACCESS_TOKEN").expect("ROLLBAR_ACCESS_TOKEN env is not set");
    static ref ROLLBAR_CLIENT: rollbar::Client =
        rollbar::Client::new((*ROLLBAR_ACCESS_TOKEN).clone(), "development".to_owned());
    static ref LAST_ROLLBAR_THREAD_HANDLE: std::sync::Mutex<Option<std::thread::JoinHandle<Option<ResponseStatus>>>> =
        std::sync::Mutex::new(None);
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
        match result {
            Ok(..) => {}
            Err(err) => {
                error!("Error sending rollbar message {:?}", err);
            }
        }
        // NOTE: on join see here https://github.com/RoxasShadow/rollbar-rs/issues/16
    }))
}

#[cfg(feature = "with_rollbar")]
pub fn set_rollbar_thread_handle(thread_handle: std::thread::JoinHandle<Option<ResponseStatus>>) {
    let mut unwrapped = LAST_ROLLBAR_THREAD_HANDLE.lock().unwrap();
    *unwrapped = Some(thread_handle);
}

#[cfg(feature = "with_rollbar")]
pub fn block_on_last_rollbar_thread_handle() {
    let mut last_join_handle_option = LAST_ROLLBAR_THREAD_HANDLE.lock().unwrap();
    if last_join_handle_option.is_some() {
        let last_join_handle_option = std::mem::replace(&mut *last_join_handle_option, None);
        let join_thread_result = last_join_handle_option.unwrap().join();
        match join_thread_result {
            Ok(..) => {}
            Err(err) => {
                error!("error blocking on last_rollbar_thread_handle {:?}", err);
            }
        }
    }
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
macro_rules! logger_warning {
    ($wal_number:expr, $table_name:expr, $message:expr) => {
        crate::logger::Logger::warning($wal_number, $table_name, function!(), $message);
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
        {
            let thread_handle = report_error_message!(ROLLBAR_CLIENT, message);
            set_rollbar_thread_handle(thread_handle);
        }
        error!("{}", message);
    }

    pub fn warning(wal_number: Option<u64>, table_name: Option<&String>, tag: &str, message: &str) {
        let message = Self::structured_format(wal_number, table_name, tag, message);

        // report_warning_message!(ROLLBAR_CLIENT, message);
        // this macro doesn't exist so be explicit
        #[cfg(feature = "with_rollbar")]
        {
            let thread_handle = ROLLBAR_CLIENT
                .build_report()
                .from_message(&message)
                .with_level(::rollbar::Level::INFO)
                .send();
            set_rollbar_thread_handle(thread_handle);
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
