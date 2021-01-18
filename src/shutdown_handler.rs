use lazy_static::lazy_static;
use std::sync::atomic::AtomicBool;
use std::sync::Mutex;

use nix::sys::signal::{self, Signal};
use nix::unistd::Pid;

#[allow(unused_imports)]
use crate::{function, logger_debug, logger_error, logger_info, logger_panic};

static SHUTDOWN_CLEANLY: AtomicBool = AtomicBool::new(false);
static SHUTDOWN_MESSILY: AtomicBool = AtomicBool::new(false);

lazy_static! {
    static ref SHUTDOWN_HANDLER: Mutex<Option<ShutdownHandler>> = Mutex::new(None);
}

pub struct ShutdownHandler {
    already_run_shutdown: bool,
    runtime_type: RuntimeType,
}

pub enum RuntimeType {
    Stdin,
    Process(Pid),
}
impl RuntimeType {
    pub fn from_pid(id: i32) -> RuntimeType {
        RuntimeType::Process(Pid::from_raw(id))
    }
    pub fn is_stdin(&self) -> bool {
        matches!(self, RuntimeType::Stdin)
    }
    pub fn run_shutdown(&self) {
        match self {
            Self::Process(pid) => {
                // https://stackoverflow.com/questions/49210815/how-do-i-send-a-signal-to-a-child-subprocess
                signal::kill(pid.clone(), Signal::SIGTERM).unwrap();
            }
            Self::Stdin => {
                // No-Op
            }
        }
    }
}

impl ShutdownHandler {
    pub fn register_shutdown_handler(runtime_type: RuntimeType) {
        let mut unwrapped = SHUTDOWN_HANDLER.lock().unwrap();
        *unwrapped = Some(ShutdownHandler {
            runtime_type,
            already_run_shutdown: false,
        });
    }

    // https://en.cppreference.com/w/cpp/atomic/memory_order#Release-Acquire_ordering
    // https://doc.rust-lang.org/std/sync/atomic/struct.AtomicBool.html
    // release acquire pairs I think is something else that should put your googling
    // on the right track iirc

    #[allow(dead_code)]
    pub fn register_clean_shutdown() {
        // we want all threads to see this write
        // synchronisation point!
        logger_error!(None, None, "register_clean_shutdown");
        Self::shutdown_shutdown_handler();
        // _technically_ this is a race condition, but I'm just not too worried about it
        // since every thread will be getting shut down when we are "shutting down messily"
        // so it'll be pretty rare that it'll be triggered.
        if Self::shutting_down_messily() {
            SHUTDOWN_CLEANLY.store(true, std::sync::atomic::Ordering::Release);
        }
    }

    pub fn register_messy_shutdown() {
        logger_error!(None, None, "register_messy_shutdown");
        Self::shutdown_shutdown_handler();
        SHUTDOWN_MESSILY.store(true, std::sync::atomic::Ordering::Release);
    }

    pub fn shutdown_shutdown_handler() {
        let mut unwrapped = SHUTDOWN_HANDLER.lock().unwrap();
        // need to deref the mutex guard to get the option.
        match *unwrapped {
            None => {
                panic!("Shutdown called before RuntimeType has been registered")
            }
            Some(ref mut shutdown_handler) => shutdown_handler.handle_shutdown(),
        }
    }
    pub fn shutting_down_messily() -> bool {
        SHUTDOWN_MESSILY.load(std::sync::atomic::Ordering::Acquire)
    }

    pub fn shutting_down_cleanly() -> bool {
        SHUTDOWN_CLEANLY.load(std::sync::atomic::Ordering::Acquire)
    }

    // this call is protected by a mutex
    pub fn handle_shutdown(&mut self) {
        if !self.already_run_shutdown {
            // mutex, no race condition
            self.runtime_type.run_shutdown();
            self.already_run_shutdown = true;
        }
    }

    pub fn shutting_down() -> bool {
        Self::shutting_down_cleanly() || Self::shutting_down_messily()
    }

    pub fn should_break_main_loop() -> bool {
        Self::shutting_down()
            && SHUTDOWN_HANDLER
                .lock()
                .unwrap()
                .as_ref()
                .expect(
                    "Should break loop called before shutdown handler has been registered. How?",
                )
                .runtime_type
                .is_stdin()
    }
}
