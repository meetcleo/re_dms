use lazy_static::lazy_static;
use std::sync::atomic::AtomicBool;
use std::sync::Mutex;

// use nix::sys::signal::{self, Signal};
use nix::unistd::Pid;

lazy_static! {
    static ref SHUTDOWN_CLEANLY: AtomicBool = AtomicBool::new(false);
    static ref SHUTDOWN_MESSILY: AtomicBool = AtomicBool::new(false);
    static ref SHUTDOWN_HANDLER: Mutex<Option<ShutdownHandler>> = Mutex::new(None);
}

pub struct ShutdownHandler {
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
}

impl ShutdownHandler {
    pub fn register_shutdown_handler(runtime_type: RuntimeType) {
        let mut unwrapped = SHUTDOWN_HANDLER.lock().unwrap();
        *unwrapped = Some(ShutdownHandler { runtime_type });
    }
}
