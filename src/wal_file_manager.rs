use glob::glob;
use lazy_static::lazy_static;
use std::fs::{self, File, OpenOptions};
use std::path::Path;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use std::io::Write;
use std::time::Duration;

use crate::shutdown_handler::ShutdownHandler;

#[allow(unused_imports)]
use crate::{function, logger_debug, logger_error, logger_info, logger_panic};

#[cfg(test)]
use mock_instant::{Instant, MockClock};

lazy_static! {
    static ref SECONDS_UNTIL_WAL_SWITCH: u64 = std::env::var("SECONDS_UNTIL_WAL_SWITCH")
        .expect("SECONDS_UNTIL_WAL_SWITCH env is not set")
        .parse::<u64>()
        .expect("SECONDS_UNTIL_WAL_SWITCH is not a valid integer");
    static ref MAX_BYTES_UNTIL_WAL_SWITCH: usize = std::env::var("MAX_BYTES_UNTIL_WAL_SWITCH")
        .unwrap_or("1000000000".to_string()) // 1 GB default
        .parse::<usize>()
        .expect("MAX_BYTES_UNTIL_WAL_SWITCH is not a valid integer");
}

#[cfg(not(test))]
use std::time::Instant;

// NOTE: these are not wal files in the sense of postgres wal files
// just files that are increasing in number that we write to before
// processing the data
#[derive(Debug, Clone)]
pub struct WalFile {
    pub file_number: u64,
    // this is the directory where wal files are kept
    // for the directory associated with this wal file see
    // path_for_wal_directory
    pub wal_directory: PathBuf,
    // we have interior mutability of the file, and synchronise with a mutex
    // NOTE: it is unsafe to create two wal_files with the same file_number
    // (keep wal file creation single threaded!)
    file: Arc<Option<Mutex<WalFileInternal>>>,
}

#[derive(Debug)]
struct WalFileInternal {
    file: File,
    // we want this to be locked by the mutex
    had_errors_loading: bool,
    pub current_number_of_bytes: usize,
}

impl WalFileInternal {
    fn new(file: File) -> WalFileInternal {
        WalFileInternal {
            file: file,
            had_errors_loading: false,
            current_number_of_bytes: 0,
        }
    }
    fn register_error(&mut self) {
        self.had_errors_loading = true;
    }
    fn has_errors(&self) -> bool {
        self.had_errors_loading
    }
}

// just pass writes straight to the file
impl std::io::Write for WalFileInternal {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.current_number_of_bytes += buf.len();
        self.file.write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

impl Eq for WalFile {}

impl PartialEq for WalFile {
    fn eq(&self, other: &Self) -> bool {
        self.file_number == other.file_number && Arc::ptr_eq(&self.file, &other.file)
    }
}

impl WalFile {
    // creates a new wal file and associated directory and returns a struct representing it.
    pub fn new(
        wal_file_number: u64,
        wal_file_directory: &Path,
        wal_file_mode: WalFileMode,
    ) -> WalFile {
        let path = Self::path_for_wal_file_class(wal_file_number, wal_file_directory);
        let directory_path =
            Self::path_for_wal_directory_class(wal_file_number, wal_file_directory);
        logger_info!(
            Some(wal_file_number),
            None,
            &format!("creating wal directory:{:?}", directory_path)
        );
        let _directory = fs::create_dir_all(directory_path.clone()).expect(&format!(
            "Unable to create directory: {}",
            directory_path
                .clone()
                .to_str()
                .unwrap_or("unprintable non-utf-8 directory")
        ));
        logger_info!(
            Some(wal_file_number),
            None,
            &format!("creating wal file {:?}", path)
        );
        let mut open_options = OpenOptions::new();
        match wal_file_mode {
            WalFileMode::Processing => {
                // use atomic file creation. Bail if a file already exists
                open_options.write(true).create_new(true);
            }
            WalFileMode::Reprocessing(_) => {
                open_options.read(true);
            }
        }
        let file = open_options.open(path.clone()).expect(&format!(
            "Unable to create wal file: {}",
            path.to_str().unwrap_or("unprintable non-utf-8 path")
        ));
        WalFile {
            file_number: wal_file_number,
            file: Arc::new(Some(Mutex::new(WalFileInternal::new(file)))),
            wal_directory: wal_file_directory.to_path_buf(),
        }
    }
    // 16 hex chars
    fn name_for_wal_file(wal_file_number: u64) -> String {
        // hex uppercase padded to 16 chars
        format!("{:0>16X}", wal_file_number)
    }
    // class method needed in constructor
    fn path_for_wal_file_class(wal_file_number: u64, wal_file_directory: &Path) -> PathBuf {
        let mut name_without_extension = Self::name_for_wal_file(wal_file_number);
        name_without_extension.push_str(".wal");
        wal_file_directory.join(name_without_extension)
    }

    // for symmetry with directory
    pub fn path_for_wal_file(&self) -> PathBuf {
        Self::path_for_wal_file_class(self.file_number, self.wal_directory.as_path())
    }

    // class method needed in constructor
    fn path_for_wal_directory_class(wal_file_number: u64, wal_file_directory: &Path) -> PathBuf {
        let wal_file_name = Self::name_for_wal_file(wal_file_number);
        wal_file_directory.join(wal_file_name)
    }

    pub fn path_for_wal_directory(&self) -> PathBuf {
        Self::path_for_wal_directory_class(self.file_number, self.wal_directory.as_path())
    }

    fn write(&mut self, string: &str) {
        self.with_locked_internal_file()
            .write_all(format!("{}\n", string).as_bytes())
            .expect("Unable to write line to wal_file");
    }
    pub fn flush(&mut self) {
        self.with_locked_internal_file()
            .flush()
            .expect("Unable to flush wal_file");
    }
    pub fn register_error(&mut self) {
        self.with_locked_internal_file().register_error();
    }

    fn with_locked_internal_file(&mut self) -> std::sync::MutexGuard<'_, WalFileInternal> {
        self.file
            .as_ref() // tbh, I don't even know why we need two as_ref here, but we do
            .as_ref() // ref to option
            .expect("Trying to lock internal file, but it's not there?") // unwrapped option, which is our mutex
            .lock() // lock the mutex
            .expect("Error unlocking mutex for wal file") // check for error on unlock
    }

    pub fn maybe_remove_wal_file(&mut self) {
        // we only want to remove the wal file if we're the only pointer to this file
        logger_debug!(
            Some(self.file_number),
            None,
            &format!(
                "maybe_remove_wal_file_arc_count:{}",
                Arc::strong_count(&self.file)
            )
        );
        if Arc::strong_count(&self.file) != 1 {
            return;
        }
        // need to do this before the immutable borrow where we get the file below
        let file_path = self.path_for_wal_file();
        let directory_path = self.path_for_wal_directory();
        // do this in a block, so we drop our borrow right after
        {
            let locked_internal_file = self.with_locked_internal_file();
            // we don't remove the wal file if there was an error loading it
            if locked_internal_file.has_errors() {
                return;
            }
            if ShutdownHandler::shutting_down_messily() {
                return;
            }
            // We've locked our mutex, so we're safe from races
            std::fs::remove_file(file_path).expect("Error removing wal file");
            std::fs::remove_dir_all(directory_path).expect("Error removing wal directory");
        }

        // borrow dropped by here
        // now we replace Arc value with None.
        self.file = Arc::new(None);
    }
    pub fn current_bytes(&mut self) -> usize {
        self.with_locked_internal_file().current_number_of_bytes
    }
}

#[derive(Debug, Clone)]
pub enum WalFileMode {
    Processing,
    Reprocessing(String),
}

impl std::fmt::Display for WalFileMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WalFileMode::Processing => write!(f, "Processing"),
            WalFileMode::Reprocessing(path) => write!(f, "Reprocessing({})", path),
        }
    }
}

#[derive(Debug)]
pub struct WalFileManager {
    // the number of our wal file. starts at 1, goes to i64::maxint at which point we break
    current_wal_file_number: u64,
    current_wal_file: WalFile,
    output_wal_directory: PathBuf,
    last_swapped_wal: Instant,
    wal_file_mode: WalFileMode,
}

impl WalFileManager {
    pub fn new(output_wal_directory: &Path) -> WalFileManager {
        let new_wal_file_number =
            Self::get_next_wal_filenumber_from_filesystem(output_wal_directory);
        let first_wal_file = WalFile::new(
            new_wal_file_number,
            output_wal_directory,
            WalFileMode::Processing,
        );
        WalFileManager {
            current_wal_file_number: new_wal_file_number,
            current_wal_file: first_wal_file,
            output_wal_directory: output_wal_directory.to_path_buf(),
            last_swapped_wal: Instant::now(),
            wal_file_mode: WalFileMode::Processing,
        }
    }

    pub fn reprocess(output_wal_directory: &Path, wal_file_path: String) -> WalFileManager {
        let file_name = Path::new(&wal_file_path)
            .file_stem()
            .expect(&format!(
                "error getting path stem of wal file: {}",
                wal_file_path
            ))
            .to_str()
            .expect(&format!(
                "error turning wal path stem to string: {}",
                wal_file_path
            ));

        let wal_file_number = u64::from_str_radix(file_name, 16).expect(&format!(
            "error parsing wal file name as u64 from: {}",
            wal_file_path
        ));
        let first_wal_file = WalFile::new(
            wal_file_number,
            output_wal_directory,
            WalFileMode::Reprocessing(wal_file_path.clone()),
        );
        WalFileManager {
            current_wal_file_number: wal_file_number,
            current_wal_file: first_wal_file,
            output_wal_directory: output_wal_directory.to_path_buf(),
            last_swapped_wal: Instant::now(),
            wal_file_mode: WalFileMode::Reprocessing(wal_file_path),
        }
    }

    fn get_next_wal_filenumber_from_filesystem(wal_directory: &Path) -> u64 {
        let wal_glob = wal_directory.join("*".to_owned() + ".wal");
        glob(
            wal_glob
                .to_str()
                .expect("Error creating next wal file glob string"),
        )
        .expect("Error running wal glob pattern on directory")
        .map(|file_path| match file_path {
            Ok(path) => {
                let file_name = path
                    .file_stem()
                    .expect("error getting path stem of wal file")
                    .to_str()
                    .expect("error turning wal path stem to string");
                u64::from_str_radix(file_name, 16).expect("error parsing wal file name as u64")
            }

            Err(_e) => panic!("unreadable path. What did you do?"),
        })
        .fold(0, std::cmp::max)
            + 1
    }

    pub fn current_wal(&self) -> WalFile {
        self.current_wal_file.clone()
    }
    fn swap_wal(&mut self) {
        logger_info!(
            Some(self.current_wal_file_number),
            None,
            &format!(
                "swapping_wal number_of_wal_bytes:{:?} swap_wal_elapsed:{:?} last_swapped_wal:{:?}",
                self.current_wal_bytes(),
                self.last_swapped_wal.elapsed(),
                self.last_swapped_wal,
            )
        );
        self.current_wal_file.flush();
        self.current_wal_file_number = self.current_wal_file_number + 1;
        self.last_swapped_wal = Instant::now();
        let next_wal = WalFile::new(
            self.current_wal_file_number,
            self.output_wal_directory.as_path(),
            self.wal_file_mode.clone(),
        );
        // this will only delete if we didn't send any changes off to the change processor
        self.current_wal_file.maybe_remove_wal_file();
        self.current_wal_file = next_wal;
    }

    fn should_swap_wal(&mut self) -> bool {
        if let WalFileMode::Reprocessing(_) = self.wal_file_mode {
            false
        } else {
            // 10 minutes
            let should_swap_wal_time =
                self.last_swapped_wal.elapsed() >= Duration::new(*SECONDS_UNTIL_WAL_SWITCH, 0);
            if should_swap_wal_time {
                logger_debug!(
                    Some(self.current_wal_file_number),
                    None,
                    &format!(
                        "swap_wal_elapsed:{:?} last_swapped_wal:{:?}",
                        self.last_swapped_wal.elapsed(),
                        self.last_swapped_wal
                    )
                );
            }
            let current_wal_bytes = self.current_wal_bytes();
            let should_swap_wal_bytes = current_wal_bytes >= *MAX_BYTES_UNTIL_WAL_SWITCH;
            if should_swap_wal_bytes {
                logger_debug!(
                    Some(self.current_wal_file_number),
                    None,
                    &format!("current_wal_bytes:{:?}", current_wal_bytes)
                )
            }
            should_swap_wal_time || should_swap_wal_bytes
        }
    }

    // we explictly don't implement Iterator because we need to be able to iterate
    // and then call a method to shut things down, which requires us to
    // close the input stream and then process the last results
    // this will require calling a mutable method on the wal file manager
    // so we can't really have the iterator (which also needs a mut ref)
    // floating around. So we're doing this manually
    pub fn next_line(&mut self, next_line_string: &String) -> WalLineResult {
        if let WalFileMode::Reprocessing(_) = self.wal_file_mode {
            WalLineResult::WalLine()
        } else {
            self.current_wal_file.write(next_line_string.as_str());
            self.handle_next_line(next_line_string.clone())
        }
    }

    fn handle_next_line(&mut self, line: String) -> WalLineResult {
        if self.should_swap_wal() && line.starts_with("COMMIT") {
            // this means the next time the iterator is called
            // we return SwapWal
            self.swap_wal();
            WalLineResult::SwapWal(self.current_wal())
        } else {
            WalLineResult::WalLine()
        }
    }

    pub fn clean_up_final_wal_file(&mut self) {
        self.current_wal_file.maybe_remove_wal_file()
    }

    // mutable as we lock the internal file
    pub fn current_wal_bytes(&mut self) -> usize {
        self.current_wal_file.current_bytes()
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum WalLineResult {
    SwapWal(WalFile),
    WalLine(),
}

#[cfg(test)]
mod tests {
    use super::*;
    use glob::{glob_with, MatchOptions};
    use std::io::{BufRead, BufReader};

    // NOTE: I think this is actually run globally before all tests. Seems fine to me though.
    #[ctor::ctor]
    fn setup_tests() {
        // init logger
        env_logger::init();
        // set required variable
        std::env::set_var("SECONDS_UNTIL_WAL_SWITCH", "600");
        std::fs::create_dir_all(TESTING_PATH).unwrap();
    }

    fn clear_testing_directory() {
        // clear directory
        let directory_path = PathBuf::from(TESTING_PATH);
        if directory_path.exists() {
            fs::remove_dir_all(directory_path.clone()).unwrap();
        }
        fs::create_dir_all(directory_path.clone()).unwrap();
    }

    // TODO stub filesystem properly
    const TESTING_PATH: &str = "/tmp/wal_testing";

    #[test]
    fn wal_file_naming() {
        let wal_file_name = WalFile::name_for_wal_file(31);
        assert_eq!(wal_file_name.as_str(), "000000000000001F");
    }

    #[test]
    fn wal_file_manager_numbering() {
        clear_testing_directory();
        // first create a wal file with a number
        let number = 127;
        let directory_path = PathBuf::from(TESTING_PATH);
        WalFile::new(number, directory_path.as_path(), WalFileMode::Processing);
        WalFile::new(1, directory_path.as_path(), WalFileMode::Processing); // couple of other smaller numbers too
        WalFile::new(
            number - 1,
            directory_path.as_path(),
            WalFileMode::Processing,
        );
        let wal_file_manager = WalFileManager::new(directory_path.as_path());
        assert_eq!(wal_file_manager.current_wal_file.file_number, number + 1)
    }

    #[test]
    fn wal_file_directory() {
        let directory_path = PathBuf::from(TESTING_PATH);
        let wal_file = WalFile::new(31, directory_path.as_path(), WalFileMode::Processing);

        assert_eq!(
            wal_file.path_for_wal_directory(),
            PathBuf::from("/tmp/wal_testing/000000000000001F")
        );
    }

    #[test]
    fn wal_file_path() {
        let directory_path = PathBuf::from(TESTING_PATH);
        let wal_file_path = WalFile::path_for_wal_file_class(1, directory_path.as_path());
        assert_eq!(
            wal_file_path,
            PathBuf::from("/tmp/wal_testing/0000000000000001.wal")
        )
    }

    #[test]
    fn new_wal_file() {
        clear_testing_directory();
        let directory_path = PathBuf::from(TESTING_PATH);
        let mut wal_file = WalFile::new(1, directory_path.as_path(), WalFileMode::Processing);
        assert_eq!(wal_file.file_number, 1);
        assert!(Path::new("/tmp/wal_testing/0000000000000001.wal").exists());
        wal_file.maybe_remove_wal_file();
        assert!(!Path::new("/tmp/wal_testing/0000000000000001.wal").exists());
    }

    #[test]
    fn wal_file_wont_be_deleted_if_cloned() {
        clear_testing_directory();
        let directory_path = PathBuf::from(TESTING_PATH);
        let mut wal_file = WalFile::new(1, directory_path.as_path(), WalFileMode::Processing);
        let _cloned_wal_file = wal_file.clone();
        assert_eq!(wal_file.file_number, 1);
        assert!(Path::new("/tmp/wal_testing/0000000000000001.wal").exists());
        wal_file.maybe_remove_wal_file();
        // it still exists
        assert!(Path::new("/tmp/wal_testing/0000000000000001.wal").exists());
    }

    #[test]
    fn wal_file_wont_be_deleted_if_there_is_an_error() {
        clear_testing_directory();
        let directory_path = PathBuf::from(TESTING_PATH);
        let mut wal_file = WalFile::new(1, directory_path.as_path(), WalFileMode::Processing);
        wal_file.register_error();
        assert_eq!(wal_file.file_number, 1);
        assert!(Path::new("/tmp/wal_testing/0000000000000001.wal").exists());
        wal_file.maybe_remove_wal_file();
        // it still exists
        assert!(Path::new("/tmp/wal_testing/0000000000000001.wal").exists());
    }

    #[test]
    fn wal_file_manager() {
        clear_testing_directory();
        let directory_path = PathBuf::from(TESTING_PATH);
        let mut wal_file_manager = WalFileManager::new(directory_path.as_path());
        wal_file_manager.swap_wal();
        assert_eq!(wal_file_manager.current_wal().file_number, 2);
    }

    fn last_line_of_wal(wal_file: &mut WalFile) -> String {
        let path = wal_file.path_for_wal_file();
        wal_file.flush();
        let file = BufReader::new(File::open(path).unwrap());
        let mut lines: Vec<_> = file.lines().map(|line| line.unwrap()).collect();
        lines.reverse();
        if let Some(line) = lines.first_mut() {
            line.clone()
        } else {
            "".to_string()
        }
    }

    #[test]
    fn wal_file_integration_test() {
        let directory_path = PathBuf::from(TESTING_PATH);
        let mut wal_file_manager = WalFileManager::new(directory_path.as_path());

        let filename = "test/parser.txt";
        let input_file = File::open(filename).unwrap();
        let reader = BufReader::new(input_file);
        let mut iter = reader.lines();

        // 3 blocks of begin, table, commit
        for _ in 0..3 {
            let mut current_wal_file = wal_file_manager.current_wal();
            let begin = wal_file_manager.next_line(&iter.next().unwrap().unwrap());
            if let WalLineResult::WalLine() = begin {
                assert!(last_line_of_wal(&mut current_wal_file).starts_with("BEGIN"));
            } else {
                panic!("begin line doesn't match {:?}", begin)
            }

            let table = wal_file_manager.next_line(&iter.next().unwrap().unwrap());
            if let WalLineResult::WalLine() = table {
                assert!(last_line_of_wal(&mut current_wal_file).starts_with("table"));
            } else {
                panic!("table line doesn't match {:?}", table);
            }
            // we advance 10 minutes before the commit line
            MockClock::advance(Duration::from_secs(600));

            let commit = wal_file_manager.next_line(&iter.next().unwrap().unwrap());
            if let WalLineResult::SwapWal(..) = commit {
                assert_ne!(wal_file_manager.current_wal(), current_wal_file);
                assert!(last_line_of_wal(&mut current_wal_file).starts_with("COMMIT"));
            } else {
                panic!("commit line doesn't match {:?}", commit);
            }
        }
    }

    #[test]
    fn wal_file_byte_swap_integration_test() {
        std::env::set_var("MAX_BYTES_UNTIL_WAL_SWITCH", "939");
        let directory_path = PathBuf::from(TESTING_PATH);
        let mut wal_file_manager = WalFileManager::new(directory_path.as_path());

        let filename = "test/same_bytes_swap_wal.txt";
        let input_file = File::open(filename).unwrap();
        let reader = BufReader::new(input_file);
        let mut iter = reader.lines();

        // 3 blocks of begin, table, commit
        for _ in 0..3 {
            let mut current_wal_file = wal_file_manager.current_wal();
            let begin = wal_file_manager.next_line(&iter.next().unwrap().unwrap());
            if let WalLineResult::WalLine() = begin {
                assert!(last_line_of_wal(&mut current_wal_file).starts_with("BEGIN"));
            } else {
                panic!("begin line doesn't match {:?}", begin)
            }

            let table = wal_file_manager.next_line(&iter.next().unwrap().unwrap());
            if let WalLineResult::WalLine() = table {
                assert!(last_line_of_wal(&mut current_wal_file).starts_with("table"));
            } else {
                panic!("table line doesn't match {:?}", table);
            }

            // We have set the number of bytes to make the wal swap occur here
            let commit = wal_file_manager.next_line(&iter.next().unwrap().unwrap());
            if let WalLineResult::SwapWal(..) = commit {
                assert_ne!(wal_file_manager.current_wal(), current_wal_file);
                assert!(last_line_of_wal(&mut current_wal_file).starts_with("COMMIT"));
            } else {
                panic!("commit line doesn't match {:?}", commit);
            }
        }

        std::env::set_var("MAX_BYTES_UNTIL_WAL_SWITCH", "1000000000");
    }

    fn len_of_file(path: &str) -> u64 {
        let metadata = fs::metadata(path).expect(&format!("Unable to find file: {}", path));
        metadata.len()
    }

    fn size_of_output_dir() -> usize {
        let options = MatchOptions {
            case_sensitive: false,
            ..Default::default()
        };
        glob_with(&format!("{}/*.wal", TESTING_PATH), options)
            .expect("Unable to check for existing WAL files")
            .count()
    }

    #[test]
    fn wal_file_reprocessing_integration_test() {
        clear_testing_directory();
        let filename = &format!("{}/0000000000000001.wal", TESTING_PATH);
        fs::copy("test/parser.txt", filename).expect(&format!(
            "Unable to copy test file to output dir, here: {}",
            filename
        ));
        assert_eq!(size_of_output_dir(), 1);
        assert!(Path::new(filename).exists());
        {
            let directory_path = PathBuf::from(TESTING_PATH);
            let mut wal_file_manager =
                WalFileManager::reprocess(directory_path.as_path(), filename.to_string());

            let input_file = File::open(filename).unwrap();
            let reader = BufReader::new(input_file);
            let mut iter = reader.lines();

            let original_length = len_of_file(filename);
            assert_ne!(0, original_length);
            // 3 blocks of begin, table, commit
            for _ in 0..3 {
                let current_wal_file = wal_file_manager.current_wal();
                let begin = wal_file_manager.next_line(&iter.next().unwrap().unwrap());
                if let WalLineResult::WalLine() = begin {
                    assert_eq!(size_of_output_dir(), 1);
                    assert_eq!(len_of_file(filename), original_length);
                } else {
                    panic!("begin line swapped WAL when it should not have")
                }

                let table = wal_file_manager.next_line(&iter.next().unwrap().unwrap());
                if let WalLineResult::WalLine() = table {
                    assert_eq!(size_of_output_dir(), 1);
                    assert_eq!(len_of_file(filename), original_length);
                } else {
                    panic!("table line swapped WAL when it should not have");
                }
                // we advance 10 minutes before the commit line
                MockClock::advance(Duration::from_secs(600));

                let commit = wal_file_manager.next_line(&iter.next().unwrap().unwrap());
                if let WalLineResult::WalLine(..) = commit {
                    assert_eq!(size_of_output_dir(), 1);
                    assert_eq!(wal_file_manager.current_wal(), current_wal_file);
                    assert_eq!(len_of_file(filename), original_length);
                } else {
                    panic!("commit line swapped WAL when it should not have");
                }
            }
            wal_file_manager.clean_up_final_wal_file();
        }
        assert!(!Path::new(filename).exists());
    }
}
