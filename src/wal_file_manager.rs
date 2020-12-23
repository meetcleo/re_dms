use std::fs::{self, File};
use std::path::Path;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use std::io::{self, BufRead, Write};
use std::time::Duration;

#[allow(unused_imports)]
use log::{debug, error, info, log_enabled, Level};

#[cfg(test)]
use mock_instant::{Instant, MockClock};

// TODO: config
const SECONDS_UNTIL_WAL_SWITCH: u64 = 600;

#[cfg(not(test))]
use std::time::Instant;

type WalInputFileIterator = io::Split<io::BufReader<File>>;

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

// this feels like a lot to do in a destructor (fs stuff!)
// lets keep it explicit for now
// impl Drop for WalFile {
//     fn drop(&mut self) {
//         self.maybe_remove_wal_file();
//     }
// }

#[derive(Debug)]
struct WalFileInternal {
    file: File,
    // we want this to be locked by the mutex
    had_errors_loading: bool,
}

impl WalFileInternal {
    fn new(file: File) -> WalFileInternal {
        WalFileInternal {
            file: file,
            had_errors_loading: false,
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
    pub fn new(wal_file_number: u64, wal_file_directory: &Path) -> WalFile {
        let path = Self::path_for_wal_file_class(wal_file_number, wal_file_directory);
        let directory_path =
            Self::path_for_wal_directory_class(wal_file_number, wal_file_directory);
        info!("creating wal file {:?}", path);
        let file = File::create(path).unwrap();
        info!("creating wal directory {:?}", directory_path);
        let _directory = fs::create_dir_all(directory_path).unwrap();
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
            .write(format!("{}\n", string).as_bytes())
            .unwrap();
    }
    pub fn flush(&mut self) {
        self.with_locked_internal_file().flush().unwrap();
    }
    pub fn register_error(&mut self) {
        self.with_locked_internal_file().register_error();
    }

    fn with_locked_internal_file(&mut self) -> std::sync::MutexGuard<'_, WalFileInternal> {
        self.file
            .as_ref() // tbh, I don't even know why we need two as_ref here, but we do
            .as_ref() // ref to option
            .unwrap() // unwrapped option, which is our mutex
            .lock() // lock the mutex
            .unwrap() // check for error on unlock
    }

    pub fn maybe_remove_wal_file(&mut self) {
        // we only want to remove the wal file if we're the only pointer to this file
        println!(
            "Maybe remove wal file {}: arc count: {}",
            self.file_number,
            Arc::strong_count(&self.file)
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
            // TODO delete file and folder.
            // We've locked our mutex, so we're safe from races
            std::fs::remove_file(file_path).unwrap();
            std::fs::remove_dir_all(directory_path).unwrap();
        }

        // borrow dropped by here
        // now we replace Arc value with None.
        self.file = Arc::new(None);
    }
}

#[derive(Debug)]
pub struct WalFileManager {
    // the number of our wal file. starts at 1, goes to i64::maxint at which point we break
    current_wal_file_number: u64,
    // filename for now, can refactor to stdin later
    input_filename: PathBuf,
    current_wal_file: WalFile,
    output_wal_directory: PathBuf,
    wal_input_file_iterator: WalInputFileIterator,
    swapped_wal: bool,
    last_swapped_wal: Instant,
}

impl WalFileManager {
    pub fn new(
        previous_wal_file_number: Option<u64>,
        input_file_name: &Path,
        output_wal_directory: &Path,
    ) -> WalFileManager {
        let new_wal_file_number = previous_wal_file_number.unwrap_or(0) + 1;
        let first_wal_file = WalFile::new(new_wal_file_number, output_wal_directory);
        WalFileManager {
            current_wal_file_number: new_wal_file_number,
            input_filename: input_file_name.to_path_buf(),
            current_wal_file: first_wal_file,
            output_wal_directory: output_wal_directory.to_path_buf(),
            wal_input_file_iterator: Self::open_file(input_file_name),
            swapped_wal: true, // we issue a "swapped wal" at the start to show that we've created a new wal file
            last_swapped_wal: Instant::now(),
        }
    }

    fn open_file(input_file_path: &Path) -> WalInputFileIterator {
        info!("{:?}", input_file_path);
        Self::read_lines(input_file_path).unwrap()
    }

    // The output is wrapped in a Result to allow matching on errors
    // Returns an Iterator to the Reader of the lines of the file.
    fn read_lines<P>(filename: P) -> io::Result<WalInputFileIterator>
    where
        P: AsRef<Path>,
    {
        let file = File::open(filename)?;
        Ok(io::BufReader::new(file).split(b'\n'))
    }

    fn current_wal(&self) -> WalFile {
        self.current_wal_file.clone()
    }
    fn swap_wal(&mut self) {
        self.current_wal_file.flush();
        self.current_wal_file_number = self.current_wal_file_number + 1;
        self.swapped_wal = true;
        self.last_swapped_wal = Instant::now();
        let next_wal = WalFile::new(
            self.current_wal_file_number,
            self.output_wal_directory.as_path(),
        );
        // this will only delete, if we didn't send any changes off to the change processor
        self.current_wal_file.maybe_remove_wal_file();
        self.current_wal_file = next_wal;
    }

    fn should_swap_wal(&self) -> bool {
        // 10 minutes
        let should_swap_wal = self.last_swapped_wal.elapsed()
            >= Duration::new(SECONDS_UNTIL_WAL_SWITCH, 0)
            && !self.swapped_wal;
        if should_swap_wal {
            info!("SWAP_WAL_ELAPSED {:?}", self.last_swapped_wal.elapsed());
            info!("LAST_SWAPPED_WAL {:?}", self.last_swapped_wal);
        }
        should_swap_wal
    }

    // we explictly don't implement Iterator because we need to be able to iterate
    // and then call a method to shut things down, which requires us to
    // close the input stream and then process the last results
    // this will require calling a mutable method on the wal file manager
    // so we can't really have the iterator (which also needs a mut ref)
    // floating around. So we're doing this manually
    pub fn next_line(&mut self) -> Option<WalLineResult> {
        if self.swapped_wal {
            self.swapped_wal = false;
            // tell our client we just swapped the wal and to flush files
            Some(WalLineResult::SwapWal(self.current_wal()))
        } else {
            // we only swap after we receive a commit line, we write it and pass it through the iterator, but then swap the wal file.
            let maybe_next_line = self.wal_input_file_iterator.next();
            if let Some(next_line_result) = maybe_next_line {
                let next_line = next_line_result.unwrap();
                let next_line_string = String::from_utf8(next_line).unwrap();
                // TODO: poisoned mutex
                self.current_wal_file.write(next_line_string.as_str());
                self.handle_next_line(next_line_string)
            } else {
                None
            }
        }
    }

    fn handle_next_line(&mut self, line: String) -> Option<WalLineResult> {
        if self.should_swap_wal() && line.starts_with("COMMIT") {
            let result = Some(WalLineResult::WalLine(self.current_wal(), line));
            // this means the next time the iterator is called
            // we return SwapWal
            self.swap_wal();
            result
        } else {
            Some(WalLineResult::WalLine(self.current_wal(), line))
        }
    }

    pub fn clean_up_final_wal_file(&mut self) {
        self.current_wal_file.maybe_remove_wal_file()
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum WalLineResult {
    SwapWal(WalFile),
    WalLine(WalFile, String),
}

#[cfg(test)]
mod tests {
    use super::*;

    // NOTE: I think this is actually run globally before all tests. Seems fine to me though.
    #[ctor::ctor]
    fn create_tmp_directory() {
        std::fs::create_dir_all(TESTING_PATH).unwrap();
    }

    // TODO stub filesystem properly
    const TESTING_PATH: &str = "/tmp/wal_testing";

    #[test]
    fn wal_file_naming() {
        let wal_file_name = WalFile::name_for_wal_file(31);
        assert_eq!(wal_file_name.as_str(), "000000000000001F");
    }

    #[test]
    fn wal_file_directory() {
        let directory_path = PathBuf::from(TESTING_PATH);
        let wal_file = WalFile::new(31, directory_path.as_path());

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
        let directory_path = PathBuf::from(TESTING_PATH);
        let mut wal_file = WalFile::new(1, directory_path.as_path());
        assert_eq!(wal_file.file_number, 1);
        assert!(Path::new("/tmp/wal_testing/0000000000000001.wal").exists());
        wal_file.maybe_remove_wal_file();
        assert!(!Path::new("/tmp/wal_testing/0000000000000001.wal").exists());
    }

    #[test]
    fn wal_file_wont_be_deleted_if_cloned() {
        let directory_path = PathBuf::from(TESTING_PATH);
        let mut wal_file = WalFile::new(1, directory_path.as_path());
        let _cloned_wal_file = wal_file.clone();
        assert_eq!(wal_file.file_number, 1);
        assert!(Path::new("/tmp/wal_testing/0000000000000001.wal").exists());
        wal_file.maybe_remove_wal_file();
        // it still exists
        assert!(Path::new("/tmp/wal_testing/0000000000000001.wal").exists());
    }

    #[test]
    fn wal_file_wont_be_deleted_if_there_is_an_error() {
        let directory_path = PathBuf::from(TESTING_PATH);
        let mut wal_file = WalFile::new(1, directory_path.as_path());
        wal_file.register_error();
        assert_eq!(wal_file.file_number, 1);
        assert!(Path::new("/tmp/wal_testing/0000000000000001.wal").exists());
        wal_file.maybe_remove_wal_file();
        // it still exists
        assert!(Path::new("/tmp/wal_testing/0000000000000001.wal").exists());
    }

    #[test]
    fn wal_file_manager() {
        let directory_path = PathBuf::from(TESTING_PATH);
        let mut wal_file_manager = WalFileManager::new(
            None,
            PathBuf::from("test/parser.txt").as_path(),
            directory_path.as_path(),
        );
        wal_file_manager.swap_wal();
        assert_eq!(wal_file_manager.current_wal().file_number, 2);
        let swap_wal = wal_file_manager.next_line();

        assert!(matches!(swap_wal, Some(WalLineResult::SwapWal(..))))
    }

    #[test]
    fn wal_file_integration_test() {
        let directory_path = PathBuf::from(TESTING_PATH);
        let mut wal_file_manager = WalFileManager::new(
            None,
            PathBuf::from("test/parser.txt").as_path(),
            directory_path.as_path(),
        );
        let mut current_wal_file = wal_file_manager.current_wal();

        // start with a wal swap
        let wal_swap = wal_file_manager.next_line();
        assert!(matches!(wal_swap, Some(WalLineResult::SwapWal(..))));

        // 6 blocks of begin, table, commit
        for _ in 0..6 {
            let begin = wal_file_manager.next_line();
            if let Some(WalLineResult::WalLine(file, line)) = begin {
                assert_eq!(file, current_wal_file);
                assert!(line.starts_with("BEGIN"))
            } else {
                panic!("begin line doesn't match {:?}", begin)
            }

            let table = wal_file_manager.next_line();
            if let Some(WalLineResult::WalLine(file, line)) = table {
                assert_eq!(file, current_wal_file);
                assert!(line.starts_with("table"));
            } else {
                panic!("table line doesn't match {:?}", table);
            }
            // we advance 10 minutes before the commit line
            MockClock::advance(Duration::from_secs(600));

            let commit = wal_file_manager.next_line();
            if let Some(WalLineResult::WalLine(file, line)) = commit {
                assert_eq!(file, current_wal_file);
                assert!(line.starts_with("COMMIT"));
            } else {
                panic!("commit line doesn't match {:?}", commit);
            }

            let wal_swap = wal_file_manager.next_line();
            assert!(matches!(wal_swap, Some(WalLineResult::SwapWal(..))));
            assert_ne!(current_wal_file, wal_file_manager.current_wal());
            current_wal_file = wal_file_manager.current_wal();
        }

        let iterator_finished = wal_file_manager.next_line();
        assert_eq!(iterator_finished, None);
        let iterator_finished = wal_file_manager.next_line();
        assert_eq!(iterator_finished, None);
    }
}
