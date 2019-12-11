use crate::error::{KvsError, Result};
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;

use std::collections::{BTreeMap, HashMap};
use std::ffi::OsStr;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::num::ParseIntError;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::result;
use std::str::FromStr;

#[derive(Debug)]
pub struct KvStore {
    path: PathBuf,
    store: HashMap<u64, BufReaderWithPos<File>>,
    index: BTreeMap<String, CommandPos>,
    log: BufWriterWithPos<File>,
    current_generation: u64,
}

impl KvStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();
        fs::create_dir_all(&path)?;

        let mut store = HashMap::new();

        let mut index = BTreeMap::new();

        let gen_list = sorted_gen_list(&path)?;
        for &gen in &gen_list {
            let mut reader = BufReaderWithPos::new(File::open(log_path(&path, gen))?)?;
            load(gen, &mut reader, &mut index)?;
            store.insert(gen, reader);
        }

        let current_generation = gen_list.last().unwrap_or(&0) + 1;

        let reader = BufReaderWithPos::new(File::create(log_path(&path, current_generation))?)?;
        let log = new_log_file(&path, current_generation)?;
        store.insert(current_generation, reader);

        Ok(KvStore {
            path,
            store,
            index,
            log,
            current_generation,
        })
    }

    pub fn set(&mut self, key: String, val: String) -> Result<()> {
        let pos = self.log.pos;
        let cmd = Command::set(key.clone(), val.clone());
        serde_json::to_writer(&mut self.log, &cmd)?;
        self.log.flush()?;

        if let Command::Set { key, .. } = cmd {
            self.index
                .insert(key, (self.current_generation, pos..self.log.pos).into());
        }
        Ok(())
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(cmd) = self.index.get(&key) {
            let reader = self
                .store
                .get_mut(&cmd.gen)
                .expect("Can not get reader by cmd.gen");
            reader.seek(SeekFrom::Start(cmd.pos))?;
            let cmd_reader = reader.take(cmd.len);
            if let Command::Set { value, .. } = serde_json::from_reader(cmd_reader)? {
                Ok(Some(value))
            } else {
                Err(KvsError::UnexpectedCommandType)
            }
        } else {
            Ok(None)
        }
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        if self.index.contains_key(&key) {
            let cmd = Command::remove(key);
            serde_json::to_writer(&mut self.log, &cmd)?;
            self.log.flush()?;
            if let Command::Remove { key } = cmd {
                self.index.remove(&key).expect("key not found");
            }
            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }
}

fn load(
    gen: u64,
    reader: &mut BufReaderWithPos<File>,
    index: &mut BTreeMap<String, CommandPos>,
) -> Result<u64> {
    let mut pos = reader.seek(SeekFrom::Start(0))?;
    let mut stream = Deserializer::from_reader(reader).into_iter::<Command>();
    while let Some(cmd) = stream.next() {
        let new_pos = stream.byte_offset() as u64;
        match cmd? {
            Command::Set { key, .. } => {
                index.insert(key, (gen, pos..new_pos).into());
            }
            Command::Remove { key } => {
                index.remove(&key).expect("key not in log file");
            }
        }
        pos = new_pos;
    }
    Ok(gen)
}

/// Returns sorted generation numbers in the given directory
fn sorted_gen_list(path: &Path) -> Result<Vec<u64>> {
    let mut gen_list: Vec<u64> = fs::read_dir(&path)?
        .flat_map(|res| -> Result<_> { Ok(res?.path()) })
        .filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
        .flat_map(|path| {
            path.file_name()
                .and_then(OsStr::to_str)
                .map(|s| s.trim_end_matches(".log"))
                .map(str::parse::<u64>)
        })
        .flatten()
        .collect();
    gen_list.sort_unstable();
    Ok(gen_list)
}

fn new_log_file(
    path: &Path,
    gen: u64,
    // readers: &mut HashMap<u64, BufReaderWithPos<File>>,
) -> Result<BufWriterWithPos<File>> {
    let path = log_path(&path, gen);
    let writer = BufWriterWithPos::new(
        OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&path)?,
    )?;
    // readers.insert(gen, BufReaderWithPos::new(File::open(&path)?)?);
    Ok(writer)
}

fn log_path(dir: &Path, gen: u64) -> PathBuf {
    dir.join(format!("{:016}.log", gen))
}

/// Struct representing a command
#[derive(Serialize, Deserialize, Debug)]
enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}

impl Command {
    fn set(key: String, value: String) -> Command {
        Command::Set { key, value }
    }

    fn remove(key: String) -> Command {
        Command::Remove { key }
    }
}

/// Represents the position and length of a json-serialized command in the log
#[derive(Debug)]
struct CommandPos {
    gen: u64,
    pos: u64,
    len: u64,
}

impl From<(u64, Range<u64>)> for CommandPos {
    fn from((gen, range): (u64, Range<u64>)) -> Self {
        CommandPos {
            gen,
            pos: range.start,
            len: range.end - range.start,
        }
    }
}

#[derive(Debug)]
struct BufReaderWithPos<R: Read + Seek> {
    reader: BufReader<R>,
    pos: u64,
}

impl<R: Read + Seek> BufReaderWithPos<R> {
    fn new(mut inner: R) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(BufReaderWithPos {
            reader: BufReader::new(inner),
            pos,
        })
    }
}

impl<R: Read + Seek> Read for BufReaderWithPos<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len = self.reader.read(buf)?;
        self.pos += len as u64;
        Ok(len)
    }
}

impl<R: Read + Seek> Seek for BufReaderWithPos<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.reader.seek(pos)?;
        Ok(self.pos)
    }
}

#[derive(Debug)]
struct BufWriterWithPos<W: Write + Seek> {
    writer: BufWriter<W>,
    pos: u64,
}

impl<W: Write + Seek> BufWriterWithPos<W> {
    fn new(mut inner: W) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(BufWriterWithPos {
            writer: BufWriter::new(inner),
            pos,
        })
    }
}

impl<W: Write + Seek> Write for BufWriterWithPos<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = self.writer.write(buf)?;
        self.pos += len as u64;
        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl<W: Write + Seek> Seek for BufWriterWithPos<W> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.writer.seek(pos)?;
        Ok(self.pos)
    }
}

type LogManagement<W: Write + Seek> = BufWriterWithPos<W>;

#[derive(Debug, PartialEq)]
struct Filename {
    general: u64,
    valid: bool,
}

impl Filename {
    pub fn new(general: u64) -> Filename {
        Filename {
            general,
            valid: true,
        }
    }
    pub fn is_valid(&self) -> bool {
        self.valid
    }
}

impl fmt::Display for Filename {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016}", self.general)
    }
}

impl From<&str> for Filename {
    fn from(general: &str) -> Self {
        match general.parse::<u64>() {
            Ok(general) => Filename {
                general,
                valid: true,
            },
            Err(_) => Filename {
                general: 0,
                valid: false,
            },
        }
    }
}

impl FromStr for Filename {
    type Err = ParseIntError;
    fn from_str(s: &str) -> result::Result<Self, Self::Err> {
        Ok(Filename::from(s))
    }
}

#[cfg(test)]
mod test_kv {
    use super::*;

    #[test]
    fn test_log_to_file() {
        let mut store = KvStore::open("data").unwrap();
        store.set("key".to_owned(), "val".to_owned());
    }

    #[test]
    fn test_format() {
        assert_eq!("00000000000000000000000000000001", format!("{:032}", 1));
        assert_eq!(Filename::new(1), "0000000000000001".parse().unwrap());
        let filename = Filename::from("0000000000000001");
        assert_eq!(Filename::new(1), filename);
    }
}
