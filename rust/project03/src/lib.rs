#[macro_use]
extern crate log;

pub use error::{KvsError, Result};
pub use kv::KvStore;

mod error;
mod kv;
