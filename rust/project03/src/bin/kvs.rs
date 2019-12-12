#[macro_use]
extern crate log;

extern crate env_logger;

use clap::{App, AppSettings, Arg, SubCommand};

use kvs::{KvStore, KvsError, Result};

use std::env::current_dir;
use std::path::PathBuf;
use std::process::exit;

fn main() -> Result<()> {
    env_logger::init();

    let matches = App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .setting(AppSettings::DisableHelpSubcommand)
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .setting(AppSettings::VersionlessSubcommands)
        .arg(Arg::with_name("data").help("data dir"))
        .subcommand(
            SubCommand::with_name("set")
                .about("Set the value of a string key to a string")
                .arg(Arg::with_name("KEY").help("A string key").required(true))
                .arg(
                    Arg::with_name("VALUE")
                        .help("The string value of the key")
                        .required(true),
                ),
        )
        .subcommand(
            SubCommand::with_name("get")
                .about("Get the string value of a given string key")
                .arg(Arg::with_name("KEY").help("A string key").required(true)),
        )
        .subcommand(
            SubCommand::with_name("rm")
                .about("Remove a given key")
                .arg(Arg::with_name("KEY").help("A string key").required(true)),
        )
        .get_matches();

    let data_arg = matches.value_of("data").unwrap_or("");
    let data_path = if !data_arg.is_empty() {
        PathBuf::from(data_arg)
    } else {
        current_dir()?
    };
    info!("data_path: {}", data_arg);
    
    match matches.subcommand() {
        ("set", Some(matches)) => {
            let key = matches.value_of("KEY").expect("KEY argument missing");
            let value = matches.value_of("VALUE").expect("VALUE argument missing");
            debug!("cmd: {}, key: {}, val: {}", "set", key, value);
            let mut store = KvStore::open(&data_path)?;
            store.set(key.to_string(), value.to_string())?;
            debug!("set success");
        }
        ("get", Some(matches)) => {
            let key = matches.value_of("KEY").expect("KEY argument missing");
            debug!("cmd: {}, key: {}", "get", key);

            let mut store = KvStore::open(&data_path)?;
            if let Some(value) = store.get(key.to_string())? {
                debug!("get key: {}", key);
                println!("{}", value);
            } else {
                error!("Key not found: {}", key);
            }
        }
        ("rm", Some(matches)) => {
            let key = matches.value_of("KEY").expect("KEY argument missing");

            let mut store = KvStore::open(&data_path)?;
            match store.remove(key.to_string()) {
                Ok(()) => {}
                Err(KvsError::KeyNotFound) => {
                    error!("key not found: {}", key);
                    exit(1);
                }
                Err(e) => return Err(e),
            }
        }
        _ => unreachable!(),
    }
    Ok(())
}
