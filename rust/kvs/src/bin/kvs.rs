use clap::{App, AppSettings, Arg, SubCommand};
use kvs;
use std::process::exit;

fn main() {
    let matches = App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .setting(AppSettings::DisableHelpSubcommand)
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .setting(AppSettings::VersionlessSubcommands)
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

    let mut store = kvs::KvStore::new();

    match matches.subcommand() {
        ("set", Some(_matches)) => {
            let key = _matches.value_of("KEY").unwrap();
            let val = _matches.value_of("VALUE").unwrap();
            println!("set {} {}", key, val);
            store.set(key.to_owned(), val.to_owned());
            exit(1);
        }
        ("get", Some(_matches)) => {
            let key = _matches.value_of("KEY").unwrap();
            println!("get {}", key);
            store.get(key.to_owned());
            exit(1);
        }
        ("rm", Some(_matches)) => {
            let key = _matches.value_of("KEY").unwrap();
            println!("rm {}", key);
            store.remove(key.to_owned());
            exit(1);
        }
        _ => unreachable!(),
    }
}
