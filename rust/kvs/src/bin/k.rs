use structopt::clap::AppSettings;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
pub struct Set {
    pub key: String,
    pub val: String,
}

#[derive(Debug, StructOpt)]
pub struct Get {
    pub key: String,
}

#[derive(Debug, StructOpt)]
pub struct Remove {
    pub key: String,
}

#[derive(Debug, StructOpt)]
pub enum Command {
    #[structopt(name = "set")]
    Set(Set),
    
    #[structopt(name = "get")]
    Get(Get),

    #[structopt(name = "rm")]
    Remove(Remove),
}

#[derive(StructOpt, Debug)]
#[structopt(name = "kvs")]
#[structopt(setting = AppSettings::DisableHelpSubcommand)]
#[structopt(setting = AppSettings::SubcommandRequiredElseHelp)]
#[structopt(setting = AppSettings::VersionlessSubcommands)]
struct Opt {
    #[structopt(subcommand)]
    pub command: Command,
}

fn main() {
    let opt = Opt::from_args();
    println!("{:#?}", opt);
}
