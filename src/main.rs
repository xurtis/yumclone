//! Yum Repository Cloning
//!
//! This clones repositories by specifying, for each repository to clone:
//!
//! * A set of tags to replace and the variants to replace them with,
//! * The source URL,
//! * The destination path, and
//! * (optionally) the URL for the repositories GPG key.
//!
//! In turn, this will diff the metadata for all variations of the remote
//! and local versions of the repository and update the local to match the
//! remote.

#![warn(missing_docs)]

use loadconf::Load;
use log::{debug, error};
use serde::Deserialize;
use structopt::StructOpt;

pub mod config;
pub mod package;
mod repo;
pub mod urlmux;

use crate::config::Config;
pub use crate::repo::Repo;

#[derive(Debug, Deserialize)]
struct Configs {
    repo: Vec<Config>,
}

impl Default for Configs {
    fn default() -> Configs {
        Configs {
            repo: Vec::default(),
        }
    }
}

#[derive(StructOpt)]
#[structopt(about = "Synchronise a remote rpm repository.")]
struct Args {
    /// Ensure that local files match their checksums
    #[structopt(short = "c", long = "check")]
    check: bool,
    /// Configuration file
    #[structopt(short = "C", long = "config")]
    config: Option<String>,
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let args = Args::from_args();
    let config_file = args.config.as_ref().map(|s| s.as_str()).unwrap_or(env!("CARGO_PKG_NAME"));
    let configs: Configs = Load::try_load(config_file)
        .expect("Could not load configuration");

    for repo in configs.repo {
        debug!("Loaded repo: {:?}", repo);
        if let Err(e) = repo.sync(args.check).await {
            error!("Error synchronising: {}'", e);
            debug!("Error backtrace:\n{:?}", e.backtrace());
        }
    }
}

