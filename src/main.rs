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

extern crate env_logger;
#[macro_use]
extern crate error_chain;
extern crate flate2;
extern crate loadconf;
#[macro_use]
extern crate log;
extern crate regex;
extern crate reqwest;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_xml_rs;
extern crate tempdir;
extern crate tree_magic;
extern crate url;
extern crate url_serde;

use loadconf::Load;

pub mod config;
pub mod error;
pub mod package;
mod repo;
pub mod urlmux;

use config::Config;
pub use repo::Repo;

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

fn main() {
    env_logger::init();
    let configs: Configs = Load::try_load(env!("CARGO_PKG_NAME"))
        .expect("Could not load configuration");

    for repo in configs.repo {
        debug!("Loaded repo: {:?}", repo);
        if let Err(e) = repo.sync() {
            panic!("Error synchronising: {}'", e);
        }
    }
}

