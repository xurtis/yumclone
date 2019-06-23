//! Configuration of the repo tool.

use std::collections::HashMap;
use std::path::Path;
use url::Url;
use serde::Deserialize;
use log::{debug, warn, info};

use crate::repo::*;
use crate::urlmux::*;
use crate::error::*;

#[derive(Debug, Deserialize)]
pub struct Config {
    #[serde(with = "url_serde")]
    src: Url,
    dest: String,
    #[serde(default)]
    tags: HashMap<String, Vec<String>>,
}

macro_rules! try_load_mirror {
    ($fn:path, $url:expr) => {
        match $fn($url) {
            Err(e) => {
                if let Some(backtrace) = e.backtrace() {
                    debug!("Error Backtrace:\n{:?}", backtrace);
                }
                warn!("Error: {}", e);
                warn!("Could not load '{}' (skipping)", $url);
                continue;
            }
            Ok(mirror) => mirror,
        }
    }
}

impl Config {
    pub fn sync(&self) -> Result<()>{
        let url_pairs = UrlMux::new(
            self.src.as_str(),
            &self.dest,
            &self.tags
        );

        // Enumerate Variants
        for (src, dest) in url_pairs {
            info!("Syncing '{}' to '{}'", src, dest);
            let remote = try_load_mirror!(Mirror::remote, &src);

            if let Some(local) = try_load_mirror!(Mirror::local, &dest) {
                if remote.same_version(&local) {
                    info!("Repository '{}' is up to date", dest);
                    continue;
                }
            }

            info!("Downloading repo from '{}'", src);
            let remote = remote.into_cache()?;
            remote.clone(&Path::new(&dest))?;
            if let Some(local) = try_load_mirror!(Mirror::local, &dest) {
                info!("Cleaning repo in '{}'", dest);
                local.clean()?;
            }
        }

        // Download new metadata
        // Load local index
        // Load remote index
        // Diff index
        // Download new
        // Download replaces
        // Replace metadata
        // Remove expired
        Ok(())
    }
}
