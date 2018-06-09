//! Configuration of the repo tool.

use std::collections::HashMap;
use std::path::Path;
use std::fs::File;
use reqwest::Client;
use url::Url;
use url_serde;

use error_chain::ChainedError;

use error::*;
use repo::*;
use urlmux::*;

#[derive(Debug, Deserialize)]
pub struct Config {
    #[serde(with = "url_serde")]
    src: Url,
    dest: String,
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
                info!("Updating repo in '{}'", dest);
                if !remote.same_version(&local) {
                    let remote = remote.into_cache()?;
                    remote.replace(&local)?;
                    info!("Cleaning repo in '{}'", dest);
                    local.clean()?;
                } else {
                    info!("Repository '{}' is up to date", dest);
                }
            } else {
                info!("Downloading fresh repo from '{}'", src);
                let remote = remote.into_cache()?;
                remote.clone_to(&dest)?;
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
