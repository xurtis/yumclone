//! Configuration of the repo tool.

use log::{debug, info, warn};
use reqwest::Client;
use serde::Deserialize;
use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

use crate::package::CheckType;
use crate::repo::*;
use crate::urlmux::*;

type Result<T> = ::std::result::Result<T, ::failure::Error>;

#[derive(Debug, Deserialize)]
pub struct Config {
    src: String,
    dest: String,
    #[serde(default)]
    tags: HashMap<String, Vec<String>>,
}

impl Config {
    pub async fn sync(&self, check: CheckType) -> Result<()> {
        let url_pairs = UrlMux::new(&self.src, &self.dest, &self.tags);

        // Use a shared connection for each repo
        let client = Client::builder()
            .timeout(Duration::from_secs(600))
            .gzip(false)
            .build()?;

        // Enumerate Variants
        for (src, dest) in url_pairs {
            info!("Syncing '{}' to '{}'", src, dest);

            if let Err(err) = self.sync_pair(&client, (&src, &dest), check).await {
                debug!("Error Backtrace:\n{:?}", err.backtrace());
                warn!("Error: {}", err);
            }
        }

        Ok(())
    }

    async fn sync_pair(&self, client: &Client, pair: (&str, &str), check: CheckType) -> Result<()> {
        let (src, dest) = pair;
        let remote = Mirror::remote(&client, &src).await?;

        if let Some(local) = Mirror::local(&dest).await? {
            if remote.same_version(&local) && check.remote_only() {
                info!("Repository '{}' is up to date", dest);
                return Ok(());
            }
        }

        info!("Downloading repo from '{}'", src);
        let remote = remote.into_cache(client).await?;
        remote.clone(client, &Path::new(&dest), check).await?;
        if let Some(local) = Mirror::local(&dest).await? {
            info!("Cleaning repo in '{}'", dest);
            local.clean().await?;
        }

        Ok(())
    }
}
