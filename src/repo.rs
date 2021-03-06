//! Represetnation of repository metadata.

use std::cmp::PartialEq;
use std::collections::HashSet;
use std::env::current_dir;
use std::marker::Unpin;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use tokio::fs::{create_dir_all, read_dir, remove_file, File, OpenOptions};
use tokio::io::{copy, AsyncRead, AsyncReadExt};

use failure::format_err;
use log::{debug, info};
use reqwest::{Client, Url};
use serde::*;
use serde_xml_rs as xml;
use tempdir::TempDir;
use walkdir::WalkDir;

use crate::package::{decode, sync_all, sync_file, Check, CheckType, Fetch, Metadata, PrestoDelta};

pub const MD_DIR: &'static str = "repodata";
pub const MD_PATH: &'static str = "repodata/repomd.xml";

type Result<T> = ::std::result::Result<T, ::failure::Error>;

/// A mirror of a repository at a particular locaiton.
pub struct Mirror {
    repo: Repo,
    location: Url,
}

impl Mirror {
    fn new(repo: Repo, location: Url) -> Mirror {
        Mirror {
            repo: repo,
            location: location,
        }
    }

    /// Download a mirror metadata from a remote location.
    pub async fn remote(client: &Client, url: &str) -> Result<Mirror> {
        let md_url = Url::parse(url)?.join(MD_PATH)?;
        debug!("Loading remote metadata from '{}'", md_url);
        let raw = client.get(md_url).send().await?.text().await?;
        let repo = Repo::decode(&mut raw.as_bytes()).await?;

        Ok(Mirror::new(repo, Url::parse(url)?))
    }

    /// Load a mirror from a local location.
    pub async fn local(path: &str) -> Result<Option<Mirror>> {
        let local_path = current_dir()?.join(path);
        let local_path_str = local_path
            .to_str()
            .ok_or(format_err!("Couldn't decode directory: {}", path))?;
        let url = Url::parse("file:///")?.join(local_path_str)?;

        let md_path = Path::new(path).join(MD_PATH);
        debug!("Loading local metadata from {:?}", md_path);

        if !md_path.exists() {
            return Ok(None);
        }

        let mut raw = String::new();
        File::open(md_path).await?.read_to_string(&mut raw).await?;
        let repo = Repo::decode(&mut raw.as_bytes()).await?;
        Ok(Some(Mirror::new(repo, url)))
    }

    /// Compare the versions of two mirrors.
    pub fn same_version(&self, other: &Mirror) -> bool {
        self.repo == other.repo
    }

    /// Create a local cache of all metadata.
    pub async fn into_cache(self, client: &Client) -> Result<Cache> {
        Cache::new(client, self).await
    }

    /// Get the package listing for the cached repository.
    pub async fn metadata(&self, base_path: &Path) -> Result<Metadata> {
        let primary_path = base_path.join(self.repo.primary_path()?);
        Ok(decode(&mut File::open(primary_path).await?).await?)
    }

    /// Get the listing of deltas.
    pub async fn prestodelta(&self, base_path: &Path) -> Result<Option<PrestoDelta>> {
        if let Some(prestodelta_path) = self.repo.prestodelta_path() {
            let prestodelta_path = base_path.join(prestodelta_path);
            Ok(Some(
                decode(&mut File::open(prestodelta_path).await?).await?,
            ))
        } else {
            Ok(None)
        }
    }

    /// Remove all extraneous files.
    pub async fn clean(&self) -> Result<()> {
        let base_path = Path::new(self.location.path());
        let metadata = self.metadata(base_path).await?;
        let prestodelta = self.prestodelta(base_path).await?;
        debug!("Removing extraneous files in '{:?}'", base_path);

        let mut files: HashSet<_> = self.repo.meta_files().into_iter().map(Path::new).collect();

        let package_files = metadata.files();

        for (file, _, _) in package_files {
            files.insert(Path::new(file));
        }

        if let Some(deltas) = &prestodelta {
            for (file, _, _) in deltas.files() {
                files.insert(Path::new(file));
            }
        }

        for entry in WalkDir::new(base_path) {
            let file = entry?;
            let rel_path = file.path().strip_prefix(base_path)?;
            debug!("Found '{:?}'", rel_path);
            if !file.file_type().is_dir() && !files.contains(&rel_path) {
                let path = base_path.join(rel_path);
                info!("Removing '{:?}'", path);
                remove_file(&path).await?;
            }
        }

        Ok(())
    }
}

pub struct Cache {
    mirror: Mirror,
    dir: TempDir,
}

impl Cache {
    async fn new(client: &Client, mirror: Mirror) -> Result<Cache> {
        let cache_dir = TempDir::new(env!("CARGO_PKG_NAME"))?;
        debug!("Caching metadata in {}", cache_dir.path().to_str().unwrap());
        mirror
            .repo
            .download_meta(client, &mirror.location, cache_dir.path())
            .await?;

        Ok(Cache {
            mirror: mirror,
            dir: cache_dir,
        })
    }

    pub async fn clone(&self, client: &Client, dest: &Path, check: CheckType) -> Result<()> {
        let packages = self.metadata(self.dir.path()).await?;
        sync_all(client, &packages, &self.mirror.location, dest, check).await?;
        if let Some(deltas) = self.prestodelta(self.dir.path()).await? {
            sync_all(client, &deltas, &self.mirror.location, dest, check).await?;
        }
        self.replace_metadata(dest).await
    }

    async fn replace_metadata(&self, dest: &Path) -> Result<()> {
        let target_meta_dir = dest.join(MD_DIR);
        let cache_meta_dir = self.dir.path().join(MD_DIR);

        if target_meta_dir.exists() {
            debug!("Replacing existing metadata in {:?}", target_meta_dir);
            // Delete existing metadata
            let mut entries = read_dir(&target_meta_dir).await?;
            while let Some(entry) = entries.next_entry().await? {
                let path = entry.path();
                debug!("Deleting {:?}", path);
                remove_file(path).await?;
            }
        } else {
            debug!("Copying metadata to {:?}", target_meta_dir);
            create_dir_all(&target_meta_dir).await?;
        }

        // Copy new metadata
        let mut entries = read_dir(&cache_meta_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let src = entry.path();
            let dest = target_meta_dir.join(src.file_name().unwrap());
            debug!("Copying {:?} to {:?}", src, dest);
            let mut src = File::open(src).await?;
            let mut dest = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(dest)
                .await?;
            copy(&mut src, &mut dest).await?;
        }

        Ok(())
    }
}

impl Deref for Cache {
    type Target = Mirror;

    fn deref(&self) -> &Mirror {
        &self.mirror
    }
}

/// Representation of a whole repository.
#[derive(Debug, Eq, Deserialize)]
pub struct Repo {
    #[serde(default)]
    revision: Option<u64>,
    #[serde(default)]
    data: Vec<Data>,
}

impl PartialEq for Repo {
    fn eq(&self, other: &Self) -> bool {
        if let (Some(this), Some(that)) = (self.revision, other.revision) {
            this == that
        } else {
            false
        }
    }
}

#[derive(Debug, PartialEq, Eq, Deserialize)]
struct Data {
    #[serde(rename = "type")]
    datum: String,
    location: Location,
}

#[derive(Debug, PartialEq, Eq, Deserialize)]
struct Location {
    href: String,
}

#[derive(Debug)]
pub struct XmlDecodeError(String);

impl std::error::Error for XmlDecodeError {}

impl From<xml::Error> for XmlDecodeError {
    fn from(error: xml::Error) -> Self {
        XmlDecodeError(format!("{}", error))
    }
}

impl From<std::io::Error> for XmlDecodeError {
    fn from(error: std::io::Error) -> Self {
        XmlDecodeError(format!("{}", error))
    }
}

impl std::fmt::Display for XmlDecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "XML decode: {}", self.0)
    }
}

impl Repo {
    /// Read metadata for an entire repository.
    pub async fn decode<R>(source: &mut R) -> ::std::result::Result<Repo, XmlDecodeError>
    where
        R: AsyncReadExt + AsyncRead + Unpin,
    {
        let mut text = String::new();
        source.read_to_string(&mut text).await?;
        Ok(xml::from_str(&text)?)
    }

    /// Returns a list of paths for metadata files to sync.
    pub fn meta_files(&self) -> Vec<&str> {
        let mut files = vec![MD_PATH];
        let mut decoded = self.data.iter().map(|d| d.location.href.as_str()).collect();
        files.append(&mut decoded);
        return files;
    }

    /// Returns the relative path of the primary data file.
    pub fn primary_path(&self) -> Result<PathBuf> {
        self.subsection_path("primary")
            .ok_or(format_err!("No primary metadata found"))
    }

    /// Returns the relative path of the prestodelta data file.
    pub fn prestodelta_path(&self) -> Option<PathBuf> {
        self.subsection_path("prestodelta")
    }

    /// Get the path of a repository subsection.
    pub fn subsection_path(&self, section: &str) -> Option<PathBuf> {
        for datum in &self.data {
            if datum.datum == section {
                let mut path = PathBuf::new();
                path.push(&datum.location.href);
                return Some(path);
            }
        }

        None
    }

    /// Download the contents of a repo to a given path.
    async fn download_meta(&self, client: &Client, src: &Url, dest: &Path) -> Result<()> {
        for file in self.meta_files() {
            sync_file(client, file, src, dest, Check::Metadata).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    const LOCAL_REPOMD: &[u8] = include_bytes!("test-data/local/repodata/repomd.xml");
    const REMOTE_REPOMD: &[u8] = include_bytes!("test-data/remote/repodata/repomd.xml");

    #[tokio::test]
    async fn check_dissimilar() {
        let local = Repo::decode(&mut LOCAL_REPOMD).await.unwrap();
        let remote = Repo::decode(&mut REMOTE_REPOMD).await.unwrap();

        println!("{:?}\n{:?}", local, remote);

        assert_ne!(local, remote);
    }

    #[tokio::test]
    async fn metadata_list() {
        let remote = Repo::decode(&mut LOCAL_REPOMD).await.unwrap();
        let expected = vec![
            MD_PATH,
            "repodata/84fe7bb9cf340186df02863647f41a4be32c86a21b80eaaeddaa97e99a24b7a6-primary.xml.gz",
            "repodata/dbb1542cb57be8d1a4026a1bb85d71aba89b4b30065414ff81feff5bdb258f72-filelists.xml.gz",
            "repodata/ad4b82bdb7098f324d9d1b4813916433af03df2a6786cad07159cca7c95fb945-other.xml.gz",
            "repodata/4a1d0d1aec2dca96bb6a47739f9e6a1dc94be7c74223ecbe1c5f3aae0134e0fd-primary.sqlite.bz2",
            "repodata/d5ef83a3b3837274a93e0887bdfe6b036f9bcea5f173ad7bf5d3cbaa7a634e3f-filelists.sqlite.bz2",
            "repodata/3abfbe409fd9f64a309027cb54047a0d82b4963f42c4280f77006b2e86e45681-other.sqlite.bz2",
            "repodata/a075404e3a20128979eb63827a791eace053fc290cfa467296ca1131945f870d-comps-Everything.x86_64.xml",
            "repodata/4c258c22ce548a792233cdb7b6db19b60d4c27076f6d3658baff2a2a76932a2a-comps-Everything.x86_64.xml.gz",
            "repodata/3943fb04171c11862d9987294da1d78f5c74f218c3cd239d73e91cf9f49de89a-prestodelta.xml.gz",
            "repodata/a567519c08f65a1fce17036b58923f652fe2a23eccd453a7aeb557e1eedd1ccd-updateinfo.xml.xz",
        ];

        assert_eq!(remote.meta_files(), expected);
    }

    #[tokio::test]
    async fn primary_path() {
        let remote = Repo::decode(&mut LOCAL_REPOMD).await.unwrap();
        let expected = Path::new("repodata/84fe7bb9cf340186df02863647f41a4be32c86a21b80eaaeddaa97e99a24b7a6-primary.xml.gz");

        assert_eq!(remote.primary_path().unwrap(), expected);
    }
}
