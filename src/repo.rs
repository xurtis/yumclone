//! Represetnation of repository metadata.

use std::env::current_dir;
use std::fs::{File, read_dir, remove_file, copy, create_dir_all};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::ops::Deref;

use reqwest;
use serde_xml_rs as xml;
use tempdir::TempDir;
use url::Url;

use error::*;
use package::{Metadata, Delta, sync_file};

pub const MD_DIR: &'static str = "repodata";
pub const MD_PATH: &'static str = "repodata/repomd.xml";

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
    pub fn remote(url: &str) -> Result<Mirror> {
        let md_url = Url::parse(url)?.join(MD_PATH)?;
        debug!("Loading remote metadata from '{}'", md_url);
        let mut raw = String::new();
        reqwest::get(md_url)?.read_to_string(&mut raw)?;
        let repo = Repo::decode(&mut raw.as_bytes())?;

        Ok(Mirror::new(repo, Url::parse(url)?))
    }

    /// Load a mirror from a local location.
    pub fn local(path: &str) -> Result<Option<Mirror>> {
        let local_path = current_dir()?
            .join(path);
        let local_path_str = local_path.to_str()
            .ok_or(ErrorKind::CurrentDirDecode)?;
        let url = Url::parse("file:///")?
            .join(local_path_str)?;

        let md_path = Path::new(path).join(MD_PATH);
        debug!("Loading local metadata from {:?}", md_path);

        if !md_path.exists() {
            return Ok(None);
        }

        let mut raw = String::new();
        File::open(md_path)?.read_to_string(&mut raw)?;
        let repo = Repo::decode(&mut raw.as_bytes())?;
        Ok(Some(Mirror::new(repo, url)))
    }

    /// Compare the versions of two mirrors.
    pub fn same_version(&self, other: &Mirror) -> bool {
        self.repo == other.repo
    }

    /// Create a local cache of all metadata.
    pub fn into_cache(self) -> Result<Cache> {
        Cache::new(self)
    }

    /// Get the package listing for the cached repository.
    pub fn metadata(&self, base_path: &Path) -> Result<Metadata> {
        let primary_path = base_path.join(self.repo.primary_path()?);
        Ok(Metadata::decode(&mut File::open(primary_path)?)?)
    }
}

pub struct Cache {
    mirror: Mirror,
    dir: TempDir,
}

impl Cache {
    fn new(mirror: Mirror) -> Result<Cache> {
        let cache_dir = TempDir::new(env!("CARGO_PKG_NAME"))?;
        mirror.repo.download_meta(&mirror.location, cache_dir.path())?;

        Ok(Cache {
            mirror: mirror,
            dir: cache_dir,
        })
    }

    pub fn clone_to(&self, dest: &str) -> Result<()> {
        let packages = self.metadata(self.dir.path())?;
        self.clone(packages.fresh_delta(), Path::new(dest))
    }

    pub fn replace(&self, dest: &Mirror) -> Result<()> {
        let dest_path = Path::new(dest.location.path());
        debug!("Replacing {:?} with {:?}", dest_path, self.location);
        let remote = self.metadata(self.dir.path())?;
        let local = dest.metadata(dest_path)?;
        self.clone(remote.delta(&local), dest_path)
    }

    fn clone<'s>(&self, operations: Vec<Delta<'s>>, dest: &Path) -> Result<()> {
        let client = reqwest::Client::new();
        let mut operations = operations.into_iter().peekable();

        // Download fresh and changed packages
        loop {
            if let Some(e) = operations.peek() {
                if e.is_delete() {
                    break;
                } else {
                    e.enact(&client, &self.location, dest)?;
                }
            } else {
                break;
            }
            operations.next().unwrap();
        }

        // Replace the metadata
        self.replace_metadata(dest)?;

        // Delete old packages
        for operation in operations {
            operation.enact(&client, &self.location, dest)?;
        }
        Ok(())
    }

    fn replace_metadata(&self, dest: &Path) -> Result<()> {
        let target_meta_dir = dest.join(MD_DIR);
        let cache_meta_dir = self.dir.path().join(MD_DIR);

        if target_meta_dir.exists() {
            debug!("Replacing existing metadata in {:?}", target_meta_dir);
            // Delete existing metadata
            for entry in read_dir(&target_meta_dir)? {
                let path = entry?.path();
                debug!("Deleting {:?}", path);
                remove_file(path)?;
            }
        } else {
            debug!("Copying metadata to {:?}", target_meta_dir);
            create_dir_all(&target_meta_dir)?;
        }

        // Copy new metadata
        for entry in read_dir(&cache_meta_dir)? {
            let src = entry?.path();
            let dest = target_meta_dir.join(src.file_name().unwrap());
            debug!("Copying {:?} to {:?}", src, dest);
            copy(src, dest)?;
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
#[derive(Debug, Deserialize)]
pub struct Repo {
    revision: u64,
    #[serde(default)]
    data: Vec<Data>,
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

impl PartialEq for Repo {
    fn eq(&self, other: &Repo) -> bool {
        self.revision == other.revision
    }
}

impl Repo {
    /// Read metadata for an entire repository.
    pub fn decode<R: Read>(source: &mut R) -> ::std::result::Result<Repo, xml::Error> {
        xml::deserialize(source)
    }

    /// Returns a list of paths for metadata files to sync.
    pub fn meta_files(&self) -> Vec<&str> {
        let mut files = vec![MD_PATH];
        let mut decoded = self.data.iter()
            .map(|d| d.location.href.as_str())
            .collect();
        files.append(&mut decoded);
        return files;
    }

    /// Returns the relative path of the primary data file.
    pub fn primary_path(&self) -> Result<PathBuf> {
        for datum in &self.data {
            if datum.datum == "primary" {
                let mut path = PathBuf::new();
                path.push(&datum.location.href);
                return Ok(path);
            }
        }

        Err(ErrorKind::NoPrimaryMeta.into())
    }

    /// Download the contents of a repo to a given path.
    fn download_meta(&self, src: &Url, dest: &Path) -> Result<()> {
        let client = reqwest::Client::new();
        for file in self.meta_files() {
            sync_file(&client, file, src, dest)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    const LOCAL_REPOMD: &[u8] = include_bytes!("test-data/local/repodata/repomd.xml");
    const REMOTE_REPOMD: &[u8] = include_bytes!("test-data/remote/repodata/repomd.xml");

    #[test]
    fn check_dissimilar() {
        let local = Repo::decode(&mut LOCAL_REPOMD).unwrap();
        let remote = Repo::decode(&mut REMOTE_REPOMD).unwrap();

        println!("{:?}\n{:?}", local, remote);

        assert_ne!(local, remote);
    }

    #[test]
    fn metadata_list() {
        let remote = Repo::decode(&mut LOCAL_REPOMD).unwrap();
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

    #[test]
    fn primary_path() {
        let remote = Repo::decode(&mut LOCAL_REPOMD).unwrap();
        let expected = "repodata/84fe7bb9cf340186df02863647f41a4be32c86a21b80eaaeddaa97e99a24b7a6-primary.xml.gz";

        assert_eq!(remote.primary_path().unwrap(), expected);
    }
}
