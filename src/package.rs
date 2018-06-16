//! Representation of package metadata from a YUM repository.

use serde_xml_rs as xml;
use tree_magic as magic;
use flate2::read::GzDecoder;
use reqwest::Client;
use std::fmt::{self, Debug, Display};
use std::fs::{OpenOptions, create_dir_all, rename};
use std::io::Read;
use std::path::Path;
use std::time::Duration;
use std::collections::HashSet;
use url::Url;

use error::*;

/// A collection of package metadata.
#[derive(Debug, Deserialize)]
pub struct Metadata {
    #[serde(rename = "package", default)]
    packages: Vec<Package>,
}

impl Metadata {
    /// Decode a stream into metadata
    pub fn decode<R: Read>(source: &mut R) -> Result<Metadata> {
        let mut bytes = Vec::new();
        source.read_to_end(&mut bytes)?;
        Metadata::decode_raw(bytes.as_slice())
    }

    /// Decode a raw slice of data
    pub fn decode_raw(source: &[u8]) -> Result<Metadata> {
        if magic::match_u8("application/gzip", source) {
            debug!("Metadata is gzip encoded");
            Ok(xml::deserialize(GzDecoder::new(source))?)
        } else if magic::match_u8("application/xml", source) {
            debug!("Metadata is raw xml");
            Ok(xml::deserialize(source)?)
        } else {
            Err(ErrorKind::IncompatiblePrimaryMeta.into())
        }
    }

    /// Generate a sorted list of packages for the repository.
    fn packages(&self) -> Vec<&Package> {
        let mut packages: Vec<&Package> = self.packages.iter().collect();

        packages.sort_unstable();
        packages
    }

    /// Get a set of all of the package files in a repo.
    pub fn package_files(&self) -> HashSet<&str> {
        self.packages().into_iter().map(|p| p.location()).collect()
    }

    /// Download all files to destination.
    pub fn sync_all(&self, src: &Url, dest: &Path) -> Result<()> {
        for package in self.packages() {
            sync_file(package.location(), src, dest)?;
        }

        Ok(())
    }
}

/// Metadata for a single package.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
pub struct Package {
    location: Location,
    version: Version,
    name: String,
    checksum: Checksum,
}

impl Package {
    fn location(&self) -> &str {
        self.location.href.as_ref()
    }
}

/// Version metadata for a single package.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
pub struct Version {
    epoch: String,
    ver: String,
    rel: String,
}

impl Debug for Version {
    fn fmt(&self, f: &mut fmt::Formatter)
        -> ::std::result::Result<(), fmt::Error>
    {
        write!(f, "ver({}, {}, {})", self.epoch, self.ver, self.rel)
    }
}

impl Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter)
        -> ::std::result::Result<(), fmt::Error>
    {
        write!(f, "{}-{}-{}", self.epoch, self.ver, self.rel)
    }
}

/// Location information for a package.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
struct Location {
    /// Location of the package relative to the root.
    href: String,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
struct Checksum {
    #[serde(rename = "type")]
    algorithm: String,
    #[serde(rename = "$value")]
    sum: String,
}

/// For the file at the given path relative to the repository root,
/// what action should be taken to advance the syncronisation.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
enum Delta<'r> {
    /// Download a new file at a given location (remote -> local)
    Fetch(&'r str),
    /// Keep the existing copy of the file (local)
    Retain(&'r str),
    /// Delete a file at a given location (local)
    Delete(&'r str),
}

/// Synchronise a remote file to a local location.
pub fn sync_file(relative: &str, src: &Url, dest: &Path) -> Result<()> {
    let remote_path = src.join(&relative)?;
    let local_path = dest.join(&relative);
    let temp_path = local_path.with_extension("sync.tmp");

    if local_path.exists() {
        debug!("Skipping (already exists) {:?}", remote_path);
        return Ok(());
    }

    info!("Downloading \"{}\" to {:?}", remote_path, local_path);

    create_dir_all(local_path.parent().expect("Invalid repository structure"))?;
    download(remote_path, &temp_path)?;
    rename(&temp_path, &local_path)?;
    Ok(())
}

/// Download a network file to a local file
fn download(src: Url, dest: &Path) -> Result<()> {
    let client = Client::builder()
        .timeout(Some(Duration::from_secs(600)))
        .gzip(false)
        .build()?;
    let mut local = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(dest)?;
    let mut remote = client.get(src).send()?;
    remote.copy_to(&mut local)?;
    Ok(())
}

#[cfg(test)]
mod test {
    use super::{Metadata, Delta};

    const LOCAL_XML: &[u8] = include_bytes!(
        "test-data/local/repodata/84fe7bb9cf340186df02863647f41a4be32c86a21b80eaaeddaa97e99a24b7a6-primary.xml.gz"
    );
    const REMOTE_XML: &[u8] = include_bytes!(
        "test-data/remote/repodata/328a9f961ff596aedac41d051634325110b8fb30b87c00f678c257644337d1d6-primary.xml.gz"
    );

    #[test]
    fn read_packages() {
        let local = Metadata::decode(&mut LOCAL_XML).unwrap();

        assert_eq!(local.packages.len(), 11331);
    }
}
