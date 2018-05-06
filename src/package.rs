//! Representation of package metadata from a YUM repository.

use serde_xml_rs as xml;
use tree_magic as magic;
use flate2::read::GzDecoder;
use reqwest::Client;
use std::fmt::{self, Debug, Display};
use std::fs::{OpenOptions, create_dir_all, rename, remove_file};
use std::iter::Peekable;
use std::io::Read;
use std::path::Path;
use std::time::Duration;
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

    /// Generate the difference between two metadata collections.
    pub fn delta<'s>(&'s self, other: &'s Metadata) -> Vec<Delta<'s>> {
        let start = self.packages();
        let mut start_iter = start.into_iter().peekable();
        let end = other.packages();
        let mut end_iter = end.into_iter().peekable();
        let mut deltas = Vec::new();

        loop {
            match (start_iter.peek(), end_iter.peek()) {
                (Some(_), Some(_)) => {
                    deltas.push(Metadata::compare_first(&mut start_iter, &mut end_iter))
                }
                (Some(_), None) => deltas.push(start_iter.next().unwrap().delete()),
                (None, Some(_)) => deltas.push(end_iter.next().unwrap().fetch()),
                (None, None) => break,
            };
        }

        deltas.sort_unstable();
        deltas
    }

    /// Generate the difference required to download a fresh clone.
    pub fn fresh_delta<'s>(&'s self) -> Vec<Delta<'s>> {
        self.packages.iter()
            .map(Package::fetch)
            .collect()
    }

    /// Compare the heads of two iterators to determine an action to take
    fn compare_first<'s, I>(start: &mut Peekable<I>, end: &mut Peekable<I>) -> Delta<'s>
    where
        I: Iterator<Item = &'s Package>,
    {
        let from = start.peek().unwrap().clone();
        let to = end.peek().unwrap().clone();

        if from.location.href < to.location.href {
            // Delete packages not found in the destination
            start.next().unwrap().delete()
        } else if from.location.href > to.location.href {
            // Fetch new packages
            end.next().unwrap().fetch()
        } else {
            // Retain unchanged packages
            end.next().unwrap();
            start.next().unwrap().retain()
        }
    }

    /// Generate a sorted list of packages for the repository.
    fn packages(&self) -> Vec<&Package> {
        let mut packages: Vec<&Package> = self.packages.iter().collect();

        packages.sort_unstable();
        packages
    }
}

/// Metadata for a single package.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
pub struct Package {
    name: String,
    location: Location,
    version: Version,
    checksum: Checksum,
}

impl Package {
    fn location(&self) -> &str {
        self.location.href.as_ref()
    }

    fn delete<'s>(&'s self) -> Delta<'s> {
        Delta::Delete(self.location())
    }

    fn fetch<'s>(&'s self) -> Delta<'s> {
        Delta::Fetch(self.location())
    }

    fn retain<'s>(&'s self) -> Delta<'s> {
        Delta::Retain(self.location())
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
pub enum Delta<'r> {
    /// Download a new file at a given location (remote -> local)
    Fetch(&'r str),
    /// Keep the existing copy of the file (local)
    Retain(&'r str),
    /// Delete a file at a given location (local)
    Delete(&'r str),
}

impl<'s> Delta<'s> {
    /// Perform the action the single delta action to move towards the new mirror revision.
    pub fn enact(&self, src: &Url, dest: &Path) -> Result<()> {
        debug!("Enacting: {:?}", self);
        match *self {
            Delta::Fetch(package) => {
                debug!("Fetching '{}'", package);
                sync_file(package, src, dest)
            }
            Delta::Retain(package) => {
                debug!("Retaining '{}'", package);
                Ok(())
            }
            Delta::Delete(package) => {
                debug!("Deleting '{}'", package);
                Delta::delete(package, dest)
            }
        }
    }

    fn delete(package: &str, dest: &Path) -> Result<()> {
        let local_path = dest.join(&package);
        if local_path.exists() {
            info!("Deleting {:?}", local_path);
            remove_file(&local_path)?;
        }
        Ok(())
    }

    /// Check if an operation would delete a package.
    pub fn is_delete(&self) -> bool {
        match *self {
            Delta::Delete(_) => true,
            _ => false,
        }
    }
}

/// Synchronise a remote file to a local location.
pub fn sync_file(relative: &str, src: &Url, dest: &Path) -> Result<()> {
    let remote_path = src.join(&relative)?;
    let local_path = dest.join(&relative);
    let temp_path = local_path.with_extension("sync.tmp");

    if local_path.exists() {
        info!("Skipping (already exists) {:?}", remote_path);
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
    pub fn deltas() {
        let local = Metadata::decode_raw(LOCAL_XML).unwrap();
        let remote = Metadata::decode_raw(REMOTE_XML).unwrap();
        let deltas = local.delta(&remote);

        let mut fetches = 0;
        let mut retains = 0;
        let mut replaces = 0;
        let mut deletes = 0;

        for delta in &deltas {
            match *delta {
                Delta::Fetch(_) => fetches += 1,
                Delta::Retain(_) => retains += 1,
                Delta::Replace(_) => replaces += 1,
                Delta::Delete(_) => deletes += 1,
            }
        }

        assert_eq!(fetches, 85);
        assert_eq!(retains, 11263);
        assert_eq!(replaces, 0);
        assert_eq!(deletes, 68);
    }

    #[test]
    fn read_packages() {
        let local = Metadata::decode(&mut LOCAL_XML).unwrap();

        assert_eq!(local.packages.len(), 11331);
    }
}
