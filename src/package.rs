//! Representation of package metadata from a YUM repository.

use serde_xml_rs as xml;
use tree_magic as magic;
use flate2::read::GzDecoder;
use std::error::Error;
use std::fmt::{self, Debug, Display};
use std::iter::Peekable;
use std::io::Read;
use serde::Deserialize;

/// A collection of package metadata.
#[derive(Debug, Deserialize)]
pub struct Metadata {
    #[serde(rename = "package", default)]
    packages: Vec<Package>,
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

    fn replace<'s>(&'s self) -> Delta<'s> {
        Delta::Replace(self.location())
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
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "ver({}, {}, {})", self.epoch, self.ver, self.rel)
    }
}

impl Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
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
    /// Replace a file at a given location (remote -> local)
    Replace(&'r str),
    /// Keep the existing copy of the file (local)
    Retain(&'r str),
    /// Delete a file at a given location (local)
    Delete(&'r str),
}

fn error_string<T, E: Error>(result: Result<T, E>) -> Result<T, String> {
    result.map_err(|e| e.description().to_string())
}

fn deserialize<'de, R: Read, T: Deserialize<'de>>(source: R) -> Result<T, String> {
    error_string(xml::deserialize(source))
}

impl Metadata {
    /// Decode a stream into metadata
    pub fn decode<R: Read>(source: &mut R) -> Result<Metadata, String> {
        let mut bytes = Vec::new();
        error_string(source.read_to_end(&mut bytes))?;
        Metadata::decode_raw(bytes.as_slice())
    }

    /// Decode a raw slice of data
    pub fn decode_raw(source: &[u8]) -> Result<Metadata, String> {
        eprintln!("Checking type");
        if magic::match_u8("application/gzip", source) {
            eprintln!("Found gzip");
            deserialize(GzDecoder::new(source))
        } else if magic::match_u8("application/xml", source) {
            eprintln!("Found xml");
            deserialize(source)
        } else {
            Err("Incompatible file type".to_string())
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

        deltas.sort();
        deltas
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
        } else if from.checksum != to.checksum {
            // Replace changed packages
            start.next().unwrap();
            end.next().unwrap().replace()
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
