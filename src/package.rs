//! Representation of package metadata from a YUM repository.

use serde_xml_rs as xml;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use tree_magic as magic;
use flate2::read::GzDecoder;
use hex;
use log::{debug, info};
use openssl::hash::{Hasher, MessageDigest};
use reqwest::Client;
use std::fmt::{self, Debug, Display};
use std::fs::{File, OpenOptions, create_dir_all, rename};
use std::io::Read;
use std::path::Path;
use std::time::Duration;
use std::collections::BTreeSet;
use url::Url;

use failure::{format_err, bail};
type Result<T> = ::std::result::Result<T, ::failure::Error>;

/// A set of files that can be loaded from XML and fetched.
pub trait Fetch: DeserializeOwned {
    /// Generate a sorted list of packages for the repository.
    fn files(&self) -> BTreeSet<(&str, &Checksum)>;

    /// Decode a stream into metadata
    fn decode<R: Read>(source: &mut R) -> Result<Self> {
        let mut bytes = Vec::new();
        source.read_to_end(&mut bytes)?;
        Self::decode_raw(bytes.as_slice())
    }

    /// Decode a raw slice of data
    fn decode_raw(source: &[u8]) -> Result<Self> {
        if magic::match_u8("application/gzip", source) {
            debug!("Metadata is gzip encoded");
            Ok(xml::deserialize(GzDecoder::new(source))?)
        } else if magic::match_u8("application/xml", source) {
            debug!("Metadata is raw xml");
            Ok(xml::deserialize(source)?)
        } else {
            Err(format_err!("Primary metadata in incompatible filetype"))
        }
    }

    /// Download all files to destination.
    fn sync_all(&self, src: &Url, dest: &Path, check: bool) -> Result<()> {
        for (file, checksum) in self.files() {
            let checksum = if check { Some(checksum) } else { None };
            sync_file(file, src, dest, checksum)?;
        }

        Ok(())
    }
}

/// A collection of package metadata.
#[derive(Debug, Deserialize)]
pub struct Metadata {
    #[serde(rename = "package", default)]
    packages: Vec<Package>,
}

impl Fetch for Metadata {
    fn files(&self) -> BTreeSet<(&str, &Checksum)> {
        self.packages().into_iter().map(|p| (p.location(), &p.checksum)).collect()
    }
}

impl Metadata {
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
    fn fmt(&self, f: &mut fmt::Formatter<'_>)
        -> ::std::result::Result<(), fmt::Error>
    {
        write!(f, "ver({}, {}, {})", self.epoch, self.ver, self.rel)
    }
}

impl Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>)
        -> ::std::result::Result<(), fmt::Error>
    {
        write!(f, "{}-{}-{}", self.epoch, self.ver, self.rel)
    }
}

/// A collection of delta files.
#[derive(Debug, Deserialize)]
pub struct PrestoDelta {
    #[serde(rename = "newpackage", default)]
    new_packages: Vec<NewPackage>,
}

impl Fetch for PrestoDelta {
    fn files(&self) -> BTreeSet<(&str, &Checksum)> {
        self.new_packages.iter()
            .fold(BTreeSet::new(), |set, new_package| {
                new_package.deltas.iter().fold(set, |mut set, delta| {
                    set.insert((delta.filename.as_ref(), &delta.checksum));
                    set
                })
            })
    }
}

#[derive(Debug, Deserialize)]
struct NewPackage {
    name: String,
    version: String,
    #[serde(rename = "delta", default)]
    deltas: Vec<Delta>,
}

#[derive(Debug, Deserialize)]
struct Delta {
    filename: String,
    checksum: Checksum,
}

/// Location information for a package.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
struct Location {
    /// Location of the package relative to the root.
    href: String,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
pub struct Checksum {
    #[serde(rename = "type")]
    algorithm: String,
    #[serde(rename = "$value")]
    sum: String,
}

impl Checksum {
    fn check(&self, path: impl AsRef<Path>) -> Result<bool> {
        let digest = match self.algorithm.as_str() {
            "md5" => MessageDigest::md5(),
            "sha1" => MessageDigest::sha1(),
            "sha224" => MessageDigest::sha224(),
            "sha256" => MessageDigest::sha256(),
            "sha384" => MessageDigest::sha384(),
            "sha512" => MessageDigest::sha512(),
            "ripemd160" => MessageDigest::ripemd160(),
            unknown => bail!("Unknown checksum alogorithm: {}", unknown),
        };

        let mut hasher = Hasher::new(digest)?;

        let mut file = File::open(path)?;
        let mut block = [0; 4096];

        loop {
            let bytes_read = file.read(&mut block)?;
            if bytes_read == 0 {
                break;
            }

            hasher.update(&block[0..bytes_read])?;
        }

        let sum_bytes = hasher.finish()?;
        let sum = hex::encode(&sum_bytes);

        Ok(sum == self.sum)
    }
}

/// Synchronise a remote file to a local location.
pub fn sync_file(relative: &str, src: &Url, dest: &Path, checksum: Option<&Checksum>) -> Result<()> {
    let remote_path = src.join(&relative)?;
    let local_path = dest.join(&relative);
    let temp_path = local_path.with_extension("sync.tmp");

    if local_path.exists() {
        if let Some(checksum) = checksum {
            if checksum.check(&local_path)? {
                debug!("Skipping (already exists with valid checksum) {:?}", remote_path);
                return Ok(());
            } else {
                debug!("Local file failed checksum {:?}", local_path);
            }
        } else {
            debug!("Skipping (already exists) {:?}", remote_path);
            return Ok(());
        }
    }

    info!("Downloading \"{}\" to {:?}", remote_path, local_path);

    create_dir_all(local_path.parent().expect("Invalid repository structure"))?;
    download(remote_path, &temp_path)?;
    if let Some(checksum) = checksum {
        if !checksum.check(&temp_path)? {
            bail!("Remote file failed checksum {:?}", temp_path);
        }
    }
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
    use super::{Metadata, Fetch};

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
