//! Representation of package metadata from a YUM repository.

use flate2::read::GzDecoder;
use hex;
use log::{debug, info};
use openssl::hash::{Hasher, MessageDigest};
use reqwest::{Client, Url};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_xml_rs as xml;
use std::collections::BTreeSet;
use std::fmt::{self, Debug, Display};
use std::marker::Unpin;
use std::path::Path;
use std::sync::Arc;
use tokio::fs::{create_dir_all, metadata, rename, File, OpenOptions};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::Mutex;
use tokio::try_join;
use tree_magic as magic;

use failure::{bail, format_err};
type Result<T> = ::std::result::Result<T, ::failure::Error>;

use crate::repo::XmlDecodeError;

/// A set of files that can be loaded from XML and fetched.
pub trait Fetch: DeserializeOwned {
    /// Generate a sorted list of packages for the repository.
    fn files(&self) -> BTreeSet<(&str, u64, &Checksum)>;

    /// Decode a raw slice of data
    fn decode_raw(source: &[u8]) -> Result<Self> {
        if magic::match_u8("application/gzip", source) {
            debug!("Metadata is gzip encoded");
            Ok(xml::from_reader(GzDecoder::new(source)).map_err(XmlDecodeError::from)?)
        } else if magic::match_u8("application/xml", source) {
            debug!("Metadata is raw xml");
            Ok(xml::from_reader(source).map_err(XmlDecodeError::from)?)
        } else {
            Err(format_err!("Primary metadata in incompatible filetype"))
        }
    }
}

/// Decode a stream into metadata
pub async fn decode<R, F>(source: &mut R) -> Result<F>
where
    R: AsyncReadExt + AsyncRead + Unpin,
    F: Fetch,
{
    let mut bytes = Vec::new();
    source.read_to_end(&mut bytes).await?;
    F::decode_raw(bytes.as_slice())
}

/// Download all files to destination.
pub async fn sync_all(
    client: &Client,
    fetch: &impl Fetch,
    src: &Url,
    dest: &Path,
    check: CheckType,
) -> Result<()> {
    let queue = Arc::new(Mutex::new(fetch.files().into_iter()));

    let worker = move || {
        let queue = queue.clone();
        async move {
            while let Some((file, size, checksum)) = queue.lock().await.next() {
                let check = match check {
                    CheckRemoteSize => Check::RemoteSize(size),
                    CheckSize => Check::Size(size),
                    CheckHash => Check::Hash(size, checksum),
                };
                sync_file(client, file, src, dest, check).await?
            }
            Ok(())
        }
    };

    try_join!(
        worker(),
        worker(),
        worker(),
        worker(),
        worker(),
        worker(),
        worker(),
        worker()
    )
    .map(|_| ())
}

/// A collection of package metadata.
#[derive(Debug, Deserialize)]
pub struct Metadata {
    #[serde(rename = "package", default)]
    packages: Vec<Package>,
}

impl Fetch for Metadata {
    fn files(&self) -> BTreeSet<(&str, u64, &Checksum)> {
        self.packages()
            .into_iter()
            .map(|p| (p.location(), p.size.package, &p.checksum))
            .collect()
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
    size: Size,
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
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> ::std::result::Result<(), fmt::Error> {
        write!(f, "ver({}, {}, {})", self.epoch, self.ver, self.rel)
    }
}

impl Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> ::std::result::Result<(), fmt::Error> {
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
    fn files(&self) -> BTreeSet<(&str, u64, &Checksum)> {
        self.new_packages
            .iter()
            .fold(BTreeSet::new(), |set, new_package| {
                new_package.deltas.iter().fold(set, |mut set, delta| {
                    set.insert((delta.filename.as_ref(), delta.size, &delta.checksum));
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
    size: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
struct Size {
    package: u64,
    installed: u64,
    archive: u64,
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
    async fn check(&self, path: impl AsRef<Path>) -> Result<bool> {
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

        let mut file = File::open(path).await?;
        let mut block = vec![0; 1024 * 1024 * 8];

        loop {
            let bytes_read = file.read(&mut block).await?;
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
pub async fn sync_file<'c>(
    client: &Client,
    relative: &str,
    src: &Url,
    dest: &Path,
    check: Check<'c>,
) -> Result<()> {
    let remote_path = src.join(&relative)?;
    let local_path = dest.join(&relative);
    let temp_path = local_path.with_extension("sync.tmp");

    if local_path.exists() {
        let local_size = metadata(&local_path).await?.len();
        if let Check::Hash(size, checksum) = check {
            info!("Verifying size and checksum of {:?}", local_path);
            if local_size != size {
                debug!("Local file incorrect size {:?}", local_path);
            } else if checksum.check(&local_path).await? {
                debug!(
                    "Skipping (already exists with valid checksum) {:?}",
                    remote_path
                );
                return Ok(());
            } else {
                debug!("Local file failed checksum {:?}", local_path);
            }
        } else if let Check::Size(size) = check {
            info!("Verifying size of {:?}", local_path);
            if local_size != size {
                debug!("Local file incorrect size {:?}", local_path);
            } else {
                debug!(
                    "Skipping (already exists with valid size) {:?}",
                    remote_path
                );
                return Ok(());
            }
        } else {
            debug!("Skipping (already exists) {:?}", remote_path);
            return Ok(());
        }
    }

    info!("Downloading \"{}\" to {:?}", remote_path, local_path);

    create_dir_all(local_path.parent().expect("Invalid repository structure")).await?;
    let download_size = download(client, &remote_path, &temp_path).await?;
    match check {
        Check::RemoteSize(size) | Check::Size(size) => {
            info!("Verifying size of {:?}", remote_path);
            if download_size != size {
                bail!("Remote file failed size {:?}", temp_path);
            }
        }
        Check::Hash(size, checksum) => {
            info!("Verifying size and checksum of {:?}", remote_path);
            if download_size != size {
                bail!("Remote file failed size {:?}", temp_path);
            } else if !checksum.check(&temp_path).await? {
                bail!("Remote file failed checksum {:?}", temp_path);
            }
        }
        Check::Metadata => {
            // Don't know size of metadata ahead of time
        }
    }
    rename(&temp_path, &local_path).await?;
    Ok(())
}

/// The kind of check to be made on a package
#[derive(Debug, Clone, Copy)]
pub enum CheckType {
    /// Only check the size of the downloadeded package
    CheckRemoteSize,
    /// Check the size of the package
    CheckSize,
    /// Check the hash of the file
    CheckHash,
}
pub use CheckType::*;

impl CheckType {
    /// Check if the type is only for remote files
    pub fn remote_only(self) -> bool {
        match self {
            CheckType::CheckRemoteSize => true,
            _ => false,
        }
    }
}

/// Check data to use when checking a package
#[derive(Debug, Clone, Copy)]
pub enum Check<'c> {
    /// Don't have check for metadata
    Metadata,
    /// Only check remote size
    RemoteSize(u64),
    /// Check the size of the file
    Size(u64),
    /// Check the size and hash of the file
    Hash(u64, &'c Checksum),
}

/// Download a network file to a local file
async fn download(client: &Client, src: &Url, dest: &Path) -> Result<u64> {
    let src = src.to_owned();
    let request = client.get(src);
    let dest = dest.to_owned();
    let (tx, mut rx) = unbounded_channel();

    let network: tokio::task::JoinHandle<Result<()>> = tokio::spawn(async move {
        let mut src = request.send().await?;

        while let Some(chunk) = src.chunk().await? {
            tx.send(chunk)?;
        }

        Ok(())
    });

    let disk: tokio::task::JoinHandle<Result<u64>> = tokio::spawn(async move {
        let mut local = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(dest)
            .await?;
        let mut size = 0;

        while let Some(chunk) = rx.recv().await {
            size += chunk.len() as u64;
            local.write_all(&chunk[..]).await?;
        }

        Ok(size)
    });

    let size = disk.await??;
    network.await??;

    Ok(size)
}

#[cfg(test)]
mod test {
    use super::{decode, Metadata};

    const LOCAL_XML: &[u8] = include_bytes!(
        "test-data/local/repodata/84fe7bb9cf340186df02863647f41a4be32c86a21b80eaaeddaa97e99a24b7a6-primary.xml.gz"
    );
    const REMOTE_XML: &[u8] = include_bytes!(
        "test-data/remote/repodata/328a9f961ff596aedac41d051634325110b8fb30b87c00f678c257644337d1d6-primary.xml.gz"
    );

    #[tokio::test]
    async fn read_packages() {
        let local: Metadata = decode(&mut LOCAL_XML).await.unwrap();

        assert_eq!(local.packages.len(), 11331);
    }
}
