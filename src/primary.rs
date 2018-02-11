use serde_xml_rs as xml;
use tree_magic as magic;
use flate2::read::GzDecoder;
use std::error::Error;
use std::fmt::{self, Debug, Display};

#[derive(Debug, Deserialize)]
pub struct Metadata {
    #[serde(rename = "package", default)]
    packages: Vec<Package>,
}

#[derive(Debug, Deserialize)]
pub struct Package {
    name: String,
    location: Location,
    checksum: Checksum,
}

#[derive(PartialEq, Eq, Deserialize)]
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

#[derive(Debug, PartialEq, Eq, Deserialize)]
struct Location {
    href: String,
}

#[derive(Debug, PartialEq, Eq, Deserialize)]
struct Checksum {
    #[serde(rename = "type")]
    algorithm: String,
    #[serde(rename = "$value")]
    sum: String,
}

impl Metadata {
    pub fn from(source: &[u8]) -> Result<Metadata, String> {
        eprintln!("Checking type");
        if magic::match_u8("application/gzip", source) {
            eprintln!("Found gzip");
            xml::deserialize(GzDecoder::new(source))
                .map_err(|e| e.description().to_string())
        } else if magic::match_u8("application/xml", source) {
            eprintln!("Found xml");
            xml::deserialize(source)
                .map_err(|e| e.description().to_string())
        } else {
            Err("Incompatible file type".to_string())
        }
    }
}

#[test]
fn read_packages() {
    let xml_gz: &[u8] = include_bytes!("test-data/local/repodata/84fe7bb9cf340186df02863647f41a4be32c86a21b80eaaeddaa97e99a24b7a6-primary.xml.gz");
    let local = Metadata::from(xml_gz).unwrap();

    assert_eq!(local.packages.len(), 11331);
}

