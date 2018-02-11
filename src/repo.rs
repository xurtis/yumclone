use serde_xml_rs as xml;
use std::io::Read;

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
    pub fn decode<R: Read>(source: &mut R) -> Result<Repo, xml::Error> {
        xml::deserialize(source)
    }

    /// Returns a list of paths for metadata files to sync.
    pub fn meta_files(&self) -> Vec<String> {
        self.data.iter().map(|d| d.location.href.clone()).collect()
    }

    /// Returns the relative path of the primary data file.
    pub fn primary_path(&self) -> Option<String> {
        for datum in &self.data {
            if datum.datum == "primary" {
                return Some(datum.location.href.clone())
            }
        }

        None
    }
}

#[cfg(test)]
mod test {
    use super::Repo;

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
