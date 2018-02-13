//! Creates URLs based on a combination of patterns from a set of inputs.

use regex::{Captures, Regex, Replacer};
use std::collections::HashMap;
use std::convert::{From, Into};

/// A Generator of URL pairs for a given set of tags.
pub struct UrlMux {
    src: String,
    dst: String,
    fields: TagFieldIter,
    tag_search: Regex,
}

impl UrlMux {
    /// Create a new URL Mux.
    fn new<T: Into<TagFieldIter>>(src: String, dst: String, fields: T) -> UrlMux {
        UrlMux {
            src: src,
            dst: dst,
            fields: fields.into(),
            tag_search: tag_finder(),
        }
    }
}

impl Iterator for UrlMux {
    type Item = (String, String);

    fn next(&mut self) -> Option<(String, String)> {
        if let Some(ref replacer) = self.fields.next() {
            Some((
                self.tag_search.replace_all(&self.src, replacer).into_owned(),
                self.tag_search.replace_all(&self.dst, replacer).into_owned()
            ))
        } else {
            None
        }
    }
}


/// A list of tags with each possible variant of a given tag.
type TagField = HashMap<String, Vec<String>>;

/// An iterator over all of the combinations in a tag field.
#[derive(Debug)]
struct TagFieldIter {
    field: Vec<(String, Vec<String>)>,
    index: Option<Vec<usize>>,
}

impl From<TagField> for TagFieldIter {
    /// Create an iterator over the tag field.
    fn from(mut field: TagField) -> TagFieldIter {
        let index = vec![0; field.len()];
        let field = field.drain().collect();
        TagFieldIter {
            field: field,
            index: Some(index),
        }
    }
}

impl<'s> From<HashMap<&'s str, Vec<&'s str>>> for TagFieldIter {
    /// Create an iterator over the tag field.
    fn from(mut field: HashMap<&'s str, Vec<&'s str>>) -> TagFieldIter {
        let index = vec![0; field.len()];
        let field = field
            .drain()
            .map(|(k, v)| (
                k.to_string(),
                v.into_iter().map(str::to_string).collect()
            ))
            .collect();
        TagFieldIter {
            field: field,
            index: Some(index),
        }
    }
}

impl Iterator for TagFieldIter {
    type Item = TagSet;

    fn next(&mut self) -> Option<TagSet> {
        let next = self.next_tagset();
        self.index = self.next_index();
        next
    }
}

impl TagFieldIter {
    /// Increment the index.
    fn next_index(&self) -> Option<Vec<usize>> {
        if let Some(mut next) = self.index.clone() {
            for i in 0..next.len() {
                next[i] += 1;
                if next[i] == self.field[i].1.len() {
                    next[i] = 0;
                } else {
                    return Some(next);
                }
            }
        }
        None
    }

    /// Get a value based on an index.
    fn next_tagset(&self) -> Option<TagSet> {
        // self.index.iter().enumerate().map(|(k, i)| self.fieldself.keys[k]
        if let Some(ref index) = self.index {
            let tagset: HashMap<String, String> = index
                .iter()
                .cloned()
                .enumerate()
                .map(|(t, v)| (self.field[t].0.clone(), self.field[t].1[v].clone()))
                .collect();
            Some(tagset.into())
        } else {
            None
        }
    }
}

/// A Set of tags that can be used to replace tags in a URL string.
struct TagSet {
    map: HashMap<String, String>,
}

impl From<HashMap<String, String>> for TagSet {
    fn from(map: HashMap<String, String>) -> TagSet {
        TagSet { map }
    }
}

impl<'t> Replacer for &'t TagSet {
    fn replace_append(&mut self, caps: &Captures, dst: &mut String) {
        if let Some(ref val) = self.map.get(&caps["tag"]) {
            dst.push_str(val);
        } else {
            panic!("Invalid tag name specified in url");
        }
    }
}

impl Replacer for TagSet {
    fn replace_append(&mut self, caps: &Captures, dst: &mut String) {
        if let Some(ref val) = self.map.get(&caps["tag"]) {
            dst.push_str(val);
        } else {
            // Push the string if not found.
            dst.push_str(&caps[0]);
        }
    }
}

/// Create a regex that finds the tags in a given URL.
fn tag_finder() -> Regex {
    Regex::new(r"\$(?P<tag>[-a-zA-Z0-9_]+)").unwrap()
}


#[cfg(test)]
mod test {
    use super::*;

    fn tags() -> HashMap<&'static str, Vec<&'static str>> {
        vec![
            ("os", vec!["fedora", "epel"]),
            ("arch", vec!["SRPMS", "x86_64", "i686"]),
        ].into_iter().collect()
    }

    fn tag_iter() -> TagFieldIter {
        tags().into()
    }

    #[test]
    fn create_tag_field() {
        let fields = tag_iter();

        let sets: Vec<_> = fields.collect();
        assert_eq!(sets.len(), 6);
    }

    #[test]
    fn url_tag_replace() {
        use std::collections::BTreeSet;

        let finder = tag_finder();
        let fields = tag_iter();
        let variants: BTreeSet<String> = fields
            .map(|f| finder.replace_all("$os/$arch", f).into_owned())
            .collect();

        assert!(variants.contains("fedora/SRPMS"));
        assert!(variants.contains("fedora/x86_64"));
        assert!(variants.contains("fedora/i686"));
        assert!(variants.contains("epel/SRPMS"));
        assert!(variants.contains("epel/x86_64"));
        assert!(variants.contains("epel/i686"));
    }

    #[test]
    fn url_mux() {
        use std::collections::BTreeSet;

        let tagset = tags();
        let mux = UrlMux::new(
            "src/$os/$arch".to_string(),
            "dst/$os/$arch".to_string(),
            tagset,
        );
        let variants: BTreeSet<(String, String)> = mux.collect();

        assert!(variants.contains(&(
            "src/fedora/SRPMS".to_owned(),
            "dst/fedora/SRPMS".to_owned()
        )));
        assert!(variants.contains(&(
            "src/fedora/x86_64".to_owned(),
            "dst/fedora/x86_64".to_owned()
        )));
        assert!(variants.contains(&(
            "src/fedora/i686".to_owned(),
            "dst/fedora/i686".to_owned()
        )));
        assert!(variants.contains(&(
            "src/epel/SRPMS".to_owned(),
            "dst/epel/SRPMS".to_owned()
        )));
        assert!(variants.contains(&(
            "src/epel/x86_64".to_owned(),
            "dst/epel/x86_64".to_owned()
        )));
        assert!(variants.contains(&(
            "src/epel/i686".to_owned(),
            "dst/epel/i686".to_owned()
        )));
    }
}
