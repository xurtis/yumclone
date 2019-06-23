//! Creates URLs based on a combination of patterns from a set of inputs.

use regex::{Captures, Regex, Replacer};
use std::collections::HashMap;
use std::convert::{From, Into};

/// A Generator of URL pairs for a given set of tags.
pub struct UrlMux<'a, 'b, 's> {
    src: &'a str,
    dst: &'b str,
    fields: TagFieldIter<'s>,
    tag_search: Regex,
}

impl<'a, 'b, 's> UrlMux<'a, 'b, 's> {
    /// Create a new URL Mux.
    ///
    /// ```rust
    /// let tags = HashMap
    /// let mux = UrlMux::new(
    /// ```
    pub fn new<T>(src: &'a str, dst: &'b str, fields: T) -> UrlMux<'a, 'b, 's>
    where
        T: Into<TagFieldIter<'s>>,
    {
        UrlMux {
            src: src,
            dst: dst,
            fields: fields.into(),
            tag_search: tag_finder(),
        }
    }
}

impl<'a, 'b, 's> Iterator for UrlMux<'a, 'b, 's> {
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
pub struct TagFieldIter<'s> {
    field: Vec<(&'s str, Vec<&'s str>)>,
    index: Option<Vec<usize>>,
}

impl<'s> From<&'s TagField> for TagFieldIter<'s> {
    /// Create an iterator over the tag field.
    fn from(field: &'s TagField) -> TagFieldIter<'s> {
        let index = vec![0; field.len()];
        let field = field
            .iter()
            .map(|(k, v)| (k.as_ref(), v.iter().map(String::as_ref).collect()))
            .collect();
        TagFieldIter {
            field: field,
            index: Some(index),
        }
    }
}

impl<'s> From<HashMap<&'s str, Vec<&'s str>>> for TagFieldIter<'s> {
    /// Create an iterator over the tag field.
    fn from(mut field: HashMap<&'s str, Vec<&'s str>>) -> TagFieldIter<'_> {
        let index = vec![0; field.len()];
        let field = field
            .drain()
            .collect();
        TagFieldIter {
            field: field,
            index: Some(index),
        }
    }
}

impl<'s> Iterator for TagFieldIter<'s> {
    type Item = TagSet<'s>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.next_tagset();
        self.index = self.next_index();
        next
    }
}

impl<'s> TagFieldIter<'s> {
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
    fn next_tagset(&self) -> Option<TagSet<'s>> {
        // self.index.iter().enumerate().map(|(k, i)| self.fieldself.keys[k]
        if let Some(ref index) = self.index {
            let tagset: HashMap<&str, &str> = index
                .iter()
                .cloned()
                .enumerate()
                .map(|(t, v)| (self.field[t].0.as_ref(), self.field[t].1[v].as_ref()))
                .collect();
            Some(tagset.into())
        } else {
            None
        }
    }
}

/// A Set of tags that can be used to replace tags in a URL string.
pub struct TagSet<'s> {
    map: HashMap<&'s str, &'s str>,
}

impl<'s> From<HashMap<&'s str, &'s str>> for TagSet<'s> {
    fn from(map: HashMap<&'s str, &'s str>) -> TagSet<'s> {
        TagSet { map }
    }
}

impl<'t, 's> Replacer for &'t TagSet<'s> {
    fn replace_append(&mut self, caps: &Captures<'_>, dst: &mut String) {
        if let Some(ref val) = self.map.get(&caps["tag"]) {
            dst.push_str(val);
        } else {
            panic!("Invalid tag name specified in url");
        }
    }
}

impl<'s> Replacer for TagSet<'s> {
    fn replace_append(&mut self, caps: &Captures<'_>, dst: &mut String) {
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

    #[test]
    fn create_tag_field() {
        let fields: TagFieldIter<'_> = tags().into();
        let sets: Vec<_> = fields.collect();
        assert_eq!(sets.len(), 6);
    }

    #[test]
    fn url_tag_replace() {
        use std::collections::BTreeSet;

        let finder = tag_finder();
        let fields: TagFieldIter<'_> = tags().into();
        let variants: BTreeSet<String> = fields
            .map(|f| finder.replace_all("$os/$arch/$other", f).into_owned())
            .collect();

        assert!(variants.contains("fedora/SRPMS/$other"));
        assert!(variants.contains("fedora/x86_64/$other"));
        assert!(variants.contains("fedora/i686/$other"));
        assert!(variants.contains("epel/SRPMS/$other"));
        assert!(variants.contains("epel/x86_64/$other"));
        assert!(variants.contains("epel/i686/$other"));
    }

    #[test]
    fn url_mux() {
        use std::collections::BTreeSet;

        let tagset = tags();
        let mux = UrlMux::new(
            "src/$os/$arch",
            "dst/$os/$arch",
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
