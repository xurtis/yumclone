//! Yum Repository Cloning
//!
//! This clones repositories by specifying, for each repository to clone:
//!
//! * A set of tags to replace and the variants to replace them with,
//! * The source URL,
//! * The destination path, and
//! * (optionally) the URL for the repositories GPG key.
//!
//! In turn, this will diff the metadata for all variations of the remote
//! and local versions of the repository and update the local to match the
//! remote.

#![warn(missing_docs)]

extern crate flate2;
extern crate regex;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_xml_rs;
extern crate tree_magic;

mod package;
mod repo;
mod urlmux;

fn main() {}
