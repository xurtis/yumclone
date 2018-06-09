//! Errors generated.

error_chain! {
    foreign_links {
        Url(::url::ParseError);
        Xml(::serde_xml_rs::Error);
        Io(::std::io::Error);
        Reqwest(::reqwest::Error);
        Format(::std::fmt::Error);
        WalkDir(::walkdir::Error);
        StripPrefix(::std::path::StripPrefixError);
    }

    errors {
        CurrentDirDecode {
            description("Couldn't decode the current directory")
        }
        NoPrimaryMeta {
            description("No primary repository metadata found")
        }
        IncompatiblePrimaryMeta {
            description("Primary metadata in incompatible filetype")
        }
    }
}
