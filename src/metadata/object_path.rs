// src/metadata/object_path.rs
use crate::error::{TdmsError, Result};
use std::fmt;

/// Represents an object path in the TDMS hierarchy
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ObjectPath {
    Root,
    Group(String),
    Channel { group: String, channel: String },
}

impl fmt::Display for ObjectPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ObjectPath::Root => write!(f, "/"),
            ObjectPath::Group(name) => write!(f, "/'{}''", name.replace('\'', "''")),
            ObjectPath::Channel { group, channel } => {
                let escaped_group = group.replace('\'', "''");
                let escaped_channel = channel.replace('\'', "''");
                write!(f, "/'{}'/''{}''", escaped_group, escaped_channel)
            }
        }
    }
}

impl ObjectPath {
    pub fn from_string(s: &str) -> Result<Self> {
        if s == "/" {
            return Ok(ObjectPath::Root);
        }

        let s = s.strip_prefix('/').ok_or_else(|| TdmsError::InvalidPath(s.to_string()))?;
        let parts: Vec<&str> = s.split("''/'").collect();

        match parts.as_slice() {
            [group] => {
                let group = group.strip_prefix('\'').and_then(|s| s.strip_suffix('\''))
                    .ok_or_else(|| TdmsError::InvalidPath(s.to_string()))?
                    .replace("''", "'");
                Ok(ObjectPath::Group(group))
            },
            [group, channel] => {
                let group = group.strip_prefix('\'')
                    .ok_or_else(|| TdmsError::InvalidPath(s.to_string()))?
                    .replace("''", "'");
                let channel = channel.strip_prefix('\'').and_then(|s| s.strip_suffix('\''))
                    .ok_or_else(|| TdmsError::InvalidPath(s.to_string()))?
                    .replace("''", "'");
                Ok(ObjectPath::Channel { group: group.to_string(), channel })
            },
            _ => Err(TdmsError::InvalidPath(s.to_string())),
        }
    }
}