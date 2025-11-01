// src/metadata/object_path.rs
use crate::error::{TdmsError, Result};

/// Represents an object path in the TDMS hierarchy
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ObjectPath {
    Root,
    Group(String),
    Channel { group: String, channel: String },
}

impl ObjectPath {
    pub fn to_string(&self) -> String {
        match self {
            ObjectPath::Root => "/".to_string(),
            ObjectPath::Group(name) => format!("/'{}''", name.replace('\'', "''")),
            ObjectPath::Channel { group, channel } => {
                let escaped_group = group.replace('\'', "''");
                let escaped_channel = channel.replace('\'', "''");
                format!("/'{}'/'{}'", escaped_group, escaped_channel)
            }
        }
    }
    
    pub fn from_string(s: &str) -> Result<Self> {
        if s == "/" {
            return Ok(ObjectPath::Root);
        }
        
        let parts: Vec<&str> = s.split("'/'").collect();
        
        if parts.len() == 1 {
            let name = parts[0]
                .trim_start_matches('/')
                .trim_start_matches('\'')
                .trim_end_matches('\'')
                .replace("''", "'");
            Ok(ObjectPath::Group(name))
        } else if parts.len() == 2 {
            let group = parts[0]
                .trim_start_matches('/')
                .trim_start_matches('\'')
                .replace("''", "'");
            let channel = parts[1]
                .trim_end_matches('\'')
                .replace("''", "'");
            Ok(ObjectPath::Channel { group, channel })
        } else {
            Err(TdmsError::InvalidPath(s.to_string()))
        }
    }
}