// src/error.rs
use std::io;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum TdmsError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    
    #[error("Invalid TDMS tag: expected {expected}, found {found}")]
    InvalidTag { expected: String, found: String },
    
    #[error("Invalid data type: {0}")]
    InvalidDataType(u32),
    
    #[error("Invalid object path: {0}")]
    InvalidPath(String),
    
    #[error("Channel not found: {0}")]
    ChannelNotFound(String),
    
    #[error("Type mismatch: expected {expected}, found {found}")]
    TypeMismatch { expected: String, found: String },
    
    #[error("Incomplete segment at offset {0}")]
    IncompleteSegment(u64),
    
    #[error("Invalid UTF-8 in string data")]
    InvalidUtf8,
    
    #[error("Unsupported feature: {0}")]
    Unsupported(String),
    
    #[error("Writer closed")]
    WriterClosed,
    
    #[error("Buffer overflow: tried to write {attempted} bytes to buffer of size {capacity}")]
    BufferOverflow { attempted: usize, capacity: usize },
}

pub type Result<T> = std::result::Result<T, TdmsError>;