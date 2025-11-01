// src/utils/string_encoding.rs
use crate::error::{TdmsError, Result};

/// Utilities for UTF-8 string encoding/decoding with error recovery
pub fn encode_tdms_string(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}

pub fn decode_tdms_string(bytes: &[u8]) -> Result<String> {
    String::from_utf8(bytes.to_vec())
        .map_err(|_| TdmsError::InvalidUtf8)
}

pub fn decode_tdms_string_lossy(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).to_string()
}