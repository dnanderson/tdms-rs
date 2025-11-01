// src/segment/info.rs
use crate::types::TocFlags;

/// Segment information for reading
#[derive(Debug, Clone)]
pub struct SegmentInfo {
    pub offset: u64,
    pub toc: TocFlags,
    pub is_big_endian: bool,
    pub metadata_size: u64,
    pub raw_data_offset: u64,
}