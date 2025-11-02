// src/segment/info.rs
use crate::types::TocFlags;

/// Segment information for reading
#[derive(Debug, Clone)]
pub struct SegmentInfo {
    pub offset: u64,
    pub toc: TocFlags,
    pub is_big_endian: bool,
    pub metadata_size: u64,
    /// This is the true size of the raw data block (Next Segment Offset - metadata_size)
    pub total_raw_data_size: u64,
}