// src/segment/header.rs
use crate::types::TocFlags;

/// TDMS segment header information
#[derive(Debug, Clone)]
pub struct SegmentHeader {
    pub offset: u64,
    pub toc: TocFlags,
    pub version: u32,
    pub next_segment_offset: u64,
    pub raw_data_offset: u64,
}

impl SegmentHeader {
    pub const LEAD_IN_SIZE: usize = 28;
    pub const TDMS_TAG: &'static [u8; 4] = b"TDSm";
    pub const INDEX_TAG: &'static [u8; 4] = b"TDSh";
    pub const VERSION: u32 = 4713;
    pub const INCOMPLETE_MARKER: u64 = 0xFFFFFFFFFFFFFFFF;
}