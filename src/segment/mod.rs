// src/segment/mod.rs
mod header;
mod info;

pub use header::SegmentHeader;
pub use info::SegmentInfo;

#[derive(Debug)]
pub struct Segment {
    pub header: SegmentHeader,
    pub metadata_size: u64,
    pub raw_data_size: u64,
}