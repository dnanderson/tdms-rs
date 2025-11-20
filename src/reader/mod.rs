// src/reader/mod.rs
mod sync_reader;
mod channel_reader;
mod streaming;

pub use sync_reader::TdmsReader;
pub use channel_reader::ChannelReader;
pub use streaming::{StreamingReader, TdmsIter, TdmsStringIter};