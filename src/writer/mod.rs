// src/writer/mod.rs
mod sync_writer;

#[cfg(feature = "async")]
mod async_writer;

pub use sync_writer::TdmsWriter;

#[cfg(feature = "async")]
pub use async_writer::AsyncTdmsWriter;