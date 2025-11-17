// src/writer/mod.rs
mod sync_writer;
mod rotating_writer;

#[cfg(feature = "async")]
mod async_writer;
#[cfg(feature = "async")]
mod rotating_async_writer;

pub use sync_writer::TdmsWriter;
pub use rotating_writer::RotatingTdmsWriter;

#[cfg(feature = "async")]
pub use async_writer::AsyncTdmsWriter;
#[cfg(feature = "async")]
pub use rotating_async_writer::AsyncRotatingTdmsWriter;