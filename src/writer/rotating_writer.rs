// src/writer/rotating_writer.rs
use std::path::{Path, PathBuf};
use crate::error::Result;
use crate::writer::sync_writer::TdmsWriter;
use crate::types::{DataType, PropertyValue};

/// A TDMS writer that rotates to a new file when the current file
/// exceeds a specified size.
pub struct RotatingTdmsWriter {
    base_path: PathBuf,
    max_size_bytes: u64,
    current_file_index: u32,
    writer: TdmsWriter,
}

impl RotatingTdmsWriter {
    /// Creates a new rotating TDMS writer.
    ///
    /// The `base_path` is the path to the file, excluding any numeric suffix.
    /// The `max_size_bytes` is the maximum size of a single file in bytes.
    pub fn new(base_path: impl AsRef<Path>, max_size_bytes: u64) -> Result<Self> {
        let base_path = base_path.as_ref().to_path_buf();
        let writer = TdmsWriter::create(Self::get_path(&base_path, 0))?;
        Ok(Self {
            base_path,
            max_size_bytes,
            current_file_index: 0,
            writer,
        })
    }

    fn get_path(base_path: &Path, index: u32) -> PathBuf {
        if index == 0 {
            base_path.with_extension("tdms")
        } else {
            base_path.with_extension(format!("{}.tdms", index))
        }
    }

    fn rotate_if_needed(&mut self) -> Result<()> {
        if self.writer.file_size()? > self.max_size_bytes {
            self.current_file_index += 1;
            let new_path = Self::get_path(&self.base_path, self.current_file_index);
            self.writer.reset_for_new_file(new_path)?;
        }
        Ok(())
    }

    pub fn set_file_property(&mut self, name: impl Into<String>, value: PropertyValue) {
        self.writer.set_file_property(name, value);
    }

    pub fn set_group_property(&mut self, group: impl Into<String>, name: impl Into<String>, value: PropertyValue) {
        self.writer.set_group_property(group, name, value);
    }

    pub fn create_channel(&mut self, group: impl Into<String>, channel: impl Into<String>, data_type: DataType) -> Result<()> {
        self.writer.create_channel(group, channel, data_type)
    }

    pub fn set_channel_property(&mut self, group: impl AsRef<str>, channel: impl AsRef<str>, name: impl Into<String>, value: PropertyValue) -> Result<()> {
        self.writer.set_channel_property(group, channel, name, value)
    }

    pub fn write_channel_data<T: Copy>(&mut self, group: impl AsRef<str>, channel: impl AsRef<str>, data: &[T]) -> Result<()> {
        self.rotate_if_needed()?;
        self.writer.write_channel_data(group, channel, data)
    }

    pub fn write_channel_strings(&mut self, group: impl AsRef<str>, channel: impl AsRef<str>, data: &[impl AsRef<str>]) -> Result<()> {
        self.rotate_if_needed()?;
        self.writer.write_channel_strings(group, channel, data)
    }

    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush()
    }
}
