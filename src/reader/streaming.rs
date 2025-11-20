use crate::error::Result;
use crate::reader::{ChannelReader, TdmsReader};
use crate::segment::SegmentInfo;
use std::io::{Read, Seek};
use std::marker::PhantomData;

/// Streaming reader state tracker
/// 
/// This struct tracks the position and chunk size for streaming operations.
/// It is used internally by the high-level iterators, or can be used manually
/// if you want to manage the `TdmsReader` borrow yourself.
pub struct StreamingReader {
    channel: ChannelReader,
    chunk_size: usize,
    current_position: u64,
}

impl StreamingReader {
    /// Create a new streaming reader
    /// 
    /// # Arguments
    /// 
    /// * `channel` - The channel to read from
    /// * `chunk_size` - Number of values per chunk
    pub fn new(channel: ChannelReader, chunk_size: usize) -> Self {
        StreamingReader {
            channel,
            chunk_size,
            current_position: 0,
        }
    }
    
    /// Read the next chunk of data
    /// 
    /// # Type Parameters
    /// 
    /// * `T` - The type to read (must match the channel's data type)
    /// * `R` - The reader type (must implement Read + Seek)
    /// 
    /// # Arguments
    /// 
    /// * `reader` - The stream to read from
    /// * `segments` - Slice of all segment information
    /// 
    /// # Returns
    /// 
    /// `Some(Vec<T>)` with the next chunk, or `None` if no more data
    pub fn next<T: Copy + Default, R: Read + Seek>(
        &mut self,
        reader: &mut R,
        segments: &[SegmentInfo],
    ) -> Result<Option<Vec<T>>> {
        if self.current_position >= self.channel.total_values() {
            return Ok(None);
        }
        
        let remaining = self.channel.total_values() - self.current_position;
        let read_count = remaining.min(self.chunk_size as u64) as usize;
        
        if read_count == 0 {
            return Ok(None);
        }
        
        let chunk = self.channel.read_chunk(
            reader,
            segments,
            self.current_position,
            read_count,
        )?;
        
        self.current_position += chunk.len() as u64;
        
        Ok(Some(chunk))
    }
    
    /// Read the next chunk of string data
    pub fn next_strings<R: Read + Seek>(
        &mut self,
        reader: &mut R,
        segments: &[SegmentInfo],
    ) -> Result<Option<Vec<String>>> {
        if self.current_position >= self.channel.total_values() {
            return Ok(None);
        }
        
        let remaining = self.channel.total_values() - self.current_position;
        let read_count = remaining.min(self.chunk_size as u64) as usize;
        
        if read_count == 0 {
            return Ok(None);
        }
        
        let chunk = self.channel.read_string_chunk(
            reader,
            segments,
            self.current_position,
            read_count,
        )?;
        
        self.current_position += chunk.len() as u64;
        
        Ok(Some(chunk))
    }
    
    /// Reset the reader to the beginning
    pub fn reset(&mut self) {
        self.current_position = 0;
    }
    
    /// Get the current position in the channel
    pub fn position(&self) -> u64 {
        self.current_position
    }
    
    /// Set the position in the channel
    /// 
    /// # Arguments
    /// 
    /// * `position` - The new position (value index)
    pub fn seek(&mut self, position: u64) {
        self.current_position = position.min(self.channel.total_values());
    }
    
    /// Get the total number of values in the channel
    pub fn total_values(&self) -> u64 {
        self.channel.total_values()
    }
    
    /// Check if there are more chunks to read
    pub fn has_more(&self) -> bool {
        self.current_position < self.channel.total_values()
    }
    
    /// Get the chunk size
    pub fn chunk_size(&self) -> usize {
        self.chunk_size
    }
    
    /// Set a new chunk size
    /// 
    /// # Arguments
    /// 
    /// * `chunk_size` - The new chunk size in values
    pub fn set_chunk_size(&mut self, chunk_size: usize) {
        self.chunk_size = chunk_size;
    }
    
    /// Calculate the number of remaining values
    pub fn remaining(&self) -> u64 {
        self.channel.total_values().saturating_sub(self.current_position)
    }
    
    /// Calculate progress as a percentage (0.0 to 100.0)
    pub fn progress_percent(&self) -> f64 {
        if self.channel.total_values() == 0 {
            return 100.0;
        }
        
        (self.current_position as f64 / self.channel.total_values() as f64) * 100.0
    }
}

/// High-level iterator for reading numeric data in chunks
///
/// This iterator holds a mutable borrow of the reader, allowing standard iteration.
pub struct TdmsIter<'a, T, R: Read + Seek> {
    reader: &'a mut TdmsReader<R>,
    tracker: StreamingReader,
    _phantom: PhantomData<T>,
}

impl<'a, T, R: Read + Seek> TdmsIter<'a, T, R> {
    pub fn new(reader: &'a mut TdmsReader<R>, channel: ChannelReader, chunk_size: usize) -> Self {
        Self {
            reader,
            tracker: StreamingReader::new(channel, chunk_size),
            _phantom: PhantomData,
        }
    }
    
    /// Get current progress percentage
    pub fn progress(&self) -> f64 {
        self.tracker.progress_percent()
    }
}

impl<'a, T: Copy + Default, R: Read + Seek> Iterator for TdmsIter<'a, T, R> {
    type Item = Result<Vec<T>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.tracker.next(&mut self.reader.file, &self.reader.segments) {
            Ok(Some(data)) => Some(Ok(data)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

/// High-level iterator for reading string data in chunks
///
/// This iterator holds a mutable borrow of the reader, allowing standard iteration.
pub struct TdmsStringIter<'a, R: Read + Seek> {
    reader: &'a mut TdmsReader<R>,
    tracker: StreamingReader,
}

impl<'a, R: Read + Seek> TdmsStringIter<'a, R> {
    pub fn new(reader: &'a mut TdmsReader<R>, channel: ChannelReader, chunk_size: usize) -> Self {
        Self {
            reader,
            tracker: StreamingReader::new(channel, chunk_size),
        }
    }
    
    /// Get current progress percentage
    pub fn progress(&self) -> f64 {
        self.tracker.progress_percent()
    }
}

impl<'a, R: Read + Seek> Iterator for TdmsStringIter<'a, R> {
    type Item = Result<Vec<String>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.tracker.next_strings(&mut self.reader.file, &self.reader.segments) {
            Ok(Some(data)) => Some(Ok(data)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::DataType;
    use crate::reader::channel_reader::ChannelInfo;
    
    fn create_test_channel() -> ChannelReader {
        let mut info = ChannelInfo::new(DataType::I32);
        info.total_values = 1000;
        ChannelReader::new("Test/Channel".to_string(), info)
    }
    
    #[test]
    fn test_streaming_reader_creation() {
        let channel = create_test_channel();
        let streaming = StreamingReader::new(channel, 100);
        
        assert_eq!(streaming.chunk_size(), 100);
        assert_eq!(streaming.position(), 0);
        assert_eq!(streaming.total_values(), 1000);
        assert!(streaming.has_more());
        assert_eq!(streaming.remaining(), 1000);
    }
    
    #[test]
    fn test_streaming_reader_seek() {
        let channel = create_test_channel();
        let mut streaming = StreamingReader::new(channel, 100);
        
        streaming.seek(500);
        assert_eq!(streaming.position(), 500);
        assert_eq!(streaming.remaining(), 500);
        
        streaming.seek(1500); // Beyond end
        assert_eq!(streaming.position(), 1000);
        assert!(!streaming.has_more());
    }
    
    #[test]
    fn test_streaming_reader_reset() {
        let channel = create_test_channel();
        let mut streaming = StreamingReader::new(channel, 100);
        
        streaming.seek(500);
        assert_eq!(streaming.position(), 500);
        
        streaming.reset();
        assert_eq!(streaming.position(), 0);
        assert!(streaming.has_more());
    }
    
    #[test]
    fn test_progress_calculation() {
        let channel = create_test_channel();
        let mut streaming = StreamingReader::new(channel, 100);
        
        assert_eq!(streaming.progress_percent(), 0.0);
        
        streaming.seek(250);
        assert_eq!(streaming.progress_percent(), 25.0);
        
        streaming.seek(500);
        assert_eq!(streaming.progress_percent(), 50.0);
        
        streaming.seek(1000);
        assert_eq!(streaming.progress_percent(), 100.0);
    }
    
    #[test]
    fn test_chunk_size_change() {
        let channel = create_test_channel();
        let mut streaming = StreamingReader::new(channel, 100);
        
        assert_eq!(streaming.chunk_size(), 100);
        
        streaming.set_chunk_size(250);
        assert_eq!(streaming.chunk_size(), 250);
    }
}