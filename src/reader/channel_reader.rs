// src/reader/channel_reader.rs
use crate::error::{TdmsError, Result};
use crate::types::DataType;
use crate::segment::SegmentInfo;
use crate::raw_data::RawDataReader;
use std::io::{Read, Seek, SeekFrom};

/// Data for a channel within a specific segment
#[derive(Debug, Clone)]
pub struct SegmentData {
    pub segment_index: usize,
    pub value_count: u64,
    pub byte_size: u64,
    pub byte_offset: u64, // Offset within the segment's raw data section
}

/// Information about a channel read from a TDMS file
#[derive(Debug, Clone)]
pub struct ChannelInfo {
    pub data_type: DataType,
    pub segments: Vec<SegmentData>,
    pub total_values: u64,
}

impl ChannelInfo {
    pub fn new(data_type: DataType) -> Self {
        ChannelInfo {
            data_type,
            segments: Vec::new(),
            total_values: 0,
        }
    }

    pub fn add_segment(&mut self, segment_data: SegmentData) {
        self.total_values += segment_data.value_count;
        self.segments.push(segment_data);
    }
}

/// Interface for reading data from a specific channel
/// 
/// Provides efficient methods for reading channel data either all at once
/// or in chunks for memory-efficient processing of large files.
pub struct ChannelReader {
    channel_key: String,
    info: ChannelInfo,
}

impl ChannelReader {
    /// Create a new channel reader
    /// 
    /// # Arguments
    /// 
    /// * `channel_key` - The key identifying this channel (format: "group/channel")
    /// * `info` - Channel information including data type and segment locations
    pub(crate) fn new(channel_key: String, info: ChannelInfo) -> Self {
        ChannelReader { channel_key, info }
    }

    /// Get the data type of this channel
    pub fn data_type(&self) -> DataType {
        self.info.data_type
    }

    /// Get the total number of values across all segments
    pub fn total_values(&self) -> u64 {
        self.info.total_values
    }

    /// Get the number of segments containing data for this channel
    pub fn segment_count(&self) -> usize {
        self.info.segments.len()
    }

    /// Get the channel key (group/channel format)
    pub fn key(&self) -> &str {
        &self.channel_key
    }

    /// Read all data from the channel
    /// 
    /// This loads all values into memory at once. For large channels, consider
    /// using `read_chunk` or `iter_chunks` instead.
    /// 
    /// # Type Parameters
    /// 
    /// * `T` - The Rust type corresponding to the channel's data type
    /// 
    /// # Arguments
    /// 
    /// * `reader` - A readable and seekable stream (typically the TDMS file)
    /// * `segments` - Slice of all segment information from the file
    /// 
    /// # Returns
    /// 
    /// A vector containing all values from all segments
    /// 
    /// # Example
    /// 
    /// ```no_run
    /// use tdms_rs::reader::TdmsReader;
    /// 
    /// let mut reader = TdmsReader::open("data.tdms").unwrap();
    /// let channel = reader.get_channel("Group1/Channel1").unwrap();
    /// 
    /// // For a channel with data type I32
    /// // This is a low-level function; typically you would use TdmsReader::read_channel_data
    /// let data: Vec<i32> = reader.read_channel_data("Group1", "Channel1").unwrap();
    /// ```
    pub fn read_all_data<T: Copy + Default, R: Read + Seek>(
        &self,
        reader: &mut R,
        segments: &[SegmentInfo],
    ) -> Result<Vec<T>> {
        if self.info.total_values > usize::MAX as u64 {
            return Err(TdmsError::Unsupported(
                "Channel has more values than can fit in memory".to_string(),
            ));
        }

        let total_values = self.info.total_values as usize;
        let mut result = Vec::with_capacity(total_values);

        for segment_data in &self.info.segments {
            let segment_info = &segments[segment_data.segment_index];
            
            // Calculate absolute position in file
            let data_offset = segment_info.offset 
                + 28 // Lead-in size
                + segment_info.raw_data_offset 
                + segment_data.byte_offset;
            
            reader.seek(SeekFrom::Start(data_offset))?;

            // Read values from this segment
            let values = RawDataReader::read_values::<T, _>(
                reader,
                segment_data.value_count as usize,
                segment_info.is_big_endian,
            )?;

            result.extend_from_slice(&values);
        }

        Ok(result)
    }

    /// Read a chunk of data from the channel
    /// 
    /// Reads a specific range of values, which may span multiple segments.
    /// 
    /// # Arguments
    /// 
    /// * `reader` - A readable and seekable stream
    /// * `segments` - Slice of all segment information
    /// * `start_index` - The first value to read (0-based)
    /// * `count` - The number of values to read
    /// 
    /// # Returns
    /// 
    /// A vector containing the requested values
    /// 
    /// # Example
    /// 
    /// ```no_run
    /// use tdms_rs::reader::TdmsReader;
    /// 
    /// let mut reader = TdmsReader::open("data.tdms").unwrap();
    /// let channel = reader.get_channel("Group1/Channel1").unwrap();
    /// 
    /// // Reading chunks is a low-level operation.
    /// // This demonstrates reading a chunk, but requires internal reader access.
    /// // In a real application, you might build a higher-level abstraction.
    /// // let values: Vec<f64> = channel.read_chunk(&mut reader.file, &reader.segments, 0, 100).unwrap();
    /// ```
    pub fn read_chunk<T: Copy + Default, R: Read + Seek>(
        &self,
        reader: &mut R,
        segments: &[SegmentInfo],
        start_index: u64,
        count: usize,
    ) -> Result<Vec<T>> {
        if start_index >= self.info.total_values {
            return Ok(Vec::new());
        }

        let end_index = (start_index + count as u64).min(self.info.total_values);
        let actual_count = (end_index - start_index) as usize;
        let mut result = Vec::with_capacity(actual_count);

        let mut current_index = 0u64;
        let mut remaining_to_read = actual_count;

        for segment_data in &self.info.segments {
            let segment_start = current_index;
            let segment_end = current_index + segment_data.value_count;

            // Check if this segment contains data we need
            if segment_end <= start_index {
                current_index = segment_end;
                continue;
            }

            if segment_start >= end_index {
                break;
            }

            // Calculate what to read from this segment
            let read_start_in_segment = if start_index > segment_start {
                start_index - segment_start
            } else {
                0
            };

            let values_available_in_segment = segment_data.value_count - read_start_in_segment;
            let values_to_read = (remaining_to_read as u64).min(values_available_in_segment) as usize;

            // Seek to position in segment
            let segment_info = &segments[segment_data.segment_index];
            let type_size = std::mem::size_of::<T>() as u64;
            let data_offset = segment_info.offset
                + 28
                + segment_info.raw_data_offset
                + segment_data.byte_offset
                + (read_start_in_segment * type_size);

            reader.seek(SeekFrom::Start(data_offset))?;

            // Read values
            let values = RawDataReader::read_values::<T, _>(
                reader,
                values_to_read,
                segment_info.is_big_endian,
            )?;

            result.extend_from_slice(&values);
            remaining_to_read -= values_to_read;
            current_index = segment_end;

            if remaining_to_read == 0 {
                break;
            }
        }

        Ok(result)
    }

    /// Read all string data from the channel
    /// 
    /// # Arguments
    /// 
    /// * `reader` - A readable and seekable stream
    /// * `segments` - Slice of all segment information
    /// 
    /// # Returns
    /// 
    /// A vector of strings
    pub fn read_all_strings<R: Read + Seek>(
        &self,
        reader: &mut R,
        segments: &[SegmentInfo],
    ) -> Result<Vec<String>> {
        if self.info.data_type != DataType::String {
            return Err(TdmsError::TypeMismatch {
                expected: "String".to_string(),
                found: format!("{:?}", self.info.data_type),
            });
        }

        if self.info.total_values > usize::MAX as u64 {
            return Err(TdmsError::Unsupported(
                "Channel has more values than can fit in memory".to_string(),
            ));
        }

        let total_values = self.info.total_values as usize;
        let mut result = Vec::with_capacity(total_values);

        for segment_data in &self.info.segments {
            let segment_info = &segments[segment_data.segment_index];
            
            let data_offset = segment_info.offset
                + 28
                + segment_info.raw_data_offset
                + segment_data.byte_offset;
            
            reader.seek(SeekFrom::Start(data_offset))?;

            let strings = RawDataReader::read_strings(
                reader,
                segment_data.value_count as usize,
                segment_info.is_big_endian,
            )?;

            result.extend(strings);
        }

        Ok(result)
    }

    /// Read a chunk of string data
    /// 
    /// # Arguments
    /// 
    /// * `reader` - A readable and seekable stream
    /// * `segments` - Slice of all segment information
    /// * `start_index` - The first string to read (0-based)
    /// * `count` - The number of strings to read
    /// 
    /// # Returns
    /// 
    /// A vector of strings
    pub fn read_string_chunk<R: Read + Seek>(
        &self,
        reader: &mut R,
        segments: &[SegmentInfo],
        start_index: u64,
        count: usize,
    ) -> Result<Vec<String>> {
        if self.info.data_type != DataType::String {
            return Err(TdmsError::TypeMismatch {
                expected: "String".to_string(),
                found: format!("{:?}", self.info.data_type),
            });
        }

        if start_index >= self.info.total_values {
            return Ok(Vec::new());
        }

        let end_index = (start_index + count as u64).min(self.info.total_values);
        let actual_count = (end_index - start_index) as usize;
        let mut result = Vec::with_capacity(actual_count);

        let mut current_index = 0u64;
        let mut remaining_to_read = actual_count;

        for segment_data in &self.info.segments {
            let segment_start = current_index;
            let segment_end = current_index + segment_data.value_count;

            if segment_end <= start_index {
                current_index = segment_end;
                continue;
            }

            if segment_start >= end_index {
                break;
            }

            // For strings, we need to read the entire segment due to cumulative offsets
            let segment_info = &segments[segment_data.segment_index];
            let data_offset = segment_info.offset
                + 28
                + segment_info.raw_data_offset
                + segment_data.byte_offset;

            reader.seek(SeekFrom::Start(data_offset))?;

            let all_strings = RawDataReader::read_strings(
                reader,
                segment_data.value_count as usize,
                segment_info.is_big_endian,
            )?;

            // Extract only the strings we need from this segment
            let read_start_in_segment = if start_index > segment_start {
                (start_index - segment_start) as usize
            } else {
                0
            };

            let values_to_read = remaining_to_read.min(all_strings.len() - read_start_in_segment);
            let end_in_segment = read_start_in_segment + values_to_read;

            result.extend_from_slice(&all_strings[read_start_in_segment..end_in_segment]);
            remaining_to_read -= values_to_read;
            current_index = segment_end;

            if remaining_to_read == 0 {
                break;
            }
        }

        Ok(result)
    }

    /// Create an iterator that yields chunks of data
    /// 
    /// This is useful for processing large channels without loading everything into memory.
    /// 
    /// # Arguments
    /// 
    /// * `chunk_size` - The number of values per chunk
    /// 
    /// # Returns
    /// 
    /// A `ChunkIterator` that yields chunks of data
    /// 
    /// # Example
    /// 
    /// ```no_run
    /// use tdms_rs::reader::TdmsReader;
    /// 
    /// let mut reader = TdmsReader::open("data.tdms").unwrap();
    /// let channel = reader.get_channel("Group1/Channel1").unwrap();
    /// 
    /// let mut iter = channel.iter_chunks::<f64>(10000);
    /// // This is a low-level API. A full example would require passing the file handle.
    /// // In a real application, you might build a higher-level abstraction for streaming.
    /// // Example usage:
    /// // while let Ok(Some(chunk)) = iter.next(&mut reader.file, &reader.segments) {
    /// //     println!("Read chunk of size {}", chunk.len());
    /// // }
    /// ```
    pub fn iter_chunks<T: Copy + Default>(&self, chunk_size: usize) -> ChunkIterator<T> {
        ChunkIterator::new(self.clone(), chunk_size)
    }

    /// Get information about a specific segment
    /// 
    /// # Arguments
    /// 
    /// * `segment_index` - The index of the segment (within this channel's segments)
    /// 
    /// # Returns
    /// 
    /// Reference to the segment data if it exists
    pub fn get_segment_data(&self, segment_index: usize) -> Option<&SegmentData> {
        self.info.segments.get(segment_index)
    }

    /// Check if the channel is empty (has no data)
    pub fn is_empty(&self) -> bool {
        self.info.total_values == 0
    }
}

// Implement Clone for ChannelReader
impl Clone for ChannelReader {
    fn clone(&self) -> Self {
        ChannelReader {
            channel_key: self.channel_key.clone(),
            info: self.info.clone(),
        }
    }
}

/// Iterator for reading channel data in chunks
/// 
/// This allows memory-efficient processing of large channels by reading
/// and processing one chunk at a time.
pub struct ChunkIterator<T: Copy + Default> {
    channel: ChannelReader,
    chunk_size: usize,
    current_position: u64,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: Copy + Default> ChunkIterator<T> {
    fn new(channel: ChannelReader, chunk_size: usize) -> Self {
        ChunkIterator {
            channel,
            chunk_size,
            current_position: 0,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Get the next chunk of data
    /// 
    /// # Arguments
    /// 
    /// * `reader` - A readable and seekable stream
    /// * `segments` - Slice of all segment information
    /// 
    /// # Returns
    /// 
    /// `Some(Vec<T>)` with the next chunk, or `None` if no more data
    pub fn next<R: Read + Seek>(
        &mut self,
        reader: &mut R,
        segments: &[SegmentInfo],
    ) -> Result<Option<Vec<T>>> {
        if self.current_position >= self.channel.total_values() {
            return Ok(None);
        }

        let chunk = self.channel.read_chunk(
            reader,
            segments,
            self.current_position,
            self.chunk_size,
        )?;

        self.current_position += chunk.len() as u64;

        Ok(Some(chunk))
    }

    /// Reset the iterator to the beginning
    pub fn reset(&mut self) {
        self.current_position = 0;
    }

    /// Get the current position in the channel
    pub fn position(&self) -> u64 {
        self.current_position
    }

    /// Get the total number of values in the channel
    pub fn total_values(&self) -> u64 {
        self.channel.total_values()
    }

    /// Check if there are more chunks to read
    pub fn has_more(&self) -> bool {
        self.current_position < self.channel.total_values()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_channel_info() -> ChannelInfo {
        let mut info = ChannelInfo::new(DataType::I32);
        
        // Add three segments with different value counts
        info.add_segment(SegmentData {
            segment_index: 0,
            value_count: 100,
            byte_size: 400,
            byte_offset: 0,
        });
        
        info.add_segment(SegmentData {
            segment_index: 1,
            value_count: 200,
            byte_size: 800,
            byte_offset: 0,
        });
        
        info.add_segment(SegmentData {
            segment_index: 2,
            value_count: 150,
            byte_size: 600,
            byte_offset: 0,
        });
        
        info
    }

    #[test]
    fn test_channel_info_creation() {
        let info = create_test_channel_info();
        
        assert_eq!(info.data_type, DataType::I32);
        assert_eq!(info.total_values, 450);
        assert_eq!(info.segments.len(), 3);
    }

    #[test]
    fn test_channel_reader_properties() {
        let info = create_test_channel_info();
        let reader = ChannelReader::new("Group1/Channel1".to_string(), info);
        
        assert_eq!(reader.key(), "Group1/Channel1");
        assert_eq!(reader.data_type(), DataType::I32);
        assert_eq!(reader.total_values(), 450);
        assert_eq!(reader.segment_count(), 3);
        assert!(!reader.is_empty());
    }

    #[test]
    fn test_chunk_iterator() {
        let info = create_test_channel_info();
        let reader = ChannelReader::new("Group1/Channel1".to_string(), info);
        
        let mut iter = reader.iter_chunks::<i32>(100);
        
        assert_eq!(iter.position(), 0);
        assert_eq!(iter.total_values(), 450);
        assert!(iter.has_more());
        
        // Simulate consuming some data
        iter.current_position = 300;
        assert_eq!(iter.position(), 300);
        assert!(iter.has_more());
        
        iter.current_position = 450;
        assert!(!iter.has_more());
        
        iter.reset();
        assert_eq!(iter.position(), 0);
        assert!(iter.has_more());
    }

    #[test]
    fn test_segment_data_access() {
        let info = create_test_channel_info();
        let reader = ChannelReader::new("Group1/Channel1".to_string(), info);
        
        let seg0 = reader.get_segment_data(0).unwrap();
        assert_eq!(seg0.value_count, 100);
        
        let seg1 = reader.get_segment_data(1).unwrap();
        assert_eq!(seg1.value_count, 200);
        
        assert!(reader.get_segment_data(10).is_none());
    }

    #[test]
    fn test_empty_channel() {
        let info = ChannelInfo::new(DataType::F64);
        let reader = ChannelReader::new("Empty/Channel".to_string(), info);
        
        assert!(reader.is_empty());
        assert_eq!(reader.total_values(), 0);
        assert_eq!(reader.segment_count(), 0);
    }
}