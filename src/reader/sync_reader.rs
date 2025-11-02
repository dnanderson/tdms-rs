// src/reader/sync_reader.rs
use crate::error::{TdmsError, Result};
use crate::types::{DataType, TocFlags};
use crate::segment::{SegmentHeader, SegmentInfo};
use crate::reader::channel_reader::{ChannelReader, SegmentData, ChannelInfo};
use crate::metadata::ObjectPath;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, BufReader};
use std::path::Path;
use std::collections::HashMap;
use byteorder::{ReadBytesExt, LittleEndian, BigEndian};

/// Synchronous TDMS file reader
/// 
/// Provides efficient reading of TDMS files with support for:
/// - Full file parsing and metadata extraction
/// - Channel discovery and listing
/// - Efficient data access through ChannelReader
/// 
/// # Example
/// 
/// ```no_run
/// use tdms_rs::reader::TdmsReader;
/// 
/// let mut reader = TdmsReader::open("data.tdms").unwrap();
/// 
/// // List all channels
/// for channel_key in reader.list_channels() {
///     println!("Found channel: {}", channel_key);
/// }
/// 
/// // Read data from a specific channel
/// if let Some(channel) = reader.get_channel("Group1/Channel1") {
///     let data: Vec<f64> = channel.read_all_data(&mut reader.file, &reader.segments).unwrap();
///     println!("Read {} values", data.len());
/// }
/// ```
pub struct TdmsReader {
    pub(crate) file: BufReader<File>,
    pub(crate) segments: Vec<SegmentInfo>,
    channels: HashMap<String, ChannelInfo>,
}

impl TdmsReader {
    /// Open a TDMS file for reading
    /// 
    /// This parses the entire file structure including all segments and metadata.
    /// 
    /// # Arguments
    /// 
    /// * `path` - Path to the TDMS file
    /// 
    /// # Returns
    /// 
    /// A TdmsReader ready to read data from the file
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let file = File::open(path)?;
        let mut reader = TdmsReader {
            file: BufReader::with_capacity(65536, file),
            segments: Vec::new(),
            channels: HashMap::new(),
        };
        
        reader.parse_file()?;
        Ok(reader)
    }
    
    /// Parse the entire file structure
    fn parse_file(&mut self) -> Result<()> {
        // First pass: discover all segments
        self.discover_segments()?;
        
        // Second pass: parse metadata and build channel map
        self.parse_metadata()?;
        
        Ok(())
    }
    
    /// Discover all segments in the file
    fn discover_segments(&mut self) -> Result<()> {
        self.file.seek(SeekFrom::Start(0))?;
        let file_size = self.file.seek(SeekFrom::End(0))?;
        self.file.seek(SeekFrom::Start(0))?;
        
        while self.file.stream_position()? < file_size {
            let segment_offset = self.file.stream_position()?;
            
            // Check if we have enough bytes for a lead-in
            if file_size - segment_offset < SegmentHeader::LEAD_IN_SIZE as u64 {
                break;
            }
            
            // Read lead-in
            let mut tag = [0u8; 4];
            self.file.read_exact(&mut tag)?;
            
            if &tag != SegmentHeader::TDMS_TAG && &tag != SegmentHeader::INDEX_TAG {
                return Err(TdmsError::InvalidTag {
                    expected: "TDSm or TDSh".to_string(),
                    found: String::from_utf8_lossy(&tag).to_string(),
                });
            }
            
            // ToC is always little-endian
            let toc_raw = self.file.read_u32::<LittleEndian>()?;
            let toc = TocFlags::new(toc_raw);
            
            let _version = self.file.read_u32::<LittleEndian>()?;
            let next_segment_offset = self.file.read_u64::<LittleEndian>()?;
            let raw_data_offset = self.file.read_u64::<LittleEndian>()?;
            
            let segment_info = SegmentInfo {
                offset: segment_offset,
                toc,
                is_big_endian: toc.is_big_endian(),
                metadata_size: raw_data_offset,
                raw_data_offset,
            };
            
            self.segments.push(segment_info);
            
            // Check for incomplete segment
            if next_segment_offset == SegmentHeader::INCOMPLETE_MARKER {
                break;
            }
            
            // Calculate next segment position
            let next_pos = segment_offset + SegmentHeader::LEAD_IN_SIZE as u64 + next_segment_offset;
            
            if next_pos > file_size || next_pos <= segment_offset {
                break;
            }
            
            self.file.seek(SeekFrom::Start(next_pos))?;
        }
        
        Ok(())
    }
    
    /// Parse metadata from all segments and build channel information
    fn parse_metadata(&mut self) -> Result<()> {
        let mut active_channels: Vec<String> = Vec::new();
        let mut channel_order_in_segment: HashMap<usize, Vec<String>> = HashMap::new();
        let mut new_segment_indices: HashMap<String, (u64, u64)> = HashMap::new();
        
        for (segment_idx, segment) in self.segments.clone().iter().enumerate() {
            let mut segment_channels = Vec::new();
            new_segment_indices.clear();
            
            if !segment.toc.has_new_obj_list() && !active_channels.is_empty() {
                // Reuse previous segment's channel list
                segment_channels = active_channels.clone();
            }
            
            if segment.toc.has_metadata() {
                // Seek to metadata start
                let metadata_start = segment.offset + SegmentHeader::LEAD_IN_SIZE as u64;
                self.file.seek(SeekFrom::Start(metadata_start))?;
                
                // Parse metadata
                let new_channels = self.parse_segment_metadata(
                    segment_idx,
                    segment,
                    &mut segment_channels,
                    &mut new_segment_indices
                )?;
                
                if segment.toc.has_new_obj_list() {
                    active_channels = segment_channels.clone();
                } else {
                    // Merge new channels
                    for channel_key in new_channels {
                        if !active_channels.contains(&channel_key) {
                            active_channels.push(channel_key);
                            segment_channels.push(active_channels.last().unwrap().clone());
                        }
                    }
                }
            }
            
            // Store channel order for this segment
            if segment.toc.has_raw_data() && !segment_channels.is_empty() {
                channel_order_in_segment.insert(segment_idx, segment_channels.clone());
                
                // Calculate byte offsets for each channel in raw data
                self.calculate_segment_offsets(segment_idx, &segment_channels, &new_segment_indices)?;
            }
        }
        
        Ok(())
    }
    
    /// Parse metadata from a single segment
    fn parse_segment_metadata(
        &mut self,
        _segment_idx: usize,
        segment: &SegmentInfo,
        segment_channels: &mut Vec<String>,
        new_segment_indices: &mut HashMap<String, (u64, u64)>,
    ) -> Result<Vec<String>> {
        let is_big_endian = segment.is_big_endian;
        let mut new_channels = Vec::new();
        
        // Read object count
        let object_count = self.read_u32(is_big_endian)?;
        
        for _ in 0..object_count {
            // Read object path
            let path_string = self.read_length_prefixed_string(is_big_endian)?;
            
            // Parse path
            let path = ObjectPath::from_string(&path_string)?;
            
            // Only process channel objects
            if let ObjectPath::Channel { group, channel } = path {
                let channel_key = format!("{}/{}", group, channel);
                
                // Read raw data index
                let raw_index_length = self.read_u32(is_big_endian)?;
                
                let has_data = raw_index_length != 0xFFFFFFFF;
                let matches_previous = raw_index_length == 0x00000000;
                
                if has_data && !matches_previous {
                    // Read new index information
                    let data_type_raw = self.read_u32(is_big_endian)?;
                    let data_type = DataType::from_u32(data_type_raw)
                        .ok_or_else(|| TdmsError::InvalidDataType(data_type_raw))?;
                    
                    let _dimension = self.read_u32(is_big_endian)?;
                    let number_of_values = self.read_u64(is_big_endian)?;
                    
                    let total_size = if data_type == DataType::String {
                        self.read_u64(is_big_endian)?
                    } else {
                        number_of_values * data_type.fixed_size().unwrap_or(0) as u64
                    };
                    
                    // Get or create channel info
                    let channel_info = self.channels.entry(channel_key.clone())
                        .or_insert_with(|| ChannelInfo::new(data_type));
                    
                    // Update data type (in case it changed)
                    channel_info.data_type = data_type;
                    
                    // Store for later when we calculate offsets
                    new_segment_indices.insert(channel_key.clone(), (number_of_values, total_size));
                    if !segment_channels.contains(&channel_key) {
                        segment_channels.push(channel_key.clone());
                        new_channels.push(channel_key.clone());
                    }
                } else if matches_previous {
                    // Reuse previous index
                    if let Some(channel_info) = self.channels.get(&channel_key) {
                        if let Some(last_segment) = channel_info.segments.last() {
                            new_segment_indices.insert(
                                channel_key.clone(),
                                (last_segment.value_count, last_segment.byte_size)
                            );
                        }
                    }
                    if !segment_channels.contains(&channel_key) {
                        segment_channels.push(channel_key.clone());
                    }
                }
                
                // Read properties (we skip them for now)
                let property_count = self.read_u32(is_big_endian)?;
                for _ in 0..property_count {
                    self.skip_property(is_big_endian)?;
                }
            } else {
                // File or group object - skip
                let raw_index_length = self.read_u32(is_big_endian)?;
                if raw_index_length != 0xFFFFFFFF && raw_index_length != 0x00000000 {
                    // Skip raw data index
                    let mut skip_buf = vec![0u8; raw_index_length as usize];
                    self.file.read_exact(&mut skip_buf)?;
                }
                
                // Skip properties
                let property_count = self.read_u32(is_big_endian)?;
                for _ in 0..property_count {
                    self.skip_property(is_big_endian)?;
                }
            }
        }
        
        Ok(new_channels)
    }
    
    /// Calculate byte offsets for channels in a segment's raw data
    fn calculate_segment_offsets(
        &mut self,
        segment_idx: usize,
        channel_keys: &[String],
        new_segment_indices: &HashMap<String, (u64, u64)>,
    ) -> Result<()> {
        let mut current_offset = 0u64;
        
        for channel_key in channel_keys {
            if let Some(channel_info) = self.channels.get_mut(channel_key) {
                // Find the index info we just parsed for this segment
                if let Some(&(value_count, byte_size)) = new_segment_indices.get(channel_key) {
                    channel_info.add_segment(SegmentData {
                        segment_index: segment_idx,
                        value_count,
                        byte_size,
                        byte_offset: current_offset,
                    });
                    
                    current_offset += byte_size;
                }
            }
        }
        
        Ok(())
    }
    
    /// Skip a property in the metadata stream
    fn skip_property(&mut self, is_big_endian: bool) -> Result<()> {
        // Skip property name
        let name_len = self.read_u32(is_big_endian)?;
        self.file.seek(SeekFrom::Current(name_len as i64))?;
        
        // Read data type
        let data_type_raw = self.read_u32(is_big_endian)?;
        let data_type = DataType::from_u32(data_type_raw)
            .ok_or_else(|| TdmsError::InvalidDataType(data_type_raw))?;
        
        // Skip value based on type
        if let Some(size) = data_type.fixed_size() {
            self.file.seek(SeekFrom::Current(size as i64))?;
        } else if data_type == DataType::String {
            let str_len = self.read_u32(is_big_endian)?;
            self.file.seek(SeekFrom::Current(str_len as i64))?;
        }
        
        Ok(())
    }
    
    /// List all channel keys in the file
    /// 
    /// Returns channel keys in the format "group/channel"
    pub fn list_channels(&self) -> Vec<String> {
        self.channels.keys().cloned().collect()
    }
    
    /// Get a channel reader for a specific channel
    /// 
    /// # Arguments
    /// 
    /// * `key` - The channel key in format "group/channel"
    /// 
    /// # Returns
    /// 
    /// A ChannelReader if the channel exists, None otherwise
    pub fn get_channel(&self, key: &str) -> Option<ChannelReader> {
        self.channels.get(key).map(|info| {
            ChannelReader::new(key.to_string(), info.clone())
        })
    }
    
    /// Get the number of segments in the file
    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }
    
    /// Get the number of channels in the file
    pub fn channel_count(&self) -> usize {
        self.channels.len()
    }
    
    /// Read data from a channel (convenience method)
    /// 
    /// # Type Parameters
    /// 
    /// * `T` - The type to read (must match the channel's data type)
    /// 
    /// # Arguments
    /// 
    /// * `group` - The group name
    /// * `channel` - The channel name
    /// 
    /// # Returns
    /// 
    /// A vector of values
    pub fn read_channel_data<T: Copy + Default>(
        &mut self,
        group: &str,
        channel: &str,
    ) -> Result<Vec<T>> {
        let key = format!("{}/{}", group, channel);
        let channel_reader = self.get_channel(&key)
            .ok_or_else(|| TdmsError::ChannelNotFound(key.clone()))?;
        
        channel_reader.read_all_data(&mut self.file, &self.segments)
    }
    
    /// Read string data from a channel (convenience method)
    pub fn read_channel_strings(
        &mut self,
        group: &str,
        channel: &str,
    ) -> Result<Vec<String>> {
        let key = format!("{}/{}", group, channel);
        let channel_reader = self.get_channel(&key)
            .ok_or_else(|| TdmsError::ChannelNotFound(key.clone()))?;
        
        channel_reader.read_all_strings(&mut self.file, &self.segments)
    }
    
    // Helper methods for reading with endianness
    
    fn read_u32(&mut self, is_big_endian: bool) -> Result<u32> {
        if is_big_endian {
            Ok(self.file.read_u32::<BigEndian>()?)
        } else {
            Ok(self.file.read_u32::<LittleEndian>()?)
        }
    }
    
    fn read_u64(&mut self, is_big_endian: bool) -> Result<u64> {
        if is_big_endian {
            Ok(self.file.read_u64::<BigEndian>()?)
        } else {
            Ok(self.file.read_u64::<LittleEndian>()?)
        }
    }
    
    fn read_length_prefixed_string(&mut self, is_big_endian: bool) -> Result<String> {
        let length = self.read_u32(is_big_endian)?;
        
        if length == 0 {
            return Ok(String::new());
        }
        
        let mut bytes = vec![0u8; length as usize];
        self.file.read_exact(&mut bytes)?;
        
        String::from_utf8(bytes).map_err(|_| TdmsError::InvalidUtf8)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    // Note: These tests would require actual TDMS files
    // In practice, you'd use integration tests with test fixtures
    
    #[test]
    fn test_segment_header_constants() {
        assert_eq!(SegmentHeader::LEAD_IN_SIZE, 28);
        assert_eq!(SegmentHeader::TDMS_TAG, b"TDSm");
    }
}