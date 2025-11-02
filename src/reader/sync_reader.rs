// src/reader/sync_reader.rs
use crate::error::{TdmsError, Result};
use crate::types::{DataType, TocFlags};
use crate::segment::{SegmentHeader, SegmentInfo};
use crate::reader::channel_reader::{ChannelReader, SegmentData, ChannelInfo};
use crate::metadata::ObjectPath;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, BufReader}; // <-- Removed 'Cursor' from here
use std::path::Path;
use std::collections::HashMap;
use byteorder::{ReadBytesExt, LittleEndian, BigEndian};

#[cfg(feature = "mmap")]
use memmap2::Mmap;
#[cfg(feature = "mmap")]
use std::io::Cursor; // <-- Added 'Cursor' import here, inside the cfg block

/// Trait alias for Read + Seek
pub trait ReadSeek: Read + Seek {}
impl<T: Read + Seek> ReadSeek for T {}

/// Synchronous TDMS file reader
/// 
/// Provides efficient reading of TDMS files with support for:
/// - Full file parsing and metadata extraction
/// - Channel discovery and listing
/// - Efficient data access through ChannelReader
/// 
/// This reader is generic over its I/O source (`R: ReadSeek`).
/// Use `TdmsReader::open(path)` for standard buffered file reading.
/// Use `TdmsReader::open_mmap(path)` (with "mmap" feature) for memory-mapped reading.
/// 
/// # Example
/// 
/// ```no_run
/// use tdms_rs::reader::TdmsReader;
/// 
/// // Standard file reading
/// let mut reader = TdmsReader::open("data.tdms").unwrap();
/// 
/// // Or, with "mmap" feature enabled:
/// // let mut reader = TdmsReader::open_mmap("data.tdms").unwrap();
/// 
/// // List all channels
/// for channel_key in reader.list_channels() {
///     println!("Found channel: {}", channel_key);
/// }
/// 
/// // Get a channel and inspect its properties
/// if let Some(channel) = reader.get_channel("/'Group1'/'Channel1'") {
///     println!("Channel {} has {} values", channel.key(), channel.total_values());
/// }
///
/// // Read data from the channel
/// let data: Vec<f64> = reader.read_channel_data("Group1", "Channel1").unwrap();
/// println!("Read {} values", data.len());
/// ```
pub struct TdmsReader<R: ReadSeek> {
    pub(crate) file: R,
    pub(crate) segments: Vec<SegmentInfo>,
    channels: HashMap<ObjectPath, ChannelInfo>,
    string_buffer: Vec<u8>,
}

/// Constructor for standard file I/O
impl TdmsReader<BufReader<File>> {
    /// Open a TDMS file for reading
    /// 
    /// This parses the entire file structure including all segments and metadata
    /// using a standard buffered file reader.
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
            string_buffer: Vec::with_capacity(256),
        };
        
        reader.parse_file()?;
        Ok(reader)
    }
}

/// Constructor for memory-mapped file I/O (requires "mmap" feature)
#[cfg(feature = "mmap")]
impl TdmsReader<Cursor<Mmap>> {
    /// Open a TDMS file for reading using memory-mapping (mmap)
    ///
    /// This maps the file into virtual memory, which is highly efficient for
    /// random access, especially on SSDs. This is enabled by the "mmap" feature.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the TDMS file
    ///
    /// # Returns
    ///
    /// A TdmsReader ready to read data from the memory-mapped file
    pub fn open_mmap(path: impl AsRef<Path>) -> Result<Self> {
        let file = File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        let cursor = Cursor::new(mmap); // Cursor takes ownership of Mmap
        
        let mut reader = TdmsReader {
            file: cursor,
            segments: Vec::new(),
            channels: HashMap::new(),
            string_buffer: Vec::with_capacity(256),
        };
        
        reader.parse_file()?;
        Ok(reader)
    }
}

/// Generic implementation for all TdmsReader variants
impl<R: ReadSeek> TdmsReader<R> {
    
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
            // Per spec: "length of the remaining segment (overall length ... minus length of the lead in)"
            let next_segment_offset = self.file.read_u64::<LittleEndian>()?;
            // Per spec: "overall length of the meta information"
            let metadata_size = self.file.read_u64::<LittleEndian>()?;
            
            // *** FIX: Calculate total raw data size ***
            let total_raw_data_size = if next_segment_offset == SegmentHeader::INCOMPLETE_MARKER {
                // This can only happen to the last segment
                // We must calculate its size from the file size
                let segment_data_start = segment_offset + SegmentHeader::LEAD_IN_SIZE as u64;
                file_size.saturating_sub(segment_data_start).saturating_sub(metadata_size)
            } else {
                // This is the normal case
                next_segment_offset.saturating_sub(metadata_size)
            };
            
            let segment_info = SegmentInfo {
                offset: segment_offset,
                toc,
                is_big_endian: toc.is_big_endian(),
                metadata_size, // Correctly named
                total_raw_data_size, // Correctly calculated
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
        let mut active_channels: Vec<ObjectPath> = Vec::new();
        let mut new_segment_indices: HashMap<ObjectPath, (u64, u64)> = HashMap::new();

        let segments: Vec<SegmentInfo> = self.segments.clone();
        for (segment_idx, segment) in segments.iter().enumerate() {
            let mut segment_channels = Vec::new();
            new_segment_indices.clear();

            let has_metadata = segment.toc.has_metadata();
            if has_metadata {
                let metadata_start = segment.offset + SegmentHeader::LEAD_IN_SIZE as u64;
                self.file.seek(SeekFrom::Start(metadata_start))?;

                self.parse_segment_metadata(
                    segment,
                    &mut segment_channels,
                    &mut new_segment_indices,
                )?;
            }

            let channels_for_this_segment = if segment.toc.has_new_obj_list() {
                active_channels = segment_channels;
                &active_channels
            } else if has_metadata {
                // Merge new channels into active list.
                for channel in segment_channels {
                    if !active_channels.contains(&channel) {
                        active_channels.push(channel);
                    }
                }
                &active_channels
            } else {
                // No metadata, reuse last active channel list
                &active_channels
            };

            if segment.toc.has_raw_data() && !channels_for_this_segment.is_empty() {
                self.calculate_segment_offsets(
                    segment,
                    segment_idx,
                    channels_for_this_segment,
                    &new_segment_indices,
                )?;
            }
        }

        Ok(())
    }
    
    /// Parse metadata from a single segment
    fn parse_segment_metadata(
        &mut self,
        segment: &SegmentInfo,
        segment_channels: &mut Vec<ObjectPath>,
        new_segment_indices: &mut HashMap<ObjectPath, (u64, u64)>,
    ) -> Result<()> {
        let is_big_endian = segment.is_big_endian;
        
        // Read object count
        let object_count = self.read_u32(is_big_endian)?;
        
        for _ in 0..object_count {
            // Read object path
            let path_string = self.read_length_prefixed_string(is_big_endian)?;
            let path = ObjectPath::from_string(&path_string)?;
            
            // Only process channel objects
            if let ObjectPath::Channel { .. } = &path {
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
                    let channel_info = self.channels.entry(path.clone())
                        .or_insert_with(|| ChannelInfo::new(data_type));
                    
                    // Update data type (in case it changed)
                    channel_info.data_type = data_type;
                    
                    // Store for later when we calculate offsets
                    new_segment_indices.insert(path.clone(), (number_of_values, total_size));
                    if !segment_channels.contains(&path) {
                        segment_channels.push(path.clone());
                    }
                } else if matches_previous {
                    // Reuse previous index
                    if let Some(channel_info) = self.channels.get(&path) {
                        if let Some(last_segment) = channel_info.segments.last() {
                            new_segment_indices.insert(
                                path.clone(),
                                (last_segment.value_count, last_segment.byte_size)
                            );
                        }
                    }
                    if !segment_channels.contains(&path) {
                        segment_channels.push(path.clone());
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
                    self.file.seek(SeekFrom::Current(raw_index_length as i64))?;
                }
                
                // Skip properties
                let property_count = self.read_u32(is_big_endian)?;
                for _ in 0..property_count {
                    self.skip_property(is_big_endian)?;
                }
            }
        }
        
        Ok(())
    }
    
    /// Calculate byte offsets for channels in a segment's raw data
    /// *** FIX APPLIED HERE ***
    fn calculate_segment_offsets(
        &mut self,
        segment: &SegmentInfo,
        segment_idx: usize,
        channel_keys: &[ObjectPath],
        new_segment_indices: &HashMap<ObjectPath, (u64, u64)>,
    ) -> Result<()> {
        
        // Calculate the size of a single "chunk" as described by the metadata
        let mut total_metadata_described_raw_size = 0u64;
        let mut has_variable_length_type = false; // *** ADDED ***
        
        for channel_key in channel_keys {
            if let Some(&(_value_count, byte_size)) = new_segment_indices.get(channel_key) {
                total_metadata_described_raw_size += byte_size;
                
                // *** ADDED: Check for variable-length types ***
                if let Some(metadata) = self.channels.get(channel_key) {
                    if metadata.data_type == DataType::String {
                        has_variable_length_type = true;
                    }
                }
            }
        }

        if total_metadata_described_raw_size == 0 {
            // No raw data in this segment, even if header says so.
            return Ok(());
        }

        let mut num_chunks = 1u64;
        
        // Check for appended data (Scenario 2)
        // *** MODIFIED: Only for fixed-size types ***
        if !has_variable_length_type && segment.total_raw_data_size > total_metadata_described_raw_size {
            // Check that total_raw_data_size is a clean multiple
            if segment.total_raw_data_size % total_metadata_described_raw_size != 0 {
                return Err(TdmsError::InvalidTag {
                    expected: format!("Raw data size ({}) to be a multiple of chunk size ({})", 
                        segment.total_raw_data_size, total_metadata_described_raw_size),
                    found: "Mismatched raw data size".to_string(),
                });
            }
            num_chunks = segment.total_raw_data_size / total_metadata_described_raw_size;
        }
        // *** ADDED COMMENT: For variable-length types, num_chunks stays 1 ***
        // This prevents incorrect multi-chunk interpretation of string data
        
        // Add a SegmentData entry for each chunk
        for chunk_idx in 0..num_chunks {
            let mut current_offset = chunk_idx * total_metadata_described_raw_size;
            
            for channel_key in channel_keys {
                if let Some(channel_info) = self.channels.get_mut(channel_key) {
                    // Find the index info we just parsed for this segment
                    if let Some(&(value_count, byte_size)) = new_segment_indices.get(channel_key) {
                        
                        // Don't add empty segments
                        if value_count == 0 && byte_size == 0 {
                            continue;
                        }

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
        self.channels.keys().map(|p| p.to_string()).collect()
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
        ObjectPath::from_string(key).ok()
            .and_then(|path| self.channels.get(&path))
            .map(|info| ChannelReader::new(key.to_string(), info.clone()))
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
        let path = ObjectPath::Channel { group: group.to_string(), channel: channel.to_string() };
        let key_string = path.to_string();
        let channel_reader = self.channels.get(&path)
            .map(|info| ChannelReader::new(key_string.clone(), info.clone()))
            .ok_or_else(|| TdmsError::ChannelNotFound(key_string))?;
        
        channel_reader.read_all_data(&mut self.file, &self.segments)
    }
    
    /// Read string data from a channel (convenience method)
    pub fn read_channel_strings(
        &mut self,
        group: &str,
        channel: &str,
    ) -> Result<Vec<String>> {
        let path = ObjectPath::Channel { group: group.to_string(), channel: channel.to_string() };
        let key_string = path.to_string();
        let channel_reader = self.channels.get(&path)
            .map(|info| ChannelReader::new(key_string.clone(), info.clone()))
            .ok_or_else(|| TdmsError::ChannelNotFound(key_string))?;
        
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
        
        self.string_buffer.clear();
        self.string_buffer.resize(length as usize, 0);
        self.file.read_exact(&mut self.string_buffer)?;
        
        String::from_utf8(self.string_buffer.clone()).map_err(|_| TdmsError::InvalidUtf8)
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