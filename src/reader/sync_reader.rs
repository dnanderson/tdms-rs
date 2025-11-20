// src/reader/sync_reader.rs
use crate::error::{TdmsError, Result};
use crate::types::{DataType, TocFlags, Property, PropertyValue}; 
use crate::segment::{SegmentHeader, SegmentInfo};
use crate::reader::channel_reader::{ChannelReader, SegmentData, ChannelInfo};
use crate::reader::streaming::{TdmsIter, TdmsStringIter, StreamingReader}; 
use crate::metadata::ObjectPath;
use crate::raw_data::RawDataReader;
use crate::reader::daqmx::{self, DaqMxMetadata}; // Import daqmx module
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, BufReader};
use std::path::Path;
use std::collections::HashMap;
use byteorder::{ReadBytesExt, LittleEndian, BigEndian};

#[cfg(feature = "mmap")]
use memmap2::Mmap;
#[cfg(feature = "mmap")]
use std::io::Cursor;

/// Trait alias for Read + Seek
pub trait ReadSeek: Read + Seek {}
impl<T: Read + Seek> ReadSeek for T {}

/// Synchronous TDMS file reader
pub struct TdmsReader<R: ReadSeek> {
    pub(crate) file: R,
    pub(crate) segments: Vec<SegmentInfo>,
    channels: HashMap<ObjectPath, ChannelInfo>,
    string_buffer: Vec<u8>,
    
    // Storage for file and group properties
    pub file_properties: HashMap<String, Property>,
    pub groups: HashMap<String, HashMap<String, Property>>,
}

/// Constructor for standard file I/O
impl TdmsReader<BufReader<File>> {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let file = File::open(path)?;
        let mut reader = TdmsReader {
            file: BufReader::with_capacity(65536, file),
            segments: Vec::new(),
            channels: HashMap::new(),
            string_buffer: Vec::with_capacity(256),
            file_properties: HashMap::new(),
            groups: HashMap::new(),
        };
        
        reader.parse_file()?;
        Ok(reader)
    }
}

/// Constructor for memory-mapped file I/O (requires "mmap" feature)
#[cfg(feature = "mmap")]
impl TdmsReader<Cursor<Mmap>> {
    pub fn open_mmap(path: impl AsRef<Path>) -> Result<Self> {
        let file = File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        let cursor = Cursor::new(mmap); 
        
        let mut reader = TdmsReader {
            file: cursor,
            segments: Vec::new(),
            channels: HashMap::new(),
            string_buffer: Vec::with_capacity(256),
            file_properties: HashMap::new(),
            groups: HashMap::new(),
        };
        
        reader.parse_file()?;
        Ok(reader)
    }
}

/// Generic implementation for all TdmsReader variants
impl<R: ReadSeek> TdmsReader<R> {
    
    fn parse_file(&mut self) -> Result<()> {
        self.discover_segments()?;
        self.parse_metadata()?;
        Ok(())
    }
    
    fn discover_segments(&mut self) -> Result<()> {
        self.file.seek(SeekFrom::Start(0))?;
        let file_size = self.file.seek(SeekFrom::End(0))?;
        self.file.seek(SeekFrom::Start(0))?;
        
        while self.file.stream_position()? < file_size {
            let segment_offset = self.file.stream_position()?;
            
            if file_size - segment_offset < SegmentHeader::LEAD_IN_SIZE as u64 {
                break;
            }
            
            let mut tag = [0u8; 4];
            self.file.read_exact(&mut tag)?;
            
            if &tag != SegmentHeader::TDMS_TAG && &tag != SegmentHeader::INDEX_TAG {
                return Err(TdmsError::InvalidTag {
                    expected: "TDSm or TDSh".to_string(),
                    found: String::from_utf8_lossy(&tag).to_string(),
                });
            }
            
            let toc_raw = self.file.read_u32::<LittleEndian>()?;
            let toc = TocFlags::new(toc_raw);
            
            let _version = self.file.read_u32::<LittleEndian>()?;
            let next_segment_offset = self.file.read_u64::<LittleEndian>()?;
            let metadata_size = self.file.read_u64::<LittleEndian>()?;
            
            let total_raw_data_size = if next_segment_offset == SegmentHeader::INCOMPLETE_MARKER {
                let segment_data_start = segment_offset + SegmentHeader::LEAD_IN_SIZE as u64;
                file_size.saturating_sub(segment_data_start).saturating_sub(metadata_size)
            } else {
                next_segment_offset.saturating_sub(metadata_size)
            };
            
            let segment_info = SegmentInfo {
                offset: segment_offset,
                toc,
                is_big_endian: toc.is_big_endian(),
                metadata_size,
                total_raw_data_size,
            };
            
            self.segments.push(segment_info);
            
            if next_segment_offset == SegmentHeader::INCOMPLETE_MARKER {
                break;
            }
            
            let next_pos = segment_offset + SegmentHeader::LEAD_IN_SIZE as u64 + next_segment_offset;
            
            if next_pos > file_size || next_pos <= segment_offset {
                break;
            }
            
            self.file.seek(SeekFrom::Start(next_pos))?;
        }
        
        Ok(())
    }
    
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
                for channel in segment_channels {
                    if !active_channels.contains(&channel) {
                        active_channels.push(channel);
                    }
                }
                &active_channels
            } else {
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
    
    fn parse_segment_metadata(
        &mut self,
        segment: &SegmentInfo,
        segment_channels: &mut Vec<ObjectPath>,
        new_segment_indices: &mut HashMap<ObjectPath, (u64, u64)>,
    ) -> Result<()> {
        let is_big_endian = segment.is_big_endian;
        let object_count = self.read_u32(is_big_endian)?;
        
        for _ in 0..object_count {
            let path_string = self.read_length_prefixed_string(is_big_endian)?;
            let path = ObjectPath::from_string(&path_string)?;
            
            // Read the Raw Data Index Header
            let raw_index_header = self.read_u32(is_big_endian)?;

            // Prepare potential new channel info data
            let mut new_data_type: Option<DataType> = None;
            let mut new_daqmx_meta: Option<DaqMxMetadata> = None;
            let mut new_parsed_index: Option<(DataType, u64, u64)> = None;
            
            let is_daqmx = raw_index_header == daqmx::FORMAT_CHANGING_SCALER || raw_index_header == daqmx::DIGITAL_LINE_SCALER;
            let mut matches_previous = false;

            if is_daqmx {
                // --- DAQmx RAW DATA ---
                let data_type_raw = self.read_u32(is_big_endian)?;
                let data_type = DataType::from_u32(data_type_raw)
                    .ok_or_else(|| TdmsError::InvalidDataType(data_type_raw))?;

                let daqmx_meta = DaqMxMetadata::read(&mut self.file, is_big_endian, raw_index_header)?;
                
                let number_of_values = daqmx_meta.chunk_size;
                let total_size: u64 = daqmx_meta.raw_data_widths.iter()
                    .map(|&w| w as u64 * number_of_values)
                    .sum();
                
                new_data_type = Some(data_type);
                new_daqmx_meta = Some(daqmx_meta);
                
                if number_of_values > 0 {
                    new_parsed_index = Some((data_type, number_of_values, total_size));
                }

            } else {
                // --- STANDARD TDMS DATA ---
                let raw_index_length = raw_index_header;
                let has_data = raw_index_length != 0xFFFFFFFF;
                matches_previous = raw_index_length == 0x00000000;

                if has_data && !matches_previous {
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
                    new_parsed_index = Some((data_type, number_of_values, total_size));
                }
            }

            // Read properties (independent of data type)
            let property_count = self.read_u32(is_big_endian)?;
            let mut local_properties = HashMap::with_capacity(property_count as usize);
            for _ in 0..property_count {
                let prop = self.read_property(is_big_endian)?;
                local_properties.insert(prop.name.clone(), prop);
            }

            // Now borrow self.channels or self.file_properties etc. to apply updates
            if let ObjectPath::Channel { .. } = &path {
                let channel_info = self.channels.entry(path.clone())
                    .or_insert_with(|| ChannelInfo::new(DataType::Void));
                
                // Apply properties
                channel_info.properties.extend(local_properties);

                // Apply DAQmx metadata if present
                if let Some(meta) = new_daqmx_meta {
                    channel_info.daqmx_metadata = Some(meta);
                }
                
                // Apply data type if present
                if let Some(dt) = new_data_type {
                    channel_info.data_type = dt;
                }

                // Handle index updates
                if let Some((data_type, number_of_values, total_size)) = new_parsed_index {
                    // If this was standard data, we update the type here too
                    if !is_daqmx {
                        channel_info.data_type = data_type;
                    }
                    
                    new_segment_indices.insert(path.clone(), (number_of_values, total_size));
                    if !segment_channels.contains(&path) {
                        segment_channels.push(path.clone());
                    }
                } else if matches_previous {
                    // For standard TDMS match previous
                    if let Some(last_segment) = channel_info.segments.last() {
                        new_segment_indices.insert(
                            path.clone(),
                            (last_segment.value_count, last_segment.byte_size)
                        );
                    }
                    if !segment_channels.contains(&path) {
                        segment_channels.push(path.clone());
                    }
                }
            } else {
                // Handle File/Group properties
                match &path {
                    ObjectPath::Root => self.file_properties.extend(local_properties),
                    ObjectPath::Group(name) => self.groups.entry(name.clone()).or_default().extend(local_properties),
                    _ => {}, 
                };
                
                // NOTE: DAQmx data on Groups/File objects is theoretically possible but rare/undefined in standard use cases for this lib.
                // We ignore index info for groups/root as per original logic.
            }
        }
        Ok(())
    }
    
    fn calculate_segment_offsets(
        &mut self,
        segment: &SegmentInfo,
        segment_idx: usize,
        channel_keys: &[ObjectPath],
        new_segment_indices: &HashMap<ObjectPath, (u64, u64)>,
    ) -> Result<()> {
        
        let mut total_metadata_described_raw_size = 0u64;
        let mut has_variable_length_type = false; 
        
        for channel_key in channel_keys {
            if let Some(&(_value_count, byte_size)) = new_segment_indices.get(channel_key) {
                total_metadata_described_raw_size += byte_size;
                
                if let Some(metadata) = self.channels.get(channel_key) {
                    if metadata.data_type == DataType::String {
                        has_variable_length_type = true;
                    }
                }
            }
        }

        if total_metadata_described_raw_size == 0 {
            return Ok(());
        }

        let mut num_chunks = 1u64;
        
        if !has_variable_length_type && segment.total_raw_data_size > total_metadata_described_raw_size {
            if segment.total_raw_data_size % total_metadata_described_raw_size != 0 {
                return Err(TdmsError::InvalidTag {
                    expected: format!("Raw data size ({}) to be a multiple of chunk size ({})", 
                        segment.total_raw_data_size, total_metadata_described_raw_size),
                    found: "Mismatched raw data size".to_string(),
                });
            }
            num_chunks = segment.total_raw_data_size / total_metadata_described_raw_size;
        }
        
        for chunk_idx in 0..num_chunks {
            let mut current_offset = chunk_idx * total_metadata_described_raw_size;
            
            for channel_key in channel_keys {
                if let Some(channel_info) = self.channels.get_mut(channel_key) {
                    if let Some(&(value_count, byte_size)) = new_segment_indices.get(channel_key) {
                        
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

    fn read_property(&mut self, is_big_endian: bool) -> Result<Property> {
        let name = self.read_length_prefixed_string(is_big_endian)?;
        let data_type_raw = self.read_u32(is_big_endian)?;
        let data_type = DataType::from_u32(data_type_raw)
            .ok_or_else(|| TdmsError::InvalidDataType(data_type_raw))?;
        let value = self.read_property_value(data_type, is_big_endian)?;
        Ok(Property { name, value })
    }

    fn read_property_value(&mut self, data_type: DataType, is_big_endian: bool) -> Result<PropertyValue> {
        match data_type {
            DataType::I8 => Ok(PropertyValue::I8(RawDataReader::read_i8(&mut self.file)?)),
            DataType::I16 => Ok(PropertyValue::I16(RawDataReader::read_i16(&mut self.file, is_big_endian)?)),
            DataType::I32 => Ok(PropertyValue::I32(RawDataReader::read_i32(&mut self.file, is_big_endian)?)),
            DataType::I64 => Ok(PropertyValue::I64(RawDataReader::read_i64(&mut self.file, is_big_endian)?)),
            DataType::U8 => Ok(PropertyValue::U8(RawDataReader::read_u8(&mut self.file)?)),
            DataType::U16 => Ok(PropertyValue::U16(RawDataReader::read_u16(&mut self.file, is_big_endian)?)),
            DataType::U32 => Ok(PropertyValue::U32(RawDataReader::read_u32(&mut self.file, is_big_endian)?)),
            DataType::U64 => Ok(PropertyValue::U64(RawDataReader::read_u64(&mut self.file, is_big_endian)?)),
            DataType::SingleFloat => Ok(PropertyValue::Float(RawDataReader::read_f32(&mut self.file, is_big_endian)?)),
            DataType::DoubleFloat => Ok(PropertyValue::Double(RawDataReader::read_f64(&mut self.file, is_big_endian)?)),
            DataType::Boolean => Ok(PropertyValue::Boolean(RawDataReader::read_bool(&mut self.file)?)),
            DataType::TimeStamp => Ok(PropertyValue::Timestamp(RawDataReader::read_timestamp(&mut self.file, is_big_endian)?)),
            DataType::String => Ok(PropertyValue::String(self.read_length_prefixed_string(is_big_endian)?)),
            _ => Err(TdmsError::Unsupported(format!("Property data type {:?}", data_type))),
        }
    }
    
    pub fn list_channels(&self) -> Vec<String> {
        self.channels.keys().map(|p| p.to_string()).collect()
    }

    pub fn list_groups(&self) -> Vec<String> {
        self.groups.keys().cloned().collect()
    }
    
    pub fn get_file_properties(&self) -> &HashMap<String, Property> {
        &self.file_properties
    }
    
    pub fn get_group_properties(&self, group_name: &str) -> Option<&HashMap<String, Property>> {
        self.groups.get(group_name)
    }
    
    pub fn get_channel_properties(&self, group: &str, channel: &str) -> Option<&HashMap<String, Property>> {
        let path = ObjectPath::Channel { group: group.to_string(), channel: channel.to_string() };
        self.channels.get(&path).map(|info| &info.properties)
    }
    
    pub fn get_channel(&self, key: &str) -> Option<ChannelReader> {
        ObjectPath::from_string(key).ok()
            .and_then(|path| self.channels.get(&path))
            .map(|info| ChannelReader::new(key.to_string(), info.clone()))
    }
    
    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }
    
    pub fn channel_count(&self) -> usize {
        self.channels.len()
    }
    
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

    pub fn iter_channel_data<T: Copy + Default>(
        &mut self,
        group: &str,
        channel: &str,
        chunk_size: usize,
    ) -> Result<TdmsIter<'_, T, R>> {
        let path = ObjectPath::Channel { 
            group: group.to_string(), 
            channel: channel.to_string() 
        };
        let key_string = path.to_string();
        
        let channel_reader = self.channels.get(&path)
            .map(|info| ChannelReader::new(key_string.clone(), info.clone()))
            .ok_or_else(|| TdmsError::ChannelNotFound(key_string))?;
            
        Ok(TdmsIter::new(self, channel_reader, chunk_size))
    }

    pub fn iter_channel_strings(
        &mut self,
        group: &str,
        channel: &str,
        chunk_size: usize,
    ) -> Result<TdmsStringIter<'_, R>> {
        let path = ObjectPath::Channel { 
            group: group.to_string(), 
            channel: channel.to_string() 
        };
        let key_string = path.to_string();
        
        let channel_reader = self.channels.get(&path)
            .map(|info| ChannelReader::new(key_string.clone(), info.clone()))
            .ok_or_else(|| TdmsError::ChannelNotFound(key_string))?;
            
        Ok(TdmsStringIter::new(self, channel_reader, chunk_size))
    }

    pub fn read_streaming_data<T: Copy + Default>(
        &mut self,
        stream: &mut StreamingReader
    ) -> Result<Option<Vec<T>>> {
        stream.next(&mut self.file, &self.segments)
    }

    pub fn read_streaming_strings(
        &mut self,
        stream: &mut StreamingReader
    ) -> Result<Option<Vec<String>>> {
        stream.next_strings(&mut self.file, &self.segments)
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
    
    #[test]
    fn test_segment_header_constants() {
        assert_eq!(SegmentHeader::LEAD_IN_SIZE, 28);
        assert_eq!(SegmentHeader::TDMS_TAG, b"TDSm");
    }
}