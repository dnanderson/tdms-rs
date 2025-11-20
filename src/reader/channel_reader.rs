// src/reader/channel_reader.rs
use crate::error::{TdmsError, Result};
use crate::types::{DataType, Property, PropertyValue}; 
use crate::segment::SegmentInfo;
use crate::raw_data::RawDataReader;
use crate::reader::daqmx::DaqMxMetadata; 
use std::io::{Read, Seek, SeekFrom};
use std::collections::HashMap;

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
    pub properties: HashMap<String, Property>,
    pub daqmx_metadata: Option<DaqMxMetadata>, 
}

impl ChannelInfo {
    pub fn new(data_type: DataType) -> Self {
        ChannelInfo {
            data_type,
            segments: Vec::new(),
            total_values: 0,
            properties: HashMap::new(),
            daqmx_metadata: None, 
        }
    }

    pub fn add_segment(&mut self, segment_data: SegmentData) {
        self.total_values += segment_data.value_count;
        self.segments.push(segment_data);
    }
}

/// Interface for reading data from a specific channel
pub struct ChannelReader {
    channel_key: String,
    info: ChannelInfo,
}

impl ChannelReader {
    pub(crate) fn new(channel_key: String, info: ChannelInfo) -> Self {
        ChannelReader { channel_key, info }
    }

    pub fn data_type(&self) -> DataType {
        self.info.data_type
    }

    pub fn total_values(&self) -> u64 {
        self.info.total_values
    }

    pub fn segment_count(&self) -> usize {
        self.info.segments.len()
    }

    pub fn key(&self) -> &str {
        &self.channel_key
    }
    
    pub fn get_properties(&self) -> &HashMap<String, Property> {
        &self.info.properties
    }

    pub fn read_all_data<T: Copy + Default + 'static, R: Read + Seek>(
        &self,
        reader: &mut R,
        segments: &[SegmentInfo],
    ) -> Result<Vec<T>> {
        // BRANCH: If this is DAQmx data, use specific reader logic
        if let Some(daqmx_meta) = &self.info.daqmx_metadata {
            return self.read_daqmx_data(reader, segments, daqmx_meta);
        }

        // STANDARD TDMS LOGIC
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
                + 28 // Lead-in size
                + segment_info.metadata_size 
                + segment_data.byte_offset;
            
            reader.seek(SeekFrom::Start(data_offset))?;

            let values = RawDataReader::read_values::<T, _>(
                reader,
                segment_data.value_count as usize,
                segment_info.is_big_endian,
            )?;

            result.extend_from_slice(&values);
        }

        Ok(result)
    }
    
    /// Logic to read DAQmx raw data segments
    fn read_daqmx_data<T: Copy + Default + 'static, R: Read + Seek>(
    &self,
    reader: &mut R,
    segments: &[SegmentInfo],
    daqmx_meta: &DaqMxMetadata,
    ) -> Result<Vec<T>> {
        // Get scaling parameters from properties
        let slope = self.info.properties
            .get("NI_Scale[1]_Linear_Slope")
            .and_then(|p| match &p.value {
                PropertyValue::Double(v) => Some(*v),
                _ => None,
            })
            .unwrap_or(1.0);
        
        let intercept = self.info.properties
            .get("NI_Scale[1]_Linear_Y_Intercept")
            .and_then(|p| match &p.value {
                PropertyValue::Double(v) => Some(*v),
                _ => None,
            })
            .unwrap_or(0.0);
        
        // Assumption: Use the first scaler
        let scaler = daqmx_meta.scalers.first().ok_or_else(|| 
            TdmsError::Unsupported("DAQmx channel has no scalers".into()))?;
        
        let mut raw_values: Vec<i16> = Vec::new();

        for segment_data in &self.info.segments {
            let segment_info = &segments[segment_data.segment_index];
            let is_big_endian = segment_info.is_big_endian;

            let num_values = segment_data.value_count as usize;
            if num_values == 0 {
                continue;
            }

            // Key insight from nptdms: DAQmx data has separate buffers that are stored sequentially.
            // Each buffer corresponds to one acquisition card.
            // Data is interleaved WITHIN each buffer, not across buffers.
            
            let raw_buffer_index = scaler.raw_buffer_index as usize;
            let raw_data_width = *daqmx_meta.raw_data_widths.get(raw_buffer_index)
                .ok_or_else(|| TdmsError::Unsupported("Invalid raw buffer index".into()))? as usize;
            
            // Calculate offset to the start of this buffer's data
            // Buffers are stored sequentially, so we need to skip previous buffers
            let mut buffer_offset = 0u64;
            for i in 0..raw_buffer_index {
                let width = daqmx_meta.raw_data_widths[i] as u64;
                buffer_offset += num_values as u64 * width;
            }
            
            let block_start = segment_info.offset 
                + 28 
                + segment_info.metadata_size 
                + segment_data.byte_offset
                + buffer_offset;

            reader.seek(SeekFrom::Start(block_start))?;

            // Read this buffer's data
            let buffer_size = num_values * raw_data_width;
            let mut buffer = vec![0u8; buffer_size];
            reader.read_exact(&mut buffer)?;

            // Extract values for this scaler from the buffer
            let byte_offset = scaler.raw_byte_offset as usize;
            let element_size = scaler.data_type.fixed_size()
                .ok_or_else(|| TdmsError::Unsupported("Variable size DAQmx types not supported".into()))?;

            for i in 0..num_values {
                let start = i * raw_data_width + byte_offset;
                let end = start + element_size;
                
                if end > buffer.len() {
                    return Err(TdmsError::Unsupported(format!(
                        "Buffer overflow: trying to read bytes {}..{} from buffer of length {}",
                        start, end, buffer.len()
                    )));
                }
                
                let bytes = &buffer[start..end];
                let mut cursor = std::io::Cursor::new(bytes);
                let raw_val = RawDataReader::read_i16(&mut cursor, is_big_endian)?;
                raw_values.push(raw_val);
            }
        }
        
        // Apply scaling and convert to output type T
        if std::mem::size_of::<T>() != 8 {
            return Err(TdmsError::TypeMismatch {
                expected: "f64 for scaled DAQmx data".to_string(),
                found: format!("Type with size {}", std::mem::size_of::<T>()),
            });
        }
        
        let mut result = Vec::with_capacity(raw_values.len());
        for &raw in &raw_values {
            let scaled = raw as f64 * slope + intercept;
            let value: T = unsafe { std::mem::transmute_copy(&scaled) };
            result.push(value);
        }
        
        Ok(result)
    }

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

            if segment_end <= start_index {
                current_index = segment_end;
                continue;
            }

            if segment_start >= end_index {
                break;
            }

            let read_start_in_segment = if start_index > segment_start {
                start_index - segment_start
            } else {
                0
            };

            let values_available_in_segment = segment_data.value_count - read_start_in_segment;
            let values_to_read = (remaining_to_read as u64).min(values_available_in_segment) as usize;

            let segment_info = &segments[segment_data.segment_index];
            let type_size = std::mem::size_of::<T>() as u64;
            let data_offset = segment_info.offset
                + 28
                + segment_info.metadata_size 
                + segment_data.byte_offset
                + (read_start_in_segment * type_size);

            reader.seek(SeekFrom::Start(data_offset))?;

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
                + segment_info.metadata_size 
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

            let read_start_in_segment = if start_index > segment_start {
                start_index - segment_start 
            } else {
                0
            } as u64;

            let values_available_in_segment = segment_data.value_count - read_start_in_segment;
            let values_to_read = (remaining_to_read as u64).min(values_available_in_segment) as usize;

            let segment_info = &segments[segment_data.segment_index];
            let is_big_endian = segment_info.is_big_endian;

            let offset_block_start = segment_info.offset
                + 28 
                + segment_info.metadata_size
                + segment_data.byte_offset;
            
            let string_data_block_start = offset_block_start + (segment_data.value_count * 4); 

            let byte_start_offset = if read_start_in_segment == 0 {
                0
            } else {
                reader.seek(SeekFrom::Start(offset_block_start + (read_start_in_segment - 1) * 4))?;
                RawDataReader::read_u32(reader, is_big_endian)? as u64
            };

            reader.seek(SeekFrom::Start(offset_block_start + (read_start_in_segment + values_to_read as u64 - 1) * 4))?;
            let byte_end_offset = RawDataReader::read_u32(reader, is_big_endian)? as u64;

            let bytes_to_read = (byte_end_offset - byte_start_offset) as usize;

            reader.seek(SeekFrom::Start(offset_block_start + read_start_in_segment * 4))?;
            let offsets_in_chunk = RawDataReader::read_values::<u32, _>( 
                reader,
                values_to_read,
                is_big_endian,
            )?;

            if bytes_to_read > 0 {
                reader.seek(SeekFrom::Start(string_data_block_start + byte_start_offset))?;
                let mut data_buf = vec![0u8; bytes_to_read];
                reader.read_exact(&mut data_buf)?;

                let mut local_start = 0;
                for &cumulative_end in &offsets_in_chunk {
                    let local_end = (cumulative_end as u64 - byte_start_offset) as usize;
                    if local_end < local_start || local_end > data_buf.len() {
                        return Err(TdmsError::InvalidTag {
                            expected: "valid string offsets".to_string(),
                            found: "corrupt offsets".to_string(),
                        });
                    }
                    
                    let s = String::from_utf8(data_buf[local_start..local_end].to_vec())
                        .map_err(|_| TdmsError::InvalidUtf8)?;
                    result.push(s);
                    local_start = local_end;
                }
            } else {
                for _ in 0..values_to_read {
                    result.push(String::new());
                }
            }
            
            remaining_to_read -= values_to_read;
            current_index = segment_end;

            if remaining_to_read == 0 {
                break;
            }
        }

        Ok(result)
    }

    pub fn iter_chunks<T: Copy + Default>(&self, chunk_size: usize) -> ChunkIterator<T> {
        ChunkIterator::new(self.clone(), chunk_size)
    }

    pub fn get_segment_data(&self, segment_index: usize) -> Option<&SegmentData> {
        self.info.segments.get(segment_index)
    }

    pub fn is_empty(&self) -> bool {
        self.info.total_values == 0
    }
}

impl Clone for ChannelReader {
    fn clone(&self) -> Self {
        ChannelReader {
            channel_key: self.channel_key.clone(),
            info: self.info.clone(),
        }
    }
}

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

    pub fn reset(&mut self) {
        self.current_position = 0;
    }

    pub fn position(&self) -> u64 {
        self.current_position
    }

    pub fn total_values(&self) -> u64 {
        self.channel.total_values()
    }

    pub fn has_more(&self) -> bool {
        self.current_position < self.channel.total_values()
    }
}