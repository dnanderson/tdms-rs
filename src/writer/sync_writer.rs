// src/writer/sync_writer.rs
// This is the TdmsWriter implementation from the original writer.rs
// (Keep the FULL implementation as provided earlier)
use crate::error::{TdmsError, Result};
use crate::types::{DataType, TocFlags, Property, PropertyValue};
use crate::metadata::{ObjectPath, ChannelMetadata, RawDataIndex};
use crate::segment::SegmentHeader;
use crate::raw_data::RawDataBuffer;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Write, BufWriter, Seek, SeekFrom};
use std::path::Path;
use byteorder::{WriteBytesExt, LittleEndian};

/// Synchronous TDMS file writer with incremental metadata optimization
pub struct TdmsWriter {
    data_file: BufWriter<File>,
    index_file: BufWriter<File>,
    
    // Object hierarchy
    file_properties: HashMap<String, Property>,
    groups: HashMap<String, HashMap<String, Property>>,
    channels: HashMap<String, ChannelMetadata>,
    channel_buffers: HashMap<String, RawDataBuffer>,
    channel_order: Vec<String>,
    
    // State tracking
    is_first_segment: bool,
    current_segment_start: u64,
    current_index_segment_start: u64,
    file_properties_modified: bool,
    groups_modified: HashMap<String, bool>,
    
    // Cached last index for matching
    last_channel_indices: HashMap<String, RawDataIndex>,
}

impl TdmsWriter {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let data_path = path.as_ref();
        let index_path = data_path.with_extension("tdms_index");
        
        let data_file = File::create(data_path)?;
        let index_file = File::create(index_path)?;
        
        Ok(TdmsWriter {
            data_file: BufWriter::new(data_file),
            index_file: BufWriter::new(index_file),
            file_properties: HashMap::new(),
            groups: HashMap::new(),
            channels: HashMap::new(),
            channel_buffers: HashMap::new(),
            channel_order: Vec::new(),
            is_first_segment: true,
            current_segment_start: 0,
            current_index_segment_start: 0,
            file_properties_modified: false,
            groups_modified: HashMap::new(),
            last_channel_indices: HashMap::new(),
        })
    }
    
    /// Set a file-level property
    pub fn set_file_property(&mut self, name: impl Into<String>, value: PropertyValue) {
        let name = name.into();
        self.file_properties.insert(name.clone(), Property::new(name, value));
        self.file_properties_modified = true;
    }
    
    /// Set a group-level property
    pub fn set_group_property(&mut self, group: impl Into<String>, name: impl Into<String>, value: PropertyValue) {
        let group = group.into();
        let name = name.into();
        self.groups.entry(group.clone())
            .or_insert_with(HashMap::new)
            .insert(name.clone(), Property::new(name, value));
        self.groups_modified.insert(group, true);
    }
    
    /// Create or get a channel
    pub fn create_channel(&mut self, group: impl Into<String>, channel: impl Into<String>, data_type: DataType) -> Result<()> {
        let group = group.into();
        let channel = channel.into();
        let key = format!("{}/{}", group, channel);
        
        if let Some(existing) = self.channels.get(&key) {
            if existing.data_type != data_type {
                return Err(TdmsError::TypeMismatch {
                    expected: format!("{:?}", existing.data_type),
                    found: format!("{:?}", data_type),
                });
            }
            return Ok(());
        }
        
        // Ensure group exists
        self.groups.entry(group.clone()).or_insert_with(HashMap::new);
        
        let metadata = ChannelMetadata::new(group, channel, data_type);
        self.channel_buffers.insert(key.clone(), RawDataBuffer::new(data_type));
        self.channels.insert(key.clone(), metadata);
        self.channel_order.push(key);
        
        Ok(())
    }
    
    /// Set a channel property
    pub fn set_channel_property(&mut self, group: impl AsRef<str>, channel: impl AsRef<str>, 
                                 name: impl Into<String>, value: PropertyValue) -> Result<()> {
        let key = format!("{}/{}", group.as_ref(), channel.as_ref());
        let metadata = self.channels.get_mut(&key)
            .ok_or_else(|| TdmsError::ChannelNotFound(key.clone()))?;
        
        metadata.set_property(name, value);
        Ok(())
    }
    
    /// Write data to a channel (generic for fixed-size types)
    pub fn write_channel_data<T: Copy>(&mut self, group: impl AsRef<str>, channel: impl AsRef<str>, 
                                        data: &[T]) -> Result<()> {
        let key = format!("{}/{}", group.as_ref(), channel.as_ref());
        let buffer = self.channel_buffers.get_mut(&key)
            .ok_or_else(|| TdmsError::ChannelNotFound(key.clone()))?;
        
        buffer.write_slice(data)
    }
    
    /// Write string data to a channel
    pub fn write_channel_strings(&mut self, group: impl AsRef<str>, channel: impl AsRef<str>, 
                                  data: &[impl AsRef<str>]) -> Result<()> {
        let key = format!("{}/{}", group.as_ref(), channel.as_ref());
        let buffer = self.channel_buffers.get_mut(&key)
            .ok_or_else(|| TdmsError::ChannelNotFound(key.clone()))?;
        
        buffer.write_strings(data)
    }
    
    /// Write buffered data to file
    pub fn write_segment(&mut self) -> Result<()> {
        let has_metadata = self.determine_metadata_needed();
        let has_raw_data = self.channel_buffers.values().any(|b| b.value_count() > 0);
        
        if !has_metadata && !has_raw_data {
            return Ok(());
        }
        
        // Update raw data indices
        for (key, buffer) in &self.channel_buffers {
            if buffer.value_count() > 0 {
                let metadata = self.channels.get_mut(key).unwrap();
                let new_index = if buffer.data_type() == DataType::String {
                    RawDataIndex::with_size(
                        buffer.data_type(),
                        buffer.value_count(),
                        buffer.byte_len() as u64,
                    )
                } else {
                    RawDataIndex::new(buffer.data_type(), buffer.value_count())
                };
                
                // Check if index changed
                if let Some(last_index) = self.last_channel_indices.get(key) {
                    metadata.index_changed = new_index.number_of_values != last_index.number_of_values
                        || new_index.data_type as u32 != last_index.data_type as u32;
                } else {
                    metadata.index_changed = true;
                }
                
                metadata.current_index = Some(new_index);
            }
        }
        
        // If no metadata and has raw data, try to append
        if !has_metadata && has_raw_data && !self.is_first_segment {
            self.append_raw_data_only()?;
        } else {
            self.write_full_segment(has_metadata, has_raw_data)?;
        }
        
        Ok(())
    }
    
    fn determine_metadata_needed(&self) -> bool {
        if self.is_first_segment {
            return true;
        }
        
        if self.file_properties_modified {
            return true;
        }
        
        if self.groups_modified.values().any(|&modified| modified) {
            return true;
        }
        
        if self.channels.values().any(|c| c.properties_modified || c.index_changed) {
            return true;
        }
        
        false
    }
    
    fn append_raw_data_only(&mut self) -> Result<()> {
        // Calculate total raw data size
        let raw_data_size: u64 = self.channel_buffers.values()
            .map(|b| b.byte_len() as u64)
            .sum();
        
        // Update segment header in both files
        let current_pos = self.data_file.stream_position()?;
        let current_segment_size = current_pos - self.current_segment_start - SegmentHeader::LEAD_IN_SIZE as u64;
        let new_segment_size = current_segment_size + raw_data_size;
        
        // Update data file
        self.data_file.seek(SeekFrom::Start(self.current_segment_start + 12))?;
        self.data_file.write_u64::<LittleEndian>(new_segment_size)?;
        self.data_file.seek(SeekFrom::Start(current_pos))?;
        
        // Write raw data
        write_raw_data(&mut self.data_file, &self.channel_order, &self.channel_buffers)?;
        
        // Update index file
        let index_pos = self.index_file.stream_position()?;
        self.index_file.seek(SeekFrom::Start(self.current_index_segment_start + 12))?;
        self.index_file.write_u64::<LittleEndian>(new_segment_size)?;
        self.index_file.seek(SeekFrom::Start(index_pos))?;
        
        // Clear buffers
        self.clear_buffers();
        
        Ok(())
    }
    
    fn write_full_segment(&mut self, has_metadata: bool, has_raw_data: bool) -> Result<()> {
        let new_obj_list = self.is_first_segment;
        
        // Build TOC flags
        let mut toc = TocFlags::empty();
        if has_metadata {
            toc.set_metadata(true);
        }
        if has_raw_data {
            toc.set_raw_data(true);
        }
        if new_obj_list {
            toc.set_new_obj_list(true);
        }
        
        // Track segment starts
        self.current_segment_start = self.data_file.stream_position()?;
        self.current_index_segment_start = self.index_file.stream_position()?;
        
        // Write lead-ins with incomplete markers
        write_lead_in(&mut self.data_file, SegmentHeader::TDMS_TAG, toc)?;
        write_lead_in(&mut self.index_file, SegmentHeader::INDEX_TAG, toc)?;
        
        // Write metadata to both files
        let metadata_start = self.data_file.stream_position()?;
        if has_metadata || new_obj_list {
            let context = MetadataContext {
                is_first_segment: self.is_first_segment,
                file_properties_modified: self.file_properties_modified,
                file_properties: &self.file_properties,
                groups: &self.groups,
                groups_modified: &self.groups_modified,
                channels: &self.channels,
                channel_order: &self.channel_order,
                channel_buffers: &self.channel_buffers,
            };
            write_metadata(&mut self.data_file, new_obj_list, &context)?;
            write_metadata(&mut self.index_file, new_obj_list, &context)?;
        }
        let metadata_end = self.data_file.stream_position()?;
        let metadata_size = metadata_end - metadata_start;
        
        // Write raw data only to data file
        let raw_data_start = self.data_file.stream_position()?;
        if has_raw_data {
            write_raw_data(&mut self.data_file, &self.channel_order, &self.channel_buffers)?;
        }
        let raw_data_end = self.data_file.stream_position()?;
        let raw_data_size = raw_data_end - raw_data_start;
        
        let total_size = metadata_size + raw_data_size;
        
        // Update lead-ins
        update_lead_in(&mut self.data_file, self.current_segment_start, total_size, metadata_size)?;
        update_lead_in(&mut self.index_file, self.current_index_segment_start, total_size, metadata_size)?;
        
        // Clear state
        self.clear_buffers();
        self.reset_modification_flags();
        self.is_first_segment = false;
        
        Ok(())
    }
    
    fn clear_buffers(&mut self) {
        for (key, buffer) in &mut self.channel_buffers {
            if buffer.value_count() > 0 {
                // Save the index for next comparison
                if let Some(metadata) = self.channels.get(key) {
                    if let Some(index) = &metadata.current_index {
                        self.last_channel_indices.insert(key.clone(), index.clone());
                    }
                }
                buffer.clear();
            }
        }
    }
    
    fn reset_modification_flags(&mut self) {
        self.file_properties_modified = false;
        self.groups_modified.clear();
        for metadata in self.channels.values_mut() {
            metadata.reset_modification_flags();
        }
    }
    
    pub fn flush(&mut self) -> Result<()> {
        self.write_segment()?;
        self.data_file.flush()?;
        self.index_file.flush()?;
        Ok(())
    }
}

impl Drop for TdmsWriter {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}

struct MetadataContext<'a> {
    is_first_segment: bool,
    file_properties_modified: bool,
    file_properties: &'a HashMap<String, Property>,
    groups: &'a HashMap<String, HashMap<String, Property>>,
    groups_modified: &'a HashMap<String, bool>,
    channels: &'a HashMap<String, ChannelMetadata>,
    channel_order: &'a [String],
    channel_buffers: &'a HashMap<String, RawDataBuffer>,
}

fn write_lead_in<W: Write>(writer: &mut W, tag: &[u8; 4], toc: TocFlags) -> Result<()> {
    writer.write_all(tag)?;
    writer.write_u32::<LittleEndian>(toc.raw_value())?;
    writer.write_u32::<LittleEndian>(SegmentHeader::VERSION)?;
    writer.write_u64::<LittleEndian>(SegmentHeader::INCOMPLETE_MARKER)?;
    writer.write_u64::<LittleEndian>(0)?; // Metadata offset placeholder
    Ok(())
}

fn update_lead_in<W: Write + Seek>(writer: &mut W, segment_start: u64,
                                   total_size: u64, metadata_size: u64) -> Result<()> {
    let current_pos = writer.stream_position()?;
    writer.seek(SeekFrom::Start(segment_start + 12))?;
    writer.write_u64::<LittleEndian>(total_size)?;
    writer.write_u64::<LittleEndian>(metadata_size)?;
    writer.seek(SeekFrom::Start(current_pos))?;
    Ok(())
}

fn write_metadata<W: Write>(writer: &mut W, new_obj_list: bool, context: &MetadataContext) -> Result<()> {
    let mut objects_to_write = Vec::new();

    if new_obj_list {
        objects_to_write.push(ObjectPath::Root);
        for group_name in context.groups.keys() {
            objects_to_write.push(ObjectPath::Group(group_name.clone()));
        }
        for key in context.channel_order {
            if let Some(metadata) = context.channels.get(key) {
                objects_to_write.push(metadata.path.clone());
            }
        }
    } else {
        if context.file_properties_modified {
            objects_to_write.push(ObjectPath::Root);
        }
        for (group_name, modified) in context.groups_modified.iter() {
            if *modified {
                objects_to_write.push(ObjectPath::Group(group_name.clone()));
            }
        }
        for metadata in context.channels.values() {
            if metadata.properties_modified || metadata.index_changed {
                objects_to_write.push(metadata.path.clone());
            }
        }
    }

    writer.write_u32::<LittleEndian>(objects_to_write.len() as u32)?;

    for path in objects_to_write {
        write_object(writer, &path, context)?;
    }

    Ok(())
}

fn write_object<W: Write>(writer: &mut W, path: &ObjectPath, context: &MetadataContext) -> Result<()> {
    let path_str = path.to_string();
    write_string(writer, &path_str)?;

    match path {
        ObjectPath::Channel { group, channel } => {
            let key = format!("{}/{}", group, channel);
            let metadata = context.channels.get(&key).unwrap();
            let buffer = context.channel_buffers.get(&key).unwrap();

            if buffer.value_count() == 0 {
                writer.write_u32::<LittleEndian>(RawDataIndex::NO_RAW_DATA)?;
            } else if !metadata.index_changed && !context.is_first_segment {
                writer.write_u32::<LittleEndian>(RawDataIndex::MATCHES_PREVIOUS)?;
            } else {
                write_raw_data_index(writer, metadata.current_index.as_ref().unwrap())?;
            }
        }
        _ => {
            writer.write_u32::<LittleEndian>(RawDataIndex::NO_RAW_DATA)?;
        }
    }

    write_properties(writer, path, context)?;

    Ok(())
}

fn write_raw_data_index<W: Write>(writer: &mut W, index: &RawDataIndex) -> Result<()> {
    let index_length = if index.data_type == DataType::String { 24u32 } else { 16u32 };
    writer.write_u32::<LittleEndian>(index_length)?;
    writer.write_u32::<LittleEndian>(index.data_type as u32)?;
    writer.write_u32::<LittleEndian>(index.array_dimension)?;
    writer.write_u64::<LittleEndian>(index.number_of_values)?;

    if index.data_type == DataType::String {
        writer.write_u64::<LittleEndian>(index.total_size_bytes)?;
    }

    Ok(())
}

fn write_properties<W: Write>(writer: &mut W, path: &ObjectPath, context: &MetadataContext) -> Result<()> {
    let properties = match path {
        ObjectPath::Root => &context.file_properties,
        ObjectPath::Group(name) => context.groups.get(name).unwrap(),
        ObjectPath::Channel { group, channel } => {
            let key = format!("{}/{}", group, channel);
            &context.channels.get(&key).unwrap().properties
        }
    };

    writer.write_u32::<LittleEndian>(properties.len() as u32)?;

    for prop in properties.values() {
        write_string(writer, &prop.name)?;
        writer.write_u32::<LittleEndian>(prop.value.data_type() as u32)?;
        prop.value.write_to(writer)?;
    }

    Ok(())
}

fn write_raw_data<W: Write>(writer: &mut W, channel_order: &[String],
                            channel_buffers: &HashMap<String, RawDataBuffer>) -> Result<()> {
    for key in channel_order {
        if let Some(buffer) = channel_buffers.get(key) {
            if buffer.value_count() > 0 {
                writer.write_all(buffer.as_bytes())?;
            }
        }
    }
    Ok(())
}

fn write_string<W: Write>(writer: &mut W, s: &str) -> Result<()> {
    let bytes = s.as_bytes();
    writer.write_u32::<LittleEndian>(bytes.len() as u32)?;
    writer.write_all(bytes)?;
    Ok(())
}