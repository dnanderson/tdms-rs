// src/writer/sync_writer.rs
use crate::error::{TdmsError, Result};
use crate::types::{DataType, TocFlags, Property, PropertyValue};
use crate::metadata::{ObjectPath, ChannelMetadata, RawDataIndex};
use crate::segment::SegmentHeader;
use crate::raw_data::RawDataBuffer;
use std::collections::{HashMap, HashSet};
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
    channels: HashMap<ObjectPath, ChannelMetadata>,
    channel_buffers: HashMap<ObjectPath, RawDataBuffer>,
    channel_order: Vec<ObjectPath>,
    
    // State tracking
    is_first_segment: bool,
    current_segment_start: u64,
    current_index_segment_start: u64,
    file_properties_modified: bool,
    groups_modified: HashMap<String, bool>,
    
    // Cached last index for matching
    last_channel_indices: HashMap<ObjectPath, RawDataIndex>,

    // Track the channels written in the last segment to detect
    // changes in the active channel list.
    last_written_channels: Vec<ObjectPath>,
    
    // Track whether the current segment has raw data
    // (cannot append raw data to a metadata-only segment)
    current_segment_has_raw_data: bool,
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
            last_written_channels: Vec::new(),
            current_segment_has_raw_data: false,
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
        let path = ObjectPath::Channel { group, channel };
        
        if let Some(existing) = self.channels.get(&path) {
            if existing.data_type != data_type {
                return Err(TdmsError::TypeMismatch {
                    expected: format!("{:?}", existing.data_type),
                    found: format!("{:?}", data_type),
                });
            }
            return Ok(());
        }
        
        // Ensure group exists
        if let ObjectPath::Channel { group, .. } = &path {
            self.groups.entry(group.clone()).or_insert_with(HashMap::new);
        }
        
        let metadata = ChannelMetadata::new(path.group().unwrap().to_string(), path.channel().unwrap().to_string(), data_type);
        self.channel_buffers.insert(path.clone(), RawDataBuffer::new(data_type));
        self.channels.insert(path.clone(), metadata);
        self.channel_order.push(path);
        
        Ok(())
    }
    
    /// Set a channel property
    pub fn set_channel_property(&mut self, group: impl AsRef<str>, channel: impl AsRef<str>, 
                                 name: impl Into<String>, value: PropertyValue) -> Result<()> {
        let path = ObjectPath::Channel { group: group.as_ref().to_string(), channel: channel.as_ref().to_string() };
        let metadata = self.channels.get_mut(&path)
            .ok_or_else(|| TdmsError::ChannelNotFound(path.to_string()))?;
        
        metadata.set_property(name, value);
        Ok(())
    }
    
    /// Write data to a channel (generic for fixed-size types)
    pub fn write_channel_data<T: Copy>(&mut self, group: impl AsRef<str>, channel: impl AsRef<str>, 
                                        data: &[T]) -> Result<()> {
        let path = ObjectPath::Channel { group: group.as_ref().to_string(), channel: channel.as_ref().to_string() };
        let buffer = self.channel_buffers.get_mut(&path)
            .ok_or_else(|| TdmsError::ChannelNotFound(path.to_string()))?;
        
        buffer.write_slice(data)
    }
    
    /// Write string data to a channel
    pub fn write_channel_strings(&mut self, group: impl AsRef<str>, channel: impl AsRef<str>, 
                                  data: &[impl AsRef<str>]) -> Result<()> {
        let path = ObjectPath::Channel { group: group.as_ref().to_string(), channel: channel.as_ref().to_string() };
        let buffer = self.channel_buffers.get_mut(&path)
            .ok_or_else(|| TdmsError::ChannelNotFound(path.to_string()))?;
        
        buffer.write_strings(data)
    }
    
    /// Write buffered data to file
    pub fn write_segment(&mut self) -> Result<()> {
        let has_raw_data = self.channel_buffers.values().any(|b| b.value_count() > 0);
        let has_property_changes = self.determine_property_changes();
        
        if !has_raw_data && !has_property_changes {
            // Nothing to write at all
            return Ok(());
        }
        
        // Get the list of channels we are *actually* writing data for in this pass
        let current_written_channels: Vec<ObjectPath> = self.channel_order.iter()
            .filter(|path| {
                self.channel_buffers.get(*path)
                    .map_or(false, |b| b.value_count() > 0)
            })
            .cloned()
            .collect();
        
        // A new object list is required if it's the first segment,
        // OR if the list of channels we are writing data for has changed.
        let new_obj_list_required = self.is_first_segment || 
            (has_raw_data && self.last_written_channels != current_written_channels);

        // Update raw data indices and check if any have changed
        let mut has_index_changes = false;
        for (path, buffer) in &self.channel_buffers {
            if buffer.value_count() > 0 {
                let metadata = self.channels.get_mut(path).unwrap();
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
                if buffer.data_type() == DataType::String {
                    metadata.index_changed = true;
                } else if let Some(last_index) = self.last_channel_indices.get(path) {
                    metadata.index_changed = new_index.number_of_values != last_index.number_of_values
                        || new_index.data_type as u32 != last_index.data_type as u32;
                } else {
                    metadata.index_changed = true;
                }

                if metadata.index_changed {
                    has_index_changes = true;
                }
                
                metadata.current_index = Some(new_index);
            }
        }
        
        let has_metadata_to_write = has_property_changes || has_index_changes || new_obj_list_required;
        
        if has_raw_data && !has_metadata_to_write && self.current_segment_has_raw_data {
            self.append_raw_data_only(&current_written_channels)?;
        } else {
            self.write_full_segment(has_raw_data, new_obj_list_required, &current_written_channels)?;
            
            if has_raw_data || new_obj_list_required {
                self.last_written_channels = current_written_channels;
            }
            
            self.current_segment_has_raw_data = has_raw_data;
        }
        
        // Clear buffers and reset flags for next pass
        self.clear_buffers();
        self.reset_modification_flags();
        self.is_first_segment = false;

        Ok(())
    }
    
    fn determine_property_changes(&self) -> bool {
        self.is_first_segment
            || self.file_properties_modified
            || self.groups_modified.values().any(|&modified| modified)
            || self.channels.values().any(|c| c.properties_modified)
    }
    
    fn append_raw_data_only(&mut self, current_written_channels: &[ObjectPath]) -> Result<()> {
        // Calculate total raw data size
        let raw_data_size: u64 = current_written_channels.iter()
            .map(|path| self.channel_buffers.get(path).map_or(0, |b| b.byte_len() as u64))
            .sum();
        
        // Update segment header in both files
        let current_pos = self.data_file.stream_position()?;
        // This is safe because is_first_segment is false
        let current_segment_size = current_pos - self.current_segment_start - SegmentHeader::LEAD_IN_SIZE as u64;
        let new_segment_size = current_segment_size + raw_data_size;
        
        // Update data file
        self.data_file.seek(SeekFrom::Start(self.current_segment_start + 12))?;
        self.data_file.write_u64::<LittleEndian>(new_segment_size)?;
        self.data_file.seek(SeekFrom::Start(current_pos))?;
        
        // Write raw data
        write_raw_data(&mut self.data_file, current_written_channels, &self.channel_buffers)?;
        
        // Update index file
        let index_pos = self.index_file.stream_position()?;
        self.index_file.seek(SeekFrom::Start(self.current_index_segment_start + 12))?;
        self.index_file.write_u64::<LittleEndian>(new_segment_size)?;
        self.index_file.seek(SeekFrom::Start(index_pos))?;
        
        Ok(())
    }
    
    fn write_full_segment(&mut self, has_raw_data: bool, new_obj_list: bool, current_written_channels: &[ObjectPath]) -> Result<()> {
        
        let mut toc = TocFlags::empty();
        
        let has_index_changes = self.channels.values().any(|c| c.index_changed);
        if self.determine_property_changes() || has_index_changes || new_obj_list || has_raw_data {
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
        if toc.has_metadata() {
            let context = MetadataContext {
                is_first_segment: self.is_first_segment,
                file_properties_modified: self.file_properties_modified,
                file_properties: &self.file_properties,
                groups: &self.groups,
                groups_modified: &self.groups_modified,
                channels: &self.channels,
                active_channels_for_segment: current_written_channels,
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
            write_raw_data(&mut self.data_file, current_written_channels, &self.channel_buffers)?;
        }
        let raw_data_end = self.data_file.stream_position()?;
        let raw_data_size = raw_data_end - raw_data_start;
        
        let total_size = metadata_size + raw_data_size;
        
        // Update lead-ins
        update_lead_in(&mut self.data_file, self.current_segment_start, total_size, metadata_size)?;
        update_lead_in(&mut self.index_file, self.current_index_segment_start, total_size, metadata_size)?;
        
        Ok(())
    }
    
    fn clear_buffers(&mut self) {
        for (path, buffer) in &mut self.channel_buffers {
            if buffer.value_count() > 0 {
                if let Some(metadata) = self.channels.get(path) {
                    if let Some(index) = &metadata.current_index {
                        self.last_channel_indices.insert(path.clone(), index.clone());
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

    /// Returns the current size of the data file on disk.
    pub fn file_size(&mut self) -> Result<u64> {
        self.flush()?;
        let file = self.data_file.get_ref();
        Ok(file.metadata()?.len())
    }

    /// Resets the writer to use a new file, carrying over all metadata.
    pub fn reset_for_new_file(&mut self, path: impl AsRef<Path>) -> Result<()> {
        self.flush()?;

        let data_path = path.as_ref();
        let index_path = data_path.with_extension("tdms_index");

        let data_file = File::create(data_path)?;
        let index_file = File::create(index_path)?;

        self.data_file = BufWriter::new(data_file);
        self.index_file = BufWriter::new(index_file);

        self.is_first_segment = true;
        self.current_segment_start = 0;
        self.current_index_segment_start = 0;

        self.file_properties_modified = true;
        for group_name in self.groups.keys() {
            self.groups_modified.insert(group_name.clone(), true);
        }
        for channel_metadata in self.channels.values_mut() {
            channel_metadata.properties_modified = true;
            channel_metadata.index_changed = true;
        }

        self.last_channel_indices.clear();
        self.last_written_channels.clear();
        self.current_segment_has_raw_data = false;

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
    channels: &'a HashMap<ObjectPath, ChannelMetadata>,
    active_channels_for_segment: &'a [ObjectPath],
    channel_buffers: &'a HashMap<ObjectPath, RawDataBuffer>,
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

        let mut active_groups = HashSet::new();
        for group_name in context.groups.keys() {
            active_groups.insert(group_name.as_str());
        }
        for path in context.active_channels_for_segment {
            if let Some(group_name) = path.group() {
                active_groups.insert(group_name);
            }
        }
        
        for group_name in active_groups {
            objects_to_write.push(ObjectPath::Group(group_name.to_string()));
        }
        
        objects_to_write.extend(context.active_channels_for_segment.iter().cloned());
    } else {
        if context.file_properties_modified {
            objects_to_write.push(ObjectPath::Root);
        }
        for (group_name, modified) in context.groups_modified.iter() {
            if *modified {
                objects_to_write.push(ObjectPath::Group(group_name.clone()));
            }
        }
        
        let mut channels_to_write = HashSet::new();
        
        for metadata in context.channels.values() {
            if metadata.properties_modified {
                channels_to_write.insert(metadata.path.clone());
            }
        }
        
        for path in context.active_channels_for_segment {
            channels_to_write.insert(path.clone());
        }
        
        objects_to_write.extend(channels_to_write);
    }

    writer.write_u32::<LittleEndian>(objects_to_write.len() as u32)?;

    for path in objects_to_write {
        write_object(writer, &path, context)?;
    }

    Ok(())
}

fn write_object<W: Write>(writer: &mut W, path: &ObjectPath, context: &MetadataContext) -> Result<()> {
    write_string(writer, &path.to_string())?;

    match path {
        ObjectPath::Channel { .. } => {
            let metadata = context.channels.get(path).unwrap();

            if let Some(buffer) = context.channel_buffers.get(path) {
                if buffer.value_count() > 0 {
                    if !metadata.index_changed && !context.is_first_segment {
                        writer.write_u32::<LittleEndian>(RawDataIndex::MATCHES_PREVIOUS)?;
                    } else {
                        write_raw_data_index(writer, metadata.current_index.as_ref().unwrap())?;
                    }
                } else {
                    writer.write_u32::<LittleEndian>(RawDataIndex::NO_RAW_DATA)?;
                }
            } else {
                writer.write_u32::<LittleEndian>(RawDataIndex::NO_RAW_DATA)?;
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
    let empty_properties = HashMap::new();
    
    let properties = match path {
        ObjectPath::Root => &context.file_properties,
        ObjectPath::Group(name) => context.groups.get(name).unwrap_or(&empty_properties),
        ObjectPath::Channel { .. } => &context.channels.get(path).unwrap().properties,
    };

    writer.write_u32::<LittleEndian>(properties.len() as u32)?;

    for prop in properties.values() {
        write_string(writer, &prop.name)?;
        writer.write_u32::<LittleEndian>(prop.value.data_type() as u32)?;
        prop.value.write_to(writer)?;
    }

    Ok(())
}

fn write_raw_data<W: Write>(writer: &mut W, channel_order: &[ObjectPath],
                            channel_buffers: &HashMap<ObjectPath, RawDataBuffer>) -> Result<()> {
    for path in channel_order {
        if let Some(buffer) = channel_buffers.get(path) {
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