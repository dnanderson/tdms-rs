// src/metadata/channel_metadata.rs
use crate::types::{Property, PropertyValue, DataType};
use crate::metadata::{ObjectPath, RawDataIndex};
use std::collections::HashMap;

/// Channel metadata tracking for TDMS channels
/// 
/// This structure maintains all metadata associated with a channel including:
/// - Object path (group and channel name)
/// - Data type
/// - Properties (metadata key-value pairs)
/// - Current raw data index information
/// - Modification tracking flags
#[derive(Debug, Clone)]
pub struct ChannelMetadata {
    /// The path identifying this channel in the TDMS hierarchy
    pub path: ObjectPath,
    
    /// The data type of values stored in this channel
    pub data_type: DataType,
    
    /// Properties attached to this channel
    pub properties: HashMap<String, Property>,
    
    /// Current raw data index for this channel (if data has been written)
    pub current_index: Option<RawDataIndex>,
    
    /// Flag indicating if properties have been modified since last write
    pub properties_modified: bool,
    
    /// Flag indicating if the raw data index has changed since last write
    pub index_changed: bool,
}

impl ChannelMetadata {
    /// Create new channel metadata
    /// 
    /// # Arguments
    /// 
    /// * `group` - The group name this channel belongs to
    /// * `channel` - The channel name
    /// * `data_type` - The data type for this channel
    /// 
    /// # Example
    /// 
    /// ```
    /// use tdms_rs::metadata::ChannelMetadata;
    /// use tdms_rs::types::DataType;
    /// 
    /// let metadata = ChannelMetadata::new("Sensors", "Temperature", DataType::DoubleFloat);
    /// ```
    pub fn new(group: impl Into<String>, channel: impl Into<String>, data_type: DataType) -> Self {
        ChannelMetadata {
            path: ObjectPath::Channel {
                group: group.into(),
                channel: channel.into(),
            },
            data_type,
            properties: HashMap::new(),
            current_index: None,
            properties_modified: false,
            index_changed: false,
        }
    }
    
    /// Set a property on this channel
    /// 
    /// If the property already exists and the value is unchanged, the modification
    /// flag is not set. This optimization allows for efficient incremental metadata writes.
    /// 
    /// # Arguments
    /// 
    /// * `name` - The property name
    /// * `value` - The property value
    /// 
    /// # Example
    /// 
    /// ```
    /// use tdms_rs::metadata::ChannelMetadata;
    /// use tdms_rs::types::{DataType, PropertyValue};
    /// 
    /// let mut metadata = ChannelMetadata::new("Group", "Channel", DataType::I32);
    /// metadata.set_property("unit", PropertyValue::String("Volts".into()));
    /// metadata.set_property("scale", PropertyValue::Double(1.0));
    /// ```
    pub fn set_property(&mut self, name: impl Into<String>, value: PropertyValue) {
        let name = name.into();
        let new_prop = Property::new(name.clone(), value);
        
        // Only mark as modified if the value actually changed
        if let Some(existing) = self.properties.get(&name) {
            // Check if the variant type changed (discriminant comparison)
            if std::mem::discriminant(&existing.value) != std::mem::discriminant(&new_prop.value) {
                self.properties_modified = true;
            } else {
                // For same variant types, check actual value
                match (&existing.value, &new_prop.value) {
                    (PropertyValue::I8(a), PropertyValue::I8(b)) => {
                        if a != b { self.properties_modified = true; }
                    }
                    (PropertyValue::I16(a), PropertyValue::I16(b)) => {
                        if a != b { self.properties_modified = true; }
                    }
                    (PropertyValue::I32(a), PropertyValue::I32(b)) => {
                        if a != b { self.properties_modified = true; }
                    }
                    (PropertyValue::I64(a), PropertyValue::I64(b)) => {
                        if a != b { self.properties_modified = true; }
                    }
                    (PropertyValue::U8(a), PropertyValue::U8(b)) => {
                        if a != b { self.properties_modified = true; }
                    }
                    (PropertyValue::U16(a), PropertyValue::U16(b)) => {
                        if a != b { self.properties_modified = true; }
                    }
                    (PropertyValue::U32(a), PropertyValue::U32(b)) => {
                        if a != b { self.properties_modified = true; }
                    }
                    (PropertyValue::U64(a), PropertyValue::U64(b)) => {
                        if a != b { self.properties_modified = true; }
                    }
                    (PropertyValue::Float(a), PropertyValue::Float(b)) => {
                        if a != b { self.properties_modified = true; }
                    }
                    (PropertyValue::Double(a), PropertyValue::Double(b)) => {
                        if a != b { self.properties_modified = true; }
                    }
                    (PropertyValue::Boolean(a), PropertyValue::Boolean(b)) => {
                        if a != b { self.properties_modified = true; }
                    }
                    (PropertyValue::String(a), PropertyValue::String(b)) => {
                        if a != b { self.properties_modified = true; }
                    }
                    (PropertyValue::Timestamp(a), PropertyValue::Timestamp(b)) => {
                        if a != b { self.properties_modified = true; }
                    }
                    _ => {
                        // Type changed, mark as modified
                        self.properties_modified = true;
                    }
                }
            }
        } else {
            // New property
            self.properties_modified = true;
        }
        
        self.properties.insert(name, new_prop);
    }
    
    /// Get a property by name
    /// 
    /// # Arguments
    /// 
    /// * `name` - The property name to retrieve
    /// 
    /// # Returns
    /// 
    /// An optional reference to the property if it exists
    pub fn get_property(&self, name: &str) -> Option<&Property> {
        self.properties.get(name)
    }
    
    /// Remove a property by name
    /// 
    /// # Arguments
    /// 
    /// * `name` - The property name to remove
    /// 
    /// # Returns
    /// 
    /// `true` if the property was removed, `false` if it didn't exist
    pub fn remove_property(&mut self, name: &str) -> bool {
        if self.properties.remove(name).is_some() {
            self.properties_modified = true;
            true
        } else {
            false
        }
    }
    
    /// Clear all properties
    pub fn clear_properties(&mut self) {
        if !self.properties.is_empty() {
            self.properties.clear();
            self.properties_modified = true;
        }
    }
    
    /// Get the number of properties
    pub fn property_count(&self) -> usize {
        self.properties.len()
    }
    
    /// Check if a property exists
    pub fn has_property(&self, name: &str) -> bool {
        self.properties.contains_key(name)
    }
    
    /// Set the raw data index for this channel
    /// 
    /// This should be called when preparing to write data to update the index information.
    /// The method automatically sets the `index_changed` flag if the index differs from
    /// the previous one.
    /// 
    /// # Arguments
    /// 
    /// * `index` - The new raw data index
    pub fn set_raw_data_index(&mut self, index: RawDataIndex) {
        // Check if index actually changed
        if let Some(current) = &self.current_index {
            self.index_changed = current.number_of_values != index.number_of_values
                || current.data_type as u32 != index.data_type as u32
                || current.total_size_bytes != index.total_size_bytes;
        } else {
            self.index_changed = true;
        }
        
        self.current_index = Some(index);
    }
    
    /// Reset all modification flags
    /// 
    /// This should be called after successfully writing metadata to disk.
    pub fn reset_modification_flags(&mut self) {
        self.properties_modified = false;
        self.index_changed = false;
    }
    
    /// Check if this channel needs metadata to be written
    /// 
    /// # Returns
    /// 
    /// `true` if either properties or index have been modified
    pub fn needs_metadata_write(&self) -> bool {
        self.properties_modified || self.index_changed
    }
    
    /// Get the group name from the path
    pub fn group_name(&self) -> Option<&str> {
        match &self.path {
            ObjectPath::Channel { group, .. } => Some(group.as_str()),
            _ => None,
        }
    }
    
    /// Get the channel name from the path
    pub fn channel_name(&self) -> Option<&str> {
        match &self.path {
            ObjectPath::Channel { channel, .. } => Some(channel.as_str()),
            _ => None,
        }
    }
    
    /// Get the full path as a string
    pub fn path_string(&self) -> String {
        self.path.to_string()
    }
    
    /// Set waveform properties (convenience method for LabVIEW waveforms)
    /// 
    /// TDMS uses specific property names to represent waveform attributes.
    /// 
    /// # Arguments
    /// 
    /// * `start_time` - The timestamp when the waveform was acquired
    /// * `increment` - The time increment between samples (dt)
    /// * `samples` - The number of samples in the waveform
    pub fn set_waveform_properties(
        &mut self,
        start_time: crate::types::Timestamp,
        increment: f64,
        samples: u64,
    ) {
        self.set_property("wf_start_time", PropertyValue::Timestamp(start_time));
        self.set_property("wf_increment", PropertyValue::Double(increment));
        self.set_property("wf_samples", PropertyValue::U64(samples));
    }
    
    /// Set the unit string for this channel (convenience method)
    /// 
    /// # Arguments
    /// 
    /// * `unit` - The unit string (e.g., "V", "A", "Hz")
    pub fn set_unit(&mut self, unit: impl Into<String>) {
        self.set_property("unit_string", PropertyValue::String(unit.into()));
    }
    
    /// Get the unit string if set
    pub fn get_unit(&self) -> Option<&str> {
        self.get_property("unit_string").and_then(|p| {
            match &p.value {
                PropertyValue::String(s) => Some(s.as_str()),
                _ => None,
            }
        })
    }
}

impl Default for ChannelMetadata {
    fn default() -> Self {
        ChannelMetadata {
            path: ObjectPath::Root,
            data_type: DataType::Void,
            properties: HashMap::new(),
            current_index: None,
            properties_modified: false,
            index_changed: false,
        }
    }
}

// Implement PartialEq for testing purposes
impl PartialEq for ChannelMetadata {
    fn eq(&self, other: &Self) -> bool {
        self.path == other.path
            && self.data_type == other.data_type
            && self.properties.len() == other.properties.len()
            && self.current_index.is_some() == other.current_index.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_channel_metadata() {
        let metadata = ChannelMetadata::new("Group1", "Channel1", DataType::I32);
        
        assert_eq!(metadata.group_name(), Some("Group1"));
        assert_eq!(metadata.channel_name(), Some("Channel1"));
        assert_eq!(metadata.data_type, DataType::I32);
        assert_eq!(metadata.property_count(), 0);
        assert!(!metadata.properties_modified);
        assert!(!metadata.index_changed);
    }

    #[test]
    fn test_set_property() {
        let mut metadata = ChannelMetadata::new("Group1", "Channel1", DataType::F64);
        
        assert!(!metadata.properties_modified);
        
        metadata.set_property("unit", PropertyValue::String("Volts".into()));
        assert!(metadata.properties_modified);
        assert_eq!(metadata.property_count(), 1);
        
        // Reset flag
        metadata.properties_modified = false;
        
        // Setting same value should not mark as modified
        metadata.set_property("unit", PropertyValue::String("Volts".into()));
        assert!(!metadata.properties_modified);
        
        // Setting different value should mark as modified
        metadata.set_property("unit", PropertyValue::String("Amps".into()));
        assert!(metadata.properties_modified);
    }

    #[test]
    fn test_get_property() {
        let mut metadata = ChannelMetadata::new("Group1", "Channel1", DataType::F64);
        
        metadata.set_property("scale", PropertyValue::Double(2.5));
        
        let prop = metadata.get_property("scale").unwrap();
        match &prop.value {
            PropertyValue::Double(v) => assert_eq!(*v, 2.5),
            _ => panic!("Expected Double property"),
        }
        
        assert!(metadata.get_property("nonexistent").is_none());
    }

    #[test]
    fn test_remove_property() {
        let mut metadata = ChannelMetadata::new("Group1", "Channel1", DataType::F64);
        
        metadata.set_property("test", PropertyValue::I32(42));
        metadata.properties_modified = false;
        
        assert!(metadata.remove_property("test"));
        assert!(metadata.properties_modified);
        assert_eq!(metadata.property_count(), 0);
        
        metadata.properties_modified = false;
        assert!(!metadata.remove_property("nonexistent"));
        assert!(!metadata.properties_modified);
    }

    #[test]
    fn test_raw_data_index() {
        let mut metadata = ChannelMetadata::new("Group1", "Channel1", DataType::I32);
        
        let index1 = RawDataIndex::new(DataType::I32, 1000);
        metadata.set_raw_data_index(index1);
        assert!(metadata.index_changed);
        
        // Reset flag
        metadata.index_changed = false;
        
        // Same index should not trigger change
        let index2 = RawDataIndex::new(DataType::I32, 1000);
        metadata.set_raw_data_index(index2);
        assert!(!metadata.index_changed);
        
        // Different index should trigger change
        let index3 = RawDataIndex::new(DataType::I32, 2000);
        metadata.set_raw_data_index(index3);
        assert!(metadata.index_changed);
    }

    #[test]
    fn test_reset_modification_flags() {
        let mut metadata = ChannelMetadata::new("Group1", "Channel1", DataType::F64);
        
        metadata.set_property("test", PropertyValue::I32(42));
        metadata.set_raw_data_index(RawDataIndex::new(DataType::F64, 100));
        
        assert!(metadata.properties_modified);
        assert!(metadata.index_changed);
        assert!(metadata.needs_metadata_write());
        
        metadata.reset_modification_flags();
        
        assert!(!metadata.properties_modified);
        assert!(!metadata.index_changed);
        assert!(!metadata.needs_metadata_write());
    }

    #[test]
    fn test_waveform_properties() {
        let mut metadata = ChannelMetadata::new("Waveforms", "Signal1", DataType::F64);
        
        let timestamp = crate::types::Timestamp::now();
        metadata.set_waveform_properties(timestamp, 0.001, 1000);
        
        assert_eq!(metadata.property_count(), 3);
        assert!(metadata.has_property("wf_start_time"));
        assert!(metadata.has_property("wf_increment"));
        assert!(metadata.has_property("wf_samples"));
    }

    #[test]
    fn test_unit_convenience_methods() {
        let mut metadata = ChannelMetadata::new("Sensors", "Temp", DataType::F64);
        
        metadata.set_unit("°C");
        assert_eq!(metadata.get_unit(), Some("°C"));
        
        metadata.set_unit("°F");
        assert_eq!(metadata.get_unit(), Some("°F"));
    }

    #[test]
    fn test_path_string() {
        let metadata = ChannelMetadata::new("Group1", "Channel1", DataType::I32);
        let path_str = metadata.path_string();
        
        assert!(path_str.contains("Group1"));
        assert!(path_str.contains("Channel1"));
    }

    #[test]
    fn test_default() {
        let metadata = ChannelMetadata::default();
        
        assert_eq!(metadata.path, ObjectPath::Root);
        assert_eq!(metadata.data_type, DataType::Void);
        assert_eq!(metadata.property_count(), 0);
    }
}