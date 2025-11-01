// src/metadata/raw_data_index.rs
use crate::types::DataType;

/// Raw data index information for a channel
#[derive(Debug, Clone)]
pub struct RawDataIndex {
    pub data_type: DataType,
    pub array_dimension: u32,
    pub number_of_values: u64,
    pub total_size_bytes: u64,
}

impl RawDataIndex {
    pub const NO_RAW_DATA: u32 = 0xFFFFFFFF;
    pub const MATCHES_PREVIOUS: u32 = 0x00000000;
    
    pub fn new(data_type: DataType, number_of_values: u64) -> Self {
        let total_size_bytes = if let Some(size) = data_type.fixed_size() {
            number_of_values * size as u64
        } else {
            0
        };
        
        RawDataIndex {
            data_type,
            array_dimension: 1,
            number_of_values,
            total_size_bytes,
        }
    }
    
    pub fn with_size(data_type: DataType, number_of_values: u64, total_size_bytes: u64) -> Self {
        RawDataIndex {
            data_type,
            array_dimension: 1,
            number_of_values,
            total_size_bytes,
        }
    }
}