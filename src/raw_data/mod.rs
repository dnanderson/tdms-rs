// src/raw_data/mod.rs
//! Raw data handling for TDMS files
//! 
//! This module provides efficient reading and writing of raw data in TDMS format.
//! It includes:
//! 
//! - [`RawDataBuffer`] - Accumulates raw data for writing to TDMS files
//! - [`RawDataReader`] - Reads raw data from TDMS files with proper endianness handling
//! 
//! # Examples
//! 
//! ## Writing Data
//! 
//! ```
//! use tdms_rs::raw_data::RawDataBuffer;
//! use tdms_rs::types::DataType;
//! 
//! let mut buffer = RawDataBuffer::new(DataType::F64);
//! 
//! // Write individual values
//! buffer.write_f64(3.14159).unwrap();
//! buffer.write_f64(2.71828).unwrap();
//! 
//! // Or write a slice efficiently
//! let data: Vec<f64> = vec![1.0, 2.0, 3.0, 4.0, 5.0];
//! buffer.write_slice(&data).unwrap();
//! 
//! assert_eq!(buffer.value_count(), 7);
//! ```
//! 
//! ## Reading Data
//! 
//! ```no_run
//! use tdms_rs::raw_data::RawDataReader;
//! use std::io::Cursor;
//! 
//! // Simulate reading from a file
//! let data = vec![1u8, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0];
//! let mut cursor = Cursor::new(data);
//! 
//! let values: Vec<i32> = RawDataReader::read_values(&mut cursor, 3, false).unwrap();
//! assert_eq!(values, vec![1, 2, 3]);
//! ```

mod buffer;
mod reader;

pub use buffer::RawDataBuffer;
pub use reader::RawDataReader;

// Re-export for convenience
pub use buffer::*;
pub use reader::*;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::DataType;
    use std::io::Cursor;

    #[test]
    fn test_buffer_and_reader_roundtrip() {
        // Write data to buffer
        let mut buffer = RawDataBuffer::new(DataType::I32);
        let original_data = vec![10i32, 20, 30, 40, 50];
        buffer.write_slice(&original_data).unwrap();
        
        // Read data back
        let bytes = buffer.as_bytes();
        let mut cursor = Cursor::new(bytes);
        let read_data: Vec<i32> = RawDataReader::read_values(&mut cursor, 5, false).unwrap();
        
        assert_eq!(original_data, read_data);
    }

    #[test]
    fn test_string_roundtrip() {
        // Write strings to buffer
        let mut buffer = RawDataBuffer::new(DataType::String);
        let original_strings = vec!["Hello", "World", "TDMS", "Rust"];
        buffer.write_strings(&original_strings).unwrap();
        
        // Read strings back
        let bytes = buffer.as_bytes();
        let mut cursor = Cursor::new(bytes);
        let read_strings = RawDataReader::read_strings(&mut cursor, 4, false).unwrap();
        
        assert_eq!(original_strings, read_strings);
    }

    #[test]
    fn test_float_roundtrip() {
        // Write floats
        let mut buffer = RawDataBuffer::new(DataType::DoubleFloat);
        let original_data = vec![3.14159, 2.71828, 1.41421, 1.73205];
        buffer.write_slice(&original_data).unwrap();
        
        // Read floats back
        let bytes = buffer.as_bytes();
        let mut cursor = Cursor::new(bytes);
        let read_data: Vec<f64> = RawDataReader::read_values(&mut cursor, 4, false).unwrap();
        
        for (original, read) in original_data.iter().zip(read_data.iter()) {
            assert!((original - read).abs() < 1e-10);
        }
    }

    #[test]
    fn test_empty_buffer() {
        let buffer = RawDataBuffer::new(DataType::I32);
        assert_eq!(buffer.value_count(), 0);
        assert_eq!(buffer.byte_len(), 0);
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_multiple_write_operations() {
        let mut buffer = RawDataBuffer::new(DataType::I32);
        
        // Multiple write operations
        buffer.write_i32(1).unwrap();
        buffer.write_i32(2).unwrap();
        buffer.write_i32(3).unwrap();
        
        let slice_data = vec![4i32, 5, 6];
        buffer.write_slice(&slice_data).unwrap();
        
        assert_eq!(buffer.value_count(), 6);
        assert_eq!(buffer.byte_len(), 24);
        
        // Read back all data
        let bytes = buffer.as_bytes();
        let mut cursor = Cursor::new(bytes);
        let read_data: Vec<i32> = RawDataReader::read_values(&mut cursor, 6, false).unwrap();
        
        assert_eq!(read_data, vec![1, 2, 3, 4, 5, 6]);
    }
}