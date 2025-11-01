// src/lib.rs
//! # tdms-rs
//!
//! A high-performance Rust library for reading and writing TDMS (Technical Data Management Streaming) files,
//! the native file format for National Instruments LabVIEW and other NI software.
//!
//! ## Features
//!
//! - 🚀 **High Performance**: Zero-copy operations, memory pooling, and buffered I/O
//! - 🔒 **Thread-Safe**: Concurrent multi-threaded writing with async support
//! - ✅ **Spec Compliant**: Full TDMS 2.0 specification support
//! - 📦 **Memory Efficient**: Streaming reads for large files
//! - 🎯 **Type Safe**: Strong typing with compile-time guarantees
//! - ⚡ **Incremental Metadata**: Optimized file size through metadata reuse
//!
//! ## Quick Start
//!
//! ### Writing TDMS Files
//!
//! ```rust,no_run
//! use tdms_rs::*;
//!
//! fn main() -> Result<()> {
//!     let mut writer = TdmsWriter::create("output.tdms")?;
//!     
//!     // Set file properties
//!     writer.set_file_property("title", PropertyValue::String("My Data".into()));
//!     
//!     // Create a channel
//!     writer.create_channel("Group1", "Voltage", DataType::DoubleFloat)?;
//!     
//!     // Write data
//!     let data: Vec<f64> = (0..1000).map(|i| (i as f64 * 0.1).sin()).collect();
//!     writer.write_channel_data("Group1", "Voltage", &data)?;
//!     
//!     writer.flush()?;
//!     Ok(())
//! }
//! ```
//!
//! ### Reading TDMS Files
//!
//! ```rust,no_run
//! use tdms_rs::*;
//!
//! fn main() -> Result<()> {
//!     let mut reader = TdmsReader::open("input.tdms")?;
//!     
//!     // List channels
//!     for channel in reader.list_channels() {
//!         println!("Channel: {}", channel);
//!     }
//!     
//!     // Read data
//!     let data: Vec<f64> = reader.read_channel_data("Group1", "Voltage")?;
//!     println!("Read {} values", data.len());
//!     
//!     Ok(())
//! }
//! ```
//!
//! ### Async Writing
//!
//! ```rust,no_run
//! # #[cfg(feature = "async")]
//! use tdms_rs::*;
//!
//! # #[cfg(feature = "async")]
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     let writer = AsyncTdmsWriter::create("async_output.tdms").await?;
//!     
//!     // Write from multiple tasks
//!     let data: Vec<f64> = vec![1.0; 1000];
//!     writer.write_channel_data(
//!         "Sensors",
//!         "Sensor1",
//!         data,
//!         DataType::DoubleFloat
//!     ).await?;
//!     
//!     writer.close().await?;
//!     Ok(())
//! }
//! ```

// Modules
pub mod error;
pub mod types;
pub mod metadata;
pub mod segment;
pub mod raw_data;
pub mod writer;
pub mod reader;

mod utils;

// Re-export commonly used types at the crate root for convenience
pub use error::{TdmsError, Result};

// Type exports
pub use types::{
    DataType,
    TocFlags,
    Timestamp,
    Property,
    PropertyValue,
};

// Metadata exports
pub use metadata::{
    ObjectPath,
    RawDataIndex,
    ChannelMetadata,
};

// Segment exports
pub use segment::{
    Segment,
    SegmentHeader,
    SegmentInfo,
};

// Raw data exports
pub use raw_data::{
    RawDataBuffer,
    RawDataReader,
};

// Writer exports
pub use writer::TdmsWriter;

#[cfg(feature = "async")]
pub use writer::AsyncTdmsWriter;

// Reader exports
pub use reader::{
    TdmsReader,
    ChannelReader,
    StreamingReader,
};

// Prelude module for glob imports
pub mod prelude {
    //! Convenient imports for common use cases.
    //! 
    //! ```rust
    //! use tdms_rs::prelude::*;
    //! ```
    
    pub use crate::error::{TdmsError, Result};
    pub use crate::types::{DataType, PropertyValue, Timestamp};
    pub use crate::writer::TdmsWriter;
    pub use crate::reader::TdmsReader;
    
    #[cfg(feature = "async")]
    pub use crate::writer::AsyncTdmsWriter;
}

// Version information
/// The version of the TDMS specification this library implements
pub const TDMS_VERSION: u32 = 4713;

/// The library version
pub const LIBRARY_VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_constants() {
        assert_eq!(TDMS_VERSION, 4713);
        assert!(!LIBRARY_VERSION.is_empty());
    }

    #[test]
    fn test_data_type_sizes() {
        assert_eq!(DataType::I8.fixed_size(), Some(1));
        assert_eq!(DataType::I16.fixed_size(), Some(2));
        assert_eq!(DataType::I32.fixed_size(), Some(4));
        assert_eq!(DataType::I64.fixed_size(), Some(8));
        assert_eq!(DataType::F64.fixed_size(), Some(8));
        assert_eq!(DataType::TimeStamp.fixed_size(), Some(16));
        assert_eq!(DataType::String.fixed_size(), None);
    }

    #[test]
    fn test_toc_flags() {
        let mut toc = TocFlags::empty();
        assert!(!toc.has_metadata());
        assert!(!toc.has_raw_data());
        
        toc.set_metadata(true);
        assert!(toc.has_metadata());
        
        toc.set_raw_data(true);
        assert!(toc.has_raw_data());
        
        toc.set_metadata(false);
        assert!(!toc.has_metadata());
        assert!(toc.has_raw_data());
    }

    #[test]
    fn test_timestamp_creation() {
        let ts = Timestamp::now();
        assert!(ts.seconds > 0);
    }

    #[test]
    fn test_object_path_formatting() {
        let root = ObjectPath::Root;
        assert_eq!(root.to_string(), "/");
        
        let group = ObjectPath::Group("MyGroup".to_string());
        assert_eq!(group.to_string(), "/'MyGroup''");
        
        let channel = ObjectPath::Channel {
            group: "Group1".to_string(),
            channel: "Channel1".to_string(),
        };
        assert_eq!(channel.to_string(), "/'Group1'/''Channel1''");
    }

    #[test]
    fn test_object_path_parsing() {
        let root = ObjectPath::from_string("/").unwrap();
        assert_eq!(root, ObjectPath::Root);
        
        let group = ObjectPath::from_string("/'MyGroup'").unwrap();
        match group {
            ObjectPath::Group(name) => assert_eq!(name, "MyGroup"),
            _ => panic!("Expected Group"),
        }
    }

    #[test]
    fn test_property_value_types() {
        let int_prop = PropertyValue::I32(42);
        assert_eq!(int_prop.data_type(), DataType::I32);
        
        let float_prop = PropertyValue::Double(3.14);
        assert_eq!(float_prop.data_type(), DataType::DoubleFloat);
        
        let str_prop = PropertyValue::String("test".to_string());
        assert_eq!(str_prop.data_type(), DataType::String);
        
        let bool_prop = PropertyValue::Boolean(true);
        assert_eq!(bool_prop.data_type(), DataType::Boolean);
    }

    #[test]
    fn test_raw_data_index_creation() {
        let index = RawDataIndex::new(DataType::I32, 1000);
        assert_eq!(index.data_type, DataType::I32);
        assert_eq!(index.number_of_values, 1000);
        assert_eq!(index.array_dimension, 1);
        assert_eq!(index.total_size_bytes, 4000); // 1000 * 4 bytes
        
        let float_index = RawDataIndex::new(DataType::DoubleFloat, 500);
        assert_eq!(float_index.total_size_bytes, 4000); // 500 * 8 bytes
        
        let string_index = RawDataIndex::new(DataType::String, 100);
        assert_eq!(string_index.total_size_bytes, 0); // Variable size
    }

    #[test]
    fn test_segment_header_constants() {
        assert_eq!(SegmentHeader::LEAD_IN_SIZE, 28);
        assert_eq!(SegmentHeader::TDMS_TAG, b"TDSm");
        assert_eq!(SegmentHeader::INDEX_TAG, b"TDSh");
        assert_eq!(SegmentHeader::VERSION, 4713);
        assert_eq!(SegmentHeader::INCOMPLETE_MARKER, 0xFFFFFFFFFFFFFFFF);
    }
}

// Integration test helpers (only compiled for tests)
#[cfg(test)]
pub mod test_helpers {
    use super::*;
    use std::path::{Path, PathBuf};
    
    /// Create a temporary test directory
    pub fn create_test_dir() -> PathBuf {
        let dir = PathBuf::from("test_output");
        std::fs::create_dir_all(&dir).ok();
        dir
    }
    
    /// Clean up test files
    pub fn cleanup_test_file(path: impl AsRef<Path>) {
        let path = path.as_ref();
        std::fs::remove_file(path).ok();
        
        // Also remove index file
        let mut index_path = path.to_path_buf();
        index_path.set_extension("tdms_index");
        std::fs::remove_file(index_path).ok();
    }
    
    /// Generate test data
    pub fn generate_test_data_i32(count: usize) -> Vec<i32> {
        (0..count).map(|i| i as i32).collect()
    }
    
    pub fn generate_test_data_f64(count: usize) -> Vec<f64> {
        (0..count).map(|i| i as f64 * 0.1).collect()
    }
    
    pub fn generate_test_strings(count: usize) -> Vec<String> {
        (0..count).map(|i| format!("String_{}", i)).collect()
    }
}

// Benchmark helpers (only compiled for benchmarks)
#[cfg(all(test, feature = "bench"))]
pub mod bench_helpers {
    use super::*;
    
    pub fn setup_bench_writer(path: &str) -> TdmsWriter {
        TdmsWriter::create(path).unwrap()
    }
    
    pub fn cleanup_bench_file(path: &str) {
        std::fs::remove_file(path).ok();
        std::fs::remove_file(format!("{}_index", path)).ok();
    }
}