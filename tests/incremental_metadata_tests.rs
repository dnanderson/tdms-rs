// tests/incremental_metadata_tests.rs
// Comprehensive tests for TDMS incremental metadata features

use tdms_rs::*;
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom};

fn setup_test(name: &str) -> String {
    fs::create_dir_all("test_output").unwrap();
    let path = format!("test_output/{}", name);
    cleanup(&path);
    path
}

fn cleanup(path: &str) {
    fs::remove_file(path).ok();
    fs::remove_file(format!("{}_index", path)).ok();
}

fn read_segment_headers(path: &str) -> Vec<(u64, u32)> {
    let mut file = File::open(path).unwrap();
    let mut segments = Vec::new();
    
    let file_size = file.seek(SeekFrom::End(0)).unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();
    
    while file.stream_position().unwrap() < file_size {
        let pos = file.stream_position().unwrap();
        let mut tag = [0u8; 4];
        if file.read_exact(&mut tag).is_err() {
            break;
        }
        
        let mut toc = [0u8; 4];
        file.read_exact(&mut toc).unwrap();
        let toc_value = u32::from_le_bytes(toc);
        
        segments.push((pos, toc_value));
        
        // Skip rest of header
        file.seek(SeekFrom::Current(20)).unwrap();
        
        // Read next segment offset
        let mut offset_bytes = [0u8; 8];
        file.seek(SeekFrom::Start(pos + 12)).unwrap();
        file.read_exact(&mut offset_bytes).unwrap();
        let next_offset = u64::from_le_bytes(offset_bytes);
        
        if next_offset == 0xFFFFFFFFFFFFFFFF {
            break;
        }
        
        let next_pos = pos + 28 + next_offset;
        if next_pos >= file_size {
            break;
        }
        file.seek(SeekFrom::Start(next_pos)).unwrap();
    }
    
    segments
}

#[test]
/// Test Scenario 2: Appending raw data without new segment
/// When no metadata changes, data should be appended to existing segment
fn test_append_raw_data_only() {
    let path = setup_test("append_only.tdms");
    
    {
        let mut writer = TdmsWriter::create(&path).unwrap();
        writer.create_channel("Data", "Channel1", DataType::I32).unwrap();
        
        // First write - creates segment 1
        writer.write_channel_data("Data", "Channel1", &[1, 2, 3]).unwrap();
        writer.flush().unwrap();
        
        // Second write - should APPEND to segment 1 (no metadata change)
        writer.write_channel_data("Data", "Channel1", &[4, 5, 6]).unwrap();
        writer.flush().unwrap();
        
        // Third write - should APPEND again
        writer.write_channel_data("Data", "Channel1", &[7, 8, 9]).unwrap();
        writer.flush().unwrap();
    }
    
    // Verify only ONE segment was created
    let segments = read_segment_headers(&path);
    assert_eq!(segments.len(), 1, "Should have only 1 segment (append-only mode)");
    
    // Verify data is correct
    let mut reader = TdmsReader::open(&path).unwrap();
    let data: Vec<i32> = reader.read_channel_data("Data", "Channel1").unwrap();
    assert_eq!(data, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
    
    cleanup(&path);
}

#[test]
/// Test that property changes force new segment
fn test_property_change_forces_new_segment() {
    let path = setup_test("property_change.tdms");
    
    {
        let mut writer = TdmsWriter::create(&path).unwrap();
        writer.create_channel("Data", "Channel1", DataType::F64).unwrap();
        writer.set_channel_property("Data", "Channel1", "unit", 
            PropertyValue::String("volts".into())).unwrap();
        
        // First write
        writer.write_channel_data("Data", "Channel1", &[1.0, 2.0]).unwrap();
        writer.flush().unwrap();
        
        // Second write - same data, same index size
        writer.write_channel_data("Data", "Channel1", &[3.0, 4.0]).unwrap();
        writer.flush().unwrap();
        
        // Change property - should force new segment
        writer.set_channel_property("Data", "Channel1", "unit", 
            PropertyValue::String("amps".into())).unwrap();
        
        // Third write
        writer.write_channel_data("Data", "Channel1", &[5.0, 6.0]).unwrap();
        writer.flush().unwrap();
    }
    
    let segments = read_segment_headers(&path);
    // Should have: Segment 1 (initial), Segment 1 appended, Segment 2 (property change)
    assert_eq!(segments.len(), 2, "Property change should create new segment");
    
    let mut reader = TdmsReader::open(&path).unwrap();
    let data: Vec<f64> = reader.read_channel_data("Data", "Channel1").unwrap();
    assert_eq!(data, vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0]);
    
    cleanup(&path);
}

#[test]
/// Test file-level property changes
fn test_file_property_forces_new_segment() {
    let path = setup_test("file_property.tdms");
    
    {
        let mut writer = TdmsWriter::create(&path).unwrap();
        writer.set_file_property("title", PropertyValue::String("Version 1".into()));
        writer.create_channel("Data", "Values", DataType::I32).unwrap();
        
        writer.write_channel_data("Data", "Values", &[1, 2]).unwrap();
        writer.flush().unwrap();
        
        writer.write_channel_data("Data", "Values", &[3, 4]).unwrap();
        writer.flush().unwrap();
        
        // Change file property
        writer.set_file_property("title", PropertyValue::String("Version 2".into()));
        writer.write_channel_data("Data", "Values", &[5, 6]).unwrap();
        writer.flush().unwrap();
    }
    
    let segments = read_segment_headers(&path);
    assert_eq!(segments.len(), 2, "File property change should create new segment");
    
    cleanup(&path);
}

#[test]
/// Test group-level property changes
fn test_group_property_forces_new_segment() {
    let path = setup_test("group_property.tdms");
    
    {
        let mut writer = TdmsWriter::create(&path).unwrap();
        writer.set_group_property("Data", "description", 
            PropertyValue::String("Test group".into()));
        writer.create_channel("Data", "Values", DataType::I32).unwrap();
        
        writer.write_channel_data("Data", "Values", &[1, 2]).unwrap();
        writer.flush().unwrap();
        
        writer.write_channel_data("Data", "Values", &[3, 4]).unwrap();
        writer.flush().unwrap();
        
        // Change group property
        writer.set_group_property("Data", "description", 
            PropertyValue::String("Updated group".into()));
        writer.write_channel_data("Data", "Values", &[5, 6]).unwrap();
        writer.flush().unwrap();
    }
    
    let segments = read_segment_headers(&path);
    assert_eq!(segments.len(), 2, "Group property change should create new segment");
    
    cleanup(&path);
}

#[test]
/// Test index size change forces new segment
fn test_index_change_forces_new_segment() {
    let path = setup_test("index_change.tdms");
    
    {
        let mut writer = TdmsWriter::create(&path).unwrap();
        writer.create_channel("Data", "Values", DataType::I32).unwrap();
        
        // Write 100 values
        writer.write_channel_data("Data", "Values", &vec![1i32; 100]).unwrap();
        writer.flush().unwrap();
        
        // Write same size - should append
        writer.write_channel_data("Data", "Values", &vec![2i32; 100]).unwrap();
        writer.flush().unwrap();
        
        // Write different size - should create new segment
        writer.write_channel_data("Data", "Values", &vec![3i32; 200]).unwrap();
        writer.flush().unwrap();
    }
    
    let segments = read_segment_headers(&path);
    assert_eq!(segments.len(), 2, "Index size change should create new segment");
    
    let mut reader = TdmsReader::open(&path).unwrap();
    let data: Vec<i32> = reader.read_channel_data("Data", "Values").unwrap();
    assert_eq!(data.len(), 400);
    
    cleanup(&path);
}

#[test]
/// Test string data with incremental metadata
fn test_strings_incremental_metadata() {
    let path = setup_test("strings_incremental.tdms");
    
    {
        let mut writer = TdmsWriter::create(&path).unwrap();
        writer.create_channel("Text", "Messages", DataType::String).unwrap();
        
        // First write
        writer.write_channel_strings("Text", "Messages", 
            &["Hello", "World"]).unwrap();
        writer.flush().unwrap();
        
        // Second write - same number of strings, should append
        writer.write_channel_strings("Text", "Messages", 
            &["Foo", "Bar"]).unwrap();
        writer.flush().unwrap();
        
        // Third write - different count, should create new segment
        writer.write_channel_strings("Text", "Messages", 
            &["One", "Two", "Three"]).unwrap();
        writer.flush().unwrap();
    }
    
    let segments = read_segment_headers(&path);
    assert_eq!(segments.len(), 3, "Strings always create new segments (no append optimization)");
    
    let mut reader = TdmsReader::open(&path).unwrap();
    let strings = reader.read_channel_strings("Text", "Messages").unwrap();
    assert_eq!(strings, vec!["Hello", "World", "Foo", "Bar", "One", "Two", "Three"]);
    
    cleanup(&path);
}

#[test]
/// Test multiple channels with mixed changes
fn test_multiple_channels_mixed_changes() {
    let path = setup_test("mixed_changes.tdms");
    
    {
        let mut writer = TdmsWriter::create(&path).unwrap();
        writer.create_channel("Group", "A", DataType::I32).unwrap();
        writer.create_channel("Group", "B", DataType::I32).unwrap();
        
        // Segment 1: Both channels
        writer.write_channel_data("Group", "A", &[1, 2]).unwrap();
        writer.write_channel_data("Group", "B", &[10, 20]).unwrap();
        writer.flush().unwrap();
        
        // Append: Both channels, same size
        writer.write_channel_data("Group", "A", &[3, 4]).unwrap();
        writer.write_channel_data("Group", "B", &[30, 40]).unwrap();
        writer.flush().unwrap();
        
        // New segment: Change property on A only
        writer.set_channel_property("Group", "A", "changed", 
            PropertyValue::Boolean(true)).unwrap();
        writer.write_channel_data("Group", "A", &[5, 6]).unwrap();
        writer.write_channel_data("Group", "B", &[50, 60]).unwrap();
        writer.flush().unwrap();
    }
    
    let segments = read_segment_headers(&path);
    assert_eq!(segments.len(), 2);
    
    let mut reader = TdmsReader::open(&path).unwrap();
    let data_a: Vec<i32> = reader.read_channel_data("Group", "A").unwrap();
    let data_b: Vec<i32> = reader.read_channel_data("Group", "B").unwrap();
    assert_eq!(data_a, vec![1, 2, 3, 4, 5, 6]);
    assert_eq!(data_b, vec![10, 20, 30, 40, 50, 60]);
    
    cleanup(&path);
}

#[test]
/// Test TOC flags are set correctly
fn test_toc_flags() {
    let path = setup_test("toc_flags.tdms");
    
    {
        let mut writer = TdmsWriter::create(&path).unwrap();
        writer.create_channel("Data", "Values", DataType::I32).unwrap();
        writer.write_channel_data("Data", "Values", &[1, 2, 3]).unwrap();
        writer.flush().unwrap();
    }
    
    let segments = read_segment_headers(&path);
    assert_eq!(segments.len(), 1);
    
    let toc = TocFlags::new(segments[0].1);
    assert!(toc.has_metadata(), "First segment should have metadata");
    assert!(toc.has_new_obj_list(), "First segment should have new obj list");
    assert!(toc.has_raw_data(), "Segment should have raw data");
    assert!(!toc.is_big_endian(), "Should be little endian");
    
    cleanup(&path);
}

#[test]
/// Test channel reordering triggers new object list
fn test_channel_reordering() {
    let path = setup_test("reorder.tdms");
    
    {
        let mut writer = TdmsWriter::create(&path).unwrap();
        writer.create_channel("G", "A", DataType::I32).unwrap();
        writer.create_channel("G", "B", DataType::I32).unwrap();
        writer.create_channel("G", "C", DataType::I32).unwrap();
        
        // Segment 1: A, B, C
        writer.write_channel_data("G", "A", &[1]).unwrap();
        writer.write_channel_data("G", "B", &[2]).unwrap();
        writer.write_channel_data("G", "C", &[3]).unwrap();
        writer.flush().unwrap();
        
        // Segment 2: B, C (drop A) - different order
        writer.write_channel_data("G", "B", &[4]).unwrap();
        writer.write_channel_data("G", "C", &[5]).unwrap();
        writer.flush().unwrap();
        
        // Segment 3: A, C (drop B) - different again
        writer.write_channel_data("G", "A", &[6]).unwrap();
        writer.write_channel_data("G", "C", &[7]).unwrap();
        writer.flush().unwrap();
    }
    
    let segments = read_segment_headers(&path);
    assert_eq!(segments.len(), 3, "Each channel list change should create new segment");
    
    // Verify all have new_obj_list flag
    for (_, toc_value) in segments {
        let toc = TocFlags::new(toc_value);
        assert!(toc.has_new_obj_list(), "Each segment should have new obj list flag");
    }
    
    let mut reader = TdmsReader::open(&path).unwrap();
    let data_a: Vec<i32> = reader.read_channel_data("G", "A").unwrap();
    let data_b: Vec<i32> = reader.read_channel_data("G", "B").unwrap();
    let data_c: Vec<i32> = reader.read_channel_data("G", "C").unwrap();
    
    assert_eq!(data_a, vec![1, 6]);
    assert_eq!(data_b, vec![2, 4]);
    assert_eq!(data_c, vec![3, 5, 7]);
    
    cleanup(&path);
}

#[test]
/// Test empty segments (metadata only, no data)
fn test_empty_segments() {
    let path = setup_test("empty_segments.tdms");
    
    {
        let mut writer = TdmsWriter::create(&path).unwrap();
        writer.set_file_property("version", PropertyValue::I32(1));
        writer.create_channel("Data", "Values", DataType::I32).unwrap();
        
        // Write metadata without data
        writer.flush().unwrap();
        
        // Change property without data
        writer.set_file_property("version", PropertyValue::I32(2));
        writer.flush().unwrap();
        
        // Now write data
        writer.write_channel_data("Data", "Values", &[1, 2, 3]).unwrap();
        writer.flush().unwrap();
    }
    
    let segments = read_segment_headers(&path);
    assert_eq!(segments.len(), 3, "Should have 3 segments");
    
    // First two should have metadata but no raw data
    assert!(TocFlags::new(segments[0].1).has_metadata());
    assert!(!TocFlags::new(segments[0].1).has_raw_data());
    
    assert!(TocFlags::new(segments[1].1).has_metadata());
    assert!(!TocFlags::new(segments[1].1).has_raw_data());
    
    // Third should have both
    assert!(TocFlags::new(segments[2].1).has_metadata());
    assert!(TocFlags::new(segments[2].1).has_raw_data());
    
    cleanup(&path);
}

#[test]
/// Test multiple data types in same file
fn test_multiple_data_types() {
    let path = setup_test("multi_types.tdms");
    
    {
        let mut writer = TdmsWriter::create(&path).unwrap();
        writer.create_channel("Mixed", "Integers", DataType::I32).unwrap();
        writer.create_channel("Mixed", "Floats", DataType::F64).unwrap();
        writer.create_channel("Mixed", "Strings", DataType::String).unwrap();
        writer.create_channel("Mixed", "Bools", DataType::Boolean).unwrap();
        
        // Write all channels
        writer.write_channel_data("Mixed", "Integers", &[1, 2, 3]).unwrap();
        writer.write_channel_data("Mixed", "Floats", &[1.1, 2.2, 3.3]).unwrap();
        writer.write_channel_strings("Mixed", "Strings", &["A", "B", "C"]).unwrap();
        let bools = vec![true, false, true];
        writer.write_channel_data("Mixed", "Bools", &bools).unwrap();
        writer.flush().unwrap();
        
        // Append more data (should use MATCHES_PREVIOUS)
        writer.write_channel_data("Mixed", "Integers", &[4, 5, 6]).unwrap();
        writer.write_channel_data("Mixed", "Floats", &[4.4, 5.5, 6.6]).unwrap();
        writer.write_channel_strings("Mixed", "Strings", &["D", "E", "F"]).unwrap();
        let more_bools = vec![false, false, true];
        writer.write_channel_data("Mixed", "Bools", &more_bools).unwrap();
        writer.flush().unwrap();
    }
    
    let segments = read_segment_headers(&path);
    assert_eq!(segments.len(), 2, "Mixed workloads with strings create new segments (no append)");
    
    let mut reader = TdmsReader::open(&path).unwrap();
    assert_eq!(reader.channel_count(), 4);
    
    let ints: Vec<i32> = reader.read_channel_data("Mixed", "Integers").unwrap();
    let floats: Vec<f64> = reader.read_channel_data("Mixed", "Floats").unwrap();
    let strings = reader.read_channel_strings("Mixed", "Strings").unwrap();
    
    assert_eq!(ints.len(), 6);
    assert_eq!(floats.len(), 6);
    assert_eq!(strings.len(), 6);
    
    cleanup(&path);
}

#[test]
/// Test large-scale append scenario
fn test_large_scale_append() {
    let path = setup_test("large_append.tdms");
    
    {
        let mut writer = TdmsWriter::create(&path).unwrap();
        writer.create_channel("Data", "Values", DataType::F64).unwrap();
        
        // Append 100 times with same size
        for i in 0..100 {
            let data = vec![i as f64; 10];
            writer.write_channel_data("Data", "Values", &data).unwrap();
            writer.flush().unwrap();
        }
    }
    
    // Should have only 1 segment (all appended)
    let segments = read_segment_headers(&path);
    assert_eq!(segments.len(), 1, "All appends should go to same segment");
    
    let mut reader = TdmsReader::open(&path).unwrap();
    let data: Vec<f64> = reader.read_channel_data("Data", "Values").unwrap();
    assert_eq!(data.len(), 1000);
    
    cleanup(&path);
}

#[test]
/// Test that index file is correctly maintained
fn test_index_file_consistency() {
    let path = setup_test("index_test.tdms");
    let index_path = format!("{}_index", path);
    
    {
        let mut writer = TdmsWriter::create(&path).unwrap();
        writer.create_channel("Data", "Values", DataType::I32).unwrap();
        writer.write_channel_data("Data", "Values", &[1, 2, 3]).unwrap();
        writer.flush().unwrap();
    }
    
    // Both files should exist
    assert!(std::path::Path::new(&path).exists());
    assert!(std::path::Path::new(&index_path).exists());
    
    // Index file should have TDSh tag
    let mut index_file = File::open(&index_path).unwrap();
    let mut tag = [0u8; 4];
    index_file.read_exact(&mut tag).unwrap();
    assert_eq!(&tag, b"TDSh", "Index file should have TDSh tag");
    
    cleanup(&path);
}

#[test]
/// Test alternating write patterns
fn test_alternating_writes() {
    let path = setup_test("alternating.tdms");
    
    {
        let mut writer = TdmsWriter::create(&path).unwrap();
        writer.create_channel("G", "A", DataType::I32).unwrap();
        writer.create_channel("G", "B", DataType::I32).unwrap();
        
        // Alternate between channels
        for i in 0..10 {
            if i % 2 == 0 {
                writer.write_channel_data("G", "A", &[i]).unwrap();
            } else {
                writer.write_channel_data("G", "B", &[i]).unwrap();
            }
            writer.flush().unwrap();
        }
    }
    
    // Each change should create new segment due to channel list change
    let segments = read_segment_headers(&path);
    assert_eq!(segments.len(), 10);
    
    let mut reader = TdmsReader::open(&path).unwrap();
    let data_a: Vec<i32> = reader.read_channel_data("G", "A").unwrap();
    let data_b: Vec<i32> = reader.read_channel_data("G", "B").unwrap();
    
    assert_eq!(data_a, vec![0, 2, 4, 6, 8]);
    assert_eq!(data_b, vec![1, 3, 5, 7, 9]);
    
    cleanup(&path);
}

#[test]
/// Test metadata-only updates don't corrupt data
fn test_metadata_only_updates() {
    let path = setup_test("metadata_only.tdms");
    
    {
        let mut writer = TdmsWriter::create(&path).unwrap();
        writer.create_channel("Data", "Values", DataType::I32).unwrap();
        
        // Write data
        writer.write_channel_data("Data", "Values", &[1, 2, 3]).unwrap();
        writer.flush().unwrap();
        
        // Update property without writing data
        writer.set_channel_property("Data", "Values", "processed", 
            PropertyValue::Boolean(true)).unwrap();
        writer.flush().unwrap();
        
        // Write more data
        writer.write_channel_data("Data", "Values", &[4, 5, 6]).unwrap();
        writer.flush().unwrap();
    }
    
    let segments = read_segment_headers(&path);
    assert_eq!(segments.len(), 2);
    
    let mut reader = TdmsReader::open(&path).unwrap();
    let data: Vec<i32> = reader.read_channel_data("Data", "Values").unwrap();
    assert_eq!(data, vec![1, 2, 3, 4, 5, 6]);
    
    cleanup(&path);
}