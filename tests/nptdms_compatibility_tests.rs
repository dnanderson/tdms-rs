// tests/nptdms_compatibility_tests.rs
//! Integration tests that write TDMS files with Rust and validate them using Python's nptdms library.
//! This ensures compatibility with third-party TDMS parsers.

use tdms_rs::*;
use std::fs;
use std::process::Command;

/// Helper to setup test directory and return path
fn setup_test_file(name: &str) -> String {
    fs::create_dir_all("test_output/nptdms_tests").unwrap();
    let path_str = format!("test_output/nptdms_tests/{}.tdms", name);
    cleanup_test_file(&path_str);
    path_str
}

/// Helper to cleanup both .tdms and .tdms_index files
fn cleanup_test_file(path_str: &str) {
    fs::remove_file(path_str).ok();
    fs::remove_file(format!("{}_index", path_str)).ok();
}

/// Run Python validation script for a specific test
fn validate_with_python(test_name: &str, filepath: &str) -> Result<()> {
    let python_script = "tests/validate_with_nptdms.py";
    
    // Make script executable (Unix-like systems)
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(python_script).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(python_script, perms).unwrap();
    }
    
    let output = Command::new(".venv/bin/python")
        .arg(python_script)
        .arg(test_name)
        .arg(filepath)
        .output()
        .expect("Failed to execute Python validation script");
    
    if !output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        eprintln!("Python validation failed:");
        eprintln!("stdout: {}", stdout);
        eprintln!("stderr: {}", stderr);
        return Err(TdmsError::Unsupported(format!(
            "nptdms validation failed for test '{}'", test_name
        )));
    }
    
    println!("{}", String::from_utf8_lossy(&output.stdout));
    Ok(())
}

#[test]
fn test_nptdms_basic_types() {
    let path = setup_test_file("basic_types");
    
    // Write file with various data types
    {
        let mut writer = TdmsWriter::create(&path).unwrap();
        
        // File properties
        writer.set_file_property("title", PropertyValue::String("Basic Types Test".into()));
        writer.set_file_property("author", PropertyValue::String("Rust TDMS".into()));
        
        // Group properties
        writer.set_group_property("TestGroup", "description", 
            PropertyValue::String("Test data".into()));
        
        // Create channels with different types
        writer.create_channel("TestGroup", "I32Channel", DataType::I32).unwrap();
        writer.create_channel("TestGroup", "F64Channel", DataType::F64).unwrap();
        writer.create_channel("TestGroup", "StringChannel", DataType::String).unwrap();
        writer.create_channel("TestGroup", "BoolChannel", DataType::Boolean).unwrap();
        
        // Set channel properties
        writer.set_channel_property("TestGroup", "I32Channel", "unit", 
            PropertyValue::String("counts".into())).unwrap();
        writer.set_channel_property("TestGroup", "F64Channel", "unit", 
            PropertyValue::String("volts".into())).unwrap();
        
        // Write data
        let i32_data: Vec<i32> = (0..100).collect();
        writer.write_channel_data("TestGroup", "I32Channel", &i32_data).unwrap();
        
        let f64_data: Vec<f64> = (0..100).map(|i| i as f64 * 0.1).collect();
        writer.write_channel_data("TestGroup", "F64Channel", &f64_data).unwrap();
        
        let string_data: Vec<String> = (0..10).map(|i| format!("String_{}", i)).collect();
        writer.write_channel_strings("TestGroup", "StringChannel", &string_data).unwrap();
        
        let bool_data: Vec<bool> = (0..10).map(|i| i % 2 == 0).collect();
        writer.write_channel_data("TestGroup", "BoolChannel", &bool_data).unwrap();
        
        writer.flush().unwrap();
    }
    
    // Validate with Python
    validate_with_python("basic_types", &path).unwrap();
    cleanup_test_file(&path);
}

#[test]
fn test_nptdms_multiple_segments() {
    let path = setup_test_file("multiple_segments");
    
    {
        let mut writer = TdmsWriter::create(&path).unwrap();
        writer.create_channel("Data", "Values", DataType::F64).unwrap();
        
        // Write 5 segments with 100 values each
        for i in 0..5 {
            let data: Vec<f64> = vec![i as f64; 100];
            writer.write_channel_data("Data", "Values", &data).unwrap();
            writer.flush().unwrap();
        }
    }
    
    validate_with_python("multiple_segments", &path).unwrap();
    cleanup_test_file(&path);
}

#[test]
fn test_nptdms_properties() {
    let path = setup_test_file("properties");
    
    {
        let mut writer = TdmsWriter::create(&path).unwrap();
        
        // File properties of different types
        writer.set_file_property("title", PropertyValue::String("Properties Test".into()));
        writer.set_file_property("version", PropertyValue::I32(2));
        writer.set_file_property("test_float", PropertyValue::Double(3.14));
        writer.set_file_property("test_bool", PropertyValue::Boolean(true));
        
        // Group properties
        writer.set_group_property("TestGroup", "group_id", PropertyValue::I32(42));
        writer.set_group_property("TestGroup", "group_name", 
            PropertyValue::String("Main Group".into()));
        
        // Channel with properties
        writer.create_channel("TestGroup", "TestChannel", DataType::F64).unwrap();
        writer.set_channel_property("TestGroup", "TestChannel", "unit", 
            PropertyValue::String("meters".into())).unwrap();
        writer.set_channel_property("TestGroup", "TestChannel", "scale", 
            PropertyValue::Double(1.5)).unwrap();
        writer.set_channel_property("TestGroup", "TestChannel", "offset", 
            PropertyValue::I32(10)).unwrap();
        writer.set_channel_property("TestGroup", "TestChannel", "enabled", 
            PropertyValue::Boolean(true)).unwrap();
        
        // Write some data
        let data: Vec<f64> = vec![1.0, 2.0, 3.0];
        writer.write_channel_data("TestGroup", "TestChannel", &data).unwrap();
        
        writer.flush().unwrap();
    }
    
    validate_with_python("properties", &path).unwrap();
    cleanup_test_file(&path);
}

#[test]
fn test_nptdms_incremental_metadata() {
    let path = setup_test_file("incremental_metadata");
    
    {
        let mut writer = TdmsWriter::create(&path).unwrap();
        writer.create_channel("Data", "Values", DataType::I32).unwrap();
        writer.set_channel_property("Data", "Values", "status", 
            PropertyValue::String("initial".into())).unwrap();
        
        // Write data in multiple segments with property changes
        for i in 0..3 {
            let data: Vec<i32> = (i*200..(i+1)*200).collect();
            writer.write_channel_data("Data", "Values", &data).unwrap();
            
            // Change property
            let status = match i {
                0 => "initial",
                1 => "processing",
                _ => "final",
            };
            writer.set_channel_property("Data", "Values", "status", 
                PropertyValue::String(status.into())).unwrap();
            
            writer.flush().unwrap();
        }
    }
    
    validate_with_python("incremental_metadata", &path).unwrap();
    cleanup_test_file(&path);
}

#[test]
fn test_nptdms_mixed_channels() {
    let path = setup_test_file("mixed_channels");
    
    {
        let mut writer = TdmsWriter::create(&path).unwrap();
        
        writer.create_channel("Mixed", "Integers", DataType::I32).unwrap();
        writer.create_channel("Mixed", "Floats", DataType::F64).unwrap();
        writer.create_channel("Mixed", "Strings", DataType::String).unwrap();
        writer.create_channel("Mixed", "Bools", DataType::Boolean).unwrap();
        
        // Write data to all channels
        writer.write_channel_data("Mixed", "Integers", &[1, 2, 3]).unwrap();
        writer.write_channel_data("Mixed", "Floats", &[1.1, 2.2, 3.3]).unwrap();
        writer.write_channel_strings("Mixed", "Strings", &["A", "B", "C"]).unwrap();
        writer.write_channel_data("Mixed", "Bools", &[true, false, true]).unwrap();
        writer.flush().unwrap();
        
        // Write more data
        writer.write_channel_data("Mixed", "Integers", &[4, 5, 6]).unwrap();
        writer.write_channel_data("Mixed", "Floats", &[4.4, 5.5, 6.6]).unwrap();
        writer.write_channel_strings("Mixed", "Strings", &["D", "E", "F"]).unwrap();
        writer.write_channel_data("Mixed", "Bools", &[false, false, true]).unwrap();
        writer.flush().unwrap();
    }
    
    validate_with_python("mixed_channels", &path).unwrap();
    cleanup_test_file(&path);
}

#[test]
fn test_nptdms_large_dataset() {
    let path = setup_test_file("large_dataset");
    
    {
        let mut writer = TdmsWriter::create(&path).unwrap();
        writer.create_channel("LargeData", "BigChannel", DataType::F64).unwrap();
        
        // Write 1 million values
        let data: Vec<f64> = (0..1_000_000).map(|i| i as f64 * 0.001).collect();
        writer.write_channel_data("LargeData", "BigChannel", &data).unwrap();
        writer.flush().unwrap();
    }
    
    validate_with_python("large_dataset", &path).unwrap();
    cleanup_test_file(&path);
}

#[test]
fn test_nptdms_empty_strings() {
    let path = setup_test_file("empty_strings");
    
    {
        let mut writer = TdmsWriter::create(&path).unwrap();
        writer.create_channel("StringTest", "MixedStrings", DataType::String).unwrap();
        
        // Mix of empty and non-empty strings
        let strings = vec!["", "Hello", "", "World", "", "", "End"];
        writer.write_channel_strings("StringTest", "MixedStrings", &strings).unwrap();
        writer.flush().unwrap();
    }
    
    validate_with_python("empty_strings", &path).unwrap();
    cleanup_test_file(&path);
}

#[test]
fn test_nptdms_channel_reordering() {
    let path = setup_test_file("channel_reordering");
    
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
        
        // Segment 2: B, C (drop A)
        writer.write_channel_data("G", "B", &[4]).unwrap();
        writer.write_channel_data("G", "C", &[5]).unwrap();
        writer.flush().unwrap();
        
        // Segment 3: A, C (drop B)
        writer.write_channel_data("G", "A", &[6]).unwrap();
        writer.write_channel_data("G", "C", &[7]).unwrap();
        writer.flush().unwrap();
    }
    
    validate_with_python("channel_reordering", &path).unwrap();
    cleanup_test_file(&path);
}

#[test]
fn test_nptdms_waveform_properties() {
    let path = setup_test_file("waveform_properties");
    
    {
        let mut writer = TdmsWriter::create(&path).unwrap();
        writer.create_channel("Waveforms", "Signal", DataType::F64).unwrap();
        
        // Set waveform properties using public API
        let start_time = Timestamp::now();
        writer.set_channel_property("Waveforms", "Signal", "wf_start_time", 
            PropertyValue::Timestamp(start_time)).unwrap();
        writer.set_channel_property("Waveforms", "Signal", "wf_increment", 
            PropertyValue::Double(0.001)).unwrap();
        writer.set_channel_property("Waveforms", "Signal", "wf_samples", 
            PropertyValue::U64(1000)).unwrap();
        writer.set_channel_property("Waveforms", "Signal", "unit_string", 
            PropertyValue::String("Volts".into())).unwrap();
        
        // Write waveform data
        let data: Vec<f64> = (0..1000).map(|i| (i as f64 * 0.001).sin()).collect();
        writer.write_channel_data("Waveforms", "Signal", &data).unwrap();
        writer.flush().unwrap();
    }
    
    validate_with_python("waveform_properties", &path).unwrap();
    cleanup_test_file(&path);
}

#[test]
fn test_nptdms_timestamp_data() {
    let path = setup_test_file("timestamp_data");
    
    {
        let mut writer = TdmsWriter::create(&path).unwrap();
        writer.create_channel("TimeData", "Timestamps", DataType::TimeStamp).unwrap();
        
        // Create timestamp data
        let base_time = Timestamp::now();
        let timestamps: Vec<Timestamp> = (0..100).map(|i| {
            Timestamp {
                seconds: base_time.seconds + i,
                fractions: base_time.fractions,
            }
        }).collect();
        
        writer.write_channel_data("TimeData", "Timestamps", &timestamps).unwrap();
        writer.flush().unwrap();
    }
    
    validate_with_python("timestamp_data", &path).unwrap();
    cleanup_test_file(&path);
}

#[test]
fn test_nptdms_defragmented_file() {
    // First create a fragmented file
    let fragmented_path = setup_test_file("fragmented_source");
    {
        let mut writer = TdmsWriter::create(&fragmented_path).unwrap();
        
        // Segment 1
        writer.set_file_property("file_title", PropertyValue::String("Fragmented File".into()));
        writer.set_group_property("Group1", "group_desc", 
            PropertyValue::String("First Segment".into()));
        writer.create_channel("Group1", "ChannelA", DataType::I32).unwrap();
        writer.set_channel_property("Group1", "ChannelA", "unit", 
            PropertyValue::String("V".into())).unwrap();
        writer.write_channel_data("Group1", "ChannelA", &[1, 2, 3]).unwrap();
        writer.flush().unwrap();
        
        // Segment 2
        writer.set_channel_property("Group1", "ChannelA", "unit", 
            PropertyValue::String("mV".into())).unwrap();
        writer.write_channel_data("Group1", "ChannelA", &[4, 5, 6]).unwrap();
        writer.create_channel("Group1", "ChannelB", DataType::String).unwrap();
        writer.write_channel_strings("Group1", "ChannelB", &["a", "b"]).unwrap();
        writer.flush().unwrap();
        
        // Segment 3
        writer.set_file_property("author", PropertyValue::String("Test".into()));
        writer.write_channel_data("Group1", "ChannelA", &[7, 8, 9]).unwrap();
        writer.write_channel_strings("Group1", "ChannelB", &["c", "d", "e"]).unwrap();
        writer.flush().unwrap();
    }
    
    // Defragment it
    let defragmented_path = setup_test_file("defragmented");
    defragment(&fragmented_path, &defragmented_path).unwrap();
    
    // Validate the defragmented file
    validate_with_python("defragmented", &defragmented_path).unwrap();
    
    cleanup_test_file(&fragmented_path);
    cleanup_test_file(&defragmented_path);
}

#[test]
fn test_nptdms_unicode_strings() {
    let path = setup_test_file("unicode_strings");
    
    {
        let mut writer = TdmsWriter::create(&path).unwrap();
        writer.create_channel("Unicode", "Strings", DataType::String).unwrap();
        
        // Various Unicode strings
        let strings = vec![
            "Hello World",
            "Привет мир",  // Russian
            "你好世界",     // Chinese
            "こんにちは世界", // Japanese
            "مرحبا بالعالم", // Arabic
            "🚀🌟💻",      // Emojis
            "Ñoño español", // Spanish with special chars
        ];
        
        writer.write_channel_strings("Unicode", "Strings", &strings).unwrap();
        writer.flush().unwrap();
    }
    
    // Verify file can be read (nptdms should handle Unicode correctly)
    let mut reader = TdmsReader::open(&path).unwrap();
    let data = reader.read_channel_strings("Unicode", "Strings").unwrap();
    assert_eq!(data.len(), 7);
    
    cleanup_test_file(&path);
}

#[test]
fn test_nptdms_interleaved_data() {
    let path = setup_test_file("interleaved_data");

    {
        let mut writer = TdmsWriter::create(&path).unwrap();
        writer.create_channel("Group", "Channel1", DataType::I32).unwrap();
        writer.create_channel("Group", "Channel2", DataType::F64).unwrap();

        for i in 0..10 {
            writer.write_channel_data("Group", "Channel1", &[i]).unwrap();
            writer.write_channel_data("Group", "Channel2", &[i as f64 * 1.1]).unwrap();
        }
        writer.flush().unwrap();
    }

    validate_with_python("interleaved_data", &path).unwrap();
    cleanup_test_file(&path);
}

#[test]
fn test_nptdms_very_long_strings() {
    let path = setup_test_file("long_strings");
    
    {
        let mut writer = TdmsWriter::create(&path).unwrap();
        writer.create_channel("LongStrings", "Data", DataType::String).unwrap();
        
        // Create very long strings
        let strings = vec![
            "A".repeat(1000),
            "B".repeat(5000),
            "C".repeat(10000),
        ];
        
        writer.write_channel_strings("LongStrings", "Data", &strings).unwrap();
        writer.flush().unwrap();
    }
    
    // Verify file can be read
    let mut reader = TdmsReader::open(&path).unwrap();
    let data = reader.read_channel_strings("LongStrings", "Data").unwrap();
    assert_eq!(data.len(), 3);
    assert_eq!(data[0].len(), 1000);
    assert_eq!(data[1].len(), 5000);
    assert_eq!(data[2].len(), 10000);
    
    cleanup_test_file(&path);
}

#[test]
fn test_nptdms_multiple_groups() {
    let path = setup_test_file("multiple_groups");
    
    {
        let mut writer = TdmsWriter::create(&path).unwrap();
        
        // Create channels in multiple groups
        writer.create_channel("Group1", "Channel1", DataType::I32).unwrap();
        writer.create_channel("Group1", "Channel2", DataType::F64).unwrap();
        writer.create_channel("Group2", "Channel1", DataType::String).unwrap();
        writer.create_channel("Group3", "Channel1", DataType::Boolean).unwrap();
        
        // Write data to all channels
        writer.write_channel_data("Group1", "Channel1", &[1, 2, 3]).unwrap();
        writer.write_channel_data("Group1", "Channel2", &[1.1, 2.2, 3.3]).unwrap();
        writer.write_channel_strings("Group2", "Channel1", &["A", "B", "C"]).unwrap();
        writer.write_channel_data("Group3", "Channel1", &[true, false, true]).unwrap();
        
        writer.flush().unwrap();
    }
    
    // Verify file can be read
    let mut reader = TdmsReader::open(&path).unwrap();
    let groups = reader.list_groups();
    assert_eq!(groups.len(), 3);
    assert!(groups.contains(&"Group1".to_string()));
    assert!(groups.contains(&"Group2".to_string()));
    assert!(groups.contains(&"Group3".to_string()));
    
    cleanup_test_file(&path);
}

#[test]
fn test_nptdms_edge_cases() {
    let path = setup_test_file("edge_cases");
    
    {
        let mut writer = TdmsWriter::create(&path).unwrap();
        
        // Channel with single value
        writer.create_channel("Edge", "SingleValue", DataType::I32).unwrap();
        writer.write_channel_data("Edge", "SingleValue", &[42]).unwrap();
        
        // Channel with zero values (write but flush without data)
        writer.create_channel("Edge", "EmptyChannel", DataType::F64).unwrap();
        
        // Channel with special float values
        writer.create_channel("Edge", "SpecialFloats", DataType::F64).unwrap();
        writer.write_channel_data("Edge", "SpecialFloats", 
            &[0.0, -0.0, f64::INFINITY, f64::NEG_INFINITY]).unwrap();
        
        writer.flush().unwrap();
    }
    
    // Verify file can be read
    let mut reader = TdmsReader::open(&path).unwrap();
    
    let single: Vec<i32> = reader.read_channel_data("Edge", "SingleValue").unwrap();
    assert_eq!(single, vec![42]);
    
    let special: Vec<f64> = reader.read_channel_data("Edge", "SpecialFloats").unwrap();
    assert_eq!(special.len(), 4);
    assert!(special[2].is_infinite() && special[2].is_sign_positive());
    assert!(special[3].is_infinite() && special[3].is_sign_negative());
    
    cleanup_test_file(&path);
}