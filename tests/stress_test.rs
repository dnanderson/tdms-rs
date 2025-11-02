// tests/stress_test.rs
use tdms_rs::*;

// Helper to create the test dir and return the full path
fn setup_test_file(name: &str) -> String {
    std::fs::create_dir_all("test_output").unwrap();
    let path_str = format!("test_output/{}", name);
    // Ensure files from previous runs are gone
    cleanup_test_file(&path_str);
    path_str
}

// Helper to clean up both .tdms and .tdms_index
fn cleanup_test_file(path_str: &str) {
    std::fs::remove_file(path_str).ok();
    std::fs::remove_file(format!("{}_index", path_str)).ok();
}

#[test]
fn test_writer_stress_test() {
    let path = setup_test_file("stress_test.tdms");
    const SMALL_WRITE_COUNT: i32 = 100;
    const LARGE_WRITE_SIZE: usize = 1000;
    const TDMS_EPOCH_OFFSET: i64 = 2082844800;

    {
        let mut writer = TdmsWriter::create(&path).unwrap();

        // Add file properties of various types
        writer.set_file_property("File Description", PropertyValue::String("Stress test file".to_string()));
        writer.set_file_property("Author", PropertyValue::String("Test Suite".to_string()));
        writer.set_file_property("Version", PropertyValue::I32(1));

        // Set group properties of various types
        writer.set_group_property("Group1", "Group Description", PropertyValue::String("Main data group".to_string()));
        writer.set_group_property("Group1", "Setup ID", PropertyValue::I32(42));

        // Create channels with different data types
        writer.create_channel("Group1", "Channel1_f64", DataType::F64).unwrap();
        writer.create_channel("Group1", "Channel2_i32", DataType::I32).unwrap();
        writer.create_channel("Group1", "Channel3_String", DataType::String).unwrap();
        writer.create_channel("Group1", "Channel4_Timestamp", DataType::TimeStamp).unwrap();

        // Add channel properties of various types
        writer.set_channel_property("Group1", "Channel1_f64", "Unit", PropertyValue::String("Volts".to_string())).unwrap();
        writer.set_channel_property("Group1", "Channel1_f64", "Gain", PropertyValue::Double(2.5)).unwrap();
        writer.set_channel_property("Group1", "Channel2_i32", "Unit", PropertyValue::String("Amps".to_string())).unwrap();
        writer.set_channel_property("Group1", "Channel3_String", "Is_Critical", PropertyValue::Boolean(true)).unwrap();

        // Perform many small writes, flushing each to a new segment
        for i in 0..SMALL_WRITE_COUNT {
            let f64_data: Vec<f64> = vec![i as f64; 10];
            let i32_data: Vec<i32> = vec![i as i32; 10];
            let string_data: Vec<String> = vec![format!("value_{}", i); 10];

            let start_time = 1672531200 + TDMS_EPOCH_OFFSET;
            let timestamp_data: Vec<Timestamp> = vec![
                Timestamp {
                    seconds: start_time + i as i64,
                    fractions: 0,
                };
                10
            ];

            writer.write_channel_data("Group1", "Channel1_f64", &f64_data).unwrap();
            writer.write_channel_data("Group1", "Channel2_i32", &i32_data).unwrap();
            writer.write_channel_strings("Group1", "Channel3_String", &string_data).unwrap();
            writer.write_channel_data("Group1", "Channel4_Timestamp", &timestamp_data).unwrap();
            writer.flush().unwrap();
        }

        // Perform large writes for all data types
        let large_f64_data: Vec<f64> = vec![100.0; LARGE_WRITE_SIZE];
        let large_i32_data: Vec<i32> = vec![100; LARGE_WRITE_SIZE];
        let large_string_data: Vec<String> = vec!["large_string".to_string(); LARGE_WRITE_SIZE];
        let large_timestamp_data: Vec<Timestamp> = vec![
            Timestamp {
                seconds: 1672531200 + TDMS_EPOCH_OFFSET + 100,
                fractions: 0,
            };
            LARGE_WRITE_SIZE
        ];
        writer.write_channel_data("Group1", "Channel1_f64", &large_f64_data).unwrap();
        writer.write_channel_data("Group1", "Channel2_i32", &large_i32_data).unwrap();
        writer.write_channel_strings("Group1", "Channel3_String", &large_string_data).unwrap();
        writer.write_channel_data("Group1", "Channel4_Timestamp", &large_timestamp_data).unwrap();
    }

    // Verify the data
    {
        let mut reader = TdmsReader::open(&path).unwrap();

        // Verify properties
        let file_props = reader.get_file_properties();
        assert_eq!(file_props.get("Version").unwrap().value, PropertyValue::I32(1));
        let group_props = reader.get_group_properties("Group1").unwrap();
        assert_eq!(group_props.get("Setup ID").unwrap().value, PropertyValue::I32(42));
        let channel_props_f64 = reader.get_channel_properties("Group1", "Channel1_f64").unwrap();
        assert_eq!(channel_props_f64.get("Gain").unwrap().value, PropertyValue::Double(2.5));
        let channel_props_str = reader.get_channel_properties("Group1", "Channel3_String").unwrap();
        assert_eq!(channel_props_str.get("Is_Critical").unwrap().value, PropertyValue::Boolean(true));

        // Verify channel data sizes
        let total_small_values = (SMALL_WRITE_COUNT * 10) as usize;
        let total_f64 = total_small_values + LARGE_WRITE_SIZE;
        let total_i32 = total_small_values + LARGE_WRITE_SIZE;
        let total_string = total_small_values + LARGE_WRITE_SIZE;
        let total_timestamp = total_small_values + LARGE_WRITE_SIZE;

        let f64_data: Vec<f64> = reader.read_channel_data("Group1", "Channel1_f64").unwrap();
        assert_eq!(f64_data.len(), total_f64);
        assert_eq!(f64_data.last().copied(), Some(100.0));

        let i32_data: Vec<i32> = reader.read_channel_data("Group1", "Channel2_i32").unwrap();
        assert_eq!(i32_data.len(), total_i32);
        assert_eq!(i32_data.last().copied(), Some(100));

        let string_data: Vec<String> = reader.read_channel_strings("Group1", "Channel3_String").unwrap();
        assert_eq!(string_data.len(), total_string);
        assert_eq!(string_data.last().cloned(), Some("large_string".to_string()));

        let timestamp_data: Vec<Timestamp> = reader.read_channel_data("Group1", "Channel4_Timestamp").unwrap();
        assert_eq!(timestamp_data.len(), total_timestamp);
        assert_eq!(timestamp_data.last().copied().unwrap().seconds, 1672531200 + TDMS_EPOCH_OFFSET + 100);
    }

    cleanup_test_file(&path);
}
