// tests/defragment_test.rs
use tdms_rs::*;
use std::fs;
// --- FIX: Removed unused import
// use std::path::Path; 

fn setup_test_file(name: &str) -> String {
    fs::create_dir_all("test_output").unwrap();
    let path_str = format!("test_output/{}", name);
    cleanup_test_file(&path_str);
    path_str
}

fn cleanup_test_file(path_str: &str) {
    fs::remove_file(path_str).ok();
    fs::remove_file(format!("{}_index", path_str)).ok();
}

/// Creates a fragmented TDMS file for testing
fn create_fragmented_file(path: &str) -> Result<()> {
    let mut writer = TdmsWriter::create(path)?;

    // Segment 1: File prop, Group prop, Channel A + data
    writer.set_file_property("file_title", PropertyValue::String("Fragmented File".into()));
    
    // --- FIX: Removed `?` operator, as set_group_property returns () ---
    writer.set_group_property("Group1", "group_desc", PropertyValue::String("First Segment".into()));
    
    writer.create_channel("Group1", "ChannelA", DataType::I32)?;
    writer.set_channel_property("Group1", "ChannelA", "unit", PropertyValue::String("V".into()))?;
    writer.write_channel_data("Group1", "ChannelA", &[1, 2, 3])?;
    writer.flush()?; // End of Segment 1

    // Segment 2: Change Channel A prop, add data to A, create Channel B
    writer.set_channel_property("Group1", "ChannelA", "unit", PropertyValue::String("mV".into()))?;
    writer.write_channel_data("Group1", "ChannelA", &[4, 5, 6])?;
    writer.create_channel("Group1", "ChannelB", DataType::String)?;
    writer.write_channel_strings("Group1", "ChannelB", &["a", "b"])?;
    writer.flush()?; // End of Segment 2

    // Segment 3: Change File prop, add data to both
    writer.set_file_property("author", PropertyValue::String("Test".into()));
    writer.write_channel_data("Group1", "ChannelA", &[7, 8, 9])?;
    writer.write_channel_strings("Group1", "ChannelB", &["c", "d", "e"])?;
    writer.flush()?; // End of Segment 3

    Ok(())
}

#[test]
fn test_defragment_file() {
    let source_path = setup_test_file("fragmented.tdms");
    let dest_path = setup_test_file("defragmented.tdms");

    // 1. Create the fragmented file
    create_fragmented_file(&source_path).unwrap();

    // Verify it is fragmented
    {
        let reader = TdmsReader::open(&source_path).unwrap();
        assert_eq!(reader.segment_count(), 3, "Source file should have 3 segments");
    }

    // 2. Run the defragment function
    defragment(&source_path, &dest_path).unwrap();

    // 3. Open and validate the new file
    {
        let mut reader = TdmsReader::open(&dest_path).unwrap();

        // Check segment count
        assert_eq!(reader.segment_count(), 1, "Defragmented file should have 1 segment");

        // Check file properties (final values)
        let file_props = reader.get_file_properties();
        assert_eq!(file_props.len(), 2);
        assert_eq!(
            file_props.get("file_title").unwrap().value,
            PropertyValue::String("Fragmented File".into())
        );
        assert_eq!(
            file_props.get("author").unwrap().value,
            PropertyValue::String("Test".into())
        );

        // Check group properties
        let group_props = reader.get_group_properties("Group1").unwrap();
        assert_eq!(group_props.len(), 1);
        assert_eq!(
            group_props.get("group_desc").unwrap().value,
            PropertyValue::String("First Segment".into())
        );

        // Check Channel A
        let chan_a_props = reader.get_channel_properties("Group1", "ChannelA").unwrap();
        assert_eq!(chan_a_props.len(), 1);
        assert_eq!(
            chan_a_props.get("unit").unwrap().value,
            PropertyValue::String("mV".into()) // Check final value
        );
        let data_a = reader.read_channel_data::<i32>("Group1", "ChannelA").unwrap();
        assert_eq!(data_a, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);

        // Check Channel B
        let chan_b_props = reader.get_channel_properties("Group1", "ChannelB").unwrap();
        assert_eq!(chan_b_props.len(), 0); // No properties were set
        let data_b = reader.read_channel_strings("Group1", "ChannelB").unwrap();
        assert_eq!(data_b, vec!["a", "b", "c", "d", "e"]);
    }

    cleanup_test_file(&source_path);
    cleanup_test_file(&dest_path);
}