// tests/writer_tests.rs
use tdms_rs::*;
use std::path::Path;

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
fn test_multiple_segments() {
    let path = setup_test_file("multi_segment.tdms");
    {
        let mut writer = TdmsWriter::create(&path).unwrap();
        
        writer.create_channel("Data", "Values", DataType::F64).unwrap();
        
        // Write multiple segments
        for i in 0..5 {
            let data: Vec<f64> = vec![i as f64; 100];
            writer.write_channel_data("Data", "Values", &data).unwrap();
            writer.write_segment().unwrap();
        }
        
        writer.flush().unwrap();
    } // Writer is dropped and flushed here

    // Verify
    {
        let mut reader = TdmsReader::open(&path).unwrap();
        let data: Vec<f64> = reader.read_channel_data("Data", "Values").unwrap();
        assert_eq!(data.len(), 500);
        assert_eq!(data[0], 0.0);
        assert_eq!(data[100], 1.0);
        assert_eq!(data[499], 4.0);
    }
    
    cleanup_test_file(&path);
}

#[test]
/// This test validates the fix for Scenario 6 (dropping a channel).
/// It writes to [A, B, C], then [A, C], then [B, C].
/// The old code would corrupt data here. The new code should pass.
fn test_new_obj_list_on_channel_drop() {
    let path = setup_test_file("new_obj_list_drop.tdms");
    
    {
        let mut writer = TdmsWriter::create(&path).unwrap();
        
        // Create all channels upfront
        writer.create_channel("Group", "A", DataType::I32).unwrap();
        writer.create_channel("Group", "B", DataType::I32).unwrap();
        writer.create_channel("Group", "C", DataType::I32).unwrap();

        // Segment 1: Write to A, B, C
        writer.write_channel_data("Group", "A", &[1, 1]).unwrap();
        writer.write_channel_data("Group", "B", &[10, 10]).unwrap();
        writer.write_channel_data("Group", "C", &[100, 100]).unwrap();
        writer.flush().unwrap(); // This writes segment 1

        // Segment 2: Write to A, C (B is skipped)
        // This *must* trigger a new_obj_list flag
        writer.write_channel_data("Group", "A", &[2, 2]).unwrap();
        writer.write_channel_data("Group", "C", &[200, 200]).unwrap();
        writer.flush().unwrap(); // This writes segment 2

        // Segment 3: Write to B, C (A is skipped)
        // This *must* trigger another new_obj_list flag
        writer.write_channel_data("Group", "B", &[30, 30]).unwrap();
        writer.write_channel_data("Group", "C", &[300, 300]).unwrap();
        writer.flush().unwrap(); // This writes segment 3

    } // Writer is dropped

    // Verify
    {
        let mut reader = TdmsReader::open(&path).unwrap();
        
        // Check that all 3 channels exist
        assert_eq!(reader.channel_count(), 3);
        
        // Read data and check totals
        let data_a: Vec<i32> = reader.read_channel_data("Group", "A").unwrap();
        let data_b: Vec<i32> = reader.read_channel_data("Group", "B").unwrap();
        let data_c: Vec<i32> = reader.read_channel_data("Group", "C").unwrap();

        // Verify correct total lengths
        assert_eq!(data_a.len(), 4);
        assert_eq!(data_b.len(), 4);
        assert_eq!(data_c.len(), 6);

        // Verify correct data content
        assert_eq!(data_a, vec![1, 1, 2, 2]);
        assert_eq!(data_b, vec![10, 10, 30, 30]);
        assert_eq!(data_c, vec![100, 100, 200, 200, 300, 300]);
    }
    
    cleanup_test_file(&path);
}

#[test]
/// This test validates the fix for Scenario 5 (adding a channel).
/// It writes to [A], then [A, B].
/// This also failed in the old code and should now pass.
fn test_new_obj_list_on_channel_add() {
    let path = setup_test_file("new_obj_list_add.tdms");
    
    {
        let mut writer = TdmsWriter::create(&path).unwrap();
        
        // Create initial channel
        writer.create_channel("Group", "A", DataType::I32).unwrap();

        // Segment 1: Write to A
        writer.write_channel_data("Group", "A", &[1, 2, 3]).unwrap();
        writer.flush().unwrap(); // This writes segment 1

        // Create a new channel
        writer.create_channel("Group", "B", DataType::I32).unwrap();

        // Segment 2: Write to A and B
        // This *must* trigger a new_obj_list flag
        writer.write_channel_data("Group", "A", &[4, 5, 6]).unwrap();
        writer.write_channel_data("Group", "B", &[100, 200, 300]).unwrap();
        writer.flush().unwrap(); // This writes segment 2

    } // Writer is dropped

    // Verify
    {
        let mut reader = TdmsReader::open(&path).unwrap();
        
        assert_eq!(reader.channel_count(), 2);
        
        let data_a: Vec<i32> = reader.read_channel_data("Group", "A").unwrap();
        let data_b: Vec<i32> = reader.read_channel_data("Group", "B").unwrap();

        // Verify correct total lengths
        assert_eq!(data_a.len(), 6);
        assert_eq!(data_b.len(), 3);

        // Verify correct data content
        assert_eq!(data_a, vec![1, 2, 3, 4, 5, 6]);
        assert_eq!(data_b, vec![100, 200, 300]);
    }
    
    cleanup_test_file(&path);
}