// tests/integration_tests.rs
use tdms_rs::*;
use std::fs;
use std::path::PathBuf;

#[test]
fn test_write_and_read_roundtrip() {
    let path = "test_output/roundtrip.tdms";
    fs::create_dir_all("test_output").unwrap();
    
    // Write
    {
        let mut writer = TdmsWriter::create(path).unwrap();
        writer.set_file_property("title", PropertyValue::String("Test".into()));
        writer.create_channel("Group1", "Chan1", DataType::I32).unwrap();
        
        let data: Vec<i32> = (0..1000).collect();
        writer.write_channel_data("Group1", "Chan1", &data).unwrap();
        writer.flush().unwrap();
    }
    
    // Read
    {
        let mut reader = TdmsReader::open(path).unwrap();
        let data: Vec<i32> = reader.read_channel_data("Group1", "Chan1").unwrap();
        assert_eq!(data.len(), 1000);
        assert_eq!(data[0], 0);
        assert_eq!(data[999], 999);
    }
    
    std::fs::remove_file(path).ok();
    std::fs::remove_file(format!("{}_index", path)).ok();
}