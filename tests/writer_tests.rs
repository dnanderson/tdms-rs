// tests/writer_tests.rs
use tdms_rs::*;

#[test]
fn test_multiple_segments() {
    let path = "test_output/multi_segment.tdms";
    let mut writer = TdmsWriter::create(path).unwrap();
    
    writer.create_channel("Data", "Values", DataType::F64).unwrap();
    
    // Write multiple segments
    for i in 0..5 {
        let data: Vec<f64> = vec![i as f64; 100];
        writer.write_channel_data("Data", "Values", &data).unwrap();
        writer.write_segment().unwrap();
    }
    
    writer.flush().unwrap();
    
    std::fs::remove_file(path).ok();
    std::fs::remove_file(format!("{}_index", path)).ok();
}