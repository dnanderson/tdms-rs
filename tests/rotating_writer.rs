// tests/rotating_writer.rs
use tdms_rs::writer::RotatingTdmsWriter;
use tdms_rs::TdmsReader;
use std::fs;
use std::path::Path;

fn setup_test_dir(dir: &str) {
    if Path::new(dir).exists() {
        fs::remove_dir_all(dir).unwrap();
    }
    fs::create_dir_all(dir).unwrap();
}

#[test]
fn test_rotating_writer_creates_new_file_on_size_limit() {
    let test_dir = "test_output/rotating_writer";
    setup_test_dir(test_dir);
    let base_path = Path::new(test_dir).join("test");

    let max_size = 1024; // 1 KB
    let mut writer = RotatingTdmsWriter::new(&base_path, max_size).unwrap();
    writer.create_channel("group", "channel", tdms_rs::DataType::I32).unwrap();

    let data: Vec<i32> = (0..1000).collect();
    for _ in 0..5 {
        writer.write_channel_data("group", "channel", &data).unwrap();
        writer.flush().unwrap();
    }

    assert!(base_path.with_extension("tdms").exists());
    assert!(base_path.with_extension("1.tdms").exists());

    // Verify data in the first file
    let mut reader = TdmsReader::open(base_path.with_extension("tdms")).unwrap();
    let read_data: Vec<i32> = reader.read_channel_data("group", "channel").unwrap();
    assert!(!read_data.is_empty());

    // Verify data in the second file
    let mut reader = TdmsReader::open(base_path.with_extension("1.tdms")).unwrap();
    let read_data: Vec<i32> = reader.read_channel_data("group", "channel").unwrap();
    assert!(!read_data.is_empty());
}
