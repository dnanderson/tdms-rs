// tests/timestamp_tests.rs
use tdms_rs::{TdmsWriter, TdmsReader, PropertyValue, DataType, Timestamp};
use std::fs;
use std::time::SystemTime;
use chrono::{DateTime, Utc, Local, TimeZone};

fn system_time_to_utc(st: SystemTime) -> DateTime<Utc> {
    DateTime::from(st)
}

fn setup_test_file(name: &str) -> String {
    let path = format!("test_output/{}.tdms", name);
    fs::create_dir_all("test_output").unwrap();
    if fs::metadata(&path).is_ok() {
        fs::remove_file(&path).unwrap();
        let index_path = format!("{}_index", path);
        if fs::metadata(&index_path).is_ok() {
            fs::remove_file(&index_path).unwrap();
        }
    }
    path
}

fn cleanup_test_file(path: &str) {
    fs::remove_file(path).ok();
    let index_path = format!("{}_index", path);
    fs::remove_file(index_path).ok();
}

#[test]
fn test_timestamp_property_roundtrip() {
    let path = setup_test_file("timestamp_property_roundtrip");

    let original_system_time = SystemTime::now();
    let original_timestamp = Timestamp::from_system_time(original_system_time);

    // Write
    {
        let mut writer = TdmsWriter::create(&path).unwrap();
        writer.set_file_property("test_timestamp", PropertyValue::Timestamp(original_timestamp));
        writer.flush().unwrap();
    }

    // Read
    {
        let reader = TdmsReader::open(&path).unwrap();
        let properties = reader.get_file_properties();
        let read_property = properties.get("test_timestamp").unwrap();

        if let PropertyValue::Timestamp(read_timestamp) = &read_property.value {
            assert_eq!(*read_timestamp, original_timestamp);

            let read_system_time = read_timestamp.to_system_time();
            let difference = if read_system_time > original_system_time {
                read_system_time.duration_since(original_system_time)
            } else {
                original_system_time.duration_since(read_system_time)
            };
            // Allow for a small difference due to precision loss
            assert!(difference.unwrap().as_nanos() < 1000);
        } else {
            panic!("Expected Timestamp property");
        }
    }

    cleanup_test_file(&path);
}

#[test]
fn test_timezone_conversion() {
    let path = setup_test_file("timezone_conversion");

    let local_time = Local.with_ymd_and_hms(2024, 7, 26, 10, 0, 0).unwrap();
    let original_timestamp = Timestamp::from_system_time(local_time.into());

    // Write
    {
        let mut writer = TdmsWriter::create(&path).unwrap();
        writer.set_file_property("local_time", PropertyValue::Timestamp(original_timestamp));
        writer.flush().unwrap();
    }

    // Read
    {
        let reader = TdmsReader::open(&path).unwrap();
        let properties = reader.get_file_properties();
        let read_property = properties.get("local_time").unwrap();

        if let PropertyValue::Timestamp(read_timestamp) = &read_property.value {
            let read_system_time = read_timestamp.to_system_time();
            let read_utc_time = system_time_to_utc(read_system_time);
            assert_eq!(read_utc_time, local_time.with_timezone(&chrono::Utc));
        } else {
            panic!("Expected Timestamp property");
        }
    }

    cleanup_test_file(&path);
}

#[test]
fn test_timestamp_channel_roundtrip() {
    let path = setup_test_file("timestamp_channel_roundtrip");

    let start_time = SystemTime::now();
    let timestamps: Vec<Timestamp> = (0..100)
        .map(|i| {
            let time = start_time + std::time::Duration::from_secs(i);
            Timestamp::from_system_time(time)
        })
        .collect();

    // Write
    {
        let mut writer = TdmsWriter::create(&path).unwrap();
        writer.create_channel("TimeGroup", "TimeChannel", DataType::TimeStamp).unwrap();
        writer.write_channel_data("TimeGroup", "TimeChannel", &timestamps).unwrap();
        writer.flush().unwrap();
    }

    // Read
    {
        let mut reader = TdmsReader::open(&path).unwrap();
        let read_timestamps: Vec<Timestamp> = reader.read_channel_data("TimeGroup", "TimeChannel").unwrap();

        assert_eq!(read_timestamps.len(), timestamps.len());
        for (original, read) in timestamps.iter().zip(read_timestamps.iter()) {
            assert_eq!(original, read);
        }
    }

    cleanup_test_file(&path);
}
