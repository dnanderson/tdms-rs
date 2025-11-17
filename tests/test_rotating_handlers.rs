
use tdms_rs::{RotatingTdmsWriter, DataType, AsyncRotatingTdmsWriter};
use std::fs;
use std::path::Path;
use std::process::Command;

const TEST_FILE_PATH: &str = "test_rotating_handlers";
const TEST_TDMS_FILE_PATH: &str = "test_rotating_handlers.tdms";
const TEST_ROTATING_FILE_PATH: &str = "test_rotating_handlers.1.tdms";

const ASYNC_TEST_FILE_PATH: &str = "test_rotating_handlers_async";
const ASYNC_TEST_TDMS_FILE_PATH: &str = "test_rotating_handlers_async.tdms";
const ASYNC_TEST_ROTATING_FILE_PATH: &str = "test_rotating_handlers_async.1.tdms";

#[test]
fn test_sync_rotating_handler() {
    // Ensure clean state before test
    let _ = fs::remove_file(TEST_TDMS_FILE_PATH);
    let _ = fs::remove_file(TEST_ROTATING_FILE_PATH);

    let mut writer = RotatingTdmsWriter::new(TEST_FILE_PATH, 1024).unwrap();
    writer.create_channel("group", "channel", DataType::I32).unwrap();
    let data1: Vec<i32> = (0..500).collect();
    let data2: Vec<i32> = (500..1000).collect();
    writer.write_channel_data("group", "channel", &data1).unwrap();
    writer.flush().unwrap();
    writer.write_channel_data("group", "channel", &data2).unwrap();
    writer.flush().unwrap();

    assert!(Path::new(TEST_ROTATING_FILE_PATH).exists());

    let status1 = Command::new("python")
        .arg("tests/verify_nptdms_first_half.py")
        .arg(TEST_TDMS_FILE_PATH)
        .status()
        .expect("failed to execute process");
    assert!(status1.success(), "Verification of first half failed for sync test");

    let status2 = Command::new("python")
        .arg("tests/verify_nptdms.py")
        .arg(TEST_ROTATING_FILE_PATH)
        .status()
        .expect("failed to execute process");
    assert!(status2.success(), "Verification of rotated file failed for sync test");

    fs::remove_file(TEST_TDMS_FILE_PATH).unwrap();
    fs::remove_file(TEST_ROTATING_FILE_PATH).unwrap();
}

#[tokio::test]
async fn test_async_rotating_handler() {
    // Ensure clean state before test
    let _ = fs::remove_file(ASYNC_TEST_TDMS_FILE_PATH);
    let _ = fs::remove_file(ASYNC_TEST_ROTATING_FILE_PATH);

    let writer = AsyncRotatingTdmsWriter::new(ASYNC_TEST_FILE_PATH, 1024).await.unwrap();
    writer.create_channel("group", "channel", DataType::I32).await.unwrap();
    let data1: Vec<i32> = (0..500).collect();
    let data2: Vec<i32> = (500..1000).collect();
    writer.write_channel_data("group", "channel", data1, DataType::I32).await.unwrap();
    writer.flush().await.unwrap();
    writer.write_channel_data("group", "channel", data2, DataType::I32).await.unwrap();
    writer.close().await.unwrap();

    assert!(Path::new(ASYNC_TEST_ROTATING_FILE_PATH).exists());

    let status1 = Command::new("python")
        .arg("tests/verify_nptdms_first_half.py")
        .arg(ASYNC_TEST_TDMS_FILE_PATH)
        .status()
        .expect("failed to execute process");
    assert!(status1.success(), "Verification of first half failed for async test");

    let status2 = Command::new("python")
        .arg("tests/verify_nptdms.py")
        .arg(ASYNC_TEST_ROTATING_FILE_PATH)
        .status()
        .expect("failed to execute process");
    assert!(status2.success(), "Verification of rotated file failed for async test");

    fs::remove_file(ASYNC_TEST_TDMS_FILE_PATH).unwrap();
    fs::remove_file(ASYNC_TEST_ROTATING_FILE_PATH).unwrap();
}
