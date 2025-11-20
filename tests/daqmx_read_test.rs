// tests/daqmx_read_test.rs
use tdms_rs::*;
use std::path::Path;

/// This test mirrors the `test_raw_format` test from nptdms.
/// It requires the file `raw1.tdms` to be present in `tests/data/`.
#[test]
fn test_daqmx_raw_read() {
    let path = Path::new("tests/data/raw1.tdms");
    
    // Skip test if file doesn't exist (so CI passes without the external artifact)
    if !path.exists() {
        eprintln!("Skipping test_daqmx_raw_read: tests/data/raw1.tdms not found");
        return;
    }

    let mut reader = TdmsReader::open(path).expect("Failed to open raw1.tdms");

    // nptdms logic: group = test_file.groups()[0]
    // Note: list_groups() returns a Vec<String> from a HashMap, so order isn't strictly guaranteed
    // by the spec, but for single-group files this is safe.
    let groups = reader.list_groups();
    assert!(!groups.is_empty(), "No groups found in file");
    let group_name = &groups[0];

    // nptdms logic: data = group['First  Channel'].data
    // Note the double space in the channel name from the nptdms test case
    let channel_name = "First  Channel";
    
    // Read data as f64.
    // NOTE: This requires the reader to correctly handle the DAQmx scalers
    // (converting raw ADC codes to floats). If DAQmx scaling logic is not 
    // fully implemented in read_channel_data, this returns raw values or fails.
    let data: Vec<f64> = reader.read_channel_data(group_name, channel_name)
        .expect("Failed to read channel data");

    let expected = [
        -0.18402661, 0.14801477, -0.24506363,
        -0.29725028, -0.20020142, 0.18158513,
        0.02380444, 0.20661031, 0.20447401,
        0.2517777
    ];

    assert!(data.len() >= 10, "Data length insufficient");

    for (i, &exp_val) in expected.iter().enumerate() {
        let val = data[i];
        assert!(
            (val - exp_val).abs() < 1e-7,
            "Mismatch at index {}: expected {}, got {}",
            i, exp_val, val
        );
    }
}