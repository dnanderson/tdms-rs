// tests/streaming_reader_tests.rs
use tdms_rs::*;
use std::fs;

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

#[test]
fn test_high_level_numeric_iteration() {
    let path = setup_test_file("streaming_numeric.tdms");
    const TOTAL_VALUES: usize = 10_000;
    const CHUNK_SIZE: usize = 1_000;

    // 1. Write data
    {
        let mut writer = TdmsWriter::create(&path).unwrap();
        writer.create_channel("Group", "Data", DataType::I32).unwrap();
        
        // Write in multiple segments to ensure streaming handles segment boundaries correctly
        for i in 0..10 {
            let data: Vec<i32> = (0..1000).map(|x| (i * 1000 + x) as i32).collect();
            writer.write_channel_data("Group", "Data", &data).unwrap();
            writer.flush().unwrap();
        }
    }

    // 2. Read using high-level iterator
    {
        let mut reader = TdmsReader::open(&path).unwrap();
        
        // Create iterator: chunks of 1000 values
        let iterator = reader.iter_channel_data::<i32>("Group", "Data", CHUNK_SIZE).unwrap();
        
        let mut total_sum: i64 = 0;
        let mut chunk_count = 0;
        let mut value_count = 0;

        for chunk_result in iterator {
            let chunk = chunk_result.unwrap();
            chunk_count += 1;
            value_count += chunk.len();
            total_sum += chunk.iter().map(|&x| x as i64).sum::<i64>();
        }

        assert_eq!(chunk_count, 10); // 10,000 / 1,000
        assert_eq!(value_count, TOTAL_VALUES);
        
        // Sum of 0..9999
        let expected_sum: i64 = (0..TOTAL_VALUES).map(|x| x as i64).sum();
        assert_eq!(total_sum, expected_sum);
    }

    cleanup_test_file(&path);
}

#[test]
fn test_high_level_string_iteration() {
    let path = setup_test_file("streaming_strings.tdms");
    const TOTAL_STRINGS: usize = 100;
    const CHUNK_SIZE: usize = 20;

    // 1. Write strings
    {
        let mut writer = TdmsWriter::create(&path).unwrap();
        writer.create_channel("Text", "Lines", DataType::String).unwrap();
        
        let strings: Vec<String> = (0..TOTAL_STRINGS)
            .map(|i| format!("Line {}", i))
            .collect();
        
        writer.write_channel_strings("Text", "Lines", &strings).unwrap();
        writer.flush().unwrap();
    }

    // 2. Read strings using iterator
    {
        let mut reader = TdmsReader::open(&path).unwrap();
        
        let iterator = reader.iter_channel_strings("Text", "Lines", CHUNK_SIZE).unwrap();
        
        let mut count = 0;
        let mut chunk_count = 0;

        for chunk_result in iterator {
            let chunk = chunk_result.unwrap();
            chunk_count += 1;
            
            for (i, s) in chunk.iter().enumerate() {
                let global_index = count + i;
                assert_eq!(s, &format!("Line {}", global_index));
            }
            
            count += chunk.len();
        }

        assert_eq!(count, TOTAL_STRINGS);
        assert_eq!(chunk_count, 5); // 100 / 20
    }

    cleanup_test_file(&path);
}

#[test]
fn test_iterator_adapters() {
    let path = setup_test_file("streaming_adapters.tdms");

    // 1. Write data
    {
        let mut writer = TdmsWriter::create(&path).unwrap();
        writer.create_channel("Group", "Data", DataType::F64).unwrap();
        let data: Vec<f64> = (0..100).map(|i| i as f64).collect();
        writer.write_channel_data("Group", "Data", &data).unwrap();
        writer.flush().unwrap();
    }

    // 2. Use standard iterator adapters (flatten, filter, collect)
    {
        let mut reader = TdmsReader::open(&path).unwrap();
        
        let iterator = reader.iter_channel_data::<f64>("Group", "Data", 10).unwrap();
        
        // "Idiomatic" Rust usage: flatten chunks into a single stream, filter, and collect
        let filtered_values: Vec<f64> = iterator
            .flatten() // Handle Result (panics on err for test)
            .flatten() // Flatten Vec<f64> into f64
            .filter(|&x| x > 90.0) // Keep only values > 90.0
            .collect();

        assert_eq!(filtered_values.len(), 9); // 91.0 to 99.0
        assert_eq!(filtered_values[0], 91.0);
        assert_eq!(filtered_values.last(), Some(&99.0));
    }

    cleanup_test_file(&path);
}

#[test]
fn test_progress_reporting() {
    let path = setup_test_file("streaming_progress.tdms");
    
    {
        let mut writer = TdmsWriter::create(&path).unwrap();
        writer.create_channel("Group", "Data", DataType::I32).unwrap();
        // Write 100 items
        writer.write_channel_data("Group", "Data", &vec![0; 100]).unwrap();
        writer.flush().unwrap();
    }

    {
        let mut reader = TdmsReader::open(&path).unwrap();
        // Chunk size 25 means 4 steps: 25%, 50%, 75%, 100%
        let mut iterator = reader.iter_channel_data::<i32>("Group", "Data", 25).unwrap();
        
        assert_eq!(iterator.progress(), 0.0);
        
        let _ = iterator.next(); // Read 0-25
        assert_eq!(iterator.progress(), 25.0);
        
        let _ = iterator.next(); // Read 25-50
        assert_eq!(iterator.progress(), 50.0);
        
        let _ = iterator.next(); // Read 50-75
        assert_eq!(iterator.progress(), 75.0);
        
        let _ = iterator.next(); // Read 75-100
        assert_eq!(iterator.progress(), 100.0);
    }

    cleanup_test_file(&path);
}

#[test]
fn test_manual_streaming_seek_and_reset() {
    let path = setup_test_file("streaming_seek.tdms");
    
    {
        let mut writer = TdmsWriter::create(&path).unwrap();
        writer.create_channel("Group", "Data", DataType::I32).unwrap();
        let data: Vec<i32> = (0..100).collect();
        writer.write_channel_data("Group", "Data", &data).unwrap();
        writer.flush().unwrap();
    }

    {
        let mut reader = TdmsReader::open(&path).unwrap();
        // FIX: Use proper TDMS path string format (/'Group'/'Data')
        let channel = reader.get_channel("/'Group'/'Data'").unwrap();
        
        // Create low-level streaming reader manually
        let mut stream = StreamingReader::new(channel, 10);
        
        // 1. Read first chunk
        let chunk = reader.read_streaming_data::<i32>(&mut stream).unwrap().unwrap();
        assert_eq!(chunk[0], 0);
        assert_eq!(stream.position(), 10);
        
        // 2. Seek forward to 50
        stream.seek(50);
        let chunk = reader.read_streaming_data::<i32>(&mut stream).unwrap().unwrap();
        assert_eq!(chunk[0], 50);
        assert_eq!(stream.position(), 60); // 50 + 10
        
        // 3. Reset to 0
        stream.reset();
        assert_eq!(stream.position(), 0);
        let chunk = reader.read_streaming_data::<i32>(&mut stream).unwrap().unwrap();
        assert_eq!(chunk[0], 0);
        
        // 4. Change chunk size dynamically
        stream.set_chunk_size(5);
        let chunk = reader.read_streaming_data::<i32>(&mut stream).unwrap().unwrap();
        assert_eq!(chunk.len(), 5);
        assert_eq!(chunk[0], 10); // Previous read was 0-10, so we are at 10
    }

    cleanup_test_file(&path);
}

#[test]
fn test_odd_chunk_sizes() {
    let path = setup_test_file("streaming_odd_sizes.tdms");
    const TOTAL: usize = 100;
    
    {
        let mut writer = TdmsWriter::create(&path).unwrap();
        writer.create_channel("G", "C", DataType::F64).unwrap();
        let data = vec![1.0; TOTAL];
        writer.write_channel_data("G", "C", &data).unwrap();
        writer.flush().unwrap();
    }

    {
        let mut reader = TdmsReader::open(&path).unwrap();
        
        // Chunk size 33: 33, 33, 33, 1
        let iterator = reader.iter_channel_data::<f64>("G", "C", 33).unwrap();
        let lengths: Vec<usize> = iterator.map(|c| c.unwrap().len()).collect();
        
        assert_eq!(lengths, vec![33, 33, 33, 1]);
        assert_eq!(lengths.iter().sum::<usize>(), TOTAL);
    }
    
    cleanup_test_file(&path);
}