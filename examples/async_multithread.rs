// examples/async_multithread.rs
#[cfg(feature = "async")]
use tdms_rs::*;
#[cfg(feature = "async")]
use tokio;
#[cfg(feature = "async")]
use std::sync::Arc;

#[cfg(feature = "async")]
#[tokio::main]
async fn main() -> Result<()> {
    let writer = Arc::new(AsyncTdmsWriter::create("examples/output/async.tdms").await?);
    
    writer.set_file_property("title", PropertyValue::String("Async Example".into()))?;
    
    // Spawn multiple tasks writing to different channels
    let mut handles = vec![];
    
    for i in 0..4 {
        let writer_clone = Arc::clone(&writer);
        let handle = tokio::spawn(async move {
            let channel_name = format!("Sensor{}", i);
            let data: Vec<f64> = (0..10000).map(|j| (i * 1000 + j) as f64).collect();
            
            writer_clone.write_channel_data(
                "Sensors",
                &channel_name,
                data,
                DataType::DoubleFloat
            ).await
        });
        handles.push(handle);
    }
    
    // Wait for all writes to complete
    for handle in handles {
        handle.await.unwrap()?;
    }
    
    writer.flush().await?;
    writer.close().await?;
    
    println!("Successfully wrote async TDMS file with 4 channels");
    
    Ok(())
}

#[cfg(not(feature = "async"))]
fn main() {
    println!("This example requires the 'async' feature.");
    println!("Run with: cargo run --example async_multithread --features async");
}