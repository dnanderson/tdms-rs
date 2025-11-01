// examples/basic_write.rs
use tdms_rs::*;

fn main() -> Result<()> {
    let mut writer = TdmsWriter::create("examples/output/basic.tdms")?;
    
    // Set file metadata
    writer.set_file_property("title", PropertyValue::String("Basic Example".into()));
    writer.set_file_property("author", PropertyValue::String("TDMS-RS".into()));
    
    // Create channels
    writer.create_channel("Measurements", "Temperature", DataType::DoubleFloat)?;
    writer.create_channel("Measurements", "Pressure", DataType::DoubleFloat)?;
    
    // Generate and write data
    let temp_data: Vec<f64> = (0..1000).map(|i| 20.0 + (i as f64 * 0.01).sin() * 5.0).collect();
    let pressure_data: Vec<f64> = (0..1000).map(|i| 101.3 + (i as f64 * 0.02).cos() * 2.0).collect();
    
    writer.write_channel_data("Measurements", "Temperature", &temp_data)?;
    writer.write_channel_data("Measurements", "Pressure", &pressure_data)?;
    
    writer.flush()?;
    
    println!("Successfully wrote TDMS file with {} temperature and {} pressure values",
             temp_data.len(), pressure_data.len());
    
    Ok(())
}