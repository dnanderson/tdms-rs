// src/raw_data/buffer.rs
use bytes::{BytesMut, BufMut};
use crate::types::DataType;
use crate::error::{TdmsError, Result};
use std::mem;

/// Efficient buffer for accumulating raw data before writing to TDMS file
/// 
/// This buffer accumulates values in the correct TDMS binary format and tracks
/// the number of values written. It uses `BytesMut` for efficient memory management
/// and to minimize allocations.
/// 
/// # Example
/// 
/// ```
/// use tdms_rs::raw_data::RawDataBuffer;
/// use tdms_rs::types::DataType;
/// 
/// let mut buffer = RawDataBuffer::new(DataType::I32);
/// buffer.write_i32(42).unwrap();
/// buffer.write_i32(100).unwrap();
/// 
/// assert_eq!(buffer.value_count(), 2);
/// assert_eq!(buffer.byte_len(), 8);
/// ```
pub struct RawDataBuffer {
    buffer: BytesMut,
    data_type: DataType,
    value_count: u64,
}

impl RawDataBuffer {
    /// Create a new buffer with default capacity (8192 bytes)
    /// 
    /// # Arguments
    /// 
    /// * `data_type` - The TDMS data type this buffer will hold
    pub fn new(data_type: DataType) -> Self {
        Self::with_capacity(data_type, 8192)
    }
    
    /// Create a new buffer with specified capacity
    /// 
    /// # Arguments
    /// 
    /// * `data_type` - The TDMS data type this buffer will hold
    /// * `capacity` - Initial capacity in bytes
    pub fn with_capacity(data_type: DataType, capacity: usize) -> Self {
        RawDataBuffer {
            buffer: BytesMut::with_capacity(capacity),
            data_type,
            value_count: 0,
        }
    }
    
    /// Write a single i8 value
    pub fn write_i8(&mut self, value: i8) -> Result<()> {
        self.check_type(DataType::I8)?;
        self.buffer.put_i8(value);
        self.value_count += 1;
        Ok(())
    }
    
    /// Write a single i16 value (little-endian)
    pub fn write_i16(&mut self, value: i16) -> Result<()> {
        self.check_type(DataType::I16)?;
        self.buffer.put_i16_le(value);
        self.value_count += 1;
        Ok(())
    }
    
    /// Write a single i32 value (little-endian)
    pub fn write_i32(&mut self, value: i32) -> Result<()> {
        self.check_type(DataType::I32)?;
        self.buffer.put_i32_le(value);
        self.value_count += 1;
        Ok(())
    }
    
    /// Write a single i64 value (little-endian)
    pub fn write_i64(&mut self, value: i64) -> Result<()> {
        self.check_type(DataType::I64)?;
        self.buffer.put_i64_le(value);
        self.value_count += 1;
        Ok(())
    }
    
    /// Write a single u8 value
    pub fn write_u8(&mut self, value: u8) -> Result<()> {
        self.check_type(DataType::U8)?;
        self.buffer.put_u8(value);
        self.value_count += 1;
        Ok(())
    }
    
    /// Write a single u16 value (little-endian)
    pub fn write_u16(&mut self, value: u16) -> Result<()> {
        self.check_type(DataType::U16)?;
        self.buffer.put_u16_le(value);
        self.value_count += 1;
        Ok(())
    }
    
    /// Write a single u32 value (little-endian)
    pub fn write_u32(&mut self, value: u32) -> Result<()> {
        self.check_type(DataType::U32)?;
        self.buffer.put_u32_le(value);
        self.value_count += 1;
        Ok(())
    }
    
    /// Write a single u64 value (little-endian)
    pub fn write_u64(&mut self, value: u64) -> Result<()> {
        self.check_type(DataType::U64)?;
        self.buffer.put_u64_le(value);
        self.value_count += 1;
        Ok(())
    }
    
    /// Write a single f32 value (little-endian)
    pub fn write_f32(&mut self, value: f32) -> Result<()> {
        self.check_type(DataType::SingleFloat)?;
        self.buffer.put_f32_le(value);
        self.value_count += 1;
        Ok(())
    }
    
    /// Write a single f64 value (little-endian)
    pub fn write_f64(&mut self, value: f64) -> Result<()> {
        self.check_type(DataType::DoubleFloat)?;
        self.buffer.put_f64_le(value);
        self.value_count += 1;
        Ok(())
    }
    
    /// Write a single boolean value (as 1 byte: 0 or 1)
    pub fn write_bool(&mut self, value: bool) -> Result<()> {
        self.check_type(DataType::Boolean)?;
        self.buffer.put_u8(if value { 1 } else { 0 });
        self.value_count += 1;
        Ok(())
    }
    
    /// Write a timestamp value
    pub fn write_timestamp(&mut self, value: crate::types::Timestamp) -> Result<()> {
        self.check_type(DataType::TimeStamp)?;
        self.buffer.extend_from_slice(&value.to_bytes_le());
        self.value_count += 1;
        Ok(())
    }
    
    /// Write a slice of values efficiently (zero-copy when possible)
    /// 
    /// This is the most efficient way to write multiple values of the same type.
    /// It performs direct memory copies when safe to do so.
    /// 
    /// # Type Parameters
    /// 
    /// * `T` - The type of values to write (must match the buffer's data type)
    /// 
    /// # Arguments
    /// 
    /// * `values` - Slice of values to write
    /// 
    /// # Example
    /// 
    /// ```
    /// use tdms_rs::raw_data::RawDataBuffer;
    /// use tdms_rs::types::DataType;
    /// 
    /// let mut buffer = RawDataBuffer::new(DataType::F64);
    /// let data: Vec<f64> = vec![1.0, 2.0, 3.0, 4.0, 5.0];
    /// buffer.write_slice(&data).unwrap();
    /// 
    /// assert_eq!(buffer.value_count(), 5);
    /// ```
    pub fn write_slice<T: Copy>(&mut self, values: &[T]) -> Result<()> {
        let type_size = mem::size_of::<T>();
        
        // Verify the type size matches the expected data type size
        let expected_size = self.data_type.fixed_size().ok_or_else(|| {
            TdmsError::TypeMismatch {
                expected: format!("{:?}", self.data_type),
                found: "Variable-size type".to_string(),
            }
        })?;
        
        if type_size != expected_size {
            return Err(TdmsError::TypeMismatch {
                expected: format!("{:?} (size {})", self.data_type, expected_size),
                found: format!("Type with size {}", type_size),
            });
        }
        
        if values.is_empty() {
            return Ok(());
        }
        
        // Direct memory copy (safe for primitive types with correct alignment)
        let bytes = unsafe {
            std::slice::from_raw_parts(
                values.as_ptr() as *const u8,
                values.len() * type_size,
            )
        };
        
        self.buffer.extend_from_slice(bytes);
        self.value_count += values.len() as u64;
        Ok(())
    }
    
    /// Write strings with proper TDMS string array format (cumulative end offsets)
    /// 
    /// TDMS stores string arrays with cumulative end offsets followed by concatenated data.
    /// For strings ["Hello", "World", "!"], this writes:
    /// - Offsets: [5, 10, 11] (4 bytes each, little-endian)
    /// - Data: "HelloWorld!" (raw UTF-8 bytes)
    /// 
    /// # Arguments
    /// 
    /// * `strings` - Slice of strings to write
    /// 
    /// # Example
    /// 
    /// ```
    /// use tdms_rs::raw_data::RawDataBuffer;
    /// use tdms_rs::types::DataType;
    /// 
    /// let mut buffer = RawDataBuffer::new(DataType::String);
    /// let strings = vec!["Hello", "World", "!"];
    /// buffer.write_strings(&strings).unwrap();
    /// 
    /// assert_eq!(buffer.value_count(), 3);
    /// // 3 offsets (12 bytes) + "HelloWorld!" (11 bytes) = 23 bytes
    /// assert_eq!(buffer.byte_len(), 23);
    /// ```
    pub fn write_strings(&mut self, strings: &[impl AsRef<str>]) -> Result<()> {
        self.check_type(DataType::String)?;
        
        if strings.is_empty() {
            return Ok(());
        }
        
        // Calculate cumulative offsets
        let mut cumulative_offset = 0u32;
        let mut offsets = Vec::with_capacity(strings.len());
        
        for s in strings {
            let bytes = s.as_ref().as_bytes();
            cumulative_offset = cumulative_offset.checked_add(bytes.len() as u32)
                .ok_or_else(|| TdmsError::BufferOverflow {
                    attempted: bytes.len(),
                    capacity: u32::MAX as usize,
                })?;
            offsets.push(cumulative_offset);
        }
        
        // Write offsets (little-endian)
        for offset in offsets {
            self.buffer.put_u32_le(offset);
        }
        
        // Write concatenated string data
        for s in strings {
            self.buffer.extend_from_slice(s.as_ref().as_bytes());
        }
        
        self.value_count += strings.len() as u64;
        Ok(())
    }
    
    /// Write an empty string array
    /// 
    /// Writes `count` empty strings efficiently
    pub fn write_empty_strings(&mut self, count: usize) -> Result<()> {
        self.check_type(DataType::String)?;
        
        if count == 0 {
            return Ok(());
        }
        
        // All offsets will be 0 for empty strings
        for _ in 0..count {
            self.buffer.put_u32_le(0);
        }
        
        self.value_count += count as u64;
        Ok(())
    }
    
    /// Get the data type of this buffer
    pub fn data_type(&self) -> DataType {
        self.data_type
    }
    
    /// Get the number of values written to this buffer
    pub fn value_count(&self) -> u64 {
        self.value_count
    }
    
    /// Get the total size in bytes
    pub fn byte_len(&self) -> usize {
        self.buffer.len()
    }
    
    /// Check if the buffer is empty
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }
    
    /// Get the buffer capacity
    pub fn capacity(&self) -> usize {
        self.buffer.capacity()
    }
    
    /// Get the buffer contents as a byte slice
    pub fn as_bytes(&self) -> &[u8] {
        &self.buffer
    }
    
    /// Clear the buffer, resetting value count and removing all data
    pub fn clear(&mut self) {
        self.buffer.clear();
        self.value_count = 0;
    }
    
    /// Take the buffer contents, leaving an empty buffer
    /// 
    /// This is useful when you want to transfer ownership of the data
    /// without copying. The buffer is reset to empty after this call.
    pub fn take(&mut self) -> BytesMut {
        self.value_count = 0;
        mem::take(&mut self.buffer)
    }
    
    /// Reserve additional capacity
    /// 
    /// # Arguments
    /// 
    /// * `additional` - Number of additional bytes to reserve
    pub fn reserve(&mut self, additional: usize) {
        self.buffer.reserve(additional);
    }
    
    /// Shrink the buffer capacity to fit the current data
    pub fn shrink_to_fit(&mut self) {
        // BytesMut doesn't have shrink_to_fit, but we can recreate if needed
        if self.buffer.capacity() > self.buffer.len() * 2 {
            let new_buffer = BytesMut::from(self.buffer.as_ref());
            self.buffer = new_buffer;
        }
    }
    
    /// Check if the buffer's data type matches the expected type
    fn check_type(&self, expected: DataType) -> Result<()> {
        if self.data_type != expected {
            Err(TdmsError::TypeMismatch {
                expected: format!("{:?}", self.data_type),
                found: format!("{:?}", expected),
            })
        } else {
            Ok(())
        }
    }
}

impl Default for RawDataBuffer {
    fn default() -> Self {
        Self::new(DataType::Void)
    }
}

// Implement Debug manually to avoid printing large buffers
impl std::fmt::Debug for RawDataBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RawDataBuffer")
            .field("data_type", &self.data_type)
            .field("value_count", &self.value_count)
            .field("byte_len", &self.buffer.len())
            .field("capacity", &self.buffer.capacity())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Timestamp;

    #[test]
    fn test_write_integers() {
        let mut buffer = RawDataBuffer::new(DataType::I32);
        
        buffer.write_i32(42).unwrap();
        buffer.write_i32(-100).unwrap();
        buffer.write_i32(0).unwrap();
        
        assert_eq!(buffer.value_count(), 3);
        assert_eq!(buffer.byte_len(), 12);
        assert!(!buffer.is_empty());
    }

    #[test]
    fn test_write_slice() {
        let mut buffer = RawDataBuffer::new(DataType::F64);
        let data: Vec<f64> = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        
        buffer.write_slice(&data).unwrap();
        
        assert_eq!(buffer.value_count(), 5);
        assert_eq!(buffer.byte_len(), 40); // 5 * 8 bytes
    }

    #[test]
    fn test_write_strings() {
        let mut buffer = RawDataBuffer::new(DataType::String);
        let strings = vec!["Hello", "World", "!"];
        
        buffer.write_strings(&strings).unwrap();
        
        assert_eq!(buffer.value_count(), 3);
        // 3 offsets (12 bytes) + "HelloWorld!" (11 bytes) = 23 bytes
        assert_eq!(buffer.byte_len(), 23);
        
        // Verify the offset values
        let bytes = buffer.as_bytes();
        assert_eq!(bytes[0..4], [5, 0, 0, 0]);  // First offset: 5
        assert_eq!(bytes[4..8], [10, 0, 0, 0]); // Second offset: 10
        assert_eq!(bytes[8..12], [11, 0, 0, 0]); // Third offset: 11
        assert_eq!(&bytes[12..23], b"HelloWorld!");
    }

    #[test]
    fn test_write_empty_strings() {
        let mut buffer = RawDataBuffer::new(DataType::String);
        let strings = vec!["", "", ""];
        
        buffer.write_strings(&strings).unwrap();
        
        assert_eq!(buffer.value_count(), 3);
        assert_eq!(buffer.byte_len(), 12); // Only offsets, all 0
    }

    #[test]
    fn test_write_booleans() {
        let mut buffer = RawDataBuffer::new(DataType::Boolean);
        
        buffer.write_bool(true).unwrap();
        buffer.write_bool(false).unwrap();
        buffer.write_bool(true).unwrap();
        
        assert_eq!(buffer.value_count(), 3);
        assert_eq!(buffer.byte_len(), 3);
        
        let bytes = buffer.as_bytes();
        assert_eq!(bytes[0], 1);
        assert_eq!(bytes[1], 0);
        assert_eq!(bytes[2], 1);
    }

    #[test]
    fn test_type_mismatch() {
        let mut buffer = RawDataBuffer::new(DataType::I32);
        
        let result = buffer.write_f64(3.14);
        assert!(result.is_err());
        
        match result {
            Err(TdmsError::TypeMismatch { .. }) => (),
            _ => panic!("Expected TypeMismatch error"),
        }
    }

    #[test]
    fn test_clear() {
        let mut buffer = RawDataBuffer::new(DataType::I32);
        
        buffer.write_i32(42).unwrap();
        buffer.write_i32(100).unwrap();
        
        assert_eq!(buffer.value_count(), 2);
        assert_eq!(buffer.byte_len(), 8);
        
        buffer.clear();
        
        assert_eq!(buffer.value_count(), 0);
        assert_eq!(buffer.byte_len(), 0);
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_take() {
        let mut buffer = RawDataBuffer::new(DataType::I32);
        
        buffer.write_i32(42).unwrap();
        buffer.write_i32(100).unwrap();
        
        let taken = buffer.take();
        
        assert_eq!(taken.len(), 8);
        assert_eq!(buffer.value_count(), 0);
        assert_eq!(buffer.byte_len(), 0);
    }

    #[test]
    fn test_write_timestamp() {
        let mut buffer = RawDataBuffer::new(DataType::TimeStamp);
        let ts = Timestamp::now();
        
        buffer.write_timestamp(ts).unwrap();
        
        assert_eq!(buffer.value_count(), 1);
        assert_eq!(buffer.byte_len(), 16);
    }

    #[test]
    fn test_multiple_data_types() {
        // Test different numeric types
        let types_and_sizes = vec![
            (DataType::I8, 1),
            (DataType::I16, 2),
            (DataType::I32, 4),
            (DataType::I64, 8),
            (DataType::U8, 1),
            (DataType::U16, 2),
            (DataType::U32, 4),
            (DataType::U64, 8),
            (DataType::SingleFloat, 4),
            (DataType::DoubleFloat, 8),
            (DataType::Boolean, 1),
        ];
        
        for (data_type, expected_size) in types_and_sizes {
            let buffer = RawDataBuffer::new(data_type);
            assert_eq!(buffer.data_type(), data_type);
            
            if let Some(size) = data_type.fixed_size() {
                assert_eq!(size, expected_size);
            }
        }
    }

    #[test]
    fn test_capacity_management() {
        let mut buffer = RawDataBuffer::with_capacity(DataType::I32, 1024);
        
        assert!(buffer.capacity() >= 1024);
        
        buffer.reserve(2048);
        assert!(buffer.capacity() >= 2048);
    }

    #[test]
    fn test_mixed_string_operations() {
        let mut buffer = RawDataBuffer::new(DataType::String);
        
        // Write some non-empty strings
        buffer.write_strings(&["Hello", "World"]).unwrap();
        assert_eq!(buffer.value_count(), 2);
        
        // Buffer should contain offsets and data
        let first_len = buffer.byte_len();
        
        // Write more strings
        buffer.write_strings(&["Test", ""]).unwrap();
        assert_eq!(buffer.value_count(), 4);
        assert!(buffer.byte_len() > first_len);
    }

    #[test]
    fn test_debug_formatting() {
        let mut buffer = RawDataBuffer::new(DataType::I32);
        buffer.write_i32(42).unwrap();
        
        let debug_str = format!("{:?}", buffer);
        assert!(debug_str.contains("I32"));
        assert!(debug_str.contains("value_count: 1"));
    }
}