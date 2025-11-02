// src/raw_data/reader.rs
use crate::error::{TdmsError, Result};
use std::io::Read;
use byteorder::{ReadBytesExt, LittleEndian, BigEndian};

/// Helper functions for reading raw data from TDMS files with proper endianness
/// 
/// This provides efficient reading of typed data from binary streams,
/// handling both little-endian and big-endian formats.
pub struct RawDataReader;

impl RawDataReader {
    /// Read an array of values from a stream
    /// 
    /// # Type Parameters
    /// 
    /// * `T` - The type to read (must be Copy + Default)
    /// * `R` - The reader type (must implement Read)
    /// 
    /// # Arguments
    /// 
    /// * `reader` - The stream to read from
    /// * `count` - Number of values to read
    /// * `is_big_endian` - Whether the data is big-endian (true) or little-endian (false)
    /// 
    /// # Returns
    /// 
    /// A vector containing the read values
    /// 
    /// # Example
    /// 
    /// ```
    /// use tdms_rs::raw_data::RawDataReader;
    /// use std::io::Cursor;
    /// 
    /// let data = vec![1u8, 0, 0, 0, 2, 0, 0, 0];
    /// let mut cursor = Cursor::new(data);
    /// 
    /// let values: Vec<i32> = RawDataReader::read_values(&mut cursor, 2, false).unwrap();
    /// assert_eq!(values, vec![1, 2]);
    /// ```
    pub fn read_values<T, R: Read>(
        reader: &mut R,
        count: usize,
        is_big_endian: bool,
    ) -> Result<Vec<T>>
    where
        T: Copy + Default,
    {
        if count == 0 {
            return Ok(Vec::new());
        }

        let mut result = vec![T::default(); count];
        let size = std::mem::size_of::<T>();
        
        let byte_count = count * size;
        
        // Read all bytes at once
        let mut bytes = vec![0u8; byte_count];
        reader.read_exact(&mut bytes)?;
        
        // Swap endianness if needed (only for multi-byte types)
        if is_big_endian && size > 1 {
            for chunk in bytes.chunks_exact_mut(size) {
                chunk.reverse();
            }
        }
        
        // Copy bytes into result array
        unsafe {
            std::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                result.as_mut_ptr() as *mut u8,
                byte_count,
            );
        }
        
        Ok(result)
    }
    
    /// Read a string array from a stream
    /// 
    /// TDMS stores string arrays with cumulative end offsets followed by concatenated data.
    /// This method handles the TDMS string format correctly.
    /// 
    /// # Arguments
    /// 
    /// * `reader` - The stream to read from
    /// * `count` - Number of strings to read
    /// * `is_big_endian` - Whether offsets are big-endian
    /// 
    /// # Returns
    /// 
    /// A vector of strings
    /// 
    /// # Example
    /// 
    /// ```
    /// use tdms_rs::raw_data::RawDataReader;
    /// use std::io::Cursor;
    /// 
    /// // Data for ["Hello", "World"]
    /// // Offsets: [5, 10] then data: "HelloWorld"
    /// let data = vec![
    ///     5, 0, 0, 0,  // offset 5
    ///     10, 0, 0, 0, // offset 10
    ///     b'H', b'e', b'l', b'l', b'o',
    ///     b'W', b'o', b'r', b'l', b'd',
    /// ];
    /// let mut cursor = Cursor::new(data);
    /// 
    /// let strings = RawDataReader::read_strings(&mut cursor, 2, false).unwrap();
    /// assert_eq!(strings, vec!["Hello", "World"]);
    /// ```
    pub fn read_strings<R: Read>(
        reader: &mut R,
        count: usize,
        is_big_endian: bool,
    ) -> Result<Vec<String>> {
        if count == 0 {
            return Ok(Vec::new());
        }

        // Read cumulative end offsets
        let mut offsets = Vec::with_capacity(count);
        for _ in 0..count {
            let offset = if is_big_endian {
                reader.read_u32::<BigEndian>()?
            } else {
                reader.read_u32::<LittleEndian>()?
            };
            offsets.push(offset);
        }
        
        // Total bytes to read
        let total_bytes = offsets.last().copied().unwrap_or(0) as usize;
        
        if total_bytes == 0 {
            // All strings are empty
            return Ok(vec![String::new(); count]);
        }
        
        // Read all string data at once
        let mut string_data = vec![0u8; total_bytes];
        reader.read_exact(&mut string_data)?;
        
        // Extract individual strings
        let mut result = Vec::with_capacity(count);
        let mut start = 0usize;
        
        for &end in &offsets {
            let end = end as usize;
            
            if end < start {
                return Err(TdmsError::InvalidUtf8);
            }
            
            if end > total_bytes {
                return Err(TdmsError::InvalidUtf8);
            }
            
            let length = end - start;
            
            if length > 0 {
                let bytes = &string_data[start..end];
                let s = String::from_utf8(bytes.to_vec())
                    .map_err(|_| TdmsError::InvalidUtf8)?;
                result.push(s);
            } else {
                result.push(String::new());
            }
            
            start = end;
        }
        
        Ok(result)
    }
    
    /// Read a single i8 value
    pub fn read_i8<R: Read>(reader: &mut R) -> Result<i8> {
        Ok(reader.read_i8()?)
    }
    
    /// Read a single i16 value
    pub fn read_i16<R: Read>(reader: &mut R, is_big_endian: bool) -> Result<i16> {
        if is_big_endian {
            Ok(reader.read_i16::<BigEndian>()?)
        } else {
            Ok(reader.read_i16::<LittleEndian>()?)
        }
    }
    
    /// Read a single i32 value
    pub fn read_i32<R: Read>(reader: &mut R, is_big_endian: bool) -> Result<i32> {
        if is_big_endian {
            Ok(reader.read_i32::<BigEndian>()?)
        } else {
            Ok(reader.read_i32::<LittleEndian>()?)
        }
    }
    
    /// Read a single i64 value
    pub fn read_i64<R: Read>(reader: &mut R, is_big_endian: bool) -> Result<i64> {
        if is_big_endian {
            Ok(reader.read_i64::<BigEndian>()?)
        } else {
            Ok(reader.read_i64::<LittleEndian>()?)
        }
    }
    
    /// Read a single u8 value
    pub fn read_u8<R: Read>(reader: &mut R) -> Result<u8> {
        Ok(reader.read_u8()?)
    }
    
    /// Read a single u16 value
    pub fn read_u16<R: Read>(reader: &mut R, is_big_endian: bool) -> Result<u16> {
        if is_big_endian {
            Ok(reader.read_u16::<BigEndian>()?)
        } else {
            Ok(reader.read_u16::<LittleEndian>()?)
        }
    }
    
    /// Read a single u32 value
    pub fn read_u32<R: Read>(reader: &mut R, is_big_endian: bool) -> Result<u32> {
        if is_big_endian {
            Ok(reader.read_u32::<BigEndian>()?)
        } else {
            Ok(reader.read_u32::<LittleEndian>()?)
        }
    }
    
    /// Read a single u64 value
    pub fn read_u64<R: Read>(reader: &mut R, is_big_endian: bool) -> Result<u64> {
        if is_big_endian {
            Ok(reader.read_u64::<BigEndian>()?)
        } else {
            Ok(reader.read_u64::<LittleEndian>()?)
        }
    }
    
    /// Read a single f32 value
    pub fn read_f32<R: Read>(reader: &mut R, is_big_endian: bool) -> Result<f32> {
        if is_big_endian {
            Ok(reader.read_f32::<BigEndian>()?)
        } else {
            Ok(reader.read_f32::<LittleEndian>()?)
        }
    }
    
    /// Read a single f64 value
    pub fn read_f64<R: Read>(reader: &mut R, is_big_endian: bool) -> Result<f64> {
        if is_big_endian {
            Ok(reader.read_f64::<BigEndian>()?)
        } else {
            Ok(reader.read_f64::<LittleEndian>()?)
        }
    }
    
    /// Read a single boolean value (1 byte)
    pub fn read_bool<R: Read>(reader: &mut R) -> Result<bool> {
        Ok(reader.read_u8()? != 0)
    }
    
    /// Read a timestamp value (16 bytes)
    pub fn read_timestamp<R: Read>(reader: &mut R, is_big_endian: bool) -> Result<crate::types::Timestamp> {
        let mut bytes = [0u8; 16];
        reader.read_exact(&mut bytes)?;
        
        if is_big_endian {
            Ok(crate::types::Timestamp::from_bytes_be(&bytes))
        } else {
            Ok(crate::types::Timestamp::from_bytes_le(&bytes))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_read_integers_little_endian() {
        let data = vec![
            1, 0, 0, 0,  // 1
            2, 0, 0, 0,  // 2
            3, 0, 0, 0,  // 3
        ];
        let mut cursor = Cursor::new(data);
        
        let values: Vec<i32> = RawDataReader::read_values(&mut cursor, 3, false).unwrap();
        assert_eq!(values, vec![1, 2, 3]);
    }

    #[test]
    fn test_read_integers_big_endian() {
        let data = vec![
            0, 0, 0, 1,  // 1
            0, 0, 0, 2,  // 2
            0, 0, 0, 3,  // 3
        ];
        let mut cursor = Cursor::new(data);
        
        let values: Vec<i32> = RawDataReader::read_values(&mut cursor, 3, true).unwrap();
        assert_eq!(values, vec![1, 2, 3]);
    }

    #[test]
    fn test_read_floats() {
        let data: Vec<u8> = vec![
            0, 0, 128, 63,  // 1.0 in f32 little-endian
            0, 0, 0, 64,    // 2.0 in f32 little-endian
        ];
        let mut cursor = Cursor::new(data);
        
        let values: Vec<f32> = RawDataReader::read_values(&mut cursor, 2, false).unwrap();
        assert!((values[0] - 1.0).abs() < 0.001);
        assert!((values[1] - 2.0).abs() < 0.001);
    }

    #[test]
    fn test_read_strings() {
        // Data for ["Hello", "World", "!"]
        let data = vec![
            5, 0, 0, 0,      // offset 5
            10, 0, 0, 0,     // offset 10
            11, 0, 0, 0,     // offset 11
            b'H', b'e', b'l', b'l', b'o',
            b'W', b'o', b'r', b'l', b'd',
            b'!',
        ];
        let mut cursor = Cursor::new(data);
        
        let strings = RawDataReader::read_strings(&mut cursor, 3, false).unwrap();
        assert_eq!(strings, vec!["Hello", "World", "!"]);
    }

    #[test]
    fn test_read_empty_strings() {
        // All strings are empty
        let data = vec![
            0, 0, 0, 0,  // offset 0
            0, 0, 0, 0,  // offset 0
            0, 0, 0, 0,  // offset 0
        ];
        let mut cursor = Cursor::new(data);
        
        let strings = RawDataReader::read_strings(&mut cursor, 3, false).unwrap();
        assert_eq!(strings, vec!["", "", ""]);
    }

    #[test]
    fn test_read_mixed_empty_strings() {
        // ["", "Hello", "", "World"]
        let data = vec![
            0, 0, 0, 0,      // offset 0 (empty)
            5, 0, 0, 0,      // offset 5
            5, 0, 0, 0,      // offset 5 (empty)
            10, 0, 0, 0,     // offset 10
            b'H', b'e', b'l', b'l', b'o',
            b'W', b'o', b'r', b'l', b'd',
        ];
        let mut cursor = Cursor::new(data);
        
        let strings = RawDataReader::read_strings(&mut cursor, 4, false).unwrap();
        assert_eq!(strings, vec!["", "Hello", "", "World"]);
    }

    #[test]
    fn test_read_single_values() {
        let data = vec![42u8];
        let mut cursor = Cursor::new(&data);
        let value = RawDataReader::read_i8(&mut cursor).unwrap();
        assert_eq!(value, 42);
        
        let data = vec![1, 0];
        let mut cursor = Cursor::new(&data);
        let value = RawDataReader::read_u16(&mut cursor, false).unwrap();
        assert_eq!(value, 1);
        
        let data = vec![1];
        let mut cursor = Cursor::new(&data);
        let value = RawDataReader::read_bool(&mut cursor).unwrap();
        assert_eq!(value, true);
        
        let data = vec![0];
        let mut cursor = Cursor::new(&data);
        let value = RawDataReader::read_bool(&mut cursor).unwrap();
        assert_eq!(value, false);
    }

    #[test]
    fn test_read_zero_count() {
        let data = vec![1u8, 2, 3];
        let mut cursor = Cursor::new(data);
        
        let values: Vec<i32> = RawDataReader::read_values(&mut cursor, 0, false).unwrap();
        assert_eq!(values.len(), 0);
        
        let strings = RawDataReader::read_strings(&mut cursor, 0, false).unwrap();
        assert_eq!(strings.len(), 0);
    }
}