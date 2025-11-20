// src/types.rs (UPDATE - fix for test compatibility)
use byteorder::{ByteOrder, LittleEndian, BigEndian};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use bytemuck::{Pod, Zeroable};

/// TDMS data type enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum DataType {
    Void = 0,
    I8 = 1,
    I16 = 2,
    I32 = 3,
    I64 = 4,
    U8 = 5,
    U16 = 6,
    U32 = 7,
    U64 = 8,
    SingleFloat = 9,
    DoubleFloat = 10,
    String = 0x20,
    Boolean = 0x21,
    TimeStamp = 0x44,
    ComplexSingleFloat = 0x08000c,
    ComplexDoubleFloat = 0x10000d,
    DAQmxRawData = 0xFFFFFFFF,
}

// Add convenient aliases
impl DataType {
    pub const F32: DataType = DataType::SingleFloat;
    pub const F64: DataType = DataType::DoubleFloat;
}

impl DataType {
    /// Get the fixed size of this data type in bytes, or None if variable-sized
    pub fn fixed_size(&self) -> Option<usize> {
        match self {
            DataType::Void => Some(0),
            DataType::I8 | DataType::U8 | DataType::Boolean => Some(1),
            DataType::I16 | DataType::U16 => Some(2),
            DataType::I32 | DataType::U32 | DataType::SingleFloat => Some(4),
            DataType::I64 | DataType::U64 | DataType::DoubleFloat => Some(8),
            DataType::ComplexSingleFloat => Some(8),
            DataType::TimeStamp | DataType::ComplexDoubleFloat => Some(16),
            DataType::String | DataType::DAQmxRawData => None,
        }
    }
    
    pub fn from_u32(value: u32) -> Option<Self> {
        match value {
            0 => Some(DataType::Void),
            1 => Some(DataType::I8),
            2 => Some(DataType::I16),
            3 => Some(DataType::I32),
            4 => Some(DataType::I64),
            5 => Some(DataType::U8),
            6 => Some(DataType::U16),
            7 => Some(DataType::U32),
            8 => Some(DataType::U64),
            9 => Some(DataType::SingleFloat),
            10 => Some(DataType::DoubleFloat),
            0x20 => Some(DataType::String),
            0x21 => Some(DataType::Boolean),
            0x44 => Some(DataType::TimeStamp),
            0x08000c => Some(DataType::ComplexSingleFloat),
            0x10000d => Some(DataType::ComplexDoubleFloat),
            0xFFFFFFFF => Some(DataType::DAQmxRawData),
            _ => None,
        }
    }

    /// Map DAQmx internal type codes to TDMS DataType
    pub fn from_daqmx_type_code(code: u32) -> Option<Self> {
        match code {
            0 => Some(DataType::U8),
            1 => Some(DataType::I8),
            2 => Some(DataType::U16),
            3 => Some(DataType::I16),
            4 => Some(DataType::U32),
            5 => Some(DataType::I32),
            6 => Some(DataType::U64),
            7 => Some(DataType::I64),
            8 => Some(DataType::SingleFloat),
            9 => Some(DataType::DoubleFloat),
            0xFFFFFFFF => Some(DataType::TimeStamp), 
            _ => None,
        }
    }
    
    /// Check if this is a numeric type
    pub fn is_numeric(&self) -> bool {
        matches!(self,
            DataType::I8 | DataType::I16 | DataType::I32 | DataType::I64 |
            DataType::U8 | DataType::U16 | DataType::U32 | DataType::U64 |
            DataType::SingleFloat | DataType::DoubleFloat
        )
    }
    
    /// Check if this is an integer type
    pub fn is_integer(&self) -> bool {
        matches!(self,
            DataType::I8 | DataType::I16 | DataType::I32 | DataType::I64 |
            DataType::U8 | DataType::U16 | DataType::U32 | DataType::U64
        )
    }
    
    /// Check if this is a floating point type
    pub fn is_float(&self) -> bool {
        matches!(self, DataType::SingleFloat | DataType::DoubleFloat)
    }
    
    /// Check if this is a complex type
    pub fn is_complex(&self) -> bool {
        matches!(self, DataType::ComplexSingleFloat | DataType::ComplexDoubleFloat)
    }
    
    /// Get the name of the data type as a string
    pub fn name(&self) -> &'static str {
        match self {
            DataType::Void => "void",
            DataType::I8 => "i8",
            DataType::I16 => "i16",
            DataType::I32 => "i32",
            DataType::I64 => "i64",
            DataType::U8 => "u8",
            DataType::U16 => "u16",
            DataType::U32 => "u32",
            DataType::U64 => "u64",
            DataType::SingleFloat => "f32",
            DataType::DoubleFloat => "f64",
            DataType::String => "string",
            DataType::Boolean => "bool",
            DataType::TimeStamp => "timestamp",
            DataType::ComplexSingleFloat => "complex_f32",
            DataType::ComplexDoubleFloat => "complex_f64",
            DataType::DAQmxRawData => "daqmx_raw",
        }
    }
}

/// Table of Contents flags
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TocFlags(u32);

impl TocFlags {
    pub const METADATA: u32 = 1 << 1;
    pub const NEW_OBJ_LIST: u32 = 1 << 2;
    pub const RAW_DATA: u32 = 1 << 3;
    pub const INTERLEAVED: u32 = 1 << 5;
    pub const BIG_ENDIAN: u32 = 1 << 6;
    pub const DAQMX_RAW_DATA: u32 = 1 << 7;
    
    pub fn new(flags: u32) -> Self {
        TocFlags(flags)
    }
    
    pub fn empty() -> Self {
        TocFlags(0)
    }
    
    pub fn has_metadata(&self) -> bool {
        self.0 & Self::METADATA != 0
    }
    
    pub fn has_new_obj_list(&self) -> bool {
        self.0 & Self::NEW_OBJ_LIST != 0
    }
    
    pub fn has_raw_data(&self) -> bool {
        self.0 & Self::RAW_DATA != 0
    }
    
    pub fn is_interleaved(&self) -> bool {
        self.0 & Self::INTERLEAVED != 0
    }
    
    pub fn is_big_endian(&self) -> bool {
        self.0 & Self::BIG_ENDIAN != 0
    }
    
    pub fn has_daqmx_data(&self) -> bool {
        self.0 & Self::DAQMX_RAW_DATA != 0
    }
    
    pub fn set_metadata(&mut self, value: bool) {
        if value {
            self.0 |= Self::METADATA;
        } else {
            self.0 &= !Self::METADATA;
        }
    }
    
    pub fn set_new_obj_list(&mut self, value: bool) {
        if value {
            self.0 |= Self::NEW_OBJ_LIST;
        } else {
            self.0 &= !Self::NEW_OBJ_LIST;
        }
    }
    
    pub fn set_raw_data(&mut self, value: bool) {
        if value {
            self.0 |= Self::RAW_DATA;
        } else {
            self.0 &= !Self::RAW_DATA;
        }
    }
    
    pub fn set_interleaved(&mut self, value: bool) {
        if value {
            self.0 |= Self::INTERLEAVED;
        } else {
            self.0 &= !Self::INTERLEAVED;
        }
    }
    
    pub fn set_big_endian(&mut self, value: bool) {
        if value {
            self.0 |= Self::BIG_ENDIAN;
        } else {
            self.0 &= !Self::BIG_ENDIAN;
        }
    }
    
    pub fn raw_value(&self) -> u32 {
        self.0
    }
}

/// TDMS timestamp (seconds since 1904-01-01 00:00:00 UTC)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Pod, Zeroable)] // <-- FIX: ADDED Default
#[repr(C)]
pub struct Timestamp {
    /// Fractions of a second (units of 2^-64)
    pub fractions: u64,
    /// Seconds since epoch (1904-01-01)
    pub seconds: i64,
}

impl Timestamp {
    const EPOCH_OFFSET_SECONDS: i64 = 2082844800; // 1904 to 1970
    
    pub fn now() -> Self {
        let duration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO);
        
        let unix_seconds = duration.as_secs() as i64;
        let nanos = duration.subsec_nanos() as u64;
        
        let seconds = unix_seconds + Self::EPOCH_OFFSET_SECONDS;
        let fractions = (nanos as u128 * (1u128 << 64) / 1_000_000_000) as u64;
        
        Timestamp { seconds, fractions }
    }
    
    pub fn from_system_time(time: SystemTime) -> Self {
        let duration = time.duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO);
        let unix_seconds = duration.as_secs() as i64;
        let nanos = duration.subsec_nanos() as u64;
        
        let seconds = unix_seconds + Self::EPOCH_OFFSET_SECONDS;
        let fractions = (nanos as u128 * (1u128 << 64) / 1_000_000_000) as u64;
        
        Timestamp { seconds, fractions }
    }
    
    pub fn to_bytes_le(&self) -> [u8; 16] {
        let mut bytes = [0u8; 16];
        LittleEndian::write_u64(&mut bytes[0..8], self.fractions);
        LittleEndian::write_i64(&mut bytes[8..16], self.seconds);
        bytes
    }
    
    pub fn to_bytes_be(&self) -> [u8; 16] {
        let mut bytes = [0u8; 16];
        BigEndian::write_i64(&mut bytes[0..8], self.seconds);
        BigEndian::write_u64(&mut bytes[8..16], self.fractions);
        bytes
    }
    
    pub fn from_bytes_le(bytes: &[u8; 16]) -> Self {
        let fractions = LittleEndian::read_u64(&bytes[0..8]);
        let seconds = LittleEndian::read_i64(&bytes[8..16]);
        Timestamp { seconds, fractions }
    }
    
    pub fn from_bytes_be(bytes: &[u8; 16]) -> Self {
        let seconds = BigEndian::read_i64(&bytes[0..8]);
        let fractions = BigEndian::read_u64(&bytes[8..16]);
        Timestamp { seconds, fractions }
    }

    pub fn to_system_time(&self) -> SystemTime {
        let unix_seconds = self.seconds - Self::EPOCH_OFFSET_SECONDS;
        let nanos = ((self.fractions as u128 * 1_000_000_000) / (1u128 << 64)) as u32;
        let duration = Duration::new(unix_seconds as u64, nanos);
        UNIX_EPOCH + duration
    }

    #[cfg(test)]
    pub fn to_date_time(&self) -> chrono::DateTime<chrono::Utc> {
        let st = self.to_system_time();
        chrono::DateTime::from_timestamp(
            st.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
            st.duration_since(UNIX_EPOCH).unwrap().subsec_nanos(),
        ).unwrap()
    }
}

/// Property value that can be attached to objects
#[derive(Debug, Clone)]
pub enum PropertyValue {
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    Float(f32),
    Double(f64),
    String(String),
    Boolean(bool),
    Timestamp(Timestamp),
}

// -- FIX: ADDED Manual implementation of PartialEq --
impl PartialEq for PropertyValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (PropertyValue::I8(a), PropertyValue::I8(b)) => a == b,
            (PropertyValue::I16(a), PropertyValue::I16(b)) => a == b,
            (PropertyValue::I32(a), PropertyValue::I32(b)) => a == b,
            (PropertyValue::I64(a), PropertyValue::I64(b)) => a == b,
            (PropertyValue::U8(a), PropertyValue::U8(b)) => a == b,
            (PropertyValue::U16(a), PropertyValue::U16(b)) => a == b,
            (PropertyValue::U32(a), PropertyValue::U32(b)) => a == b,
            (PropertyValue::U64(a), PropertyValue::U64(b)) => a == b,
            (PropertyValue::Float(a), PropertyValue::Float(b)) => a == b,
            (PropertyValue::Double(a), PropertyValue::Double(b)) => a == b,
            (PropertyValue::String(a), PropertyValue::String(b)) => a == b,
            (PropertyValue::Boolean(a), PropertyValue::Boolean(b)) => a == b,
            (PropertyValue::Timestamp(a), PropertyValue::Timestamp(b)) => a == b,
            _ => false, // Different types
        }
    }
}
// -- END FIX --


impl PropertyValue {
    pub fn data_type(&self) -> DataType {
        match self {
            PropertyValue::I8(_) => DataType::I8,
            PropertyValue::I16(_) => DataType::I16,
            PropertyValue::I32(_) => DataType::I32,
            PropertyValue::I64(_) => DataType::I64,
            PropertyValue::U8(_) => DataType::U8,
            PropertyValue::U16(_) => DataType::U16,
            PropertyValue::U32(_) => DataType::U32,
            PropertyValue::U64(_) => DataType::U64,
            PropertyValue::Float(_) => DataType::SingleFloat,
            PropertyValue::Double(_) => DataType::DoubleFloat,
            PropertyValue::String(_) => DataType::String,
            PropertyValue::Boolean(_) => DataType::Boolean,
            PropertyValue::Timestamp(_) => DataType::TimeStamp,
        }
    }
    
    pub fn write_to<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        use byteorder::WriteBytesExt;
        
        match self {
            PropertyValue::I8(v) => writer.write_i8(*v),
            PropertyValue::I16(v) => writer.write_i16::<LittleEndian>(*v),
            PropertyValue::I32(v) => writer.write_i32::<LittleEndian>(*v),
            PropertyValue::I64(v) => writer.write_i64::<LittleEndian>(*v),
            PropertyValue::U8(v) => writer.write_u8(*v),
            PropertyValue::U16(v) => writer.write_u16::<LittleEndian>(*v),
            PropertyValue::U32(v) => writer.write_u32::<LittleEndian>(*v),
            PropertyValue::U64(v) => writer.write_u64::<LittleEndian>(*v),
            PropertyValue::Float(v) => writer.write_f32::<LittleEndian>(*v),
            PropertyValue::Double(v) => writer.write_f64::<LittleEndian>(*v),
            PropertyValue::Boolean(v) => writer.write_u8(if *v { 1 } else { 0 }),
            PropertyValue::Timestamp(ts) => writer.write_all(&ts.to_bytes_le()),
            PropertyValue::String(s) => {
                let bytes = s.as_bytes();
                writer.write_u32::<LittleEndian>(bytes.len() as u32)?;
                writer.write_all(bytes)
            }
        }
    }
}

/// Represents a property with name and value
#[derive(Debug, Clone, PartialEq)] // <-- FIX: ADDED PartialEq
pub struct Property {
    pub name: String,
    pub value: PropertyValue,
}

impl Property {
    pub fn new(name: impl Into<String>, value: PropertyValue) -> Self {
        Property {
            name: name.into(),
            value,
        }
    }
}