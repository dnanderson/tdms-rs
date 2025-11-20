// src/reader/daqmx.rs
use crate::error::{TdmsError, Result};
use crate::types::DataType;
use std::io::Read; // Removed Seek, SeekFrom
use byteorder::{ReadBytesExt, LittleEndian, BigEndian};

pub const FORMAT_CHANGING_SCALER: u32 = 0x00001269;
pub const DIGITAL_LINE_SCALER: u32 = 0x0000126A;

#[derive(Debug, Clone)]
pub struct DaqMxMetadata {
    pub chunk_size: u64,
    pub scalers: Vec<Scaler>,
    pub raw_data_widths: Vec<u32>,
}

#[derive(Debug, Clone)]
pub struct Scaler {
    pub scaler_type: u32,
    pub data_type: DataType,
    pub raw_buffer_index: u32,
    pub raw_byte_offset: u32,
    pub sample_format_bitmap: u32,
    pub scale_id: u32,
    pub raw_bit_offset: Option<u32>,
}

impl DaqMxMetadata {
    pub fn read<R: Read>(reader: &mut R, is_big_endian: bool, scaler_type: u32) -> Result<Self> {
        // 1. Read Dimensions and Sizes
        let dimension = read_u32(reader, is_big_endian)?;
        if dimension != 1 {
             return Err(TdmsError::Unsupported(format!("DAQmx dimension {} (expected 1)", dimension)));
        }
        
        let chunk_size = read_u64(reader, is_big_endian)?;
        let scaler_count = read_u32(reader, is_big_endian)?;

        // 2. Read Scalers
        let mut scalers = Vec::with_capacity(scaler_count as usize);
        for _ in 0..scaler_count {
            scalers.push(Scaler::read(reader, is_big_endian, scaler_type)?);
        }

        // 3. Read Raw Data Widths Vector
        let width_count = read_u32(reader, is_big_endian)?;
        let mut raw_data_widths = Vec::with_capacity(width_count as usize);
        for _ in 0..width_count {
            raw_data_widths.push(read_u32(reader, is_big_endian)?);
        }

        Ok(DaqMxMetadata {
            chunk_size,
            scalers,
            raw_data_widths,
        })
    }
}

impl Scaler {
    pub fn read<R: Read>(reader: &mut R, is_big_endian: bool, scaler_type: u32) -> Result<Self> {
        let data_type_code = read_u32(reader, is_big_endian)?;
        let raw_buffer_index = read_u32(reader, is_big_endian)?;
        let raw_offset_val = read_u32(reader, is_big_endian)?; // byte or bit offset
        let sample_format_bitmap = read_u32(reader, is_big_endian)?;
        let scale_id = read_u32(reader, is_big_endian)?;

        let data_type = DataType::from_daqmx_type_code(data_type_code)
            .ok_or_else(|| TdmsError::InvalidDataType(data_type_code))?;

        // Calculate offsets based on scaler type
        let (raw_byte_offset, raw_bit_offset) = if scaler_type == DIGITAL_LINE_SCALER {
            (raw_offset_val / 8, Some(raw_offset_val))
        } else {
            (raw_offset_val, None)
        };

        Ok(Scaler {
            scaler_type,
            data_type,
            raw_buffer_index,
            raw_byte_offset,
            sample_format_bitmap,
            scale_id,
            raw_bit_offset,
        })
    }
}

fn read_u32<R: Read>(reader: &mut R, is_big_endian: bool) -> std::io::Result<u32> {
    if is_big_endian { reader.read_u32::<BigEndian>() } else { reader.read_u32::<LittleEndian>() }
}

fn read_u64<R: Read>(reader: &mut R, is_big_endian: bool) -> std::io::Result<u64> {
    if is_big_endian { reader.read_u64::<BigEndian>() } else { reader.read_u64::<LittleEndian>() }
}