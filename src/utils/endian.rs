// src/utils/endian.rs
use byteorder::{ByteOrder, LittleEndian, BigEndian};

pub fn swap_endianness<T: Copy>(data: &mut [T]) {
    let size = std::mem::size_of::<T>();
    if size <= 1 {
        return;
    }
    
    unsafe {
        let bytes = std::slice::from_raw_parts_mut(
            data.as_mut_ptr() as *mut u8,
            data.len() * size,
        );
        
        for chunk in bytes.chunks_exact_mut(size) {
            chunk.reverse();
        }
    }
}