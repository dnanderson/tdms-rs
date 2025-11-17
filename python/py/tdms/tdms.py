# py/tdms/tdms.py
"""High-level Python API for TDMS file I/O with automatic type detection"""

import numpy as np
from typing import Union, List, Dict, Any, Optional
from .tdms_python import (
    TdmsWriter as _TdmsWriter,
    RotatingTdmsWriter as _RotatingTdmsWriter,  # <-- ADDED
    AsyncTdmsWriter,                         # <-- ADDED (for re-export)
    AsyncRotatingTdmsWriter,                 # <-- ADDED (for re-export)
    TdmsReader as _TdmsReader,
    defragment as _defragment,
    __version__
)

# Re-export version
__version__ = __version__

class DataType:
    """TDMS data type constants"""
    VOID = 0
    I8 = 1
    I16 = 2
    I32 = 3
    I64 = 4
    U8 = 5
    U16 = 6
    U32 = 7
    U64 = 8
    F32 = 9
    F64 = 10
    STRING = 0x20
    BOOLEAN = 0x21
    TIMESTAMP = 0x44

    # Friendly aliases
    INT8 = I8
    INT16 = I16
    INT32 = I32
    INT64 = I64
    UINT8 = U8
    UINT16 = U16
    UINT32 = U32
    UINT64 = U64
    FLOAT32 = F32
    FLOAT64 = F64
    BOOL = BOOLEAN

    @staticmethod
    def from_numpy_dtype(dtype: np.dtype) -> int:
        """Convert NumPy dtype to TDMS DataType"""
        dtype_map = {
            np.dtype('int8'): DataType.I8,
            np.dtype('int16'): DataType.I16,
            np.dtype('int32'): DataType.I32,
            np.dtype('int64'): DataType.I64,
            np.dtype('uint8'): DataType.U8,
            np.dtype('uint16'): DataType.U16,
            np.dtype('uint32'): DataType.U32,
            np.dtype('uint64'): DataType.U64,
            np.dtype('float32'): DataType.F32,
            np.dtype('float64'): DataType.F64,
            np.dtype('bool'): DataType.BOOLEAN,
        }
        return dtype_map.get(dtype, DataType.F64)


class TdmsWriter:
    """
    High-level TDMS file writer with automatic type detection.
    
    This class provides a Pythonic interface for writing TDMS files,
    automatically detecting NumPy array types and handling conversions.
    
    Examples:
        >>> import numpy as np
        >>> with TdmsWriter("output.tdms") as writer:
        ...     writer.set_file_property("title", "My Data")
        ...     writer.create_channel("Group1", "Voltage", DataType.F64)
        ...     data = np.random.randn(1000)
        ...     writer.write_data("Group1", "Voltage", data)
    """
    
    def __init__(self, path: str):
        """
        Create a new TDMS file for writing.
        
        Args:
            path: Path to the TDMS file to create
        """
        self._writer = _TdmsWriter(path)
        
    def set_file_property(self, name: str, value: Union[int, float, str, bool]) -> None:
        """
        Set a file-level property.
        
        Args:
            name: Property name
            value: Property value (int, float, str, or bool)
        """
        self._writer.set_file_property(name, value)
        
    def set_group_property(self, group: str, name: str, value: Union[int, float, str, bool]) -> None:
        """
        Set a group-level property.
        
        Args:
            group: Group name
            name: Property name
            value: Property value (int, float, str, or bool)
        """
        self._writer.set_group_property(group, name, value)
        
    def set_channel_property(self, group: str, channel: str, name: str, 
                            value: Union[int, float, str, bool]) -> None:
        """
        Set a channel property.
        
        Args:
            group: Group name
            channel: Channel name
            name: Property name
            value: Property value (int, float, str, or bool)
        """
        self._writer.set_channel_property(group, channel, name, value)
        
    def create_channel(self, group: str, channel: str, data_type: Optional[int] = None) -> None:
        """
        Create a channel with specified or inferred data type.
        
        Args:
            group: Group name
            channel: Channel name
            data_type: Data type (use DataType constants), or None to infer from first write
        """
        if data_type is None:
            data_type = DataType.F64  # Default to F64
        self._writer.create_channel(group, channel, data_type)
        
    def write_data(self, group: str, channel: str, data: Union[np.ndarray, List]) -> None:
        """
        Write data to a channel with automatic type detection.
        
        This method automatically detects the data type from the NumPy array
        or Python list and calls the appropriate write method.
        
        Args:
            group: Group name
            channel: Channel name
            data: NumPy array or list of values
            
        Raises:
            TypeError: If data type is not supported
        """
        if isinstance(data, list):
            # Keep your existing list-to-array conversion logic
            if len(data) > 0:
                if isinstance(data[0], str):
                    self.write_strings(group, channel, data)
                    return
                # ... other list conversions ...
                data = np.array(data) 
            else:
                raise ValueError("Cannot write empty list")
        
        if not isinstance(data, np.ndarray):
            raise TypeError(f"Data must be a numpy array or list, got {type(data)}")
        
        # Ensure contiguous array
        if not data.flags['C_CONTIGUOUS']:
            data = np.ascontiguousarray(data)
        
        # The Rust function now handles all the type logic.
        try:
            self._writer.write_data(group, channel, data)
        except TypeError as e:
            # Re-raise with a more helpful message if needed
            raise TypeError(f"Unsupported numpy dtype: {data.dtype}. {e}")
    
    def write_strings(self, group: str, channel: str, data: List[str]) -> None:
        """
        Write string data to a channel.
        
        Args:
            group: Group name
            channel: Channel name
            data: List of strings
        """
        self._writer.write_strings(group, channel, data)
    
    def flush(self) -> None:
        """Flush buffered data to disk"""
        self._writer.flush()
    
    def close(self) -> None:
        """Close the writer (automatically flushes)"""
        self._writer.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

# --- START NEW CLASS: RotatingTdmsWriter ---

class RotatingTdmsWriter:
    """
    High-level TDMS file writer that automatically rotates to a new file
    when a size limit is exceeded.
    
    This class provides a Pythonic interface, automatically detecting
    NumPy array types and handling conversions.
    
    Examples:
        >>> import numpy as np
        >>> # Create a writer that rotates every 10MB
        >>> with RotatingTdmsWriter("output.tdms", 10 * 1024 * 1024) as writer:
        ...     for i in range(100):
        ...         data = np.random.randn(10000)
        ...         writer.write_data("Data", "Signal", data)
    """
    
    def __init__(self, path: str, max_size_bytes: int):
        """
        Create a new rotating TDMS file writer.
        
        Args:
            path: Base path for the TDMS files (e.g., "output.tdms")
            max_size_bytes: Maximum size of a single file before rotating.
                            Rotated files will be named "output.1.tdms",
                            "output.2.tdms", etc.
        """
        self._writer = _RotatingTdmsWriter(path, max_size_bytes)
        
    def set_file_property(self, name: str, value: Union[int, float, str, bool]) -> None:
        """
        Set a file-level property.
        
        Args:
            name: Property name
            value: Property value (int, float, str, or bool)
        """
        self._writer.set_file_property(name, value)
        
    def set_group_property(self, group: str, name: str, value: Union[int, float, str, bool]) -> None:
        """
        Set a group-level property.
        
        Args:
            group: Group name
            name: Property name
            value: Property value (int, float, str, or bool)
        """
        self._writer.set_group_property(group, name, value)
        
    def set_channel_property(self, group: str, channel: str, name: str, 
                            value: Union[int, float, str, bool]) -> None:
        """
        Set a channel property.
        
        Args:
            group: Group name
            channel: Channel name
            name: Property name
            value: Property value (int, float, str, or bool)
        """
        self._writer.set_channel_property(group, channel, name, value)
        
    def create_channel(self, group: str, channel: str, data_type: Optional[int] = None) -> None:
        """
        Create a channel with specified or inferred data type.
        
        Args:
            group: Group name
            channel: Channel name
            data_type: Data type (use DataType constants), or None to infer from first write
        """
        if data_type is None:
            data_type = DataType.F64  # Default to F64
        self._writer.create_channel(group, channel, data_type)
        
    def write_data(self, group: str, channel: str, data: Union[np.ndarray, List]) -> None:
        """
        Write data to a channel, rotating the file if the size limit is hit.
        
        Args:
            group: Group name
            channel: Channel name
            data: NumPy array or list of values
            
        Raises:
            TypeError: If data type is not supported
        """
        if isinstance(data, list):
            if len(data) > 0:
                if isinstance(data[0], str):
                    self.write_strings(group, channel, data)
                    return
                data = np.array(data) 
            else:
                raise ValueError("Cannot write empty list")
        
        if not isinstance(data, np.ndarray):
            raise TypeError(f"Data must be a numpy array or list, got {type(data)}")
        
        if not data.flags['C_CONTIGUOUS']:
            data = np.ascontiguousarray(data)
        
        try:
            self._writer.write_data(group, channel, data)
        except TypeError as e:
            raise TypeError(f"Unsupported numpy dtype: {data.dtype}. {e}")
    
    def write_strings(self, group: str, channel: str, data: List[str]) -> None:
        """
        Write string data to a channel, rotating the file if the size limit is hit.
        
        Args:
            group: Group name
            channel: Channel name
            data: List of strings
        """
        self._writer.write_strings(group, channel, data)
    
    def flush(self) -> None:
        """Flush buffered data to disk"""
        self._writer.flush()
    
    def close(self) -> None:
        """Close the writer (automatically flushes)"""
        self._writer.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

# --- END NEW CLASS ---

class TdmsReader:
    """
    High-level TDMS file reader.
    
    This class provides a Pythonic interface for reading TDMS files,
    automatically returning NumPy arrays for efficient data processing.
    
    Examples:
        >>> with TdmsReader("input.tdms") as reader:
        ...     channels = reader.list_channels()
        ...     print(f"Found {len(channels)} channels")
        ...     
        ...     data = reader.read_data("Group1", "Voltage")
        ...     print(f"Mean: {data.mean():.3f}")
    """
    
    def __init__(self, path: str):
        """
        Open a TDMS file for reading.
        
        Args:
            path: Path to the TDMS file to read
        """
        self._reader = _TdmsReader(path)
    
    def list_channels(self) -> List[str]:
        """
        List all channels in the file.
        
        Returns:
            List of channel paths in format "/'Group'/'Channel'"
        """
        return self._reader.list_channels()
    
    def list_groups(self) -> List[str]:
        """
        List all groups in the file.
        
        Returns:
            List of group names
        """
        return self._reader.list_groups()
    
    def get_file_properties(self) -> Dict[str, Any]:
        """
        Get all file-level properties.
        
        Returns:
            Dictionary mapping property names to values
        """
        return self._reader.get_file_properties()
    
    def get_group_properties(self, group: str) -> Optional[Dict[str, Any]]:
        """
        Get all properties for a group.
        
        Args:
            group: Group name
            
        Returns:
            Dictionary mapping property names to values, or None if group doesn't exist
        """
        return self._reader.get_group_properties(group)
    
    def get_channel_properties(self, group: str, channel: str) -> Optional[Dict[str, Any]]:
        """
        Get all properties for a channel.
        
        Args:
            group: Group name
            channel: Channel name
            
        Returns:
            Dictionary mapping property names to values, or None if channel doesn't exist
        """
        return self._reader.get_channel_properties(group, channel)
    
    def read_data(self, group: str, channel: str, dtype: Optional[np.dtype] = None) -> np.ndarray:
        """
        Read data from a channel with automatic type detection.
        
        The data is read using the type specified in the TDMS file.
        If a 'dtype' is provided, the data will be cast to that type.
        
        Args:
            group: Group name
            channel: Channel name
            dtype: Optional NumPy dtype to cast the result to.
            
        Returns:
            NumPy array of the data.
        """
        
        # Call the new unified Rust function
        # This will return a NumPy array with the correct type (e.g., f64, i32, datetime64, or object for strings)
        data = self._reader.read_data(group, channel)

        # Apply dtype conversion if requested by the user
        if dtype is not None and data.dtype != dtype:
            try:
                return data.astype(dtype)
            except Exception as e:
                raise TypeError(f"Could not cast channel '{group}/{channel}' from {data.dtype} to {dtype}") from e
        
        return data
    
    def read_strings(self, group: str, channel: str) -> List[str]:
        """
        Read string data from a channel.
        
        Args:
            group: Group name
            channel: Channel name
            
        Returns:
            List of strings
        """
        return self._reader.read_strings(group, channel)
    
    @property
    def segment_count(self) -> int:
        """Get the number of segments in the file"""
        return self._reader.segment_count
    
    @property
    def channel_count(self) -> int:
        """Get the number of channels in the file"""
        return self._reader.channel_count
    
    def close(self) -> None:
        """Close the reader"""
        self._reader.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False


def defragment(source_path: str, dest_path: str) -> None:
    """
    Defragment a TDMS file by consolidating all segments.
    
    This function reads all metadata and raw data from the source file
    and writes it into a new, optimized file with a single segment.
    This can improve read performance and enable zero-copy memory mapping.
    
    Args:
        source_path: Path to the fragmented TDMS file
        dest_path: Path where the defragmented file will be created
        
    Examples:
        >>> defragment("fragmented.tdms", "optimized.tdms")
    """
    _defragment(source_path, dest_path)