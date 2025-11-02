# py/tdms/__init__.py
"""
TDMS Python - High-performance Python bindings for TDMS file I/O

This package provides Python bindings to the tdms-rs Rust library,
offering high-performance reading and writing of TDMS (Technical Data
Management Streaming) files, the native format for National Instruments
LabVIEW and other NI software.

Examples:
    Writing TDMS files:
    
    >>> import tdms
    >>> import numpy as np
    >>>
    >>> with tdms.TdmsWriter("output.tdms") as writer:
    ...     writer.set_file_property("title", "My Experiment")
    ...     writer.create_channel("Data", "Voltage", tdms.DataType.F64)
    ...     
    ...     # Write data using NumPy arrays
    ...     data = np.sin(np.linspace(0, 2*np.pi, 1000))
    ...     writer.write_data("Data", "Voltage", data)
    
    Reading TDMS files:
    
    >>> with tdms.TdmsReader("input.tdms") as reader:
    ...     channels = reader.list_channels()
    ...     data = reader.read_data("Data", "Voltage")
    ...     print(f"Read {len(data)} samples")
"""

from .tdms import (
    TdmsWriter,
    TdmsReader,
    DataType,
    defragment,
    __version__
)

__all__ = [
    'TdmsWriter',
    'TdmsReader',
    'DataType',
    'defragment',
    '__version__',
]