# python/tests/test_write_benchmark.py
"""
Benchmark comparison for writing data between tdms-python (this library)
and the nptdms library.

This expanded benchmark includes:
- Parameterized array sizes for numeric and timestamp data
- Benchmarks for string data
- A "complex" benchmark that interleaves data writes with property updates
  and segment flushes, simulating a real-time acquisition.

To run this benchmark:
1. Ensure dev dependencies are installed:
   pip install -r python/requirements-dev.txt
   (This includes pytest, pytest-benchmark, and nptdms)
2. Build the tdms-python library:
   maturin develop --release
3. Run pytest:
   pytest python/tests/test_write_benchmark.py
"""

import pytest
import numpy as np
import tempfile
import os
from datetime import datetime

# Import both libraries for comparison
import tdms  # This is tdms-python (our library)
from nptdms import TdmsWriter as NpTdmsWriter, ChannelObject as NpChannelObject, RootObject

# --- Fixtures ---

@pytest.fixture
def temp_tdms_file():
    """
    Create a temporary TDMS file path for each benchmark run.
    """
    fd, path = tempfile.mkstemp(suffix='.tdms')
    os.close(fd)
    yield path
    # Cleanup
    try:
        os.unlink(path)
        # Our library (tdms-python) creates an index file, nptdms does not
        os.unlink(path + '_index')
    except FileNotFoundError:
        pass

# --- Helper functions for benchmarking (callables) ---

# --- Numeric (f64) Helpers ---
def write_tdms_rs_numeric(filepath, data):
    with tdms.TdmsWriter(filepath) as writer:
        writer.create_channel("Group", "Channel", tdms.DataType.F64)
        writer.write_data("Group", "Channel", data)

def write_nptdms_numeric(filepath, data):
    with NpTdmsWriter(filepath) as writer:
        channel = NpChannelObject('Group', 'Channel', data)
        writer.write_segment([channel])

# --- String Helpers ---
def write_tdms_rs_strings(filepath, data_list):
    with tdms.TdmsWriter(filepath) as writer:
        writer.create_channel("Group", "Channel", tdms.DataType.STRING)
        writer.write_strings("Group", "Channel", data_list)

def write_nptdms_strings(filepath, data_list):
    # nptdms requires an object array for strings
    data_np = np.array(data_list, dtype=object)
    with NpTdmsWriter(filepath) as writer:
        channel = NpChannelObject('Group', 'Channel', data_np)
        writer.write_segment([channel])

# --- Timestamp (datetime64) Helpers ---
def write_tdms_rs_timestamps(filepath, data):
    with tdms.TdmsWriter(filepath) as writer:
        writer.create_channel("Group", "Channel", tdms.DataType.TIMESTAMP)
        # Our Rust backend directly supports numpy.datetime64 arrays
        writer.write_data("Group", "Channel", data)

def write_nptdms_timestamps(filepath, data):
    with NpTdmsWriter(filepath) as writer:
        channel = NpChannelObject('Group', 'Channel', data)
        writer.write_segment([channel])

# --- Complex (Interleaved) Helpers ---
NUM_CHUNKS = 100
CHUNK_SIZE = 1000  # 100 chunks * 1000 samples = 100k total samples

def write_tdms_rs_complex(filepath):
    with tdms.TdmsWriter(filepath) as writer:
        writer.set_file_property("title", "Complex Test")
        writer.create_channel("Group", "Channel", tdms.DataType.F64)
        for i in range(NUM_CHUNKS):
            # Simulate a real-time acquisition loop
            data = np.full(CHUNK_SIZE, i, dtype=np.float64)
            writer.write_data("Group", "Channel", data)
            # Update a property and force a new segment
            writer.set_channel_property("Group", "Channel", "last_write_index", i)
            writer.flush()

def write_nptdms_complex(filepath):
    with NpTdmsWriter(filepath) as writer:
        # Set file property by writing a RootObject
        root = RootObject(properties={"title": "Complex Test"})
        
        # Write first segment with file properties
        data = np.full(CHUNK_SIZE, 0, dtype=np.float64)
        channel = NpChannelObject('Group', 'Channel', data)
        
        # --- FIX IS HERE ---
        channel.properties = {} # Initialize the properties dictionary
        channel.properties['last_write_index'] = 0
        writer.write_segment([root, channel])

        # Write remaining segments
        for i in range(1, NUM_CHUNKS): # Start from 1
            data = np.full(CHUNK_SIZE, i, dtype=np.float64)
            channel = NpChannelObject('Group', 'Channel', data)
            
            # --- AND HERE ---
            channel.properties = {} # Initialize the properties dictionary
            channel.properties['last_write_index'] = i
            writer.write_segment([channel])

# --- Benchmark Tests ---

# --- 1. Numeric (f64) Tests ---
@pytest.mark.parametrize("size", [100_000, 1_000_000, 5_000_000])
def test_write_numeric_tdms_python(benchmark, temp_tdms_file, size):
    # Setup (not timed)
    data = np.arange(size, dtype=np.float64)
    # Benchmark (timed)
    benchmark(write_tdms_rs_numeric, temp_tdms_file, data)

@pytest.mark.parametrize("size", [100_000, 1_000_000, 5_000_000])
def test_write_numeric_nptdms(benchmark, temp_tdms_file, size):
    # Setup (not timed)
    data = np.arange(size, dtype=np.float64)
    # Benchmark (timed)
    benchmark(write_nptdms_numeric, temp_tdms_file, data)

# --- 2. String Tests ---
@pytest.mark.parametrize("size", [100_000, 1_000_000])
def test_write_strings_tdms_python(benchmark, temp_tdms_file, size):
    data_list = [f"String_value_{i}" for i in range(size)]
    benchmark(write_tdms_rs_strings, temp_tdms_file, data_list)

@pytest.mark.parametrize("size", [100_000, 1_000_000])
def test_write_strings_nptdms(benchmark, temp_tdms_file, size):
    data_list = [f"String_value_{i}" for i in range(size)]
    benchmark(write_nptdms_strings, temp_tdms_file, data_list)

# --- 3. Timestamp (datetime64) Tests ---
@pytest.mark.parametrize("size", [100_000, 1_000_000, 5_000_000])
def test_write_timestamps_tdms_python(benchmark, temp_tdms_file, size):
    base_time = np.datetime64('2025-01-01T00:00:00')
    deltas = np.arange(size, dtype='timedelta64[ns]')
    data = base_time + deltas
    benchmark(write_tdms_rs_timestamps, temp_tdms_file, data)

@pytest.mark.parametrize("size", [100_000, 1_000_000, 5_000_000])
def test_write_timestamps_nptdms(benchmark, temp_tdms_file, size):
    base_time = np.datetime64('2025-01-01T00:00:00')
    deltas = np.arange(size, dtype='timedelta64[ns]')
    data = base_time + deltas
    benchmark(write_nptdms_timestamps, temp_tdms_file, data)

# --- 4. Complex (Interleaved) Tests ---
def test_write_complex_tdms_python(benchmark, temp_tdms_file):
    benchmark(write_tdms_rs_complex, temp_tdms_file)

def test_write_complex_nptdms(benchmark, temp_tdms_file):
    benchmark(write_nptdms_complex, temp_tdms_file)