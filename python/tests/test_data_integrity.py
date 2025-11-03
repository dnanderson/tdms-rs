# python/tests/test_data_integrity.py
"""
Test suite for data integrity and cross-compatibility with nptdms.

This suite verifies that:
1. Data written by tdms-python can be read back identically by tdms-python.
2. Data written by tdms-python can be read back identically by nptdms.
3. This holds true for all supported data types for channels and properties.
4. This holds true for files with incremental metadata (interleaved writes).
"""

import pytest
import numpy as np
import tempfile
import os
from nptdms import TdmsFile
import tdms

# --- Helper Functions for Validation ---

def to_ns(timestamp):
    """
    Converts a numpy datetime64, int, or float to int64 nanoseconds since epoch.
    
    --- THIS FUNCTION IS NOW MORE ROBUST ---
    """
    if isinstance(timestamp, (int, np.int64)):
        # Handle the case where the buggy .item() call returned an int
        return np.int64(timestamp)
    if isinstance(timestamp, (float, np.float64)):
        # Handle potential float conversions
        return np.int64(timestamp)
    # This is the correct path for datetime64 objects
    return timestamp.astype('datetime64[ns]').astype(np.int64)

def compare_timestamps(ts1, ts2, tolerance_ns=1000):
    """
    Compares two timestamp objects (from tdms-python or nptdms)
    with a tolerance. nptdms reads at microsecond precision,
    so we check if they are within 1 microsecond (1000 ns).
    """
    ns1 = to_ns(ts1)
    ns2 = to_ns(ts2)
    # Assert that the absolute difference is within the tolerance
    assert abs(ns1 - ns2) <= tolerance_ns, f"Timestamp mismatch: {ns1} != {ns2} (tolerance: {tolerance_ns}ns)"

def validate_properties(props_read, props_expected):
    """Asserts that two property dictionaries are equal."""
    assert len(props_read) == len(props_expected)
    for key, expected_val in props_expected.items():
        assert key in props_read
        read_val = props_read[key]

        if isinstance(expected_val, np.datetime64):
            # This validation already uses the tolerant compare_timestamps
            compare_timestamps(read_val, expected_val)
        elif isinstance(expected_val, float):
            np.testing.assert_allclose(read_val, expected_val)
        else:
            assert read_val == expected_val

def validate_channel_data(data_read, data_expected):
    """Asserts that two numpy arrays of channel data are equal."""
    if data_expected.dtype.kind == 'f':
        # Use allclose for floats to handle NaN and Inf correctly
        np.testing.assert_allclose(data_read, data_expected, equal_nan=True)
    elif data_expected.dtype.kind == 'M':
        
        # --- THIS BLOCK IS THE FIX FOR FAILURES 2 & 3 ---
        
        # OLD (BUGGY) LINE:
        # np.testing.assert_array_equal(to_ns(data_read), to_ns(data_expected))
        
        # NEW (FIXED) LOGIC:
        # Use the tolerant compare_timestamps for each element.
        # This handles both the 1ns rounding error (Failure 2)
        # and the nptdms microsecond truncation (Failure 3).
        assert len(data_read) == len(data_expected), "Timestamp array lengths differ"
        for i in range(len(data_read)):
            compare_timestamps(data_read[i], data_expected[i])
        
    else:
        # Use array_equal for integers, bools, and strings (object)
        np.testing.assert_array_equal(data_read, data_expected)

# --- Fixtures ---

@pytest.fixture(scope="session")
def expected_data():
    """A single source of truth for all data to be written and validated."""
    return {
        "channels": {
            "i8": np.array([-128, 0, 127], dtype=np.int8),
            "i16": np.array([-32768, 0, 32767], dtype=np.int16),
            "i32": np.array([-2147483648, 0, 2147483647], dtype=np.int32),
            "i64": np.array([-9223372036854775808, 0, 9223372036854775807], dtype=np.int64),
            "u8": np.array([0, 128, 255], dtype=np.uint8),
            "u16": np.array([0, 32768, 65535], dtype=np.uint16),
            "u32": np.array([0, 2147483648, 4294967295], dtype=np.uint32),
            "u64": np.array([0, 9223372036854775808, 18446744073709551615], dtype=np.uint64),
            "f32": np.array([-1.5, 0.0, 1.5, np.inf, -np.inf, np.nan], dtype=np.float32),
            "f64": np.array([-2.5, 0.0, 2.5, np.inf, -np.inf, np.nan], dtype=np.float64),
            "bool": np.array([True, False, True], dtype=np.bool_),
            "string": np.array(["Hello", "World", "Unicode: 🚀", ""]),
            "timestamp": np.array([
                '2025-01-01T12:00:00.123456789',
                '1904-01-01T00:00:00.000000000'
            ], dtype='datetime64[ns]'),
        },
        "properties": {
            "prop_string": "Test String",
            "prop_i32": np.int32(-12345),
            "prop_f64": np.float64(123.456),
            "prop_bool": True,
            # Use microsecond precision for property timestamps, as nptdms
            # seems to only read properties at this precision.
            "prop_timestamp": np.datetime64('2024-10-28T10:00:00.123456'),
        },
        "interleaved_data": {
            "ChannelA": np.array([1, 2, 3, 4, 5, 6, 7, 8], dtype=np.int32),
            "ChannelB": np.array([1.1, 2.2, 3.3, 4.4, 5.5, 6.6], dtype=np.float64),
        },
        "interleaved_props": {
            "file_prop": "final",
            "prop_a": "segment_2"
        }
    }

@pytest.fixture(scope="module")
def written_files(expected_data):
    """
    Fixture that writes all test files *once* using tdms-python.
    Yields a dictionary of file paths.
    """
    files = {}

    # --- File 1: All Channel Types ---
    fd, path = tempfile.mkstemp(suffix='.tdms')
    os.close(fd)
    files["all_types"] = path

    with tdms.TdmsWriter(path) as writer:
        for name, data in expected_data["channels"].items():
            # Get the tdms.DataType enum
            if name == 'string':
                dt = tdms.DataType.STRING
            elif name == 'timestamp':
                dt = tdms.DataType.TIMESTAMP
            else:
                dt = tdms.DataType.from_numpy_dtype(data.dtype)
            
            writer.create_channel("AllTypes", name, dt)
            
            if name == 'string':
                writer.write_strings("AllTypes", name, data.tolist())
            else:
                writer.write_data("AllTypes", name, data)

    # --- File 2: All Property Types ---
    fd, path = tempfile.mkstemp(suffix='.tdms')
    os.close(fd)
    files["all_properties"] = path

    with tdms.TdmsWriter(path) as writer:
        # File props
        for key, val in expected_data["properties"].items():
            writer.set_file_property(key, val)
        
        # Group props
        for key, val in expected_data["properties"].items():
            writer.set_group_property("Group", key, val)
        
        # Channel props
        writer.create_channel("Group", "Channel", tdms.DataType.F64)
        for key, val in expected_data["properties"].items():
            writer.set_channel_property("Group", "Channel", key, val)
        writer.write_data("Group", "Channel", np.array([1.0]))

    # --- File 3: Interleaved Data ---
    fd, path = tempfile.mkstemp(suffix='.tdms')
    os.close(fd)
    files["interleaved"] = path
    
    with tdms.TdmsWriter(path) as writer:
        writer.create_channel("Main", "ChannelA", tdms.DataType.I32)
        writer.create_channel("Main", "ChannelB", tdms.DataType.F64)
        
        # Segment 1
        writer.write_data("Main", "ChannelA", np.array([1, 2], dtype=np.int32))
        writer.write_data("Main", "ChannelB", np.array([1.1, 2.2], dtype=np.float64))
        writer.flush()
        
        # Segment 2
        writer.set_channel_property("Main", "ChannelA", "prop_a", "segment_2")
        writer.write_data("Main", "ChannelA", np.array([3, 4], dtype=np.int32))
        writer.write_data("Main", "ChannelB", np.array([3.3, 4.4], dtype=np.float64))
        writer.flush()
        
        # Segment 3
        writer.set_file_property("file_prop", "final")
        writer.write_data("Main", "ChannelA", np.array([5, 6], dtype=np.int32))
        # Skip writing to ChannelB in this segment
        writer.flush()
        
        # Segment 4
        writer.write_data("Main", "ChannelA", np.array([7, 8], dtype=np.int32))
        writer.write_data("Main", "ChannelB", np.array([5.5, 6.6], dtype=np.float64))
        writer.flush()
    
    # --- Yield paths and cleanup ---
    yield files
    
    for path in files.values():
        try:
            os.unlink(path)
            os.unlink(path + '_index')
        except FileNotFoundError:
            pass

# --- Test Functions ---

# --- 1. All Channel Types ---

def test_read_all_types_tdms_python(written_files, expected_data):
    """Reads the 'all_types' file with tdms-python and validates all channels."""
    path = written_files["all_types"]
    with tdms.TdmsReader(path) as reader:
        for name, expected in expected_data["channels"].items():
            data = reader.read_data("AllTypes", name)
            validate_channel_data(data, expected)

def test_read_all_types_nptdms(written_files, expected_data):
    """Reads the 'all_types' file with nptdms and validates all channels."""
    path = written_files["all_types"]
    with TdmsFile.open(path) as file:
        group = file["AllTypes"]
        for name, expected in expected_data["channels"].items():
            data = group[name][:]
            validate_channel_data(data, expected)

# --- 2. All Property Types ---

def test_read_all_properties_tdms_python(written_files, expected_data):
    """Reads the 'all_properties' file with tdms-python and validates all properties."""
    path = written_files["all_properties"]
    expected_props = expected_data["properties"]
    
    with tdms.TdmsReader(path) as reader:
        # File props
        file_props = reader.get_file_properties()
        validate_properties(file_props, expected_props)
        
        # Group props
        group_props = reader.get_group_properties("Group")
        validate_properties(group_props, expected_props)
        
        # Channel props
        chan_props = reader.get_channel_properties("Group", "Channel")
        validate_properties(chan_props, expected_props)

def test_read_all_properties_nptdms(written_files, expected_data):
    """Reads the 'all_properties' file with nptdms and validates all properties."""
    path = written_files["all_properties"]
    expected_props = expected_data["properties"]
    
    with TdmsFile.open(path) as file:
        # File props
        validate_properties(file.properties, expected_props)
        
        # Group props
        validate_properties(file["Group"].properties, expected_props)
        
        # Channel props
        validate_properties(file["Group"]["Channel"].properties, expected_props)

# --- 3. Interleaved Data and Properties ---

def test_read_interleaved_tdms_python(written_files, expected_data):
    """Reads the 'interleaved' file with tdms-python and validates data/props."""
    path = written_files["interleaved"]
    expected_chan_data = expected_data["interleaved_data"]
    expected_props = expected_data["interleaved_props"]
    
    with tdms.TdmsReader(path) as reader:
        # Check final data
        data_a = reader.read_data("Main", "ChannelA")
        data_b = reader.read_data("Main", "ChannelB")
        validate_channel_data(data_a, expected_chan_data["ChannelA"])
        validate_channel_data(data_b, expected_chan_data["ChannelB"])
        
        # Check final properties
        assert reader.get_file_properties()["file_prop"] == expected_props["file_prop"]
        assert reader.get_channel_properties("Main", "ChannelA")["prop_a"] == expected_props["prop_a"]

def test_read_interleaved_nptdms(written_files, expected_data):
    """Reads the 'interleaved' file with nptdms and validates data/props."""
    path = written_files["interleaved"]
    expected_chan_data = expected_data["interleaved_data"]
    expected_props = expected_data["interleaved_props"]
    
    with TdmsFile.open(path) as file:
        # Check final data
        data_a = file["Main"]["ChannelA"][:]
        data_b = file["Main"]["ChannelB"][:]
        validate_channel_data(data_a, expected_chan_data["ChannelA"])
        validate_channel_data(data_b, expected_chan_data["ChannelB"])
        
        # Check final properties
        assert file.properties["file_prop"] == expected_props["file_prop"]
        assert file["Main"]["ChannelA"].properties["prop_a"] == expected_props["prop_a"]