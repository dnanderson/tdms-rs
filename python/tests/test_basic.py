# python/tests/test_basic.py
"""Basic tests for TDMS Python bindings"""

import pytest
import numpy as np
import tempfile
import os
from pathlib import Path

import tdms


@pytest.fixture
def temp_tdms_file():
    """Create a temporary TDMS file path"""
    fd, path = tempfile.mkstemp(suffix='.tdms')
    os.close(fd)
    yield path
    # Cleanup
    try:
        os.unlink(path)
        os.unlink(path + '_index')
    except FileNotFoundError:
        pass


def test_write_and_read_f64(temp_tdms_file):
    """Test writing and reading float64 data"""
    # Write
    with tdms.TdmsWriter(temp_tdms_file) as writer:
        writer.create_channel("Group1", "Channel1", tdms.DataType.F64)
        data = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
        writer.write_data("Group1", "Channel1", data)
    
    # Read
    with tdms.TdmsReader(temp_tdms_file) as reader:
        read_data = reader.read_data("Group1", "Channel1")
        np.testing.assert_array_equal(read_data, data)


def test_write_and_read_i32(temp_tdms_file):
    """Test writing and reading int32 data"""
    # Write
    with tdms.TdmsWriter(temp_tdms_file) as writer:
        writer.create_channel("Group1", "Channel1", tdms.DataType.I32)
        data = np.array([1, 2, 3, 4, 5], dtype=np.int32)
        writer.write_data("Group1", "Channel1", data)
    
    # Read
    with tdms.TdmsReader(temp_tdms_file) as reader:
        read_data = reader.read_data("Group1", "Channel1", dtype=np.int32)
        np.testing.assert_array_equal(read_data, data)


def test_write_and_read_strings(temp_tdms_file):
    """Test writing and reading string data"""
    # Write
    with tdms.TdmsWriter(temp_tdms_file) as writer:
        writer.create_channel("Group1", "Strings", tdms.DataType.STRING)
        data = ["Hello", "World", "TDMS", "Python"]
        writer.write_strings("Group1", "Strings", data)
    
    # Read
    with tdms.TdmsReader(temp_tdms_file) as reader:
        read_data = reader.read_strings("Group1", "Strings")
        assert read_data == data


def test_file_properties(temp_tdms_file):
    """Test setting and reading file properties"""
    # Write
    with tdms.TdmsWriter(temp_tdms_file) as writer:
        writer.set_file_property("title", "Test File")
        writer.set_file_property("version", 42)
        writer.set_file_property("test_float", 3.14)
        writer.set_file_property("test_bool", True)
        writer.create_channel("Group1", "Channel1", tdms.DataType.F64)
        writer.write_data("Group1", "Channel1", np.array([1.0]))
    
    # Read
    with tdms.TdmsReader(temp_tdms_file) as reader:
        props = reader.get_file_properties()
        assert props["title"] == "Test File"
        assert props["version"] == 42
        assert props["test_float"] == 3.14
        assert props["test_bool"] == True


def test_group_properties(temp_tdms_file):
    """Test setting and reading group properties"""
    # Write
    with tdms.TdmsWriter(temp_tdms_file) as writer:
        writer.set_group_property("TestGroup", "description", "Test Description")
        writer.set_group_property("TestGroup", "id", 123)
        writer.create_channel("TestGroup", "Channel1", tdms.DataType.F64)
        writer.write_data("TestGroup", "Channel1", np.array([1.0]))
    
    # Read
    with tdms.TdmsReader(temp_tdms_file) as reader:
        props = reader.get_group_properties("TestGroup")
        assert props is not None
        assert props["description"] == "Test Description"
        assert props["id"] == 123


def test_channel_properties(temp_tdms_file):
    """Test setting and reading channel properties"""
    # Write
    with tdms.TdmsWriter(temp_tdms_file) as writer:
        writer.create_channel("Group1", "Voltage", tdms.DataType.F64)
        writer.set_channel_property("Group1", "Voltage", "unit", "V")
        writer.set_channel_property("Group1", "Voltage", "max", 10.0)
        writer.write_data("Group1", "Voltage", np.array([1.0, 2.0, 3.0]))
    
    # Read
    with tdms.TdmsReader(temp_tdms_file) as reader:
        props = reader.get_channel_properties("Group1", "Voltage")
        assert props is not None
        assert props["unit"] == "V"
        assert props["max"] == 10.0


def test_list_channels(temp_tdms_file):
    """Test listing channels"""
    # Write
    with tdms.TdmsWriter(temp_tdms_file) as writer:
        writer.create_channel("Group1", "Channel1", tdms.DataType.F64)
        writer.create_channel("Group1", "Channel2", tdms.DataType.I32)
        writer.create_channel("Group2", "Channel3", tdms.DataType.STRING)
        writer.write_data("Group1", "Channel1", np.array([1.0]))
        writer.write_data("Group1", "Channel2", np.array([1], dtype=np.int32))
        writer.write_strings("Group2", "Channel3", ["test"])
    
    # Read
    with tdms.TdmsReader(temp_tdms_file) as reader:
        channels = reader.list_channels()
        assert len(channels) == 3
        assert any("Channel1" in ch for ch in channels)
        assert any("Channel2" in ch for ch in channels)
        assert any("Channel3" in ch for ch in channels)


def test_list_groups(temp_tdms_file):
    """Test listing groups"""
    # Write
    with tdms.TdmsWriter(temp_tdms_file) as writer:
        writer.create_channel("Group1", "Channel1", tdms.DataType.F64)
        writer.create_channel("Group2", "Channel2", tdms.DataType.F64)
        writer.write_data("Group1", "Channel1", np.array([1.0]))
        writer.write_data("Group2", "Channel2", np.array([2.0]))
    
    # Read
    with tdms.TdmsReader(temp_tdms_file) as reader:
        groups = reader.list_groups()
        assert "Group1" in groups
        assert "Group2" in groups


def test_large_dataset(temp_tdms_file):
    """Test writing and reading large dataset"""
    size = 1_000_000
    
    # Write
    with tdms.TdmsWriter(temp_tdms_file) as writer:
        writer.create_channel("Data", "Large", tdms.DataType.F64)
        data = np.random.randn(size)
        writer.write_data("Data", "Large", data)
    
    # Read
    with tdms.TdmsReader(temp_tdms_file) as reader:
        read_data = reader.read_data("Data", "Large")
        assert len(read_data) == size
        np.testing.assert_array_equal(read_data, data)


def test_multiple_segments(temp_tdms_file):
    """Test writing multiple segments"""
    # Write multiple flushes
    with tdms.TdmsWriter(temp_tdms_file) as writer:
        writer.create_channel("Data", "Values", tdms.DataType.I32)
        
        for i in range(5):
            data = np.array([i] * 100, dtype=np.int32)
            writer.write_data("Data", "Values", data)
            writer.flush()
    
    # Read
    with tdms.TdmsReader(temp_tdms_file) as reader:
        read_data = reader.read_data("Data", "Values", dtype=np.int32)
        assert len(read_data) == 500
        # Verify segment pattern
        for i in range(5):
            segment = read_data[i*100:(i+1)*100]
            assert np.all(segment == i)


def test_context_manager(temp_tdms_file):
    """Test that context managers work correctly"""
    # Write using context manager
    with tdms.TdmsWriter(temp_tdms_file) as writer:
        writer.create_channel("Group", "Channel", tdms.DataType.F64)
        writer.write_data("Group", "Channel", np.array([1.0, 2.0, 3.0]))
    
    # Verify file was closed and flushed
    with tdms.TdmsReader(temp_tdms_file) as reader:
        data = reader.read_data("Group", "Channel")
        assert len(data) == 3


def test_auto_type_detection(temp_tdms_file):
    """Test automatic type detection from numpy arrays"""
    # Write
    with tdms.TdmsWriter(temp_tdms_file) as writer:
        writer.create_channel("Data", "Float32", tdms.DataType.F32)
        writer.create_channel("Data", "Float64", tdms.DataType.F64)
        writer.create_channel("Data", "Int32", tdms.DataType.I32)
        writer.create_channel("Data", "Bool", tdms.DataType.BOOL)
        
        # Auto-detection from numpy dtype
        writer.write_data("Data", "Float32", np.array([1.0, 2.0], dtype=np.float32))
        writer.write_data("Data", "Float64", np.array([1.0, 2.0], dtype=np.float64))
        writer.write_data("Data", "Int32", np.array([1, 2], dtype=np.int32))
        writer.write_data("Data", "Bool", np.array([True, False], dtype=np.bool_))
    
    # Read and verify
    with tdms.TdmsReader(temp_tdms_file) as reader:
        assert len(reader.read_data("Data", "Float32", dtype=np.float32)) == 2
        assert len(reader.read_data("Data", "Float64", dtype=np.float64)) == 2
        assert len(reader.read_data("Data", "Int32", dtype=np.int32)) == 2
        assert len(reader.read_data("Data", "Bool", dtype=np.bool_)) == 2


def test_empty_strings(temp_tdms_file):
    """Test writing and reading empty strings"""
    # Write
    with tdms.TdmsWriter(temp_tdms_file) as writer:
        writer.create_channel("Data", "Strings", tdms.DataType.STRING)
        data = ["", "Hello", "", "World", ""]
        writer.write_strings("Data", "Strings", data)
    
    # Read
    with tdms.TdmsReader(temp_tdms_file) as reader:
        read_data = reader.read_strings("Data", "Strings")
        assert read_data == data


def test_defragment(temp_tdms_file):
    """Test file defragmentation"""
    # Create a fragmented file
    with tdms.TdmsWriter(temp_tdms_file) as writer:
        writer.create_channel("Data", "Values", tdms.DataType.I32)
        
        # Multiple segments with property changes
        for i in range(3):
            writer.set_channel_property("Data", "Values", "iteration", i)
            data = np.array(list(range(i*100, (i+1)*100)), dtype=np.int32)
            writer.write_data("Data", "Values", data)
            writer.flush()
    
    # Defragment
    defrag_path = temp_tdms_file.replace(".tdms", "_defrag.tdms")
    try:
        tdms.defragment(temp_tdms_file, defrag_path)
        
        # Verify defragmented file
        with tdms.TdmsReader(defrag_path) as reader:
            assert reader.segment_count == 1  # Should be single segment
            data = reader.read_data("Data", "Values", dtype=np.int32)
            assert len(data) == 300
            np.testing.assert_array_equal(data, np.arange(300, dtype=np.int32))
    finally:
        try:
            os.unlink(defrag_path)
            os.unlink(defrag_path + '_index')
        except FileNotFoundError:
            pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"])