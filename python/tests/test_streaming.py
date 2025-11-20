import pytest
import numpy as np
import tempfile
import os
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

def test_streaming_numeric_data(temp_tdms_file):
    """Test iterating over numeric data in chunks"""
    total_values = 10_000
    chunk_size = 1_000
    
    # 1. Write data
    with tdms.TdmsWriter(temp_tdms_file) as writer:
        writer.create_channel("Group", "Data", tdms.DataType.I32)
        # Write in 10 segments of 1000
        for i in range(10):
            data = np.arange(i * 1000, (i + 1) * 1000, dtype=np.int32)
            writer.write_data("Group", "Data", data)
            writer.flush()

    # 2. Read using iterator
    with tdms.TdmsReader(temp_tdms_file) as reader:
        iterator = reader.iter_data("Group", "Data", chunk_size=chunk_size)
        
        chunk_count = 0
        total_sum = 0
        total_len = 0
        
        for chunk in iterator:
            assert isinstance(chunk, np.ndarray)
            assert chunk.dtype == np.int32
            chunk_count += 1
            total_len += len(chunk)
            total_sum += np.sum(chunk, dtype=np.int64)
            
        assert chunk_count == 10  # 10,000 / 1,000
        assert total_len == total_values
        
        expected_sum = np.sum(np.arange(total_values, dtype=np.int64))
        assert total_sum == expected_sum

def test_streaming_string_data(temp_tdms_file):
    """Test iterating over string data in chunks"""
    total_strings = 100
    chunk_size = 20
    
    # 1. Write strings
    with tdms.TdmsWriter(temp_tdms_file) as writer:
        writer.create_channel("Text", "Lines", tdms.DataType.STRING)
        strings = [f"Line {i}" for i in range(total_strings)]
        writer.write_strings("Text", "Lines", strings)

    # 2. Read using iterator
    with tdms.TdmsReader(temp_tdms_file) as reader:
        iterator = reader.iter_strings("Text", "Lines", chunk_size=chunk_size)
        
        count = 0
        chunk_count = 0
        
        for chunk in iterator:
            assert isinstance(chunk, list) # Strings come back as lists
            chunk_count += 1
            
            for i, s in enumerate(chunk):
                expected = f"Line {count + i}"
                assert s == expected
            
            count += len(chunk)
            
        assert count == total_strings
        assert chunk_count == 5 # 100 / 20

def test_streaming_odd_chunk_size(temp_tdms_file):
    """Test streaming with a chunk size that doesn't align with total size"""
    total_values = 100
    chunk_size = 33
    
    # 1. Write data
    with tdms.TdmsWriter(temp_tdms_file) as writer:
        writer.create_channel("Group", "Data", tdms.DataType.F64)
        data = np.zeros(total_values, dtype=np.float64)
        writer.write_data("Group", "Data", data)

    # 2. Read
    with tdms.TdmsReader(temp_tdms_file) as reader:
        iterator = reader.iter_data("Group", "Data", chunk_size=chunk_size)
        lengths = [len(chunk) for chunk in iterator]
        
        # Should be [33, 33, 33, 1]
        assert lengths == [33, 33, 33, 1]
        assert sum(lengths) == total_values

def test_streaming_multiple_iterators(temp_tdms_file):
    """Test that multiple iterators can coexist (though sequential access is safer)"""
    # Write
    with tdms.TdmsWriter(temp_tdms_file) as writer:
        writer.create_channel("G", "C1", tdms.DataType.I32)
        writer.create_channel("G", "C2", tdms.DataType.I32)
        data = np.arange(100, dtype=np.int32)
        writer.write_data("G", "C1", data)
        writer.write_data("G", "C2", data)

    # Read
    with tdms.TdmsReader(temp_tdms_file) as reader:
        # We can create two iterators
        it1 = reader.iter_data("G", "C1", chunk_size=50)
        it2 = reader.iter_data("G", "C2", chunk_size=50)
        
        # Read partially from 1
        chunk1 = next(it1)
        assert len(chunk1) == 50
        assert chunk1[0] == 0
        
        # Read partially from 2 (this seeks the file under the hood)
        chunk2 = next(it2)
        assert len(chunk2) == 50
        assert chunk2[0] == 0
        
        # Resume 1 (this seeks back)
        chunk1_b = next(it1)
        assert len(chunk1_b) == 50
        assert chunk1_b[0] == 50

def test_streaming_invalid_channel(temp_tdms_file):
    """Test error handling for invalid channel"""
    with tdms.TdmsWriter(temp_tdms_file) as writer:
        writer.create_channel("G", "C", tdms.DataType.I32)
        writer.write_data("G", "C", np.array([1], dtype=np.int32))
        
    with tdms.TdmsReader(temp_tdms_file) as reader:
        with pytest.raises(ValueError, match="Channel not found"):
            reader.iter_data("G", "NonExistent", chunk_size=10)

def test_streaming_type_mismatch(temp_tdms_file):
    """Test using iter_strings on numeric data raises error"""
    with tdms.TdmsWriter(temp_tdms_file) as writer:
        writer.create_channel("G", "Numeric", tdms.DataType.I32)
        writer.write_data("G", "Numeric", np.array([1, 2, 3], dtype=np.int32))
        
    with tdms.TdmsReader(temp_tdms_file) as reader:
        # iter_strings checks types internally
        with pytest.raises(TypeError, match="is not string type"):
            reader.iter_strings("G", "Numeric", chunk_size=1)