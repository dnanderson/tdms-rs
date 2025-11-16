
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

def test_write_and_read_timestamps(temp_tdms_file):
    """Test writing and reading timestamp data"""
    with tdms.TdmsWriter(temp_tdms_file) as writer:
        writer.create_channel("Time", "Timestamps", tdms.DataType.TIMESTAMP)
        start_time = np.datetime64('2024-01-01T12:00:00', 'ns')
        time_deltas = np.arange(10, dtype='timedelta64[s]')
        timestamps = start_time + time_deltas
        writer.write_data("Time", "Timestamps", timestamps)

    with tdms.TdmsReader(temp_tdms_file) as reader:
        read_timestamps = reader.read_data("Time", "Timestamps")
        assert len(read_timestamps) == 10
        assert all(isinstance(ts, np.datetime64) for ts in read_timestamps)
        np.testing.assert_array_equal(read_timestamps, timestamps)
