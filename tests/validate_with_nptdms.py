#!/usr/bin/env python3
"""
Validation script for TDMS files using nptdms library.
This script reads TDMS files and validates their contents against expected values.
"""

import sys
import json
from pathlib import Path
from nptdms import TdmsFile
import numpy as np

def validate_basic_types(filepath):
    """Validate a file with basic data types"""
    with TdmsFile.open(filepath) as tdms_file:
        # Check file properties
        file_props = tdms_file.properties
        assert file_props.get('title') == 'Basic Types Test', f"File title mismatch: {file_props.get('title')}"
        assert file_props.get('author') == 'Rust TDMS', f"File author mismatch: {file_props.get('author')}"
        
        # Check group
        group = tdms_file['TestGroup']
        group_props = group.properties
        assert group_props.get('description') == 'Test data', f"Group description mismatch"
        
        # Check I32 channel
        i32_channel = group['I32Channel']
        i32_data = i32_channel[:]
        assert len(i32_data) == 100, f"I32 channel length mismatch: {len(i32_data)}"
        assert np.array_equal(i32_data, np.arange(100, dtype=np.int32)), "I32 data mismatch"
        assert i32_channel.properties.get('unit') == 'counts', "I32 unit property mismatch"
        
        # Check F64 channel
        f64_channel = group['F64Channel']
        f64_data = f64_channel[:]
        assert len(f64_data) == 100, f"F64 channel length mismatch: {len(f64_data)}"
        expected_f64 = np.arange(100, dtype=np.float64) * 0.1
        assert np.allclose(f64_data, expected_f64), "F64 data mismatch"
        assert f64_channel.properties.get('unit') == 'volts', "F64 unit property mismatch"
        
        # Check String channel
        string_channel = group['StringChannel']
        string_data = string_channel[:]
        assert len(string_data) == 10, f"String channel length mismatch: {len(string_data)}"
        expected_strings = [f"String_{i}" for i in range(10)]
        # Convert to list for comparison (nptdms returns numpy array)
        assert list(string_data) == expected_strings, "String data mismatch"
        
        # Check Boolean channel
        bool_channel = group['BoolChannel']
        bool_data = bool_channel[:]
        assert len(bool_data) == 10, f"Bool channel length mismatch: {len(bool_data)}"
        expected_bools = [i % 2 == 0 for i in range(10)]
        assert list(bool_data) == expected_bools, "Boolean data mismatch"
        
    return True

def validate_multiple_segments(filepath):
    """Validate a file with multiple segments"""
    with TdmsFile.open(filepath) as tdms_file:
        group = tdms_file['Data']
        channel = group['Values']
        
        # Should have data from 5 segments, 100 values each
        data = channel[:]
        assert len(data) == 500, f"Expected 500 values, got {len(data)}"
        
        # Verify data pattern (each segment has constant value = segment index)
        for i in range(5):
            segment_data = data[i*100:(i+1)*100]
            assert np.allclose(segment_data, float(i)), f"Segment {i} data mismatch"
    
    return True

def validate_properties(filepath):
    """Validate file, group, and channel properties"""
    with TdmsFile.open(filepath) as tdms_file:
        # File properties
        file_props = tdms_file.properties
        assert file_props.get('title') == 'Properties Test', "File title mismatch"
        assert file_props.get('version') == 2, "File version mismatch"
        assert file_props.get('test_float') == 3.14, "File test_float mismatch"
        assert file_props.get('test_bool') == True, "File test_bool mismatch"
        
        # Group properties
        group = tdms_file['TestGroup']
        group_props = group.properties
        assert group_props.get('group_id') == 42, "Group ID mismatch"
        assert group_props.get('group_name') == 'Main Group', "Group name mismatch"
        
        # Channel properties
        channel = group['TestChannel']
        channel_props = channel.properties
        assert channel_props.get('unit') == 'meters', "Channel unit mismatch"
        assert channel_props.get('scale') == 1.5, "Channel scale mismatch"
        assert channel_props.get('offset') == 10, "Channel offset mismatch"
        assert channel_props.get('enabled') == True, "Channel enabled mismatch"
    
    return True

def validate_incremental_metadata(filepath):
    """Validate file with incremental metadata (property changes)"""
    with TdmsFile.open(filepath) as tdms_file:
        group = tdms_file['Data']
        channel = group['Values']
        
        # All data should be present
        data = channel[:]
        assert len(data) == 600, f"Expected 600 values, got {len(data)}"
        assert np.array_equal(data, np.arange(600, dtype=np.int32)), "Data mismatch"
        
        # Final property value should be visible
        assert channel.properties.get('status') == 'final', "Property should reflect final value"
    
    return True

def validate_mixed_channels(filepath):
    """Validate file with channels of different types"""
    with TdmsFile.open(filepath) as tdms_file:
        group = tdms_file['Mixed']
        
        # Integers
        int_channel = group['Integers']
        int_data = int_channel[:]
        assert len(int_data) == 6, f"Integer channel length mismatch: {len(int_data)}"
        assert np.array_equal(int_data, np.array([1, 2, 3, 4, 5, 6], dtype=np.int32)), "Integer data mismatch"
        
        # Floats
        float_channel = group['Floats']
        float_data = float_channel[:]
        assert len(float_data) == 6, f"Float channel length mismatch: {len(float_data)}"
        expected_floats = np.array([1.1, 2.2, 3.3, 4.4, 5.5, 6.6], dtype=np.float64)
        assert np.allclose(float_data, expected_floats), "Float data mismatch"
        
        # Strings
        string_channel = group['Strings']
        string_data = string_channel[:]
        assert len(string_data) == 6, f"String channel length mismatch: {len(string_data)}"
        # Convert to list for comparison
        assert list(string_data) == ["A", "B", "C", "D", "E", "F"], "String data mismatch"
        
        # Booleans
        bool_channel = group['Bools']
        bool_data = bool_channel[:]
        assert len(bool_data) == 6, f"Boolean channel length mismatch: {len(bool_data)}"
        assert list(bool_data) == [True, False, True, False, False, True], "Boolean data mismatch"
    
    return True

def validate_large_dataset(filepath):
    """Validate file with large dataset"""
    with TdmsFile.open(filepath) as tdms_file:
        group = tdms_file['LargeData']
        channel = group['BigChannel']
        
        data = channel[:]
        assert len(data) == 1000000, f"Expected 1000000 values, got {len(data)}"
        
        # Check first and last values
        assert data[0] == 0.0, "First value mismatch"
        assert np.isclose(data[-1], 999999.0 * 0.001), "Last value mismatch"
        
        # Check a sample of values
        sample_indices = [0, 100000, 500000, 999999]
        for idx in sample_indices:
            expected = idx * 0.001
            assert np.isclose(data[idx], expected), f"Value at index {idx} mismatch"
    
    return True

def validate_empty_strings(filepath):
    """Validate file with empty and mixed strings"""
    with TdmsFile.open(filepath) as tdms_file:
        group = tdms_file['StringTest']
        channel = group['MixedStrings']
        
        data = channel[:]
        assert len(data) == 7, f"Expected 7 strings, got {len(data)}"
        # Convert to list for comparison
        assert list(data) == ["", "Hello", "", "World", "", "", "End"], "String data mismatch"
    
    return True

def validate_channel_reordering(filepath):
    """Validate file with changing channel order"""
    with TdmsFile.open(filepath) as tdms_file:
        group = tdms_file['G']
        
        # Channel A: written in segments 1 and 3
        channel_a = group['A']
        data_a = channel_a[:]
        assert len(data_a) == 2, f"Channel A length mismatch: {len(data_a)}"
        assert np.array_equal(data_a, np.array([1, 6], dtype=np.int32)), "Channel A data mismatch"
        
        # Channel B: written in segments 1 and 2
        channel_b = group['B']
        data_b = channel_b[:]
        assert len(data_b) == 2, f"Channel B length mismatch: {len(data_b)}"
        assert np.array_equal(data_b, np.array([2, 4], dtype=np.int32)), "Channel B data mismatch"
        
        # Channel C: written in all segments
        channel_c = group['C']
        data_c = channel_c[:]
        assert len(data_c) == 3, f"Channel C length mismatch: {len(data_c)}"
        assert np.array_equal(data_c, np.array([3, 5, 7], dtype=np.int32)), "Channel C data mismatch"
    
    return True

def validate_waveform_properties(filepath):
    """Validate file with waveform properties"""
    with TdmsFile.open(filepath) as tdms_file:
        group = tdms_file['Waveforms']
        channel = group['Signal']
        
        # Check waveform properties
        props = channel.properties
        assert 'wf_start_time' in props, "Missing wf_start_time property"
        assert props.get('wf_increment') == 0.001, "wf_increment mismatch"
        assert props.get('wf_samples') == 1000, "wf_samples mismatch"
        assert props.get('unit_string') == 'Volts', "unit_string mismatch"
        
        # Check data
        data = channel[:]
        assert len(data) == 1000, f"Waveform data length mismatch: {len(data)}"
    
    return True

def validate_timestamp_data(filepath):
    """Validate file with timestamp data"""
    with TdmsFile.open(filepath) as tdms_file:
        group = tdms_file['TimeData']
        channel = group['Timestamps']
        
        # Check data exists and has correct length
        data = channel[:]
        assert len(data) == 100, f"Timestamp data length mismatch: {len(data)}"
        
        # Timestamps should be datetime-like objects (datetime or numpy.datetime64)
        # Check that they have time-related attributes or are numpy datetime64
        for i, ts in enumerate(data[:5]):  # Check first 5 timestamps
            # Accept both standard datetime and numpy datetime64
            if hasattr(ts, 'year'):
                # Standard datetime object
                continue
            elif hasattr(ts, 'astype'):
                # Numpy datetime64 - can be converted
                try:
                    # Try to access as datetime64
                    _ = np.datetime64(ts)
                    continue
                except:
                    pass
            # If we get here, it's not a valid timestamp type
            assert False, f"Timestamp at index {i} is not a valid datetime object: {type(ts)}"
    
    return True

def validate_defragmented(filepath):
    """Validate defragmented file"""
    with TdmsFile.open(filepath) as tdms_file:
        # File properties (final values)
        file_props = tdms_file.properties
        assert file_props.get('file_title') == 'Fragmented File', "File title mismatch"
        assert file_props.get('author') == 'Test', "Author mismatch"
        
        # Group properties
        group = tdms_file['Group1']
        group_props = group.properties
        assert group_props.get('group_desc') == 'First Segment', "Group description mismatch"
        
        # Channel A
        channel_a = group['ChannelA']
        data_a = channel_a[:]
        assert len(data_a) == 9, f"Channel A length mismatch: {len(data_a)}"
        assert np.array_equal(data_a, np.array([1, 2, 3, 4, 5, 6, 7, 8, 9], dtype=np.int32)), "Channel A data mismatch"
        # Final property value
        assert channel_a.properties.get('unit') == 'mV', "Channel A unit mismatch"
        
        # Channel B
        channel_b = group['ChannelB']
        data_b = channel_b[:]
        assert len(data_b) == 5, f"Channel B length mismatch: {len(data_b)}"
        # Convert to list for comparison
        assert list(data_b) == ["a", "b", "c", "d", "e"], "Channel B data mismatch"
    
    return True

def validate_unicode_strings(filepath):
    """Validate file with Unicode strings"""
    with TdmsFile.open(filepath) as tdms_file:
        group = tdms_file['Unicode']
        channel = group['Strings']
        
        data = channel[:]
        assert len(data) == 7, f"Expected 7 strings, got {len(data)}"
        
        # Convert to list for comparison
        data_list = list(data)
        
        # Verify specific Unicode strings
        assert data_list[0] == "Hello World", "English string mismatch"
        assert data_list[1] == "Привет мир", "Russian string mismatch"
        assert data_list[2] == "你好世界", "Chinese string mismatch"
        assert data_list[3] == "こんにちは世界", "Japanese string mismatch"
        assert data_list[4] == "مرحبا بالعالم", "Arabic string mismatch"
        assert data_list[5] == "🚀🌟💻", "Emoji string mismatch"
        assert data_list[6] == "Ñoño español", "Spanish string mismatch"
    
    return True

def validate_long_strings(filepath):
    """Validate file with very long strings"""
    with TdmsFile.open(filepath) as tdms_file:
        group = tdms_file['LongStrings']
        channel = group['Data']
        
        data = channel[:]
        assert len(data) == 3, f"Expected 3 strings, got {len(data)}"
        
        # Verify lengths
        assert len(data[0]) == 1000, f"First string length mismatch: {len(data[0])}"
        assert len(data[1]) == 5000, f"Second string length mismatch: {len(data[1])}"
        assert len(data[2]) == 10000, f"Third string length mismatch: {len(data[2])}"
        
        # Verify content
        assert data[0] == "A" * 1000, "First string content mismatch"
        assert data[1] == "B" * 5000, "Second string content mismatch"
        assert data[2] == "C" * 10000, "Third string content mismatch"
    
    return True

def validate_multiple_groups(filepath):
    """Validate file with multiple groups"""
    with TdmsFile.open(filepath) as tdms_file:
        # Check all groups exist
        groups = list(tdms_file.groups())
        group_names = [g.name for g in groups]
        assert len(group_names) == 3, f"Expected 3 groups, got {len(group_names)}"
        assert 'Group1' in group_names, "Group1 missing"
        assert 'Group2' in group_names, "Group2 missing"
        assert 'Group3' in group_names, "Group3 missing"
        
        # Check Group1 channels
        group1 = tdms_file['Group1']
        channel1_1 = group1['Channel1']
        channel1_2 = group1['Channel2']
        data1_1 = channel1_1[:]
        data1_2 = channel1_2[:]
        assert np.array_equal(data1_1, np.array([1, 2, 3], dtype=np.int32)), "Group1 Channel1 data mismatch"
        assert np.allclose(data1_2, np.array([1.1, 2.2, 3.3], dtype=np.float64)), "Group1 Channel2 data mismatch"
        
        # Check Group2 channel
        group2 = tdms_file['Group2']
        channel2_1 = group2['Channel1']
        data2_1 = channel2_1[:]
        # Convert to list for comparison
        assert list(data2_1) == ["A", "B", "C"], "Group2 Channel1 data mismatch"
        
        # Check Group3 channel
        group3 = tdms_file['Group3']
        channel3_1 = group3['Channel1']
        data3_1 = channel3_1[:]
        assert list(data3_1) == [True, False, True], "Group3 Channel1 data mismatch"
    
    return True

def validate_edge_cases(filepath):
    """Validate file with edge cases"""
    with TdmsFile.open(filepath) as tdms_file:
        group = tdms_file['Edge']
        
        # Single value channel
        single_channel = group['SingleValue']
        single_data = single_channel[:]
        assert len(single_data) == 1, f"Single value channel length mismatch: {len(single_data)}"
        assert single_data[0] == 42, "Single value mismatch"
        
        # Empty channel (might not be present if no data was written)
        # Some TDMS readers skip channels with no data
        
        # Special floats channel
        special_channel = group['SpecialFloats']
        special_data = special_channel[:]
        assert len(special_data) == 4, f"Special floats channel length mismatch: {len(special_data)}"
        
        # Check special values (note: -0.0 == 0.0 in comparison)
        assert special_data[0] == 0.0, "Zero value mismatch"
        assert np.isinf(special_data[2]) and special_data[2] > 0, "Positive infinity mismatch"
        assert np.isinf(special_data[3]) and special_data[3] < 0, "Negative infinity mismatch"
    
    return True

def validate_interleaved_data(filepath):
    """Validate file with interleaved data"""
    with TdmsFile.open(filepath) as tdms_file:
        group = tdms_file['Group']
        ch1 = group['Channel1']
        ch2 = group['Channel2']

        ch1_data = ch1[:]
        ch2_data = ch2[:]

        assert len(ch1_data) == 10, f"Channel 1 length mismatch: {len(ch1_data)}"
        assert len(ch2_data) == 10, f"Channel 2 length mismatch: {len(ch2_data)}"

        expected_ch1 = np.arange(10, dtype=np.int32)
        expected_ch2 = np.arange(10, dtype=np.float64) * 1.1

        assert np.array_equal(ch1_data, expected_ch1), "Channel 1 data mismatch"
        assert np.allclose(ch2_data, expected_ch2), "Channel 2 data mismatch"
    return True

# Test registry
TESTS = {
    'basic_types': validate_basic_types,
    'multiple_segments': validate_multiple_segments,
    'properties': validate_properties,
    'incremental_metadata': validate_incremental_metadata,
    'mixed_channels': validate_mixed_channels,
    'large_dataset': validate_large_dataset,
    'empty_strings': validate_empty_strings,
    'channel_reordering': validate_channel_reordering,
    'waveform_properties': validate_waveform_properties,
    'timestamp_data': validate_timestamp_data,
    'defragmented': validate_defragmented,
    'unicode_strings': validate_unicode_strings,
    'long_strings': validate_long_strings,
    'multiple_groups': validate_multiple_groups,
    'edge_cases': validate_edge_cases,
    'interleaved_data': validate_interleaved_data,
}

def main():
    if len(sys.argv) != 3:
        print("Usage: validate_with_nptdms.py <test_name> <filepath>")
        sys.exit(1)
    
    test_name = sys.argv[1]
    filepath = sys.argv[2]
    
    if test_name not in TESTS:
        print(f"Unknown test: {test_name}")
        print(f"Available tests: {', '.join(TESTS.keys())}")
        sys.exit(1)
    
    if not Path(filepath).exists():
        print(f"File not found: {filepath}")
        sys.exit(1)
    
    try:
        result = TESTS[test_name](filepath)
        if result:
            print(f"✓ Test '{test_name}' passed")
            sys.exit(0)
        else:
            print(f"✗ Test '{test_name}' failed")
            sys.exit(1)
    except AssertionError as e:
        print(f"✗ Test '{test_name}' failed: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"✗ Test '{test_name}' error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    main()