#!/usr/bin/env python3
"""
Example demonstrating waveform data handling with tdms-python
"""

import tdms
import numpy as np
from datetime import datetime

def create_waveform_file():
    """Create a TDMS file with waveform data and properties"""
    print("Creating waveform TDMS file...")
    
    # Sampling parameters
    sample_rate = 1000.0  # Hz
    duration = 10.0  # seconds
    num_samples = int(sample_rate * duration)
    
    # Time vector
    time = np.linspace(0, duration, num_samples)
    
    # Generate waveform data
    # Signal 1: 5 Hz sine wave with amplitude 2.5V
    freq1 = 5.0
    signal1 = 2.5 * np.sin(2 * np.pi * freq1 * time)
    
    # Signal 2: 10 Hz sine wave with amplitude 1.5V
    freq2 = 10.0
    signal2 = 1.5 * np.sin(2 * np.pi * freq2 * time)
    
    # Signal 3: Combined signal with noise
    signal3 = signal1 + signal2 + np.random.normal(0, 0.1, num_samples)
    
    with tdms.TdmsWriter("waveforms.tdms") as writer:
        # File properties
        writer.set_file_property("title", "Waveform Data Example")
        writer.set_file_property("description", "Multiple sine wave signals")
        writer.set_file_property("author", "Python Example")
        writer.set_file_property("date", datetime.now().isoformat())
        
        # Group properties
        writer.set_group_property("Waveforms", "sample_rate", sample_rate)
        writer.set_group_property("Waveforms", "duration", duration)
        writer.set_group_property("Waveforms", "num_samples", num_samples)
        
        # Create channels for each signal
        for i, (signal, freq, amp) in enumerate([
            (signal1, freq1, 2.5),
            (signal2, freq2, 1.5),
            (signal3, freq1 + freq2, 4.0)  # Combined frequency info
        ], 1):
            channel_name = f"Signal{i}"
            
            # Create channel
            writer.create_channel("Waveforms", channel_name, tdms.DataType.F64)
            
            # Set waveform properties (LabVIEW compatible)
            writer.set_channel_property("Waveforms", channel_name, "wf_increment", 1.0/sample_rate)
            writer.set_channel_property("Waveforms", channel_name, "wf_samples", num_samples)
            writer.set_channel_property("Waveforms", channel_name, "unit_string", "V")
            
            # Custom properties
            writer.set_channel_property("Waveforms", channel_name, "frequency", freq)
            writer.set_channel_property("Waveforms", channel_name, "amplitude", amp)
            
            if i == 3:
                writer.set_channel_property("Waveforms", channel_name, "type", "combined")
                writer.set_channel_property("Waveforms", channel_name, "noise_level", 0.1)
            else:
                writer.set_channel_property("Waveforms", channel_name, "type", "pure")
            
            # Write the waveform data
            writer.write_data("Waveforms", channel_name, signal)
    
    print(f"Created waveform file with {num_samples} samples per channel")


def analyze_waveforms():
    """Read and analyze the waveform data"""
    print("\nAnalyzing waveform data...")
    
    with tdms.TdmsReader("waveforms.tdms") as reader:
        # Display file info
        file_props = reader.get_file_properties()
        print(f"\nFile: {file_props.get('title')}")
        print(f"Description: {file_props.get('description')}")
        print(f"Date: {file_props.get('date')}")
        
        # Display acquisition parameters
        group_props = reader.get_group_properties("Waveforms")
        sample_rate = group_props.get("sample_rate")
        duration = group_props.get("duration")
        num_samples = group_props.get("num_samples")
        
        print(f"\nAcquisition Parameters:")
        print(f"  Sample Rate: {sample_rate} Hz")
        print(f"  Duration: {duration} seconds")
        print(f"  Samples: {num_samples}")
        
        # Analyze each channel
        channels = reader.list_channels()
        print(f"\nFound {len(channels)} channels:")
        
        for channel_path in channels:
            # Extract channel name from path
            channel_name = channel_path.split("'")[-2]
            
            # Read data
            data = reader.read_data("Waveforms", channel_name)
            
            # Read properties
            props = reader.get_channel_properties("Waveforms", channel_name)
            
            # Display analysis
            print(f"\n{channel_name}:")
            print(f"  Type: {props.get('type')}")
            print(f"  Frequency: {props.get('frequency')} Hz")
            print(f"  Amplitude: {props.get('amplitude')} V")
            print(f"  Unit: {props.get('unit_string')}")
            
            # Statistical analysis
            print(f"  Mean: {data.mean():.6f} V")
            print(f"  RMS: {np.sqrt(np.mean(data**2)):.6f} V")
            print(f"  Peak-to-Peak: {data.max() - data.min():.6f} V")
            print(f"  Min: {data.min():.6f} V")
            print(f"  Max: {data.max():.6f} V")
            
            if props.get('type') == 'combined':
                print(f"  Noise Level: {props.get('noise_level')} V")


def compare_signals():
    """Demonstrate signal processing on TDMS waveforms"""
    print("\n\nPerforming signal analysis...")
    
    with tdms.TdmsReader("waveforms.tdms") as reader:
        # Read all signals
        signal1 = reader.read_data("Waveforms", "Signal1")
        signal2 = reader.read_data("Waveforms", "Signal2")
        signal3 = reader.read_data("Waveforms", "Signal3")
        
        # Get sample rate for time axis
        group_props = reader.get_group_properties("Waveforms")
        sample_rate = group_props.get("sample_rate")
        
        # Calculate correlation between signals
        corr_1_2 = np.corrcoef(signal1, signal2)[0, 1]
        corr_1_3 = np.corrcoef(signal1, signal3)[0, 1]
        corr_2_3 = np.corrcoef(signal2, signal3)[0, 1]
        
        print("\nSignal Correlations:")
        print(f"  Signal1 vs Signal2: {corr_1_2:.6f}")
        print(f"  Signal1 vs Signal3: {corr_1_3:.6f}")
        print(f"  Signal2 vs Signal3: {corr_2_3:.6f}")
        
        # FFT analysis
        print("\nFrequency Analysis (FFT):")
        for i, signal in enumerate([signal1, signal2, signal3], 1):
            # Compute FFT
            fft = np.fft.fft(signal)
            freqs = np.fft.fftfreq(len(signal), 1/sample_rate)
            
            # Find dominant frequency
            positive_freqs = freqs[:len(freqs)//2]
            positive_fft = np.abs(fft[:len(fft)//2])
            dominant_idx = np.argmax(positive_fft[1:]) + 1  # Skip DC component
            dominant_freq = positive_freqs[dominant_idx]
            
            print(f"  Signal{i} dominant frequency: {dominant_freq:.2f} Hz")


if __name__ == "__main__":
    create_waveform_file()
    analyze_waveforms()
    compare_signals()
    
    print("\n✓ Waveform example completed successfully!")
    print("\nTip: You can open 'waveforms.tdms' in LabVIEW or other TDMS viewers")