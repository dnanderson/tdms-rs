#!/usr/bin/env python3
"""
Basic example of writing and reading TDMS files with tdms-python
"""

import tdms
import numpy as np

def write_example():
    """Create a simple TDMS file"""
    print("Writing TDMS file...")
    
    with tdms.TdmsWriter("example.tdms") as writer:
        # Set file properties
        writer.set_file_property("title", "Example TDMS File")
        writer.set_file_property("author", "Python Example")
        writer.set_file_property("version", 1)
        
        # Set group properties
        writer.set_group_property("Sensors", "location", "Lab A")
        writer.set_group_property("Sensors", "experiment_id", 42)
        
        # Create channels with different data types
        writer.create_channel("Sensors", "Temperature", tdms.DataType.F64)
        writer.create_channel("Sensors", "Pressure", tdms.DataType.F64)
        writer.create_channel("Sensors", "Status", tdms.DataType.STRING)
        writer.create_channel("Sensors", "Enabled", tdms.DataType.BOOL)
        
        # Set channel properties
        writer.set_channel_property("Sensors", "Temperature", "unit", "°C")
        writer.set_channel_property("Sensors", "Temperature", "min", -40.0)
        writer.set_channel_property("Sensors", "Temperature", "max", 125.0)
        
        writer.set_channel_property("Sensors", "Pressure", "unit", "kPa")
        
        # Generate sample data
        time = np.linspace(0, 10, 1000)
        temperature = 20 + 5 * np.sin(2 * np.pi * 0.5 * time) + np.random.normal(0, 0.1, 1000)
        pressure = 101.3 + 2 * np.cos(2 * np.pi * 0.3 * time) + np.random.normal(0, 0.05, 1000)
        
        # Write numeric data
        writer.write_data("Sensors", "Temperature", temperature)
        writer.write_data("Sensors", "Pressure", pressure)
        
        # Write string data
        status_messages = ["OK", "WARNING", "OK"] * 333 + ["OK"]
        writer.write_strings("Sensors", "Status", status_messages)
        
        # Write boolean data
        enabled = np.random.choice([True, False], size=1000)
        writer.write_data("Sensors", "Enabled", enabled)
    
    print("File written successfully!")


def read_example():
    """Read and display contents of the TDMS file"""
    print("\nReading TDMS file...")
    
    with tdms.TdmsReader("example.tdms") as reader:
        # Display file info
        print(f"Segments: {reader.segment_count}")
        print(f"Channels: {reader.channel_count}")
        
        # Display file properties
        print("\nFile Properties:")
        file_props = reader.get_file_properties()
        for name, value in file_props.items():
            print(f"  {name}: {value}")
        
        # Display groups
        print("\nGroups:")
        for group in reader.list_groups():
            print(f"  {group}")
            group_props = reader.get_group_properties(group)
            if group_props:
                for name, value in group_props.items():
                    print(f"    {name}: {value}")
        
        # Display channels
        print("\nChannels:")
        for channel_path in reader.list_channels():
            print(f"  {channel_path}")
        
        # Read and analyze temperature data
        print("\nTemperature Data Analysis:")
        temp_data = reader.read_data("Sensors", "Temperature")
        print(f"  Samples: {len(temp_data)}")
        print(f"  Mean: {temp_data.mean():.2f}°C")
        print(f"  Min: {temp_data.min():.2f}°C")
        print(f"  Max: {temp_data.max():.2f}°C")
        print(f"  Std Dev: {temp_data.std():.2f}°C")
        
        # Read channel properties
        temp_props = reader.get_channel_properties("Sensors", "Temperature")
        if temp_props:
            print("\n  Temperature Channel Properties:")
            for name, value in temp_props.items():
                print(f"    {name}: {value}")
        
        # Read pressure data
        print("\nPressure Data Analysis:")
        pressure_data = reader.read_data("Sensors", "Pressure")
        print(f"  Samples: {len(pressure_data)}")
        print(f"  Mean: {pressure_data.mean():.2f} kPa")
        print(f"  Min: {pressure_data.min():.2f} kPa")
        print(f"  Max: {pressure_data.max():.2f} kPa")
        
        # Read string data
        status_data = reader.read_strings("Sensors", "Status")
        print(f"\nStatus Messages:")
        print(f"  Total: {len(status_data)}")
        print(f"  Unique values: {set(status_data)}")
        
        # Read boolean data
        enabled_data = reader.read_data("Sensors", "Enabled")
        print(f"\nEnabled Status:")
        print(f"  True: {np.sum(enabled_data)}")
        print(f"  False: {len(enabled_data) - np.sum(enabled_data)}")


if __name__ == "__main__":
    write_example()
    read_example()
    
    print("\n✓ Example completed successfully!")