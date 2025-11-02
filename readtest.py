import sys
from nptdms import TdmsFile
import numpy as np

# Number of data points to show from the start and end of a large channel
PREVIEW_LIMIT = 5

def print_properties(props, indent_level=1):
    """Helper function to pretty-print a properties dictionary."""
    indent = "  " * indent_level
    if not props:
        print(f"{indent}Properties: (None)")
        return
        
    print(f"{indent}Properties:")
    for key, value in props.items():
        # Truncate long property values for display
        value_str = str(value)
        if len(value_str) > 100:
            value_str = value_str[:100] + "..."
        print(f"{indent}  - {key}: {value_str}")

def print_channel_data(channel):
    """Prints a preview of the channel's data."""
    indent = "  " * 3
    channel_len = len(channel)

    if channel_len == 0:
        print(f"{indent}Data: (Empty)")
        return

    print(f"{indent}Data Preview:")
    try:
        if channel_len <= (PREVIEW_LIMIT * 2):
            # If the channel is small, print all data
            data = channel[:]  #
            print(f"{indent}  ({channel_len} items): {data}")
        else:
            # Otherwise, print a snippet from the start and end
            first_data = channel[0:PREVIEW_LIMIT]  #
            last_data = channel[-PREVIEW_LIMIT:]
            print(f"{indent}  First {PREVIEW_LIMIT}: {first_data}")
            print(f"{indent}  ...")
            print(f"{indent}  Last {PREVIEW_LIMIT}: {last_data}")
    except Exception as e:
        print(f"{indent}  Could not read data: {e}")


def main():
    if len(sys.argv) < 2:
        print(f"Usage: python {sys.argv[0]} <your_file.tdms>")
        print("This script reads a TDMS file and prints a summary of its contents.")
        sys.exit(1)

    filepath = sys.argv[1]
    
    # Set numpy to print full arrays instead of truncating
    np.set_printoptions(threshold=sys.maxsize)

    try:
        # Use TdmsFile.open() as a context manager
        # This is memory-efficient as it only reads metadata initially
        with TdmsFile.open(filepath) as tdms_file:
            print(f"--- File: {filepath} ---")
            print_properties(tdms_file.properties, indent_level=1) #

            # Iterate through all groups in the file
            for group in tdms_file.groups():
                print("\n" + "="*40)
                print(f"  Group: {group.name}")
                print(f"  Group Path: {group.path}") #
                print_properties(group.properties, indent_level=2) #

                # Iterate through all channels in the group
                for channel in group.channels():
                    print("\n" + "-"*30)
                    print(f"    Channel: {channel.name}")
                    print(f"    Channel Path: {channel.path}") #
                    
                    # Print channel size (length), as requested
                    channel_len = len(channel)
                    print(f"    Channel Size (Length): {channel_len}")

                    # Print data type
                    if channel.data_type:
                        print(f"    Data Type: {channel.data_type.__name__}")
                    else:
                        print(f"    Data Type: (No data)")

                    # Print channel properties
                    print_properties(channel.properties, indent_level=3)

                    # Print a preview of the data
                    print_channel_data(channel)

    except FileNotFoundError:
        print(f"\nError: File not found at '{filepath}'")
    except Exception as e:
        print(f"\nAn error occurred while reading the file: {e}")

if __name__ == "__main__":
    main()