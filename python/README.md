# TDMS Python

High-performance Python bindings for TDMS (Technical Data Management Streaming) file I/O, powered by Rust.

## Features

- 🚀 **Blazingly Fast**: Built on Rust for maximum performance
- 🐍 **Pythonic API**: Natural Python interface with NumPy integration
- 🔒 **Type Safe**: Strong typing with automatic type detection
- 📦 **Zero-Copy**: Efficient data transfer between Rust and Python
- ✅ **Fully Compatible**: Reads/writes TDMS 2.0 files compatible with LabVIEW and nptdms

## Installation

### From Source (Development)

1. Install Rust (if not already installed):
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

2. Install Python dependencies:
```bash
pip install maturin numpy
```

3. Build and install the package:
```bash
cd python
maturin develop --release
```

### From PyPI (Coming Soon)

```bash
pip install tdms-python
```

## Quick Start

### Writing TDMS Files

```python
import tdms
import numpy as np

# Create a new TDMS file
with tdms.TdmsWriter("output.tdms") as writer:
    # Set file properties
    writer.set_file_property("title", "My Experiment")
    writer.set_file_property("author", "Python")
    
    # Set group properties
    writer.set_group_property("Sensors", "location", "Lab A")
    
    # Create channels
    writer.create_channel("Sensors", "Voltage", tdms.DataType.F64)
    writer.create_channel("Sensors", "Current", tdms.DataType.F64)
    
    # Set channel properties
    writer.set_channel_property("Sensors", "Voltage", "unit", "V")
    writer.set_channel_property("Sensors", "Current", "unit", "A")
    
    # Write data (NumPy arrays)
    voltage = np.sin(np.linspace(0, 2*np.pi, 1000))
    current = np.cos(np.linspace(0, 2*np.pi, 1000))
    
    writer.write_data("Sensors", "Voltage", voltage)
    writer.write_data("Sensors", "Current", current)
```

### Reading TDMS Files

```python
import tdms

# Open a TDMS file
with tdms.TdmsReader("input.tdms") as reader:
    # List all channels
    channels = reader.list_channels()
    print(f"Found {len(channels)} channels")
    
    # Get file properties
    props = reader.get_file_properties()
    print(f"Title: {props.get('title')}")
    
    # Read data from a channel (returns NumPy array)
    voltage = reader.read_data("Sensors", "Voltage")
    print(f"Voltage shape: {voltage.shape}")
    print(f"Mean voltage: {voltage.mean():.3f} V")
    
    # Get channel properties
    channel_props = reader.get_channel_properties("Sensors", "Voltage")
    print(f"Unit: {channel_props.get('unit')}")
```

### Working with Strings

```python
import tdms

with tdms.TdmsWriter("strings.tdms") as writer:
    writer.create_channel("Data", "Messages", tdms.DataType.STRING)
    
    messages = ["Hello", "World", "TDMS", "Python"]
    writer.write_strings("Data", "Messages", messages)

with tdms.TdmsReader("strings.tdms") as reader:
    messages = reader.read_strings("Data", "Messages")
    print(messages)  # ['Hello', 'World', 'TDMS', 'Python']
```

### Automatic Type Detection

```python
import tdms
import numpy as np

with tdms.TdmsWriter("auto_type.tdms") as writer:
    writer.create_channel("Data", "Values", tdms.DataType.F64)
    
    # Automatically detects int32
    int_data = np.array([1, 2, 3, 4, 5], dtype=np.int32)
    writer.write_data("Data", "Values", int_data)
    
    # Automatically detects float64
    float_data = np.array([1.1, 2.2, 3.3], dtype=np.float64)
    writer.write_data("Data", "Values", float_data)
```

## API Reference

### TdmsWriter

```python
class TdmsWriter:
    def __init__(self, path: str)
    def set_file_property(self, name: str, value: Union[int, float, str, bool])
    def set_group_property(self, group: str, name: str, value: Union[int, float, str, bool])
    def set_channel_property(self, group: str, channel: str, name: str, value: Union[int, float, str, bool])
    def create_channel(self, group: str, channel: str, data_type: int)
    def write_data(self, group: str, channel: str, data: np.ndarray)
    def write_strings(self, group: str, channel: str, data: List[str])
    def flush(self)
    def close(self)
```

### TdmsReader

```python
class TdmsReader:
    def __init__(self, path: str)
    def list_channels(self) -> List[str]
    def list_groups(self) -> List[str]
    def get_file_properties(self) -> Dict[str, Any]
    def get_group_properties(self, group: str) -> Optional[Dict[str, Any]]
    def get_channel_properties(self, group: str, channel: str) -> Optional[Dict[str, Any]]
    def read_data(self, group: str, channel: str) -> np.ndarray
    def read_strings(self, group: str, channel: str) -> List[str]
    @property segment_count: int
    @property channel_count: int
    def close(self)
```

### DataType Constants

```python
class DataType:
    VOID = 0
    I8 = 1      # INT8 (alias)
    I16 = 2     # INT16 (alias)
    I32 = 3     # INT32 (alias)
    I64 = 4     # INT64 (alias)
    U8 = 5      # UINT8 (alias)
    U16 = 6     # UINT16 (alias)
    U32 = 7     # UINT32 (alias)
    U64 = 8     # UINT64 (alias)
    F32 = 9     # FLOAT32 (alias)
    F64 = 10    # FLOAT64 (alias)
    STRING = 0x20
    BOOLEAN = 0x21  # BOOL (alias)
    TIMESTAMP = 0x44
```

### Utility Functions

```python
def defragment(source_path: str, dest_path: str) -> None
    """Defragment a TDMS file by consolidating all segments"""
```

## Performance

TDMS Python leverages Rust's performance while providing zero-copy data transfer through NumPy. Typical performance:

- **Write**: ~1.2 GB/s for numeric data
- **Read**: ~2.5 GB/s for numeric data
- **Strings**: ~450 MB/s write, ~800 MB/s read

Benchmarks performed on M1 MacBook Pro.

## Compatibility

- Python 3.8+
- NumPy 1.20+
- Compatible with TDMS files created by:
  - LabVIEW 2014+
  - nptdms
  - Other TDMS 2.0 compatible software

## Examples

See the `examples/` directory for more comprehensive examples:

- `basic_example.py` - Basic reading and writing
- `waveform_example.py` - Working with waveform data
- `large_file_example.py` - Handling large datasets efficiently

## Development

### Building from Source

```bash
# Clone the repository
git clone https://github.com/yourusername/tdms-rs
cd tdms-rs/python

# Create a virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install development dependencies
pip install -r requirements-dev.txt

# Build and install in development mode
maturin develop
```

### Running Tests

```bash
pytest tests/
```

### Building Wheels

```bash
maturin build --release
```

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](../LICENSE-APACHE))
- MIT license ([LICENSE-MIT](../LICENSE-MIT))

at your option.

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](../CONTRIBUTING.md) for details.

## Acknowledgments

- Built with [PyO3](https://pyo3.rs/) for Rust-Python interoperability
- Uses [maturin](https://github.com/PyO3/maturin) for building
- Inspired by [nptdms](https://github.com/adamreeve/npTDMS) for API design