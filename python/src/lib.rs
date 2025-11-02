// python/src/lib.rs
//! Python bindings for tdms-rs using PyO3

use pyo3::prelude::*;
use pyo3::exceptions::{PyIOError, PyValueError, PyTypeError};
use pyo3::types::{PyList, PyDict};
use numpy::{PyArray1, PyReadonlyArray1, IntoPyArray};
use std::collections::HashMap;

// Re-export the main library
use tdms_rs as tdms;

/// Convert Rust errors to Python exceptions
impl std::convert::From<tdms::TdmsError> for PyErr {
    fn from(err: tdms::TdmsError) -> PyErr {
        match err {
            tdms::TdmsError::Io(e) => PyIOError::new_err(e.to_string()),
            tdms::TdmsError::InvalidTag { expected, found } => {
                PyValueError::new_err(format!("Invalid tag: expected {}, found {}", expected, found))
            }
            tdms::TdmsError::InvalidDataType(dt) => {
                PyValueError::new_err(format!("Invalid data type: {}", dt))
            }
            tdms::TdmsError::ChannelNotFound(ch) => {
                PyValueError::new_err(format!("Channel not found: {}", ch))
            }
            tdms::TdmsError::TypeMismatch { expected, found } => {
                PyTypeError::new_err(format!("Type mismatch: expected {}, found {}", expected, found))
            }
            _ => PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(err.to_string()),
        }
    }
}

/// TDMS Data Type enumeration
#[pyclass(name = "DataType")]
#[derive(Clone)]
pub struct PyDataType {
    inner: tdms::DataType,
}

#[pymethods]
impl PyDataType {
    #[classattr]
    const VOID: u32 = 0;
    #[classattr]
    const I8: u32 = 1;
    #[classattr]
    const I16: u32 = 2;
    #[classattr]
    const I32: u32 = 3;
    #[classattr]
    const I64: u32 = 4;
    #[classattr]
    const U8: u32 = 5;
    #[classattr]
    const U16: u32 = 6;
    #[classattr]
    const U32: u32 = 7;
    #[classattr]
    const U64: u32 = 8;
    #[classattr]
    const F32: u32 = 9;
    #[classattr]
    const F64: u32 = 10;
    #[classattr]
    const STRING: u32 = 0x20;
    #[classattr]
    const BOOLEAN: u32 = 0x21;
    #[classattr]
    const TIMESTAMP: u32 = 0x44;

    fn __repr__(&self) -> String {
        format!("DataType.{}", self.inner.name())
    }
}

impl PyDataType {
    fn from_rust(dt: tdms::DataType) -> Self {
        PyDataType { inner: dt }
    }

    fn to_rust(&self) -> tdms::DataType {
        self.inner
    }
}

/// Convert Python value to PropertyValue
fn py_to_property_value(_py: Python, value: &Bound<'_, PyAny>) -> PyResult<tdms::PropertyValue> {
    if let Ok(v) = value.extract::<i32>() {
        Ok(tdms::PropertyValue::I32(v))
    } else if let Ok(v) = value.extract::<i64>() {
        Ok(tdms::PropertyValue::I64(v))
    } else if let Ok(v) = value.extract::<f64>() {
        Ok(tdms::PropertyValue::Double(v))
    } else if let Ok(v) = value.extract::<f32>() {
        Ok(tdms::PropertyValue::Float(v))
    } else if let Ok(v) = value.extract::<bool>() {
        Ok(tdms::PropertyValue::Boolean(v))
    } else if let Ok(v) = value.extract::<String>() {
        Ok(tdms::PropertyValue::String(v))
    } else {
        Err(PyTypeError::new_err("Unsupported property value type"))
    }
}

/// Convert PropertyValue to Python object
fn property_value_to_py(py: Python, value: &tdms::PropertyValue) -> PyResult<PyObject> {
    Ok(match value {
        tdms::PropertyValue::I8(v) => v.into_py(py),
        tdms::PropertyValue::I16(v) => v.into_py(py),
        tdms::PropertyValue::I32(v) => v.into_py(py),
        tdms::PropertyValue::I64(v) => v.into_py(py),
        tdms::PropertyValue::U8(v) => v.into_py(py),
        tdms::PropertyValue::U16(v) => v.into_py(py),
        tdms::PropertyValue::U32(v) => v.into_py(py),
        tdms::PropertyValue::U64(v) => v.into_py(py),
        tdms::PropertyValue::Float(v) => v.into_py(py),
        tdms::PropertyValue::Double(v) => v.into_py(py),
        tdms::PropertyValue::Boolean(v) => v.into_py(py),
        tdms::PropertyValue::String(v) => v.into_py(py),
        tdms::PropertyValue::Timestamp(ts) => {
            // Convert to Python datetime
            let system_time = ts.to_system_time();
            let datetime = pyo3::types::PyDateTime::from_timestamp(
                py,
                system_time.duration_since(std::time::UNIX_EPOCH).unwrap().as_secs_f64(),
                None,
            )?;
            datetime.into_py(py)
        }
    })
}

/// TDMS Writer for creating TDMS files
#[pyclass(name = "TdmsWriter")]
pub struct PyTdmsWriter {
    writer: Option<tdms::TdmsWriter>,
}

#[pymethods]
impl PyTdmsWriter {
    #[new]
    fn new(path: &str) -> PyResult<Self> {
        let writer = tdms::TdmsWriter::create(path)?;
        Ok(PyTdmsWriter {
            writer: Some(writer),
        })
    }

    /// Set a file-level property
    fn set_file_property(&mut self, py: Python, name: &str, value: &Bound<'_, PyAny>) -> PyResult<()> {
        let writer = self.writer.as_mut()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Writer is closed"))?;
        let prop_value = py_to_property_value(py, value)?;
        writer.set_file_property(name, prop_value);
        Ok(())
    }

    /// Set a group-level property
    fn set_group_property(&mut self, py: Python, group: &str, name: &str, value: &Bound<'_, PyAny>) -> PyResult<()> {
        let writer = self.writer.as_mut()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Writer is closed"))?;
        let prop_value = py_to_property_value(py, value)?;
        writer.set_group_property(group, name, prop_value);
        Ok(())
    }

    /// Set a channel property
    fn set_channel_property(&mut self, py: Python, group: &str, channel: &str, name: &str, value: &Bound<'_, PyAny>) -> PyResult<()> {
        let writer = self.writer.as_mut()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Writer is closed"))?;
        let prop_value = py_to_property_value(py, value)?;
        writer.set_channel_property(group, channel, name, prop_value)?;
        Ok(())
    }

    /// Create a channel
    fn create_channel(&mut self, group: &str, channel: &str, data_type: u32) -> PyResult<()> {
        let writer = self.writer.as_mut()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Writer is closed"))?;
        let dt = tdms::DataType::from_u32(data_type)
            .ok_or_else(|| PyValueError::new_err(format!("Invalid data type: {}", data_type)))?;
        writer.create_channel(group, channel, dt)?;
        Ok(())
    }

    /// Write numeric data to a channel (supports NumPy arrays)
    fn write_data_i32(&mut self, group: &str, channel: &str, data: PyReadonlyArray1<i32>) -> PyResult<()> {
        let writer = self.writer.as_mut()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Writer is closed"))?;
        let data_slice = data.as_slice()?;
        writer.write_channel_data(group, channel, data_slice)?;
        Ok(())
    }

    /// Write i64 data to a channel
    fn write_data_i64(&mut self, group: &str, channel: &str, data: PyReadonlyArray1<i64>) -> PyResult<()> {
        let writer = self.writer.as_mut()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Writer is closed"))?;
        let data_slice = data.as_slice()?;
        writer.write_channel_data(group, channel, data_slice)?;
        Ok(())
    }

    /// Write f32 data to a channel
    fn write_data_f32(&mut self, group: &str, channel: &str, data: PyReadonlyArray1<f32>) -> PyResult<()> {
        let writer = self.writer.as_mut()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Writer is closed"))?;
        let data_slice = data.as_slice()?;
        writer.write_channel_data(group, channel, data_slice)?;
        Ok(())
    }

    /// Write f64 data to a channel
    fn write_data_f64(&mut self, group: &str, channel: &str, data: PyReadonlyArray1<f64>) -> PyResult<()> {
        let writer = self.writer.as_mut()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Writer is closed"))?;
        let data_slice = data.as_slice()?;
        writer.write_channel_data(group, channel, data_slice)?;
        Ok(())
    }

    /// Write boolean data to a channel
    fn write_data_bool(&mut self, group: &str, channel: &str, data: PyReadonlyArray1<bool>) -> PyResult<()> {
        let writer = self.writer.as_mut()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Writer is closed"))?;
        let data_slice = data.as_slice()?;
        writer.write_channel_data(group, channel, data_slice)?;
        Ok(())
    }

    /// Write string data to a channel
    fn write_strings(&mut self, group: &str, channel: &str, data: Vec<String>) -> PyResult<()> {
        let writer = self.writer.as_mut()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Writer is closed"))?;
        writer.write_channel_strings(group, channel, &data)?;
        Ok(())
    }

    /// Flush buffered data to disk
    fn flush(&mut self) -> PyResult<()> {
        let writer = self.writer.as_mut()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Writer is closed"))?;
        writer.flush()?;
        Ok(())
    }

    /// Close the writer (automatically flushes)
    fn close(&mut self) -> PyResult<()> {
        if let Some(mut writer) = self.writer.take() {
            writer.flush()?;
        }
        Ok(())
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __exit__(&mut self, _exc_type: Option<&Bound<'_, PyAny>>, _exc_value: Option<&Bound<'_, PyAny>>, _traceback: Option<&Bound<'_, PyAny>>) -> PyResult<bool> {
        self.close()?;
        Ok(false)
    }
}

/// TDMS Reader for reading TDMS files
#[pyclass(name = "TdmsReader")]
pub struct PyTdmsReader {
    reader: Option<tdms::TdmsReader<std::io::BufReader<std::fs::File>>>,
}

#[pymethods]
impl PyTdmsReader {
    #[new]
    fn new(path: &str) -> PyResult<Self> {
        let reader = tdms::TdmsReader::open(path)?;
        Ok(PyTdmsReader {
            reader: Some(reader),
        })
    }

    /// List all channels in the file
    fn list_channels(&self) -> PyResult<Vec<String>> {
        let reader = self.reader.as_ref()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Reader is closed"))?;
        Ok(reader.list_channels())
    }

    /// List all groups in the file
    fn list_groups(&self) -> PyResult<Vec<String>> {
        let reader = self.reader.as_ref()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Reader is closed"))?;
        Ok(reader.list_groups())
    }

    /// Get file properties
    fn get_file_properties(&self, py: Python) -> PyResult<PyObject> {
        let reader = self.reader.as_ref()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Reader is closed"))?;
        let props = reader.get_file_properties();
        let dict = PyDict::new(py);
        for (name, prop) in props.iter() {
            dict.set_item(name, property_value_to_py(py, &prop.value)?)?;
        }
        Ok(dict.into_py(py))
    }

    /// Get group properties
    fn get_group_properties(&self, py: Python, group: &str) -> PyResult<Option<PyObject>> {
        let reader = self.reader.as_ref()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Reader is closed"))?;
        if let Some(props) = reader.get_group_properties(group) {
            let dict = PyDict::new(py);
            for (name, prop) in props.iter() {
                dict.set_item(name, property_value_to_py(py, &prop.value)?)?;
            }
            Ok(Some(dict.into_py(py)))
        } else {
            Ok(None)
        }
    }

    /// Get channel properties
    fn get_channel_properties(&self, py: Python, group: &str, channel: &str) -> PyResult<Option<PyObject>> {
        let reader = self.reader.as_ref()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Reader is closed"))?;
        if let Some(props) = reader.get_channel_properties(group, channel) {
            let dict = PyDict::new(py);
            for (name, prop) in props.iter() {
                dict.set_item(name, property_value_to_py(py, &prop.value)?)?;
            }
            Ok(Some(dict.into_py(py)))
        } else {
            Ok(None)
        }
    }

    /// Read i32 data from a channel
    fn read_data_i32<'py>(&mut self, py: Python<'py>, group: &str, channel: &str) -> PyResult<Bound<'py, PyArray1<i32>>> {
        let reader = self.reader.as_mut()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Reader is closed"))?;
        let data: Vec<i32> = reader.read_channel_data(group, channel)?;
        Ok(data.into_pyarray(py))
    }

    /// Read i64 data from a channel
    fn read_data_i64<'py>(&mut self, py: Python<'py>, group: &str, channel: &str) -> PyResult<Bound<'py, PyArray1<i64>>> {
        let reader = self.reader.as_mut()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Reader is closed"))?;
        let data: Vec<i64> = reader.read_channel_data(group, channel)?;
        Ok(data.into_pyarray(py))
    }

    /// Read f32 data from a channel
    fn read_data_f32<'py>(&mut self, py: Python<'py>, group: &str, channel: &str) -> PyResult<Bound<'py, PyArray1<f32>>> {
        let reader = self.reader.as_mut()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Reader is closed"))?;
        let data: Vec<f32> = reader.read_channel_data(group, channel)?;
        Ok(data.into_pyarray(py))
    }

    /// Read f64 data from a channel
    fn read_data_f64<'py>(&mut self, py: Python<'py>, group: &str, channel: &str) -> PyResult<Bound<'py, PyArray1<f64>>> {
        let reader = self.reader.as_mut()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Reader is closed"))?;
        let data: Vec<f64> = reader.read_channel_data(group, channel)?;
        Ok(data.into_pyarray(py))
    }

    /// Read boolean data from a channel
    fn read_data_bool<'py>(&mut self, py: Python<'py>, group: &str, channel: &str) -> PyResult<Bound<'py, PyArray1<bool>>> {
        let reader = self.reader.as_mut()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Reader is closed"))?;
        let data: Vec<bool> = reader.read_channel_data(group, channel)?;
        Ok(data.into_pyarray(py))
    }

    /// Read string data from a channel
    fn read_strings(&mut self, group: &str, channel: &str) -> PyResult<Vec<String>> {
        let reader = self.reader.as_mut()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Reader is closed"))?;
        let data = reader.read_channel_strings(group, channel)?;
        Ok(data)
    }

    /// Get the number of segments in the file
    fn segment_count(&self) -> PyResult<usize> {
        let reader = self.reader.as_ref()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Reader is closed"))?;
        Ok(reader.segment_count())
    }

    /// Get the number of channels in the file
    fn channel_count(&self) -> PyResult<usize> {
        let reader = self.reader.as_ref()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Reader is closed"))?;
        Ok(reader.channel_count())
    }

    /// Close the reader
    fn close(&mut self) {
        self.reader.take();
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __exit__(&mut self, _exc_type: Option<&Bound<'_, PyAny>>, _exc_value: Option<&Bound<'_, PyAny>>, _traceback: Option<&Bound<'_, PyAny>>) -> PyResult<bool> {
        self.close();
        Ok(false)
    }
}

/// Defragment a TDMS file
#[pyfunction]
fn defragment(source_path: &str, dest_path: &str) -> PyResult<()> {
    tdms::defragment(source_path, dest_path)?;
    Ok(())
}

/// Python module for TDMS file I/O
#[pymodule]
fn tdms_python(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyDataType>()?;
    m.add_class::<PyTdmsWriter>()?;
    m.add_class::<PyTdmsReader>()?;
    m.add_function(wrap_pyfunction!(defragment, m)?)?;
    
    // Add version info
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    
    Ok(())
}