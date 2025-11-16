// python/src/lib.rs
//! Python bindings for tdms-rs using PyO3

use pyo3::prelude::*;
use pyo3::exceptions::{PyValueError, PyTypeError};
use pyo3::types::{PyDict, PyAny, PyDateTime, PyModule}; // <-- ADDED PyDateTime, PyModule
use numpy::{PyArray1, PyArrayMethods, IntoPyArray}; // <-- REMOVED IntoPyArray

// Re-export the main library
use tdms_rs as tdms;

// TDMS epoch (1904-01-01) is 2082844800 seconds before the UNIX epoch (1970-01-01)
const TDMS_EPOCH_OFFSET_SECONDS: i64 = 2082844800;
const NANOS_PER_SECOND: i64 = 1_000_000_000;

fn tdms_error_to_pyerr(err: tdms::TdmsError) -> PyErr {
    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(err.to_string())
}

/// Helper function to convert Python float timestamp to TDMS Timestamp
fn unix_to_tdms_timestamp(unix_seconds: i64, nanos_subsec: u32) -> tdms::Timestamp {
    let nanos_subsec_u64 = nanos_subsec as u64;
    let fractions = (nanos_subsec_u64 as u128 * (1u128 << 64) / 1_000_000_000) as u64;
    let tdms_seconds = unix_seconds + TDMS_EPOCH_OFFSET_SECONDS;
    tdms::Timestamp { seconds: tdms_seconds, fractions }
}

/// Helper function to convert nanoseconds (from numpy) to TDMS Timestamp
fn nanos_to_tdms_timestamp(nanos_since_1970: i64) -> tdms::Timestamp {
    let unix_seconds = nanos_since_1970.div_euclid(NANOS_PER_SECOND);
    // Use u32 for nanos_subsec as it's always < 1,000,000,000
    let nanos_subsec = nanos_since_1970.rem_euclid(NANOS_PER_SECOND) as u32;
    unix_to_tdms_timestamp(unix_seconds, nanos_subsec)
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


/// Convert Python value to PropertyValue
/// UPDATED to support datetime and numpy.datetime64
fn py_to_property_value(_py: Python, value: &Bound<'_, PyAny>) -> PyResult<tdms::PropertyValue> { // <-- FIX: Renamed py to _py
    
    // Check for standard Python datetime
    if value.is_instance_of::<PyDateTime>() {
        // It's a standard datetime.datetime object.
        // Call .timestamp() to get float seconds since UNIX epoch
        let timestamp_float = value.call_method0("timestamp")?.extract::<f64>()?;
        
        let unix_seconds = timestamp_float.floor() as i64;
        let nanos_subsec = (timestamp_float.fract() * 1_000_000_000.0).round() as u32;

        return Ok(tdms::PropertyValue::Timestamp(unix_to_tdms_timestamp(unix_seconds, nanos_subsec)));
    }

    // Check for numpy.datetime64 scalar
    let type_name = value.get_type().name()?;
    if type_name == "datetime64" {
         // It's a numpy.datetime64 scalar.
         // Convert to nanoseconds since epoch (i64)
        let nanos_since_1970 = value.call_method1("astype", ("datetime64[ns]",))?
                                    .call_method1("astype", ("int64",))?
                                    .extract::<i64>()?;

        return Ok(tdms::PropertyValue::Timestamp(nanos_to_tdms_timestamp(nanos_since_1970)));
    }
    
    // --- Keep existing checks ---
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
         // Fallback error
        Err(PyTypeError::new_err(format!("Unsupported property value type: {}", value.get_type().name()?)))
    }
}

/// Convert PropertyValue to Python object
/// UPDATED to return numpy.datetime64[ns] for timestamps
fn property_value_to_py(py: Python, value: &tdms::PropertyValue) -> PyResult<Py<PyAny>> {
    Ok(match value {
        tdms::PropertyValue::I8(v) => (*v).into_pyobject(py)?.into_any().unbind(),
        tdms::PropertyValue::I16(v) => (*v).into_pyobject(py)?.into_any().unbind(),
        tdms::PropertyValue::I32(v) => (*v).into_pyobject(py)?.into_any().unbind(),
        tdms::PropertyValue::I64(v) => (*v).into_pyobject(py)?.into_any().unbind(),
        tdms::PropertyValue::U8(v) => (*v).into_pyobject(py)?.into_any().unbind(),
        tdms::PropertyValue::U16(v) => (*v).into_pyobject(py)?.into_any().unbind(),
        tdms::PropertyValue::U32(v) => (*v).into_pyobject(py)?.into_any().unbind(),
        tdms::PropertyValue::U64(v) => (*v).into_pyobject(py)?.into_any().unbind(),
        tdms::PropertyValue::Float(v) => (*v).into_pyobject(py)?.into_any().unbind(),
        tdms::PropertyValue::Double(v) => (*v).into_pyobject(py)?.into_any().unbind(),
        tdms::PropertyValue::Boolean(v) => (*v).into_pyobject(py)?.as_any().clone().unbind(),
        tdms::PropertyValue::String(v) => v.as_str().into_pyobject(py)?.into_any().unbind(),
        tdms::PropertyValue::Timestamp(ts) => {
            // Convert to numpy.datetime64[ns] for full precision
            
            // 1. Convert tdms::Timestamp to i64 nanoseconds since UNIX epoch
            let unix_seconds = ts.seconds - TDMS_EPOCH_OFFSET_SECONDS;
            let nanos_subsec = ((ts.fractions as u128 * 1_000_000_000) / (1u128 << 64)) as i64;
            let nanos_since_1970 = (unix_seconds * NANOS_PER_SECOND) + nanos_subsec;

            // 2. Create a 0-d numpy array (scalar) with this value
            let np = PyModule::import(py, "numpy")?;
            let scalar_array = np.call_method1("array", (nanos_since_1970,))?;
            
            // 3. Cast it to datetime64[ns]
            let datetime_dtype = np.call_method1("dtype", ("datetime64[ns]",))?;
            let datetime_scalar = scalar_array.call_method1("astype", (datetime_dtype,))?;
            
            // 4. Extract the scalar value from the 0-d array
            // Return the numpy.datetime64 scalar object itself
            datetime_scalar.into_any().unbind()
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
        let writer = tdms::TdmsWriter::create(path).map_err(tdms_error_to_pyerr)?;
        Ok(PyTdmsWriter {
            writer: Some(writer),
        })
    }

    /// Set a file-level property
    fn set_file_property(&mut self, py: Python, name: &str, value: &Bound<'_, PyAny>) -> PyResult<()> {
        let writer = self.writer.as_mut()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Writer is closed"))?;
        let prop_value = py_to_property_value(py, value)?; // <-- USES UPDATED FUNCTION
        writer.set_file_property(name, prop_value);
        Ok(())
    }

    /// Set a group-level property
    fn set_group_property(&mut self, py: Python, group: &str, name: &str, value: &Bound<'_, PyAny>) -> PyResult<()> {
        let writer = self.writer.as_mut()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Writer is closed"))?;
        let prop_value = py_to_property_value(py, value)?; // <-- USES UPDATED FUNCTION
        writer.set_group_property(group, name, prop_value);
        Ok(())
    }

    /// Set a channel property
    fn set_channel_property(&mut self, py: Python, group: &str, channel: &str, name: &str, value: &Bound<'_, PyAny>) -> PyResult<()> {
        let writer = self.writer.as_mut()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Writer is closed"))?;
        let prop_value = py_to_property_value(py, value)?; // <-- USES UPDATED FUNCTION
        writer.set_channel_property(group, channel, name, prop_value).map_err(tdms_error_to_pyerr)?;
        Ok(())
    }



    /// Create a channel
    fn create_channel(&mut self, group: &str, channel: &str, data_type: u32) -> PyResult<()> {
        let writer = self.writer.as_mut()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Writer is closed"))?;
        let dt = tdms::DataType::from_u32(data_type)
            .ok_or_else(|| PyValueError::new_err(format!("Invalid data type: {}", data_type)))?;
        writer.create_channel(group, channel, dt).map_err(tdms_error_to_pyerr)?;
        Ok(())
    }

    /// Write numeric data to a channel (supports NumPy arrays)
    /// This function accepts a NumPy array and dispatches to the
    /// correct internal writer based on the array's dtype.
    #[pyo3(name = "write_data")]
    fn write_data_any<'py>(
        &mut self,
        _py: Python<'py>, // We take this to allow using Bound<'py, PyAny>
        group: &str,
        channel: &str,
        data: &Bound<'py, PyAny> // Generic NumPy array input
    ) -> PyResult<()> {
        let writer = self.writer.as_mut()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Writer is closed"))?;

        let dtype = data.getattr("dtype")?;
        let dtype_char = dtype.getattr("char")?.extract::<char>()?;

        // Check for datetime64 ('M')
        if dtype_char == 'M' {
            // Call numpy.astype('int64') to convert datetime64 to i64
            let arr_i64 = data.call_method1("astype", ("int64",))
                .map_err(|e| PyTypeError::new_err(format!("Failed to cast datetime64[ns] to int64: {}", e)))?;

            // Now cast the resulting i64 PyAny to a PyArray
            let arr = arr_i64.cast::<PyArray1<i64>>()
                .map_err(|_| PyTypeError::new_err("Could not cast result of astype('int64') to PyArray1<i64>"))?;

            let readonly_arr = arr.readonly();
            let data_slice = readonly_arr.as_slice()?;

            let timestamps: Vec<tdms::Timestamp> = data_slice.iter().map(|&nanos_since_1970| {
                nanos_to_tdms_timestamp(nanos_since_1970)
            }).collect();

            writer.write_channel_data(group, channel, &timestamps).map_err(tdms_error_to_pyerr)?;
        }
        // Try f64
        else if let Ok(arr) = data.cast::<PyArray1<f64>>() {
            let readonly_arr = arr.readonly();
            let data_slice = readonly_arr.as_slice()?;
            writer.write_channel_data(group, channel, data_slice).map_err(tdms_error_to_pyerr)?;
        }
        // Try f32
        else if let Ok(arr) = data.cast::<PyArray1<f32>>() {
            let readonly_arr = arr.readonly();
            let data_slice = readonly_arr.as_slice()?;
            writer.write_channel_data(group, channel, data_slice).map_err(tdms_error_to_pyerr)?;
        }
        // Try i32
        else if let Ok(arr) = data.cast::<PyArray1<i32>>() {
            let readonly_arr = arr.readonly();
            let data_slice = readonly_arr.as_slice()?;
            writer.write_channel_data(group, channel, data_slice).map_err(tdms_error_to_pyerr)?;
        }
        // Try i64
        else if let Ok(arr) = data.cast::<PyArray1<i64>>() {
            let readonly_arr = arr.readonly();
            let data_slice = readonly_arr.as_slice()?;
            writer.write_channel_data(group, channel, data_slice).map_err(tdms_error_to_pyerr)?;
        }
        // Try bool
        else if let Ok(arr) = data.cast::<PyArray1<bool>>() {
            let readonly_arr = arr.readonly();
            let data_slice = readonly_arr.as_slice()?;
            writer.write_channel_data(group, channel, data_slice).map_err(tdms_error_to_pyerr)?;
        }
        // Try u32
        else if let Ok(arr) = data.cast::<PyArray1<u32>>() {
            let readonly_arr = arr.readonly();
            let data_slice = readonly_arr.as_slice()?;
            writer.write_channel_data(group, channel, data_slice).map_err(tdms_error_to_pyerr)?;
        }
        // Try u64
        else if let Ok(arr) = data.cast::<PyArray1<u64>>() {
            let readonly_arr = arr.readonly();
            let data_slice = readonly_arr.as_slice()?;
            writer.write_channel_data(group, channel, data_slice).map_err(tdms_error_to_pyerr)?;
        }
        // Try i16
        else if let Ok(arr) = data.cast::<PyArray1<i16>>() {
            let readonly_arr = arr.readonly();
            let data_slice = readonly_arr.as_slice()?;
            writer.write_channel_data(group, channel, data_slice).map_err(tdms_error_to_pyerr)?;
        }
        // Try u16
        else if let Ok(arr) = data.cast::<PyArray1<u16>>() {
            let readonly_arr = arr.readonly();
            let data_slice = readonly_arr.as_slice()?;
            writer.write_channel_data(group, channel, data_slice).map_err(tdms_error_to_pyerr)?;
        }
        // Try i8
        else if let Ok(arr) = data.cast::<PyArray1<i8>>() {
            let readonly_arr = arr.readonly();
            let data_slice = readonly_arr.as_slice()?;
            writer.write_channel_data(group, channel, data_slice).map_err(tdms_error_to_pyerr)?;
        }
        // Try u8
        else if let Ok(arr) = data.cast::<PyArray1<u8>>() {
            let readonly_arr = arr.readonly();
            let data_slice = readonly_arr.as_slice()?;
            writer.write_channel_data(group, channel, data_slice).map_err(tdms_error_to_pyerr)?;
        }
        // Fallback error
        else {
            return Err(PyTypeError::new_err(format!(
                "Unsupported numpy dtype '{}' for channel '{}/{}'",
                dtype.getattr("name")?.extract::<String>()?, group, channel
            )));
        }

        Ok(())
    }

    /// Write string data to a channel
    fn write_strings(&mut self, group: &str, channel: &str, data: Vec<String>) -> PyResult<()> {
        let writer = self.writer.as_mut()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Writer is closed"))?;
        writer.write_channel_strings(group, channel, &data).map_err(tdms_error_to_pyerr)?;
        Ok(())
    }

    /// Flush buffered data to disk
    fn flush(&mut self) -> PyResult<()> {
        let writer = self.writer.as_mut()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Writer is closed"))?;
        writer.flush().map_err(tdms_error_to_pyerr)?;
        Ok(())
    }

    /// Close the writer (automatically flushes)
    fn close(&mut self) -> PyResult<()> {
        if let Some(mut writer) = self.writer.take() {
            writer.flush().map_err(tdms_error_to_pyerr)?;
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

/// An asynchronous TDMS writer that can be used from multiple Python threads.
#[pyclass(name = "AsyncTdmsWriter")]
pub struct PyAsyncTdmsWriter {
    writer: Option<tdms::AsyncTdmsWriter>,
    runtime: Option<tokio::runtime::Runtime>,
}

#[pymethods]
impl PyAsyncTdmsWriter {
    #[new]
    fn new(path: &str) -> PyResult<Self> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        let writer = runtime.block_on(tdms::AsyncTdmsWriter::create(path))
            .map_err(tdms_error_to_pyerr)?;
        Ok(PyAsyncTdmsWriter {
            writer: Some(writer),
            runtime: Some(runtime),
        })
    }

    fn create_channel(&mut self, group: &str, channel: &str, data_type: u32) -> PyResult<()> {
        let writer = self.writer.as_mut().ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Writer is closed"))?;
        let runtime = self.runtime.as_ref().ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Runtime is closed"))?;
        let dt = tdms::DataType::from_u32(data_type).ok_or_else(|| PyValueError::new_err(format!("Invalid data type: {}", data_type)))?;
        runtime.block_on(writer.create_channel(group.to_string(), channel.to_string(), dt)).map_err(tdms_error_to_pyerr)
    }

    fn set_file_property(&self, py: Python, name: &str, value: &Bound<'_, PyAny>) -> PyResult<()> {
        let writer = self.writer.as_ref().ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Writer is closed"))?;
        let prop_value = py_to_property_value(py, value)?;
        writer.set_file_property(name, prop_value).map_err(tdms_error_to_pyerr)
    }

    #[pyo3(name = "write_data")]
    fn write_data_any<'py>(
        &self,
        _py: Python<'py>,
        group: &str,
        channel: &str,
        data: &Bound<'py, PyAny>
    ) -> PyResult<()> {
        let writer = self.writer.as_ref().ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Writer is closed"))?;
        let runtime = self.runtime.as_ref().ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Runtime is closed"))?;

        let dtype = data.getattr("dtype")?;
        let dtype_char = dtype.getattr("char")?.extract::<char>()?;

        if dtype_char == 'M' {
            let arr_i64 = data.call_method1("astype", ("int64",))?;
            let arr = arr_i64.cast::<PyArray1<i64>>()?;
            let readonly_arr = arr.readonly();
            let data_slice = readonly_arr.as_slice()?;
            let timestamps: Vec<tdms::Timestamp> = data_slice.iter().map(|&ns| nanos_to_tdms_timestamp(ns)).collect();
            runtime.block_on(writer.write_channel_data(group.to_string(), channel.to_string(), timestamps, tdms::DataType::TimeStamp)).map_err(tdms_error_to_pyerr)?;
        } else if let Ok(arr) = data.cast::<PyArray1<f64>>() {
            let data_vec = arr.readonly().to_vec()?;
            runtime.block_on(writer.write_channel_data(group.to_string(), channel.to_string(), data_vec, tdms::DataType::DoubleFloat)).map_err(tdms_error_to_pyerr)?;
        } else if let Ok(arr) = data.cast::<PyArray1<f32>>() {
            let data_vec = arr.readonly().to_vec()?;
            runtime.block_on(writer.write_channel_data(group.to_string(), channel.to_string(), data_vec, tdms::DataType::SingleFloat)).map_err(tdms_error_to_pyerr)?;
        } else if let Ok(arr) = data.cast::<PyArray1<i32>>() {
            let data_vec = arr.readonly().to_vec()?;
            runtime.block_on(writer.write_channel_data(group.to_string(), channel.to_string(), data_vec, tdms::DataType::I32)).map_err(tdms_error_to_pyerr)?;
        } else if let Ok(arr) = data.cast::<PyArray1<i64>>() {
            let data_vec = arr.readonly().to_vec()?;
            runtime.block_on(writer.write_channel_data(group.to_string(), channel.to_string(), data_vec, tdms::DataType::I64)).map_err(tdms_error_to_pyerr)?;
        } else if let Ok(arr) = data.cast::<PyArray1<bool>>() {
            let data_vec = arr.readonly().to_vec()?;
            runtime.block_on(writer.write_channel_data(group.to_string(), channel.to_string(), data_vec, tdms::DataType::Boolean)).map_err(tdms_error_to_pyerr)?;
        } else if let Ok(arr) = data.cast::<PyArray1<u32>>() {
            let data_vec = arr.readonly().to_vec()?;
            runtime.block_on(writer.write_channel_data(group.to_string(), channel.to_string(), data_vec, tdms::DataType::U32)).map_err(tdms_error_to_pyerr)?;
        } else if let Ok(arr) = data.cast::<PyArray1<u64>>() {
            let data_vec = arr.readonly().to_vec()?;
            runtime.block_on(writer.write_channel_data(group.to_string(), channel.to_string(), data_vec, tdms::DataType::U64)).map_err(tdms_error_to_pyerr)?;
        } else if let Ok(arr) = data.cast::<PyArray1<i16>>() {
            let data_vec = arr.readonly().to_vec()?;
            runtime.block_on(writer.write_channel_data(group.to_string(), channel.to_string(), data_vec, tdms::DataType::I16)).map_err(tdms_error_to_pyerr)?;
        } else if let Ok(arr) = data.cast::<PyArray1<u16>>() {
            let data_vec = arr.readonly().to_vec()?;
            runtime.block_on(writer.write_channel_data(group.to_string(), channel.to_string(), data_vec, tdms::DataType::U16)).map_err(tdms_error_to_pyerr)?;
        } else if let Ok(arr) = data.cast::<PyArray1<i8>>() {
            let data_vec = arr.readonly().to_vec()?;
            runtime.block_on(writer.write_channel_data(group.to_string(), channel.to_string(), data_vec, tdms::DataType::I8)).map_err(tdms_error_to_pyerr)?;
        } else if let Ok(arr) = data.cast::<PyArray1<u8>>() {
            let data_vec = arr.readonly().to_vec()?;
            runtime.block_on(writer.write_channel_data(group.to_string(), channel.to_string(), data_vec, tdms::DataType::U8)).map_err(tdms_error_to_pyerr)?;
        } else {
            return Err(PyTypeError::new_err(format!(
                "Unsupported numpy dtype '{}' for channel '{}/{}'",
                dtype.getattr("name")?.extract::<String>()?, group, channel
            )));
        }
        Ok(())
    }

    fn write_strings(&self, group: &str, channel: &str, data: Vec<String>) -> PyResult<()> {
        let writer = self.writer.as_ref().ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Writer is closed"))?;
        let runtime = self.runtime.as_ref().ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Runtime is closed"))?;
        runtime.block_on(writer.write_channel_strings(group.to_string(), channel.to_string(), data)).map_err(tdms_error_to_pyerr)
    }

    fn flush(&self) -> PyResult<()> {
        let writer = self.writer.as_ref().ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Writer is closed"))?;
        let runtime = self.runtime.as_ref().ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Runtime is closed"))?;
        runtime.block_on(writer.flush()).map_err(tdms_error_to_pyerr)
    }

    fn close(&mut self) -> PyResult<()> {
        if let Some(writer) = self.writer.take() {
            if let Some(runtime) = self.runtime.take() {
                runtime.block_on(writer.close()).map_err(tdms_error_to_pyerr)?;
            }
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
        let reader = tdms::TdmsReader::open(path).map_err(tdms_error_to_pyerr)?;
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
    fn get_file_properties(&self, py: Python) -> PyResult<Py<PyAny>> {
        let reader = self.reader.as_ref()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Reader is closed"))?;
        let props = reader.get_file_properties();
        let dict = PyDict::new(py);
        for (name, prop) in props.iter() {
            dict.set_item(name, property_value_to_py(py, &prop.value)?)?; // <-- USES UPDATED FUNCTION
        }
        Ok(dict.into())
    }

    /// Get group properties
    fn get_group_properties(&self, py: Python, group: &str) -> PyResult<Option<Py<PyAny>>> {
        let reader = self.reader.as_ref()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Reader is closed"))?;
        if let Some(props) = reader.get_group_properties(group) {
            let dict = PyDict::new(py);
            for (name, prop) in props.iter() {
                dict.set_item(name, property_value_to_py(py, &prop.value)?)?; // <-- USES UPDATED FUNCTION
            }
            Ok(Some(dict.into()))
        } else {
            Ok(None)
        }
    }

    /// Get channel properties
    fn get_channel_properties(&self, py: Python, group: &str, channel: &str) -> PyResult<Option<Py<PyAny>>> {
        let reader = self.reader.as_ref()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Reader is closed"))?;
        if let Some(props) = reader.get_channel_properties(group, channel) {
            let dict = PyDict::new(py);
            for (name, prop) in props.iter() {
                dict.set_item(name, property_value_to_py(py, &prop.value)?)?; // <-- USES UPDATED FUNCTION
            }
            Ok(Some(dict.into()))
        } else {
            Ok(None)
        }
    }

    fn get_channel_data_type(&self, group: &str, channel: &str) -> PyResult<u32> {
        let reader = self.reader.as_ref()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Reader is closed"))?;
        
        let path_str = format!("/'{}/'{}'", group.replace("'", "''"), channel.replace("'", "''"));
        
        if let Some(channel_reader) = reader.get_channel(&path_str) {
            Ok(channel_reader.data_type() as u32)
        } else {
            Err(PyValueError::new_err(format!("Channel not found: {}", path_str)))
        }
    }

    /// Read i32 data from a channel
    fn read_data_i32<'py>(&mut self, py: Python<'py>, group: &str, channel: &str) -> PyResult<Bound<'py, PyArray1<i32>>> {
        let reader = self.reader.as_mut()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Reader is closed"))?;
        let data: Vec<i32> = reader.read_channel_data(group, channel).map_err(tdms_error_to_pyerr)?;
        Ok(data.into_pyarray(py)) // <-- FIX: Changed to into_pyarray
    }

    /// Read i64 data from a channel
    fn read_data_i64<'py>(&mut self, py: Python<'py>, group: &str, channel: &str) -> PyResult<Bound<'py, PyArray1<i64>>> {
        let reader = self.reader.as_mut()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Reader is closed"))?;
        let data: Vec<i64> = reader.read_channel_data(group, channel).map_err(tdms_error_to_pyerr)?;
        Ok(data.into_pyarray(py)) // <-- FIX: Changed to into_pyarray
    }

    /// Read f32 data from a channel
    fn read_data_f32<'py>(&mut self, py: Python<'py>, group: &str, channel: &str) -> PyResult<Bound<'py, PyArray1<f32>>> {
        let reader = self.reader.as_mut()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Reader is closed"))?;
        let data: Vec<f32> = reader.read_channel_data(group, channel).map_err(tdms_error_to_pyerr)?;
        Ok(data.into_pyarray(py)) // <-- FIX: Changed to into_pyarray
    }

    /// Read f64 data from a channel
    fn read_data_f64<'py>(&mut self, py: Python<'py>, group: &str, channel: &str) -> PyResult<Bound<'py, PyArray1<f64>>> {
        let reader = self.reader.as_mut()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Reader is closed"))?;
        let data: Vec<f64> = reader.read_channel_data(group, channel).map_err(tdms_error_to_pyerr)?;
        Ok(data.into_pyarray(py)) // <-- FIX: Changed to into_pyarray
    }

    /// Read boolean data from a channel
    fn read_data_bool<'py>(&mut self, py: Python<'py>, group: &str, channel: &str) -> PyResult<Bound<'py, PyArray1<bool>>> {
        let reader = self.reader.as_mut()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Reader is closed"))?;
        let data: Vec<bool> = reader.read_channel_data(group, channel).map_err(tdms_error_to_pyerr)?;
        Ok(data.into_pyarray(py)) // <-- FIX: Changed to into_pyarray
    }

    /// Read datetime64[ns] data from a channel
    fn read_data_datetime64<'py>(&mut self, py: Python<'py>, group: &str, channel: &str) -> PyResult<Bound<'py, PyAny>> {
        let reader = self.reader.as_mut()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Reader is closed"))?;
        
        let data: Vec<tdms::Timestamp> = reader.read_channel_data(group, channel).map_err(tdms_error_to_pyerr)?;

        let nanos: Vec<i64> = data.iter().map(|&ts| {
            let unix_seconds = ts.seconds - TDMS_EPOCH_OFFSET_SECONDS;
            let nanos_subsec = ((ts.fractions as u128 * 1_000_000_000) / (1u128 << 64)) as i64;
            (unix_seconds * NANOS_PER_SECOND) + nanos_subsec
        }).collect();

        let nanos_array = nanos.into_pyarray(py); // <-- FIX: Changed to into_pyarray

        let np = PyModule::import(py, "numpy")?;
        let datetime_dtype = np.call_method1("dtype", ("datetime64[ns]",))?;
        
        let datetime_array = nanos_array.call_method1("astype", (datetime_dtype,))?;
        Ok(datetime_array)
    }

    /// Read data from a channel, automatically detecting its type.
  #[pyo3(name = "read_data")]
    fn read_data_auto<'py>(&mut self, py: Python<'py>, group: &str, channel: &str) -> PyResult<Bound<'py, PyAny>> {
        
        // 1. Get the channel's data type in a separate, immutable scope
        let data_type = {
            let reader_immut = self.reader.as_ref()
                .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Reader is closed"))?;
            
            let path_str = format!("/'{group}'/'{channel}'", 
                group = group.replace('\'', "''"), 
                channel = channel.replace('\'', "''")
            );
            
            let channel_reader = reader_immut.get_channel(&path_str)
                .ok_or_else(|| PyValueError::new_err(format!("Channel not found: {}", path_str)))?;
            
            channel_reader.data_type()
        }; // <-- Immutable borrow of self.reader ends here.

        // 2. Now, get the mutable borrow.
        let reader_mut = self.reader.as_mut()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Reader is closed"))?;
        
        // 3. Match on the type and call the correct typed read function
        match data_type {
            tdms::DataType::DoubleFloat => {
                let data: Vec<f64> = reader_mut.read_channel_data(group, channel).map_err(tdms_error_to_pyerr)?;
                Ok(data.into_pyarray(py).into_any()) // <-- FIX
            }
            tdms::DataType::SingleFloat => {
                let data: Vec<f32> = reader_mut.read_channel_data(group, channel).map_err(tdms_error_to_pyerr)?;
                Ok(data.into_pyarray(py).into_any()) // <-- FIX
            }
            tdms::DataType::I32 => {
                let data: Vec<i32> = reader_mut.read_channel_data(group, channel).map_err(tdms_error_to_pyerr)?;
                Ok(data.into_pyarray(py).into_any()) // <-- FIX
            }
            tdms::DataType::I64 => {
                let data: Vec<i64> = reader_mut.read_channel_data(group, channel).map_err(tdms_error_to_pyerr)?;
                Ok(data.into_pyarray(py).into_any()) // <-- FIX
            }
            tdms::DataType::I16 => {
                let data: Vec<i16> = reader_mut.read_channel_data(group, channel).map_err(tdms_error_to_pyerr)?;
                Ok(data.into_pyarray(py).into_any()) // <-- FIX
            }
            tdms::DataType::I8 => {
                let data: Vec<i8> = reader_mut.read_channel_data(group, channel).map_err(tdms_error_to_pyerr)?;
                Ok(data.into_pyarray(py).into_any()) // <-- FIX
            }
            tdms::DataType::U32 => {
                let data: Vec<u32> = reader_mut.read_channel_data(group, channel).map_err(tdms_error_to_pyerr)?;
                Ok(data.into_pyarray(py).into_any()) // <-- FIX
            }
            tdms::DataType::U64 => {
                let data: Vec<u64> = reader_mut.read_channel_data(group, channel).map_err(tdms_error_to_pyerr)?;
                Ok(data.into_pyarray(py).into_any()) // <-- FIX
            }
            tdms::DataType::U16 => {
                let data: Vec<u16> = reader_mut.read_channel_data(group, channel).map_err(tdms_error_to_pyerr)?;
                Ok(data.into_pyarray(py).into_any()) // <-- FIX
            }
            tdms::DataType::U8 => {
                let data: Vec<u8> = reader_mut.read_channel_data(group, channel).map_err(tdms_error_to_pyerr)?;
                Ok(data.into_pyarray(py).into_any()) // <-- FIX
            }
            tdms::DataType::Boolean => {
                let data: Vec<bool> = reader_mut.read_channel_data(group, channel).map_err(tdms_error_to_pyerr)?;
                Ok(data.into_pyarray(py).into_any()) // <-- FIX
            }
            tdms::DataType::TimeStamp => {
                let data: Vec<tdms::Timestamp> = reader_mut.read_channel_data(group, channel).map_err(tdms_error_to_pyerr)?;
                let nanos: Vec<i64> = data.iter().map(|&ts| {
                    let unix_seconds = ts.seconds - TDMS_EPOCH_OFFSET_SECONDS;
                    let nanos_subsec = ((ts.fractions as u128 * 1_000_000_000) / (1u128 << 64)) as i64;
                    (unix_seconds * NANOS_PER_SECOND) + nanos_subsec
                }).collect();
                let nanos_array = nanos.into_pyarray(py); // <-- FIX
                let np = PyModule::import(py, "numpy")?;
                let datetime_dtype = np.call_method1("dtype", ("datetime64[ns]",))?;
                let datetime_array = nanos_array.call_method1("astype", (datetime_dtype,))?;
                Ok(datetime_array)
            }
            tdms::DataType::String => {
                let data = reader_mut.read_channel_strings(group, channel).map_err(tdms_error_to_pyerr)?;
                let np = PyModule::import(py, "numpy")?;
                let object_array = np.call_method1("array", (data, "object"))?;
                Ok(object_array)
            }
            _ => Err(PyTypeError::new_err(format!(
                "Unsupported data type {:?} for channel '{}/{}'",
                data_type, group, channel
            ))),
        }
    }

    /// Read string data from a channel
    fn read_strings(&mut self, group: &str, channel: &str) -> PyResult<Vec<String>> {
        let reader = self.reader.as_mut()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Reader is closed"))?;
        let data = reader.read_channel_strings(group, channel).map_err(tdms_error_to_pyerr)?;
        Ok(data)
    }

    /// Get the number of segments in the file
    #[getter] // <-- FIX: Added getter
    fn segment_count(&self) -> PyResult<usize> {
        let reader = self.reader.as_ref()
            .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Reader is closed"))?;
        Ok(reader.segment_count())
    }

    /// Get the number of channels in the file
    #[getter] // <-- FIX: Added getter
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
    tdms::defragment(source_path, dest_path).map_err(tdms_error_to_pyerr)?;
    Ok(())
}

/// Python module for TDMS file I/O
#[pymodule]
fn tdms_python(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyDataType>()?;
    m.add_class::<PyTdmsWriter>()?;
    m.add_class::<PyAsyncTdmsWriter>()?;
    m.add_class::<PyTdmsReader>()?;
    m.add_function(wrap_pyfunction!(defragment, m)?)?;
    
    // Add version info
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    
    Ok(())
}