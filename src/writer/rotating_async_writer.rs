// src/writer/rotating_async_writer.rs
#![cfg(feature = "async")]
use crate::error::{Result, TdmsError};
use crate::writer::rotating_writer::RotatingTdmsWriter;
use crate::types::{DataType, PropertyValue};
use std::path::Path;
use tokio::sync::mpsc;
use tokio::task;
use parking_lot::Mutex;
use std::sync::Arc;
use bytemuck;

enum WriteCommand {
    CreateChannel {
        group: String,
        channel: String,
        data_type: DataType,
        response: tokio::sync::oneshot::Sender<Result<()>>,
    },
    WriteData {
        group: String,
        channel: String,
        data: Vec<u8>,
        data_type: DataType,
        response: tokio::sync::oneshot::Sender<Result<()>>,
    },
    WriteStrings {
        group: String,
        channel: String,
        strings: Vec<String>,
        response: tokio::sync::oneshot::Sender<Result<()>>,
    },
    SetFileProperty {
        name: String,
        value: PropertyValue,
    },
    Flush {
        response: tokio::sync::oneshot::Sender<Result<()>>,
    },
    Close,
}

pub struct AsyncRotatingTdmsWriter {
    command_tx: mpsc::UnboundedSender<WriteCommand>,
    handle: Arc<Mutex<Option<task::JoinHandle<Result<()>>>>>,
}

impl AsyncRotatingTdmsWriter {
    pub async fn new(path: impl AsRef<Path>, max_size_bytes: u64) -> Result<Self> {
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let writer = RotatingTdmsWriter::new(path, max_size_bytes)?;
        let handle = task::spawn_blocking(move || {
            Self::writer_task(writer, command_rx)
        });

        Ok(AsyncRotatingTdmsWriter {
            command_tx,
            handle: Arc::new(Mutex::new(Some(handle))),
        })
    }

    fn writer_task(
        mut writer: RotatingTdmsWriter,
        mut command_rx: mpsc::UnboundedReceiver<WriteCommand>,
    ) -> Result<()> {
        while let Some(command) = command_rx.blocking_recv() {
            match command {
                WriteCommand::CreateChannel { group, channel, data_type, response } => {
                    let result = writer.create_channel(&group, &channel, data_type);
                    let _ = response.send(result);
                }
                WriteCommand::WriteData { group, channel, data, data_type, response } => {
                    let result = Self::handle_write_data(&mut writer, &group, &channel, &data, data_type);
                    let _ = response.send(result);
                }
                WriteCommand::WriteStrings { group, channel, strings, response } => {
                    let result = writer.write_channel_strings(&group, &channel, &strings);
                    let _ = response.send(result);
                }
                WriteCommand::SetFileProperty { name, value } => {
                    writer.set_file_property(name, value);
                }
                WriteCommand::Flush { response } => {
                    let result = writer.flush();
                    let _ = response.send(result);
                }
                WriteCommand::Close => {
                    writer.flush()?;
                    break;
                }
            }
        }
        Ok(())
    }

    fn handle_write_data(
        writer: &mut RotatingTdmsWriter,
        group: &str,
        channel: &str,
        data: &[u8],
        data_type: DataType,
    ) -> Result<()> {
        match data_type {
            DataType::I8 => writer.write_channel_data(group, channel, bytemuck::cast_slice::<u8, i8>(data)),
            DataType::I16 => writer.write_channel_data(group, channel, bytemuck::cast_slice::<u8, i16>(data)),
            DataType::I32 => writer.write_channel_data(group, channel, bytemuck::cast_slice::<u8, i32>(data)),
            DataType::I64 => writer.write_channel_data(group, channel, bytemuck::cast_slice::<u8, i64>(data)),
            DataType::U8 => writer.write_channel_data(group, channel, bytemuck::cast_slice::<u8, u8>(data)),
            DataType::U16 => writer.write_channel_data(group, channel, bytemuck::cast_slice::<u8, u16>(data)),
            DataType::U32 => writer.write_channel_data(group, channel, bytemuck::cast_slice::<u8, u32>(data)),
            DataType::U64 => writer.write_channel_data(group, channel, bytemuck::cast_slice::<u8, u64>(data)),
            DataType::SingleFloat => writer.write_channel_data(group, channel, bytemuck::cast_slice::<u8, f32>(data)),
            DataType::DoubleFloat => writer.write_channel_data(group, channel, bytemuck::cast_slice::<u8, f64>(data)),
            DataType::Boolean => {
                let bools: &[bool] = unsafe {
                    std::slice::from_raw_parts(
                        data.as_ptr() as *const bool,
                        data.len(),
                    )
                };
                let bytes: Vec<u8> = bools.iter().map(|&b| b as u8).collect();
                writer.write_channel_data(group, channel, &bytes)
            }
            DataType::TimeStamp => writer.write_channel_data(group, channel, bytemuck::cast_slice::<u8, crate::types::Timestamp>(data)),
            _ => Err(TdmsError::Unsupported(format!("Async write for {:?}", data_type))),
        }
    }

    pub async fn create_channel(
        &self,
        group: impl Into<String>,
        channel: impl Into<String>,
        data_type: DataType,
    ) -> Result<()> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        self.command_tx.send(WriteCommand::CreateChannel {
            group: group.into(),
            channel: channel.into(),
            data_type,
            response: response_tx,
        }).map_err(|_| TdmsError::WriterClosed)?;
        response_rx.await.map_err(|_| TdmsError::WriterClosed)?
    }

    pub async fn write_channel_data<T: Copy + Send + 'static>(
        &self,
        group: impl Into<String>,
        channel: impl Into<String>,
        data: Vec<T>,
        data_type: DataType,
    ) -> Result<()> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        let bytes = unsafe {
            std::slice::from_raw_parts(
                data.as_ptr() as *const u8,
                data.len() * std::mem::size_of::<T>(),
            ).to_vec()
        };
        self.command_tx.send(WriteCommand::WriteData {
            group: group.into(),
            channel: channel.into(),
            data: bytes,
            data_type,
            response: response_tx,
        }).map_err(|_| TdmsError::WriterClosed)?;
        response_rx.await.map_err(|_| TdmsError::WriterClosed)?
    }

    pub async fn write_channel_strings(
        &self,
        group: impl Into<String>,
        channel: impl Into<String>,
        strings: Vec<String>,
    ) -> Result<()> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        self.command_tx.send(WriteCommand::WriteStrings {
            group: group.into(),
            channel: channel.into(),
            strings,
            response: response_tx,
        }).map_err(|_| TdmsError::WriterClosed)?;
        response_rx.await.map_err(|_| TdmsError::WriterClosed)?
    }

    pub fn set_file_property(&self, name: impl Into<String>, value: PropertyValue) -> Result<()> {
        self.command_tx.send(WriteCommand::SetFileProperty {
            name: name.into(),
            value,
        }).map_err(|_| TdmsError::WriterClosed)
    }

    pub async fn flush(&self) -> Result<()> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();
        self.command_tx.send(WriteCommand::Flush {
            response: response_tx,
        }).map_err(|_| TdmsError::WriterClosed)?;
        response_rx.await.map_err(|_| TdmsError::WriterClosed)?
    }

    pub async fn close(&self) -> Result<()> {
        self.command_tx.send(WriteCommand::Close).map_err(|_| TdmsError::WriterClosed)?;
        if let Some(handle) = self.handle.lock().take() {
            handle.await.map_err(|_| TdmsError::WriterClosed)??;
        }
        Ok(())
    }
}
