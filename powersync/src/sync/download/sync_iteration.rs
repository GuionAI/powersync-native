use std::sync::Arc;

use futures_lite::{StreamExt, future, stream::Boxed as BoxedStream};
use log::{debug, info, trace, warn};
use powersync_sqlite_nostd::{Destructor, ManagedStmt, ResultCode};
use serde::Serialize;
use serde_json::value::RawValue;

use crate::db::connection::{SqliteConnection, TransactionGuard};
use crate::schema::SchemaOrCustom;
use crate::{
    SyncOptions,
    db::internal::InnerPowerSyncState,
    error::PowerSyncError,
    sync::{
        download::http::sync_stream,
        instruction::{CloseSyncStream, Instruction, LogSeverity},
        streams::StreamKey,
    },
};

pub struct DownloadClient {
    db: Arc<InnerPowerSyncState>,
    stream: Option<BoxedStream<Result<DownloadEvent, PowerSyncError>>>,
    receive_commands: async_channel::Receiver<DownloadEvent>,
}

impl DownloadClient {
    pub fn new(
        db: Arc<InnerPowerSyncState>,
        events: async_channel::Receiver<DownloadEvent>,
    ) -> Self {
        Self {
            db,
            stream: None,
            receive_commands: events,
        }
    }

    pub async fn run(mut self, options: SyncOptions) -> Result<CloseSyncStream, PowerSyncError> {
        'event: loop {
            let event = match &mut self.stream {
                Some(stream) => {
                    future::or(
                        Self::receive_command(&self.receive_commands),
                        Self::receive_on_stream(stream),
                    )
                    .await
                }
                None => Self::receive_command(&self.receive_commands).await,
            }?;

            trace!("Handling event {event:?}");
            let instructions = {
                let mut conn = self.db.writer().await?;
                event.invoke_control(conn.sqlite_connection_mut())?
            };

            for instr in instructions {
                trace!("Handling instruction {instr:?}");

                match instr {
                    Instruction::LogLine { severity, line } => match severity {
                        LogSeverity::Debug => debug!("{}", line),
                        LogSeverity::Info => info!("{}", line),
                        LogSeverity::Warning => warn!("{}", line),
                    },
                    Instruction::UpdateSyncStatus { status } => {
                        self.db.status.update(|s| s.update_from_core(status))
                    }
                    Instruction::EstablishSyncStream { request } => {
                        trace!("Establishing sync stream with {request}");
                        Self::establish_sync_stream(
                            Arc::clone(&self.db),
                            &mut self.stream,
                            request,
                            &options,
                        )
                        .await?;

                        // Trigger a crud upload after establishing a sync stream.
                        if let Some(sync) = self.db.sync.upgrade() {
                            sync.trigger_crud_uploads().await;
                        }
                    }
                    Instruction::FetchCredentials { .. } => {
                        // TODO: Pre-fetching credentials
                        // If did_expire is true, the core extension will also emit a stop
                        // instruction. So we don't have to handle that separately.
                    }
                    Instruction::CloseSyncStream(close) => {
                        break 'event Ok(close);
                    }
                    Instruction::FlushFileSystem {} => {
                        // Not applicable outside of Dart web.
                    }
                    Instruction::DidCompleteSync {} => self
                        .db
                        .status
                        .update(|status| status.clear_download_errors()),
                }
            }
        }
    }

    async fn establish_sync_stream(
        db: Arc<InnerPowerSyncState>,
        stream: &mut Option<BoxedStream<Result<DownloadEvent, PowerSyncError>>>,
        request: Box<RawValue>,
        options: &SyncOptions,
    ) -> Result<(), PowerSyncError> {
        let credentials = options.connector.fetch_credentials().await?;
        let request = request.get().to_string();

        *stream = Some(sync_stream(db, credentials, request).boxed());
        Ok(())
    }

    async fn receive_command(
        channel: &async_channel::Receiver<DownloadEvent>,
    ) -> Result<DownloadEvent, PowerSyncError> {
        Ok(channel.recv().await.unwrap_or(DownloadEvent::Stop))
    }

    async fn receive_on_stream(
        stream: &mut BoxedStream<Result<DownloadEvent, PowerSyncError>>,
    ) -> Result<DownloadEvent, PowerSyncError> {
        Ok(stream
            .try_next()
            .await?
            .unwrap_or(DownloadEvent::ResponseStreamEnd))
    }
}

/// An event that triggers the downloading client to advance.
///
/// This is typically a received line from the PowerSync service, but local events are also
/// included.
#[derive(Debug)]
pub enum DownloadEvent {
    /// `connect()` has been called and we need to start establishing a connection.
    Start(StartDownloadIteration),
    /// `disconnect()` has been called or the token has expired.
    Stop,
    /// A textual JSON sync line has been received from the service.
    TextLine { data: String },
    /// A binary BSON sync line has been received from the service.
    BinaryLine { data: Vec<u8> },
    /// A CRUD upload was completed, so the client can re-try applying data.
    CompletedUpload,
    /// HTTP response headers for the sync response have been received, meaning that the sync status
    /// can be set to connected.
    ConnectionEstablished,
    /// The sync response stream has ended.
    ResponseStreamEnd,
    /// Active subscriptions for the application have changed, which might require a reconnect.
    UpdateSubscriptions { keys: Vec<StreamKey> },
}

impl DownloadEvent {
    fn into_powersync_control_argument(self) -> (&'static str, PowerSyncControlArgument) {
        use PowerSyncControlArgument::*;

        match self {
            DownloadEvent::Start(start_download_iteration) => {
                let serialized = serde_json::to_string(&start_download_iteration)
                    .expect("should serialize to string");
                ("start", String(serialized))
            }
            DownloadEvent::Stop => ("stop", Null),
            DownloadEvent::TextLine { data } => ("line_text", String(data)),
            DownloadEvent::BinaryLine { data } => ("line_binary", Bytes(data)),
            DownloadEvent::CompletedUpload => ("completed_upload", Null),
            DownloadEvent::ConnectionEstablished => ("connection", StaticString("established")),
            DownloadEvent::ResponseStreamEnd => ("connection", StaticString("end")),
            DownloadEvent::UpdateSubscriptions { keys } => {
                let serialized = serde_json::to_string(&keys).expect("should serialize to string");
                ("update_subscriptions", String(serialized))
            }
        }
    }

    /// Forwards the event to the core extension, and returns instructions that the SDK needs to
    /// perform.
    pub fn invoke_control(
        self,
        conn: &mut SqliteConnection,
    ) -> Result<Vec<Instruction>, PowerSyncError> {
        let tx = TransactionGuard::new(conn)?;

        let instructions = {
            let (op, arg) = self.into_powersync_control_argument();
            let stmt = tx.inner.prepare("SELECT powersync_control(?, ?)")?;

            stmt.bind_text(1, op, Destructor::STATIC)?;
            // SAFETY: `arg` was declared before `stmt`, so it outlives `stmt` on every exit.
            unsafe { arg.bind_to(&stmt, 2)? };

            let instructions = if let ResultCode::ROW = stmt.step()? {
                let instructions = stmt.column_text(0).map_err(|_| {
                    PowerSyncError::argument_error("Could not read powersync_control instructions")
                })?;

                serde_json::from_str(instructions)?
            } else {
                panic!("Expected a row") // Can't happen, scalar select
            };

            drop(stmt);
            instructions
        };

        tx.commit()?;
        Ok(instructions)
    }
}

enum PowerSyncControlArgument {
    Null,
    StaticString(&'static str),
    String(String),
    Bytes(Vec<u8>),
}

impl PowerSyncControlArgument {
    /// # Safety
    ///
    /// The argument must outlive `stmt`.
    unsafe fn bind_to(&self, stmt: &ManagedStmt, index: i32) -> Result<(), ResultCode> {
        match self {
            PowerSyncControlArgument::Null => stmt.bind_null(index),
            PowerSyncControlArgument::StaticString(str) => {
                stmt.bind_text(index, str, Destructor::STATIC)
            }
            PowerSyncControlArgument::String(str) => stmt.bind_text(index, str, Destructor::STATIC),
            PowerSyncControlArgument::Bytes(bytes) => {
                stmt.bind_blob(index, bytes, Destructor::STATIC)
            }
        }?;
        Ok(())
    }
}

#[cfg(feature = "rusqlite")]
#[cfg(test)]
mod tests {
    use std::{pin::Pin, sync::Arc, time::Duration};

    use futures_lite::future;
    use rusqlite::Connection;

    use super::*;
    use crate::{
        db::pool::ConnectionPool,
        env::{PowerSyncEnvironment, Timer},
        http::{HttpClient, Request, Response},
        schema::Schema,
        sync::coordinator::SyncCoordinator,
    };

    struct UnusedClient;

    #[async_trait::async_trait]
    impl HttpClient for UnusedClient {
        async fn send(&self, _request: Request) -> Result<Response, PowerSyncError> {
            panic!("the test does not make HTTP requests")
        }
    }

    struct UnusedTimer;

    impl Timer for UnusedTimer {
        fn delay_once(&self, _duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>> {
            Box::pin(future::pending())
        }
    }

    async fn invoke(event: DownloadEvent) {
        PowerSyncEnvironment::powersync_auto_extension().unwrap();
        let pool = ConnectionPool::single_connection(Connection::open_in_memory().unwrap());
        let environment = PowerSyncEnvironment::custom(UnusedClient, pool, UnusedTimer);
        let coordinator = Arc::new(SyncCoordinator::default());
        let db = InnerPowerSyncState::new(environment, Schema::default().into(), &coordinator);
        let mut writer = db.writer().await.unwrap();

        DownloadEvent::Start(StartDownloadIteration {
            parameters: serde_json::Value::Object(Default::default()),
            schema: Arc::clone(&db.schema),
            include_defaults: true,
            active_streams: vec![],
        })
        .invoke_control(writer.sqlite_connection_mut())
        .unwrap();

        event
            .invoke_control(writer.sqlite_connection_mut())
            .unwrap();
    }

    #[test]
    fn dynamic_control_arguments_reach_the_core_extension() {
        future::block_on(async {
            invoke(DownloadEvent::TextLine {
                data: r#"{"checkpoint":{"last_op_id":"1","buckets":[],"streams":[]}}"#.to_owned(),
            })
            .await;
            invoke(DownloadEvent::BinaryLine {
                data: b"\x85\x00\x00\x00\x03checkpoint\x00t\x00\x00\x00\x02last_op_id\x00\x02\x00\x00\x001\x00\x0awrite_checkpoint\x00\x04buckets\x00B\x00\x00\x00\x030\x00:\x00\x00\x00\x02bucket\x00\x02\x00\x00\x00a\x00\x10checksum\x00\x00\x00\x00\x00\x10priority\x00\x03\x00\x00\x00\x10count\x00\x01\x00\x00\x00\x00\x00\x00\x00".to_vec(),
            })
            .await;
        });
    }
}

#[derive(Debug, Serialize)]
pub struct StartDownloadIteration {
    pub parameters: serde_json::Value,
    pub schema: Arc<SchemaOrCustom>,
    pub include_defaults: bool,
    pub active_streams: Vec<StreamKey>,
}
