use anyhow::Context as _;
use environment_protocol::data::{
    methods::{FS_CAPTURE_METHOD, FS_MATERIALIZE_METHOD},
    transfer::*,
};
use environment_protocol::{
    data::{
        fs::{
            CopyParams, CreateDirectoryParams, GetMetadataParams, GlobFilesParams,
            ReadDirectoryParams, ReadFileParams, RemoveParams, SearchTextParams, WriteFileParams,
        },
        handshake::{InitializeParams, InitializeResponse, InitializedParams},
        idle::IdleParams,
        jobs::{CancelJobsParams, ListJobsParams, ReadJobsParams, StartJobsParams},
        methods::{
            ENV_IDLE_METHOD, FS_COPY_METHOD, FS_CREATE_DIRECTORY_METHOD, FS_GET_METADATA_METHOD,
            FS_GLOB_FILES_METHOD, FS_READ_DIRECTORY_METHOD, FS_READ_FILE_METHOD, FS_REMOVE_METHOD,
            FS_SEARCH_TEXT_METHOD, FS_WRITE_FILE_METHOD,
            INITIALIZE_METHOD as DATA_INITIALIZE_METHOD, INITIALIZED_METHOD, JOB_CANCEL_METHOD,
            JOB_LIST_METHOD, JOB_READ_METHOD, JOB_START_METHOD, PROCESS_READ_METHOD,
            PROCESS_RESIZE_METHOD, PROCESS_START_METHOD, PROCESS_TERMINATE_METHOD,
            PROCESS_WRITE_METHOD,
        },
        process::{
            ReadProcessParams, ResizeProcessParams, StartProcessParams, TerminateProcessParams,
            WriteProcessParams,
        },
    },
    error::{EnvironmentProtocolError, EnvironmentProtocolErrorCode},
    shared::CURRENT_PROTOCOL_VERSION,
};
use futures_util::{SinkExt as _, StreamExt as _};
use serde_json::Value;
use tokio::net::TcpListener;
use tokio::sync::{Semaphore, mpsc};
use tokio_tungstenite::{accept_async, tungstenite::Message};

/// Serve the daemon until a transport fails terminally: the passive listener
/// when one is configured, the outbound registration loop when a gateway
/// URL is configured, or both.
pub async fn run(runtime: DaemonRuntime) -> anyhow::Result<()> {
    let listen = runtime.config().listen.clone();
    let registration = runtime.config().registration.clone();
    match (listen, registration) {
        (Some(listen), None) => run_listener(runtime, listen).await,
        (None, Some(registration)) => {
            crate::registration::run_outbound(runtime, registration).await
        }
        (Some(listen), Some(registration)) => {
            let listener = tokio::spawn(run_listener(runtime.clone(), listen));
            let outbound = tokio::spawn(crate::registration::run_outbound(runtime, registration));
            tokio::select! {
                result = listener => result.context("listener task")?,
                result = outbound => result.context("registration task")?,
            }
        }
        (None, None) => anyhow::bail!("configure a listener or a gateway URL"),
    }
}

async fn run_listener(runtime: DaemonRuntime, listen: ListenAddr) -> anyhow::Result<()> {
    match listen {
        ListenAddr::Tcp(addr) => run_tcp_listener(runtime, addr).await,
        #[cfg(unix)]
        ListenAddr::Unix(path) => run_unix_listener(runtime, path).await,
    }
}

async fn run_tcp_listener(
    runtime: DaemonRuntime,
    listen: std::net::SocketAddr,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(listen)
        .await
        .context("bind environment daemon listener")?;
    tracing_line(&format!("listener ready on {listen}"));
    loop {
        match listener.accept().await {
            Ok((stream, peer)) => serve_accepted(runtime.clone(), stream, peer.to_string()),
            Err(error) => accept_failed(error).await,
        }
    }
}

#[cfg(unix)]
async fn run_unix_listener(runtime: DaemonRuntime, path: std::path::PathBuf) -> anyhow::Result<()> {
    let listener = bind_unix_listener(&path)?;
    let label = ListenAddr::Unix(path).to_string();
    tracing_line(&format!("listener ready on {label}"));
    loop {
        match listener.accept().await {
            Ok((stream, _)) => serve_accepted(runtime.clone(), stream, label.clone()),
            Err(error) => accept_failed(error).await,
        }
    }
}

/// Accept errors are transient or specific to one connection (descriptor
/// exhaustion, a peer that hung up mid-accept), so the listener keeps serving.
/// The pause stops a persistent error from spinning the loop.
async fn accept_failed(error: std::io::Error) {
    tracing_line(&format!("accept failed: {error}"));
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
}

/// Bind a Unix-socket listener only this daemon's user can connect to. A
/// socket node left by an earlier run is replaced; a live listener or any
/// other kind of file at the path is not.
#[cfg(unix)]
fn bind_unix_listener(path: &std::path::Path) -> anyhow::Result<tokio::net::UnixListener> {
    use std::os::unix::fs::PermissionsExt as _;

    remove_stale_socket(path)?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    let listener = tokio::net::UnixListener::bind(path)
        .with_context(|| format!("bind environment daemon listener {}", path.display()))?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
        .with_context(|| format!("restrict {}", path.display()))?;
    Ok(listener)
}

/// Remove a socket node whose listener is gone. Only a refused connection
/// proves that; any other probe failure (such as a socket this user may not
/// connect to) leaves the node alone.
#[cfg(unix)]
fn remove_stale_socket(path: &std::path::Path) -> anyhow::Result<()> {
    use std::os::unix::fs::FileTypeExt as _;

    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_socket() => {}
        Ok(_) => anyhow::bail!("refusing to replace non-socket {}", path.display()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(error).with_context(|| format!("inspect {}", path.display()));
        }
    }
    match std::os::unix::net::UnixStream::connect(path) {
        Ok(_) => anyhow::bail!("another listener is serving {}", path.display()),
        Err(error) if error.kind() == std::io::ErrorKind::ConnectionRefused => {
            std::fs::remove_file(path)
                .with_context(|| format!("remove stale socket {}", path.display()))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => {
            Err(error).with_context(|| format!("cannot tell whether {} is in use", path.display()))
        }
    }
}

fn serve_accepted<S>(runtime: DaemonRuntime, stream: S, peer: String)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        match accept_async(stream).await {
            Ok(socket) => {
                if let Err(error) = run_data_connection(&runtime, socket).await {
                    eprintln!("lightspeed-envd connection {peer} failed: {error:#}");
                }
            }
            Err(error) => {
                eprintln!("lightspeed-envd websocket handshake {peer} failed: {error}")
            }
        }
    });
}

/// Serve the data protocol on one accepted or dialed socket. Outbound data
/// sockets and passive connections run this same loop.
pub(crate) async fn run_data_connection<S>(
    runtime: &DaemonRuntime,
    socket: tokio_tungstenite::WebSocketStream<S>,
) -> anyhow::Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let (mut writer, mut reader) = socket.split();
    let (responses, mut response_rx) = mpsc::channel::<Value>(64);
    let concurrency = std::sync::Arc::new(Semaphore::new(32));
    loop {
        tokio::select! {
            response = response_rx.recv() => {
                let Some(response) = response else { return Ok(()) };
                writer.send(Message::Text(response.to_string().into())).await?;
            }
            message = reader.next() => {
                let Some(message) = message else { return Ok(()) };
                match message? {
                    Message::Text(text) => dispatch_gateway_json(runtime, &responses, &concurrency, &text).await?,
                    Message::Binary(bytes) => {
                        let text = std::str::from_utf8(&bytes).context("binary provider frame is not UTF-8")?;
                        dispatch_gateway_json(runtime, &responses, &concurrency, text).await?;
                    }
                    Message::Ping(bytes) => writer.send(Message::Pong(bytes)).await?,
                    Message::Pong(_) | Message::Frame(_) => {}
                    Message::Close(_) => {
                        // Tungstenite queues the close reply while reading the
                        // peer's frame; flush it before dropping the socket.
                        writer.flush().await?;
                        return Ok(())
                    },
                }
            }
        }
    }
}

use crate::{
    DaemonRuntime,
    config::ListenAddr,
    rpc::{
        decode_params, encode_result, error_response, invalid_request, method_not_found,
        parse_request, success_response,
    },
};

async fn dispatch_gateway_json(
    runtime: &DaemonRuntime,
    responses: &mpsc::Sender<Value>,
    concurrency: &std::sync::Arc<Semaphore>,
    text: &str,
) -> anyhow::Result<()> {
    let value: Value = serde_json::from_str(text)?;
    let request = match parse_request(value) {
        Ok(request) => request,
        Err(error) => {
            responses.send(error_response(None, error)).await?;
            return Ok(());
        }
    };
    let Some(id) = request.id else {
        if request.method.as_deref() == Some(INITIALIZED_METHOD) {
            let _ = decode_params::<InitializedParams>(request.params);
        }
        return Ok(());
    };
    let Ok(permit) = concurrency.clone().try_acquire_owned() else {
        responses
            .send(error_response(
                Some(id),
                EnvironmentProtocolError::new(
                    EnvironmentProtocolErrorCode::Conflict,
                    "daemon request limit reached",
                ),
            ))
            .await?;
        return Ok(());
    };
    let runtime = runtime.clone();
    let responses = responses.clone();
    tokio::spawn(async move {
        let _permit = permit;
        let result = match request.method.as_deref() {
            Some(method) => handle_data(&runtime, method, request.params).await,
            None => Err(invalid_request("request missing method")),
        };
        let response = match result {
            Ok(result) => success_response(id, result),
            Err(error) => error_response(Some(id), error),
        };
        let _ = responses.send(response).await;
    });
    Ok(())
}

async fn handle_data(
    runtime: &DaemonRuntime,
    method: &str,
    params: Value,
) -> Result<Value, EnvironmentProtocolError> {
    // Handshakes and idle reports are observation, not use: only real
    // filesystem/process/job requests count as activity.
    if !matches!(method, DATA_INITIALIZE_METHOD | ENV_IDLE_METHOD) {
        runtime.touch_activity();
    }
    match method {
        ENV_IDLE_METHOD => {
            decode_params::<IdleParams>(params)?;
            encode_result(runtime.idle_report().await)
        }
        DATA_INITIALIZE_METHOD => {
            let params = decode_params::<InitializeParams>(params)?;
            if params.protocol_version != CURRENT_PROTOCOL_VERSION {
                return Err(EnvironmentProtocolError::new(
                    EnvironmentProtocolErrorCode::Unsupported,
                    format!(
                        "unsupported data protocol version {}; expected {CURRENT_PROTOCOL_VERSION}",
                        params.protocol_version
                    ),
                ));
            }
            encode_result(InitializeResponse {
                home_directory: std::env::var("HOME").ok(),
                protocol_version: CURRENT_PROTOCOL_VERSION,
                connection_id: runtime.next_connection_id(),
                capabilities: runtime.capabilities(),
                default_cwd: Some(runtime.config().cwd.to_string_lossy().into_owned()),
                implementation: runtime.implementation(),
            })
        }
        FS_READ_FILE_METHOD => encode_result(
            runtime
                .filesystem()
                .read_file(decode_params::<ReadFileParams>(params)?)
                .await?,
        ),
        FS_WRITE_FILE_METHOD => encode_result(
            runtime
                .filesystem()
                .write_file(decode_params::<WriteFileParams>(params)?)
                .await?,
        ),
        FS_CREATE_DIRECTORY_METHOD => encode_result(
            runtime
                .filesystem()
                .create_directory(decode_params::<CreateDirectoryParams>(params)?)
                .await?,
        ),
        FS_GET_METADATA_METHOD => encode_result(
            runtime
                .filesystem()
                .get_metadata(decode_params::<GetMetadataParams>(params)?)
                .await?,
        ),
        FS_READ_DIRECTORY_METHOD => encode_result(
            runtime
                .filesystem()
                .read_directory(decode_params::<ReadDirectoryParams>(params)?)
                .await?,
        ),
        FS_REMOVE_METHOD => encode_result(
            runtime
                .filesystem()
                .remove(decode_params::<RemoveParams>(params)?)
                .await?,
        ),
        environment_protocol::data::methods::FS_SCAN_METHOD => {
            encode_result(runtime.filesystem().scan(decode_params(params)?).await?)
        }
        environment_protocol::data::methods::FS_TRANSFER_METHOD => encode_result(
            runtime
                .filesystem()
                .transfer(decode_params(params)?)
                .await?,
        ),
        FS_CAPTURE_METHOD => encode_result(
            runtime
                .filesystem()
                .capture(decode_params::<CaptureParams>(params)?)
                .await?,
        ),
        FS_MATERIALIZE_METHOD => encode_result(
            runtime
                .filesystem()
                .materialize(decode_params::<MaterializeParams>(params)?)
                .await?,
        ),
        FS_COPY_METHOD => encode_result(
            runtime
                .filesystem()
                .copy(decode_params::<CopyParams>(params)?)
                .await?,
        ),
        FS_SEARCH_TEXT_METHOD => encode_result(
            runtime
                .filesystem()
                .search_text(decode_params::<SearchTextParams>(params)?)
                .await?,
        ),
        FS_GLOB_FILES_METHOD => encode_result(
            runtime
                .filesystem()
                .glob_files(decode_params::<GlobFilesParams>(params)?)
                .await?,
        ),
        PROCESS_START_METHOD => encode_result(
            runtime
                .processes()
                .start_process(decode_params::<StartProcessParams>(params)?)
                .await?,
        ),
        PROCESS_READ_METHOD => encode_result(
            runtime
                .processes()
                .read_process(decode_params::<ReadProcessParams>(params)?)
                .await?,
        ),
        PROCESS_WRITE_METHOD => encode_result(
            runtime
                .processes()
                .write_process(decode_params::<WriteProcessParams>(params)?)
                .await?,
        ),
        PROCESS_TERMINATE_METHOD => encode_result(
            runtime
                .processes()
                .terminate_process(decode_params::<TerminateProcessParams>(params)?)
                .await?,
        ),
        PROCESS_RESIZE_METHOD => encode_result(
            runtime
                .processes()
                .resize_process(decode_params::<ResizeProcessParams>(params)?)
                .await?,
        ),
        JOB_START_METHOD => encode_result(
            runtime
                .jobs()
                .start_jobs(decode_params::<StartJobsParams>(params)?)
                .await?,
        ),
        JOB_LIST_METHOD => encode_result(
            runtime
                .jobs()
                .list_jobs(decode_params::<ListJobsParams>(params)?)
                .await?,
        ),
        JOB_READ_METHOD => encode_result(
            runtime
                .jobs()
                .read_jobs(decode_params::<ReadJobsParams>(params)?)
                .await?,
        ),
        JOB_CANCEL_METHOD => encode_result(
            runtime
                .jobs()
                .cancel_jobs(decode_params::<CancelJobsParams>(params)?)
                .await?,
        ),
        other => Err(method_not_found(other)),
    }
}

pub(crate) fn tracing_line(message: &str) {
    eprintln!("lightspeed-envd {message}");
}

#[cfg(all(test, unix))]
mod unix_listener_tests {
    use std::os::unix::fs::PermissionsExt as _;

    use environment_protocol::{data::handshake::InitializeParams, shared::EnvironmentScope};
    use tokio_tungstenite::client_async;

    use super::*;
    use crate::config::DaemonConfig;

    fn runtime(root: &std::path::Path, socket: &std::path::Path) -> DaemonRuntime {
        DaemonRuntime::new(DaemonConfig {
            listen: Some(ListenAddr::Unix(socket.to_path_buf())),
            cwd: root.to_path_buf(),
            fs_root: root.to_path_buf(),
            state_dir: root.join("state"),
            read_only_fs: false,
            registration: None,
            scrubbed_env: vec![],
        })
        .unwrap()
    }

    #[tokio::test(flavor = "current_thread")]
    async fn serves_the_data_protocol_on_a_private_unix_socket() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        // The parent does not exist yet: guest tmpfs paths start empty.
        let socket = root.join("run").join("envd.sock");
        let server = tokio::spawn(run(runtime(&root, &socket)));
        while !socket.exists() {
            tokio::task::yield_now().await;
        }
        let mode = std::fs::metadata(&socket).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);

        let stream = tokio::net::UnixStream::connect(&socket).await.unwrap();
        let (mut client, _) = client_async("ws://envd/", stream).await.unwrap();
        let params = InitializeParams {
            protocol_version: CURRENT_PROTOCOL_VERSION,
            client_name: "unix-listener-test".to_owned(),
            scope: EnvironmentScope::Default,
            resume_connection_id: None,
        };
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": DATA_INITIALIZE_METHOD,
            "params": params,
        });
        client
            .send(Message::Text(request.to_string().into()))
            .await
            .unwrap();
        let Some(Ok(Message::Text(reply))) = client.next().await else {
            panic!("expected an initialize response");
        };
        let reply: Value = serde_json::from_str(reply.as_str()).unwrap();
        assert_eq!(reply["id"], 1);
        assert_eq!(reply["result"]["protocolVersion"], CURRENT_PROTOCOL_VERSION);
        server.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stale_sockets_are_replaced_but_live_ones_and_files_are_not() {
        let temp = tempfile::tempdir().unwrap();
        let socket = temp.path().join("envd.sock");

        drop(std::os::unix::net::UnixListener::bind(&socket).unwrap());
        assert!(socket.exists(), "a dropped listener leaves its node behind");
        let listener = bind_unix_listener(&socket).expect("stale socket replaced");

        assert!(bind_unix_listener(&socket).is_err(), "live listener kept");
        drop(listener);

        let file = temp.path().join("not-a-socket");
        std::fs::write(&file, b"data").unwrap();
        assert!(bind_unix_listener(&file).is_err());
        assert_eq!(std::fs::read(&file).unwrap(), b"data");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_live_socket_that_cannot_be_probed_is_left_in_place() {
        let temp = tempfile::tempdir().unwrap();
        let socket = temp.path().join("envd.sock");
        let _live = std::os::unix::net::UnixListener::bind(&socket).unwrap();
        // Without write permission, connect fails with EACCES rather than
        // ECONNREFUSED (root bypasses this and sees the live listener).
        std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o000)).unwrap();

        assert!(bind_unix_listener(&socket).is_err());
        assert!(
            std::fs::symlink_metadata(&socket).is_ok(),
            "live socket kept"
        );
    }
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod transfer_tests {
    use super::*;
    use crate::config::DaemonConfig;
    use environment_protocol::shared::{ByteChunk, EnvironmentPath};

    #[tokio::test(flavor = "current_thread")]
    async fn transfer_dispatch_and_read_only_capabilities() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().to_path_buf();
        for read_only in [false, true] {
            let runtime = DaemonRuntime::new(DaemonConfig {
                listen: None,
                cwd: root.clone(),
                fs_root: root.clone(),
                state_dir: root.join("state"),
                read_only_fs: read_only,
                registration: None,
                scrubbed_env: vec![],
            })
            .unwrap();
            assert!(runtime.capabilities().filesystem_capture);
            assert_eq!(runtime.capabilities().filesystem_materialize, !read_only);
            let limits = TransferLimits {
                max_entries: 1,
                max_depth: 0,
                max_file_bytes: 2,
                max_total_bytes: 2,
                // This checks dispatch and access, not storage latency. Allow
                // shared-runner delays across the transfer's journal fsyncs.
                max_duration_ms: MAX_TRANSFER_DURATION_MS,
            };
            let entries = vec![TransferEntry {
                path: "".into(),
                content: TransferContent::File {
                    data: ByteChunk::from(vec![0, 255]),
                    executable: true,
                },
            }];
            let request = MaterializeParams {
                destination: EnvironmentPath::new("binary").unwrap(),
                entries: entries.clone(),
                limits,
                on_existing: TransferOnExisting::Replace,
            };
            let result = handle_data(
                &runtime,
                FS_MATERIALIZE_METHOD,
                serde_json::to_value(request).unwrap(),
            )
            .await;
            if read_only {
                assert_eq!(
                    result.unwrap_err().code,
                    EnvironmentProtocolErrorCode::CapabilityUnavailable
                );
            } else {
                assert_eq!(result.unwrap()["bytes"], 2);
            }
            let captured: CaptureResponse = serde_json::from_value(
                handle_data(
                    &runtime,
                    FS_CAPTURE_METHOD,
                    serde_json::to_value(CaptureParams {
                        source: EnvironmentPath::new("binary").unwrap(),
                        limits,
                    })
                    .unwrap(),
                )
                .await
                .unwrap(),
            )
            .unwrap();
            assert_eq!(captured.entries, entries);
        }
    }
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
#[path = "filesystem/transfer/tests.rs"]
mod streaming_transfer_tests;
