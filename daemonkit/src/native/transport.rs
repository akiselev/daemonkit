#[cfg(unix)]
use std::path::PathBuf;
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) enum NativeEndpoint {
    #[cfg(unix)]
    Unix(PathBuf),
    #[cfg(windows)]
    WindowsPipe(String),
}

pub(crate) struct NativeListener {
    #[cfg(unix)]
    listener: tokio::net::UnixListener,
    #[cfg(unix)]
    path: PathBuf,
    #[cfg(unix)]
    file_identity: (u64, u64),
    #[cfg(windows)]
    server: tokio::net::windows::named_pipe::NamedPipeServer,
    #[cfg(windows)]
    name: String,
}

impl NativeListener {
    pub(crate) fn bind(endpoint: &NativeEndpoint) -> io::Result<Self> {
        match endpoint {
            #[cfg(unix)]
            NativeEndpoint::Unix(path) => bind_unix(path),
            #[cfg(windows)]
            NativeEndpoint::WindowsPipe(name) => bind_windows(name),
        }
    }

    pub(crate) async fn accept(&mut self) -> io::Result<NativeStream> {
        #[cfg(unix)]
        {
            let (stream, _) = self.listener.accept().await?;
            Ok(NativeStream::Unix(stream))
        }
        #[cfg(windows)]
        {
            self.server.connect().await?;
            // Replenish before returning the connected instance so another
            // client can wait while this one authenticates.
            let next = create_pipe_server(&self.name, false)?;
            let connected = std::mem::replace(&mut self.server, next);
            Ok(NativeStream::WindowsServer(connected))
        }
    }
}

#[cfg(unix)]
fn bind_unix(path: &std::path::Path) -> io::Result<NativeListener> {
    use std::os::unix::{ffi::OsStrExt, fs::MetadataExt};
    // 100 leaves room beneath both Linux's 108-byte and macOS's shorter
    // sockaddr_un budgets, including the terminating NUL.
    if path.as_os_str().as_bytes().len() > 100 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Unix control endpoint exceeds the portable path budget",
        ));
    }
    let listener = tokio::net::UnixListener::bind(path)?;
    let metadata = std::fs::symlink_metadata(path)?;
    Ok(NativeListener {
        listener,
        path: path.to_owned(),
        file_identity: (metadata.dev(), metadata.ino()),
    })
}

#[cfg(unix)]
impl Drop for NativeListener {
    fn drop(&mut self) {
        use std::os::unix::fs::MetadataExt;
        // An old generation must never unlink a successor's reused pathname.
        // Generation-specific names make reuse exceptional; file identity is
        // the final guard for cleanup races.
        if let Ok(metadata) = std::fs::symlink_metadata(&self.path) {
            if (metadata.dev(), metadata.ino()) == self.file_identity {
                let _ = std::fs::remove_file(&self.path);
            }
        }
    }
}

#[cfg(windows)]
fn bind_windows(name: &str) -> io::Result<NativeListener> {
    if !name.starts_with(r"\\.\pipe\daemonkit-") || name.len() > 240 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid daemonkit named-pipe endpoint",
        ));
    }
    Ok(NativeListener {
        server: create_pipe_server(name, true)?,
        name: name.to_owned(),
    })
}

#[cfg(windows)]
fn create_pipe_server(
    name: &str,
    first_instance: bool,
) -> io::Result<tokio::net::windows::named_pipe::NamedPipeServer> {
    use tokio::net::windows::named_pipe::ServerOptions;
    super::root::windows::with_current_user_security(|attributes| {
        let mut options = ServerOptions::new();
        options
            .first_pipe_instance(first_instance)
            .reject_remote_clients(true);
        // Safety: the security descriptor, ACL, and SID referenced by the
        // attributes remain live throughout this synchronous creation call.
        unsafe {
            options.create_with_security_attributes_raw(
                name,
                attributes.cast_mut().cast::<std::ffi::c_void>(),
            )
        }
    })
}

pub(crate) async fn connect(endpoint: &NativeEndpoint) -> io::Result<NativeStream> {
    match endpoint {
        #[cfg(unix)]
        NativeEndpoint::Unix(path) => tokio::net::UnixStream::connect(path)
            .await
            .map(NativeStream::Unix),
        #[cfg(windows)]
        NativeEndpoint::WindowsPipe(name) => {
            use tokio::net::windows::named_pipe::ClientOptions;
            use windows_sys::Win32::Foundation::ERROR_PIPE_BUSY;
            let started = tokio::time::Instant::now();
            loop {
                match ClientOptions::new().open(name) {
                    Ok(stream) => return Ok(NativeStream::WindowsClient(stream)),
                    Err(error)
                        if error.raw_os_error() == Some(ERROR_PIPE_BUSY as i32)
                            && started.elapsed() < std::time::Duration::from_secs(5) =>
                    {
                        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
                    }
                    Err(error) => return Err(error),
                }
            }
        }
    }
}

#[cfg(all(test, windows))]
mod windows_tests {
    use super::*;
    use crate::protocol::{
        client_handshake, server_handshake, Capabilities, ClientHello, ConnectionRole,
        ProtocolRange, ServerIdentity,
    };
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn pipe_is_first_instance_local_only_and_replenishes_accepts() {
        let name = format!(r"\\.\pipe\daemonkit-{}", uuid::Uuid::new_v4());
        let endpoint = NativeEndpoint::WindowsPipe(name);
        let mut listener = NativeListener::bind(&endpoint).unwrap();
        assert!(NativeListener::bind(&endpoint).is_err());

        let mut clients = Vec::new();
        for byte in [1u8, 2] {
            let endpoint = endpoint.clone();
            clients.push(tokio::spawn(async move {
                let mut stream = connect(&endpoint).await.unwrap();
                client_handshake(&mut stream, &test_hello(), Duration::from_secs(1))
                    .await
                    .unwrap();
                stream.write_all(&[byte]).await.unwrap();
                let mut response = [0];
                stream.read_exact(&mut response).await.unwrap();
                response[0]
            }));
        }
        let mut received = Vec::new();
        for _ in 0..2 {
            let mut stream = listener.accept().await.unwrap();
            let (_, role) = server_handshake(&mut stream, &test_identity(), Duration::from_secs(1))
                .await
                .unwrap();
            assert_eq!(role, ConnectionRole::Application);
            let mut request = [0];
            stream.read_exact(&mut request).await.unwrap();
            stream.write_all(&request).await.unwrap();
            received.push(request[0]);
        }
        received.sort_unstable();
        assert_eq!(received, [1, 2]);
        for client in clients {
            assert!(matches!(client.await.unwrap(), 1 | 2));
        }
    }

    #[test]
    fn arbitrary_pipe_names_are_rejected() {
        assert!(matches!(
            NativeListener::bind(&NativeEndpoint::WindowsPipe(r"\\.\pipe\other".into())),
            Err(error) if error.kind() == io::ErrorKind::InvalidInput
        ));
    }

    fn test_identity() -> ServerIdentity {
        ServerIdentity {
            app: [1; 32],
            key: [2; 32],
            isolation: [3; 32],
            generation: 4,
            token: [5; 32],
            versions: ProtocolRange {
                minimum: 1,
                maximum: 1,
            },
            process_id: std::process::id(),
            process_identity: vec![6],
            build: [7; 32],
            config: [8; 32],
            state: 3,
            capabilities: Capabilities::APPLICATION_STREAM,
            service_endpoint: None,
        }
    }

    fn test_hello() -> ClientHello {
        let identity = test_identity();
        ClientHello {
            app: identity.app,
            key: identity.key,
            isolation: identity.isolation,
            generation: identity.generation,
            token: identity.token,
            versions: identity.versions,
            role: ConnectionRole::Application,
        }
    }
}

pub(crate) enum NativeStream {
    #[cfg(unix)]
    Unix(tokio::net::UnixStream),
    #[cfg(windows)]
    WindowsClient(tokio::net::windows::named_pipe::NamedPipeClient),
    #[cfg(windows)]
    WindowsServer(tokio::net::windows::named_pipe::NamedPipeServer),
}

impl NativeStream {
    pub(crate) fn into_authenticated(self) -> crate::AuthenticatedStream {
        crate::AuthenticatedStream::from_stream(self)
    }
}

impl AsyncRead for NativeStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match &mut *self {
            #[cfg(unix)]
            Self::Unix(stream) => Pin::new(stream).poll_read(context, buffer),
            #[cfg(windows)]
            Self::WindowsClient(stream) => Pin::new(stream).poll_read(context, buffer),
            #[cfg(windows)]
            Self::WindowsServer(stream) => Pin::new(stream).poll_read(context, buffer),
        }
    }
}

impl AsyncWrite for NativeStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        match &mut *self {
            #[cfg(unix)]
            Self::Unix(stream) => Pin::new(stream).poll_write(context, buffer),
            #[cfg(windows)]
            Self::WindowsClient(stream) => Pin::new(stream).poll_write(context, buffer),
            #[cfg(windows)]
            Self::WindowsServer(stream) => Pin::new(stream).poll_write(context, buffer),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            #[cfg(unix)]
            Self::Unix(stream) => Pin::new(stream).poll_flush(context),
            #[cfg(windows)]
            Self::WindowsClient(stream) => Pin::new(stream).poll_flush(context),
            #[cfg(windows)]
            Self::WindowsServer(stream) => Pin::new(stream).poll_flush(context),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            #[cfg(unix)]
            Self::Unix(stream) => Pin::new(stream).poll_shutdown(context),
            #[cfg(windows)]
            Self::WindowsClient(stream) => Pin::new(stream).poll_shutdown(context),
            #[cfg(windows)]
            Self::WindowsServer(stream) => Pin::new(stream).poll_shutdown(context),
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::protocol::{
        client_handshake, server_handshake, Capabilities, ClientHello, ConnectionRole,
        ProtocolRange, ServerIdentity,
    };
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn unix_listener_is_exclusive_and_stream_is_transparent() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("control.sock");
        let endpoint = NativeEndpoint::Unix(path.clone());
        let mut listener = NativeListener::bind(&endpoint).unwrap();
        assert!(
            NativeListener::bind(&endpoint).is_err(),
            "squatter was replaced"
        );
        let client = tokio::spawn(async move {
            let mut stream = connect(&endpoint).await.unwrap();
            stream.write_all(b"ping").await.unwrap();
            let mut response = [0; 4];
            stream.read_exact(&mut response).await.unwrap();
            response
        });
        let mut server = listener.accept().await.unwrap();
        let mut request = [0; 4];
        server.read_exact(&mut request).await.unwrap();
        server.write_all(&request).await.unwrap();
        assert_eq!(client.await.unwrap(), *b"ping");
        drop(listener);
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn unix_application_stream_is_authenticated_before_handoff() {
        let directory = tempfile::tempdir().unwrap();
        let endpoint = NativeEndpoint::Unix(directory.path().join("authenticated.sock"));
        let mut listener = NativeListener::bind(&endpoint).unwrap();
        let client = tokio::spawn(async move {
            let mut stream = connect(&endpoint).await.unwrap();
            client_handshake(&mut stream, &test_hello(), Duration::from_secs(1))
                .await
                .unwrap();
            let mut stream = stream.into_authenticated();
            stream.write_all(b"echo").await.unwrap();
            let mut response = [0; 4];
            stream.read_exact(&mut response).await.unwrap();
            response
        });
        let mut stream = listener.accept().await.unwrap();
        let (_, role) = server_handshake(&mut stream, &test_identity(), Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(role, ConnectionRole::Application);
        let mut request = [0; 4];
        stream.read_exact(&mut request).await.unwrap();
        stream.write_all(&request).await.unwrap();
        assert_eq!(client.await.unwrap(), *b"echo");
    }

    #[tokio::test]
    async fn old_listener_does_not_unlink_replacement_file() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("control.sock");
        let endpoint = NativeEndpoint::Unix(path.clone());
        let listener = NativeListener::bind(&endpoint).unwrap();
        std::fs::remove_file(&path).unwrap();
        std::fs::write(&path, b"successor").unwrap();
        drop(listener);
        assert_eq!(std::fs::read(path).unwrap(), b"successor");
    }

    #[test]
    fn portable_path_budget_is_enforced_before_bind() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("x".repeat(101));
        assert!(matches!(
            NativeListener::bind(&NativeEndpoint::Unix(path)),
            Err(error) if error.kind() == io::ErrorKind::InvalidInput
        ));
    }

    fn test_identity() -> ServerIdentity {
        ServerIdentity {
            app: [1; 32],
            key: [2; 32],
            isolation: [3; 32],
            generation: 4,
            token: [5; 32],
            versions: ProtocolRange {
                minimum: 1,
                maximum: 1,
            },
            process_id: std::process::id(),
            process_identity: vec![6],
            build: [7; 32],
            config: [8; 32],
            state: 3,
            capabilities: Capabilities::APPLICATION_STREAM,
            service_endpoint: None,
        }
    }

    fn test_hello() -> ClientHello {
        let identity = test_identity();
        ClientHello {
            app: identity.app,
            key: identity.key,
            isolation: identity.isolation,
            generation: identity.generation,
            token: identity.token,
            versions: identity.versions,
            role: ConnectionRole::Application,
        }
    }
}
