use serde::Deserialize;
use std::{
    convert::Infallible,
    net::SocketAddr,
    path::PathBuf,
    pin::Pin,
    str::FromStr,
    sync::Arc,
    task::{Poll, ready},
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::{TcpListener, TcpSocket, TcpStream, UnixListener, UnixSocket, UnixStream, tcp, unix},
};
use tokio_rustls::{TlsAcceptor, rustls::ServerConfig, server::TlsStream};

#[derive(Debug, Clone)]
pub enum Address {
    Tcp(SocketAddr),
    Unix(PathBuf),
}

impl std::fmt::Display for Address {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Tcp(addr) => addr.fmt(f),
            Self::Unix(path) => path.display().fmt(f),
        }
    }
}

impl FromStr for Address {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(if let Ok(addr) = s.parse() {
            Self::Tcp(addr)
        } else {
            Self::Unix(s.into())
        })
    }
}

impl<'de> Deserialize<'de> for Address {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(String::deserialize(deserializer)?.parse().unwrap())
    }
}

pub enum Socket {
    Tcp(TcpSocket),
    Unix(UnixSocket),
}

impl Socket {
    pub fn bind(addr: Address) -> std::io::Result<(Self, Address)> {
        match addr {
            Address::Tcp(addr) => {
                let socket = match addr {
                    SocketAddr::V4(_) => TcpSocket::new_v4(),
                    SocketAddr::V6(_) => TcpSocket::new_v6(),
                };
                let socket = socket?;
                socket.set_keepalive(true)?;
                socket.set_reuseaddr(true)?;
                socket.bind(addr)?;
                let local_addr = socket.local_addr()?;
                Ok((Self::Tcp(socket), Address::Tcp(local_addr)))
            }
            Address::Unix(path) => {
                let _ = std::fs::remove_file(&path);
                let socket = UnixSocket::new_stream()?;
                socket.bind(&path)?;
                Ok((Self::Unix(socket), Address::Unix(path)))
            }
        }
    }

    pub fn listen(self, backlog: u32) -> std::io::Result<ListenerKind> {
        Ok(match self {
            Self::Tcp(socket) => ListenerKind::Tcp(socket.listen(backlog)?),
            Self::Unix(socket) => ListenerKind::Unix(socket.listen(backlog)?),
        })
    }
}

pub enum ListenerKind {
    Tcp(TcpListener),
    Unix(UnixListener),
}

pub trait Listener {
    type Stream: AsyncRead + AsyncWrite + Unpin + Send + 'static;

    fn poll_accept(&self, _cx: &mut std::task::Context<'_>) -> Poll<std::io::Result<Self::Stream>> {
        Poll::Ready(Err(std::io::Error::from(std::io::ErrorKind::Unsupported)))
    }

    fn accept(&self) -> impl Future<Output = std::io::Result<Self::Stream>>
    where
        Self: Sized,
    {
        struct Accept<'a, T>(&'a T);

        impl<T: Listener> Future for Accept<'_, T> {
            type Output = std::io::Result<T::Stream>;

            fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
                self.0.poll_accept(cx)
            }
        }

        Accept(self)
    }
}

impl Listener for TcpListener {
    type Stream = TcpStream;

    fn poll_accept(&self, cx: &mut std::task::Context<'_>) -> Poll<std::io::Result<Self::Stream>> {
        Self::poll_accept(self, cx).map_ok(|(stream, _)| {
            let _ = stream.set_nodelay(true);
            stream
        })
    }
}

impl Listener for UnixListener {
    type Stream = UnixStream;

    fn poll_accept(&self, cx: &mut std::task::Context<'_>) -> Poll<std::io::Result<Self::Stream>> {
        Self::poll_accept(self, cx).map_ok(|(stream, _)| stream)
    }
}

pub struct TlsListener<T> {
    inner: T,
    acceptor: TlsAcceptor,
}

impl<T> TlsListener<T> {
    pub fn new(inner: T, config: Arc<ServerConfig>) -> Self {
        Self {
            inner,
            acceptor: config.into(),
        }
    }
}

impl<T: Listener + Sync> Listener for TlsListener<T> {
    type Stream = TlsStream<T::Stream>;

    fn accept(&self) -> impl Future<Output = std::io::Result<Self::Stream>> {
        #[expect(clippy::large_enum_variant)]
        enum Accept<'a, T: Listener> {
            Inner(&'a TlsListener<T>),
            Tls(tokio_rustls::Accept<T::Stream>),
            Accepted,
        }

        impl<T: Listener> Future for Accept<'_, T> {
            type Output = std::io::Result<TlsStream<T::Stream>>;

            fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
                let this = self.get_mut();
                loop {
                    match this {
                        Self::Inner(TlsListener { inner, acceptor }) => {
                            let stream = ready!(inner.poll_accept(cx))?;
                            *this = Self::Tls(acceptor.accept(stream));
                        }
                        Self::Tls(inner) => {
                            let stream = ready!(Pin::new(inner).poll(cx))?;
                            *this = Self::Accepted;
                            return Poll::Ready(Ok(stream));
                        }
                        Self::Accepted => panic!("future polled after completion"),
                    }
                }
            }
        }

        Accept::Inner(self)
    }
}

pub trait IntoSplit {
    type ReadHalf: AsyncRead + Unpin + Send;
    type WriteHalf: AsyncWrite + Unpin + Send;

    fn into_split(self) -> (Self::ReadHalf, Self::WriteHalf);
}

impl IntoSplit for TcpStream {
    type ReadHalf = tcp::OwnedReadHalf;
    type WriteHalf = tcp::OwnedWriteHalf;

    fn into_split(self) -> (Self::ReadHalf, Self::WriteHalf) {
        self.into_split()
    }
}

impl IntoSplit for UnixStream {
    type ReadHalf = unix::OwnedReadHalf;
    type WriteHalf = unix::OwnedWriteHalf;

    fn into_split(self) -> (Self::ReadHalf, Self::WriteHalf) {
        self.into_split()
    }
}

impl<T> IntoSplit for TlsStream<T>
where
    T: AsyncRead + AsyncWrite + Unpin + Send,
{
    type ReadHalf = tokio::io::ReadHalf<Self>;
    type WriteHalf = tokio::io::WriteHalf<Self>;

    fn into_split(self) -> (Self::ReadHalf, Self::WriteHalf) {
        tokio::io::split(self)
    }
}
