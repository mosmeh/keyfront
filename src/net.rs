use serde::Deserialize;
use std::{
    convert::Infallible, net::SocketAddr, path::PathBuf, pin::Pin, str::FromStr, task::Poll,
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::{TcpListener, TcpSocket, UnixListener, UnixSocket, tcp, unix},
};

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
    type ReadHalf: AsyncRead + Send + Unpin + 'static;
    type WriteHalf: AsyncWrite + Send + Unpin + 'static;

    fn poll_accept(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<std::io::Result<(Self::ReadHalf, Self::WriteHalf)>>;

    fn accept(&mut self) -> Accept<'_, Self>
    where
        Self: Sized,
    {
        Accept { listener: self }
    }
}

pub struct Accept<'a, T> {
    listener: &'a mut T,
}

impl<T: Listener> Future for Accept<'_, T> {
    type Output = std::io::Result<(T::ReadHalf, T::WriteHalf)>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        self.listener.poll_accept(cx)
    }
}

impl Listener for TcpListener {
    type ReadHalf = tcp::OwnedReadHalf;
    type WriteHalf = tcp::OwnedWriteHalf;

    fn poll_accept(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<std::io::Result<(Self::ReadHalf, Self::WriteHalf)>> {
        Self::poll_accept(self, cx).map_ok(|(stream, _)| {
            let _ = stream.set_nodelay(true);
            stream.into_split()
        })
    }
}

impl Listener for UnixListener {
    type ReadHalf = unix::OwnedReadHalf;
    type WriteHalf = unix::OwnedWriteHalf;

    fn poll_accept(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<std::io::Result<(Self::ReadHalf, Self::WriteHalf)>> {
        Self::poll_accept(self, cx).map_ok(|(stream, _)| stream.into_split())
    }
}
