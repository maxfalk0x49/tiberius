use crate::Config;
use futures_util::io::{AsyncRead, AsyncWrite};

#[cfg(all(feature = "native-tls", not(feature = "rustls"), not(feature = "vendored-openssl")))]
mod native_tls_stream;

#[cfg(feature = "rustls")]
mod rustls_tls_stream;

#[cfg(all(feature = "vendored-openssl", not(feature = "rustls")))]
mod opentls_tls_stream;

#[cfg(all(feature = "native-tls", not(feature = "rustls"), not(feature = "vendored-openssl")))]
pub(crate) use native_tls_stream::TlsStream;

#[cfg(feature = "rustls")]
pub(crate) use rustls_tls_stream::TlsStream;

#[cfg(all(feature = "vendored-openssl", not(feature = "rustls")))]
pub(crate) use opentls_tls_stream::TlsStream;

#[cfg(feature = "rustls")]
pub(crate) async fn create_tls_stream<S: AsyncRead + AsyncWrite + Unpin + Send>(
    config: &Config,
    stream: S,
) -> crate::Result<TlsStream<S>> {
    TlsStream::new(config, stream).await
}

#[cfg(all(feature = "native-tls", not(feature = "rustls"), not(feature = "vendored-openssl")))]
pub(crate) async fn create_tls_stream<S: AsyncRead + AsyncWrite + Unpin + Send>(
    config: &Config,
    stream: S,
) -> crate::Result<TlsStream<S>> {
    native_tls_stream::create_tls_stream(config, stream).await
}

#[cfg(all(feature = "vendored-openssl", not(feature = "rustls")))]
pub(crate) async fn create_tls_stream<S: AsyncRead + AsyncWrite + Unpin + Send>(
    config: &Config,
    stream: S,
) -> crate::Result<TlsStream<S>> {
    opentls_tls_stream::create_tls_stream(config, stream).await
}
