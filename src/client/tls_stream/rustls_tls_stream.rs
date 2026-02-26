use crate::{
    client::{config::Config, TrustConfig},
    error::IoErrorKind,
    Error,
};
use futures_util::io::{AsyncRead, AsyncWrite};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use std::{
    fs, io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio_rustls::{
    rustls::{
        client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
        ClientConfig, DigitallySignedStruct, Error as RustlsError, RootCertStore, SignatureScheme,
    },
    TlsConnector,
};
use tokio_util::compat::{Compat, FuturesAsyncReadCompatExt, TokioAsyncReadCompatExt};
use tracing::{event, Level};

impl From<tokio_rustls::rustls::Error> for Error {
    fn from(e: tokio_rustls::rustls::Error) -> Self {
        crate::Error::Tls(e.to_string())
    }
}

pub(crate) struct TlsStream<S: AsyncRead + AsyncWrite + Unpin + Send>(
    Compat<tokio_rustls::client::TlsStream<Compat<S>>>,
);

#[derive(Debug)]
struct NoCertVerifier;

impl ServerCertVerifier for NoCertVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, RustlsError> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

fn get_server_name(config: &Config) -> crate::Result<ServerName<'static>> {
    match (ServerName::try_from(config.get_host()), &config.trust) {
        (Ok(sn), _) => Ok(sn.to_owned()),
        (Err(_), TrustConfig::TrustAll) => {
            Ok(ServerName::try_from("placeholder.domain.com")
                .unwrap()
                .to_owned())
        }
        (Err(e), _) => Err(crate::Error::Tls(e.to_string())),
    }
}

fn parse_pem_certs(input: &[u8]) -> crate::Result<Vec<CertificateDer<'static>>> {
    let mut certs = Vec::new();
    let content = String::from_utf8_lossy(input);
    let mut start = 0;
    while let Some(begin) = content[start..].find("-----BEGIN CERTIFICATE-----") {
        let begin_pos = start + begin + "-----BEGIN CERTIFICATE-----".len();
        if let Some(end) = content[begin_pos..].find("-----END CERTIFICATE-----") {
            let end_pos = begin_pos + end;
            let base64_content = &content[begin_pos..end_pos]
                .lines()
                .collect::<Vec<_>>()
                .join("");
            
            use base64::{Engine as _, engine::general_purpose};
            let der = general_purpose::STANDARD
                .decode(base64_content)
                .map_err(|e| crate::Error::Tls(format!("Failed to decode PEM: {}", e)))?;
            
            certs.push(CertificateDer::from(der));
            start = end_pos + "-----END CERTIFICATE-----".len();
        } else {
            break;
        }
    }
    Ok(certs)
}

impl<S: AsyncRead + AsyncWrite + Unpin + Send> TlsStream<S> {
    pub(super) async fn new(config: &Config, stream: S) -> crate::Result<Self> {
        event!(Level::INFO, "Performing a TLS handshake");

        // Ensure a crypto provider is installed
        let _ = rustls::crypto::ring::default_provider().install_default();

        let client_config = match &config.trust {
            TrustConfig::CaCertificateLocation(path) => {
                if let Ok(buf) = fs::read(path) {
                    let certs = match path.extension() {
                        Some(ext)
                        if ext.to_ascii_lowercase() == "pem"
                            || ext.to_ascii_lowercase() == "crt" =>
                        {
                            parse_pem_certs(&buf)?
                        }
                        Some(ext) if ext.to_ascii_lowercase() == "der" => {
                            vec![CertificateDer::from(buf)]
                        }
                        Some(_) | None => {
                            return Err(crate::Error::Io {
                                kind: IoErrorKind::InvalidInput,
                                message: "Provided CA certificate with unsupported file-extension! Supported types are pem, crt and der.".to_string(),
                            })
                        }
                    };

                    if certs.len() != 1 {
                        return Err(crate::Error::Io {
                            kind: IoErrorKind::InvalidInput,
                            message: format!(
                                "Certificate file {} contain 0 or more than 1 certs",
                                path.to_string_lossy()
                            ),
                        });
                    }

                    let mut cert_store = RootCertStore::empty();
                    cert_store.add(certs.into_iter().next().unwrap())?;
                    ClientConfig::builder()
                        .with_root_certificates(cert_store)
                        .with_no_client_auth()
                } else {
                    return Err(Error::Io {
                        kind: IoErrorKind::InvalidData,
                        message: "Could not read provided CA certificate!".to_string(),
                    });
                }
            }
            TrustConfig::TrustAll => {
                event!(
                    Level::WARN,
                    "Trusting the server certificate without validation."
                );
                ClientConfig::builder()
                    .dangerous()
                    .with_custom_certificate_verifier(Arc::new(NoCertVerifier))
                    .with_no_client_auth()
            }
            TrustConfig::Default => {
                event!(Level::INFO, "Using default trust configuration.");
                let mut roots = RootCertStore::empty();
                for cert in rustls_native_certs::load_native_certs()
                    .expect("could not load platform certs")
                {
                    roots.add(cert)?;
                }
                ClientConfig::builder()
                    .with_root_certificates(roots)
                    .with_no_client_auth()
            }
        };

        let connector = TlsConnector::from(Arc::new(client_config));

        let tls_stream = connector
            .connect(get_server_name(config)?, stream.compat())
            .await?;

        Ok(TlsStream(tls_stream.compat()))
    }

    pub(crate) fn get_mut(&mut self) -> &mut S {
        self.0.get_mut().get_mut().0.get_mut()
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin + Send> AsyncRead for TlsStream<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let inner = Pin::get_mut(self);
        Pin::new(&mut inner.0).poll_read(cx, buf)
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin + Send> AsyncWrite for TlsStream<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let inner = Pin::get_mut(self);
        Pin::new(&mut inner.0).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let inner = Pin::get_mut(self);
        Pin::new(&mut inner.0).poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let inner = Pin::get_mut(self);
        Pin::new(&mut inner.0).poll_close(cx)
    }
}
