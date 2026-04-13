use ext_php_rs::exception::PhpException;
use ext_php_rs::zend::ce;

// Ref: https://github.com/BSN4/grpc-php-rs/blob/main/src/error.rs
pub type StreamError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, thiserror::Error)]
pub enum QuicheError {
    #[error("failed to initialize tokio runtime: {0}")]
    RuntimeInit(#[from] std::io::Error),

    #[error("gRPC status {code}: {message}")]
    Status { code: i32, message: String },

    #[error("invalid argument: {0}")]
    InvalidArg(String),

    #[error("invalid URI: {0}")]
    InvalidUri(String),

    #[error("callback failed: {0}")]
    CallbackFailed(String),
}

impl From<QuicheError> for PhpException {
    fn from(err: QuicheError) -> Self {
        PhpException::new(err.to_string(), 0, ce::exception())
    }
}
