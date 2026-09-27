//! Error types for kotatsu.

/// Result alias for kotatsu operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors produced by the kotatsu control plane.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// An AWS API call failed.
    #[error("aws {op} failed: {source}")]
    Aws {
        /// Name of the AWS operation that failed.
        op: &'static str,
        /// Whether retrying the call may succeed — transport timeouts,
        /// dispatch failures, 429 and 5xx responses. Permanent failures
        /// (validation, access denied, request build errors) carry
        /// `false` so callers do not bury them behind retries.
        transient: bool,
        /// Underlying SDK error.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// The service rejected the call because the resource is already
    /// undergoing a conflicting transition (HTTP 409 `ConflictException`).
    ///
    /// In waiters this is transient by definition — the next `get` poll
    /// disambiguates whether the in-flight transition reached the desired
    /// state — so [`Error::is_transient`] returns `true` here.
    #[error("aws {op} conflict: {source}")]
    Conflict {
        /// Name of the AWS operation that reported the conflict.
        op: &'static str,
        /// Underlying SDK error.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// Caller supplied a value outside the service contract.
    #[error("invalid input: {0}")]
    InvalidInput(String),

    /// A MicroVM was observed in a state the caller did not expect.
    #[error("microvm {id} in unexpected state: expected {expected}, got {got}")]
    UnexpectedState {
        /// MicroVM identifier.
        id: String,
        /// Expected state(s).
        expected: String,
        /// Observed state.
        got: String,
    },

    /// A waiter timed out before the MicroVM reached the desired state.
    #[error("microvm {id} did not reach state {state} within {timeout:?}")]
    WaitTimeout {
        /// MicroVM identifier.
        id: String,
        /// Desired state.
        state: String,
        /// Timeout budget that elapsed.
        timeout: std::time::Duration,
    },

    /// The MicroVM has already terminated and can no longer be used.
    #[error("microvm {0} is terminated")]
    Terminated(String),

    /// The referenced MicroVM does not exist (anymore).
    ///
    /// Mapped from the service's `ResourceNotFoundException` on the AWS
    /// control plane, and from lookups into [`crate::mock::MockControlPlane`].
    /// Reapers and routers can rely on matching this variant.
    #[error("microvm {id} not found")]
    NotFound {
        /// MicroVM identifier that was not found.
        id: String,
    },

    /// HTTP request to a MicroVM endpoint failed.
    #[error("http: {0}")]
    Http(#[from] reqwest::Error),

    /// WebSocket handshake or transport failed.
    #[error("websocket: {0}")]
    Ws(#[source] Box<dyn std::error::Error + Send + Sync>),

    /// The pool is at its configured VM cap and cannot launch more.
    #[error("pool exhausted: {0} managed microvms at cap")]
    PoolExhausted(usize),

    /// Authentication token handling failed.
    #[error("token: {0}")]
    Token(String),

    /// State store failure.
    #[error("store: {0}")]
    Store(String),

    /// Configuration failure.
    #[error("config: {0}")]
    Config(String),

    /// IO failure.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    /// Catch-all.
    #[error("{0}")]
    Other(String),
}

impl Error {
    /// Wraps an AWS SDK failure, classifying retryability from the
    /// structured `SdkError` variant instead of guessing from the
    /// rendered message.
    pub(crate) fn aws<E: std::error::Error + Send + Sync + 'static>(
        op: &'static str,
        e: aws_sdk_lambdamicrovms::error::SdkError<E>,
    ) -> Self {
        use aws_sdk_lambdamicrovms::error::SdkError;
        // A 409 means the resource is already mid-transition — surface it
        // as a typed conflict so waiters keep polling instead of failing
        // the request that triggered the transition.
        if matches!(&e, SdkError::ServiceError(_) | SdkError::ResponseError(_))
            && e.raw_response().is_some_and(|r| r.status().as_u16() == 409)
        {
            return Self::Conflict {
                op,
                source: Box::new(e),
            };
        }
        let transient = match &e {
            // Transport-level failures: no response ever arrived.
            SdkError::TimeoutError(_) | SdkError::DispatchFailure(_) => true,
            // Service rejections: retry only 429 and 5xx.
            SdkError::ServiceError(_) | SdkError::ResponseError(_) => e
                .raw_response()
                .is_some_and(|r| r.status().as_u16() == 429 || r.status().is_server_error()),
            // Request construction and anything else is caller-side — permanent.
            _ => false,
        };
        Self::Aws {
            op,
            transient,
            source: Box::new(e),
        }
    }

    /// Whether retrying the failed operation may succeed.
    ///
    /// `true` for AWS transport timeouts, dispatch failures, conflicts
    /// (409), throttling (429) and 5xx responses, HTTP timeouts/connect
    /// failures and 429/5xx, and local IO/store errors. `false` for
    /// everything else — `NotFound`, `Terminated`, `InvalidInput`,
    /// `Token`, `Config`, auth/validation AWS rejections — where
    /// retrying would only delay surfacing the real failure.
    pub fn is_transient(&self) -> bool {
        match self {
            Self::Aws { transient, .. } => *transient,
            Self::Conflict { .. } => true,
            Self::Http(e) => {
                e.is_timeout()
                    || e.is_connect()
                    || e.status()
                        .is_some_and(|s| s.as_u16() == 429 || s.is_server_error())
            }
            Self::Io(_) | Self::Store(_) => true,
            _ => false,
        }
    }

    pub(crate) fn invalid(msg: impl Into<String>) -> Self {
        Self::InvalidInput(msg.into())
    }
}

impl From<tokio_tungstenite::tungstenite::Error> for Error {
    fn from(e: tokio_tungstenite::tungstenite::Error) -> Self {
        Self::Ws(Box::new(e))
    }
}
