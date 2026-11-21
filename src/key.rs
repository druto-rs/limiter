//! Key resolution strategies and client fingerprinting.

use std::sync::Arc;

use router::{HeaderName, HttpContext};

/// Custom key resolution closure wrapped in an [`Arc`].
pub type CustomKeyResolver = Arc<dyn Fn(&HttpContext) -> Option<String> + Send + Sync>;

/// Extraction strategy for rate limiting keys from an incoming request.
#[derive(Clone, Default)]
pub enum KeyResolver {
    /// Extracts the client IP address, honoring proxy headers if configured.
    #[default]
    Ip,
    /// Extracts a specific HTTP request header (e.g. `X-API-Key` or `Authorization`).
    Header(HeaderName),
    /// Combines the requested route path with the client IP address.
    RouteAndIp,
    /// Custom closure deriving the key dynamically from [`HttpContext`].
    Custom(CustomKeyResolver),
}

impl KeyResolver {
    /// Resolves the rate limit fingerprint key from the given context.
    #[must_use]
    pub fn resolve(&self, ctx: &HttpContext) -> String {
        match self {
            Self::Ip => ctx.request.ip().unwrap_or_else(|| "unknown_ip".to_string()),
            Self::Header(header_name) => ctx
                .request
                .header(header_name.clone())
                .map(ToString::to_string)
                .unwrap_or_else(|| ctx.request.ip().unwrap_or_else(|| "unknown".to_string())),
            Self::RouteAndIp => {
                let ip = ctx.request.ip().unwrap_or_else(|| "unknown_ip".to_string());
                format!("{}:{ip}", ctx.request.path())
            }
            Self::Custom(resolver) => resolver(ctx)
                .unwrap_or_else(|| ctx.request.ip().unwrap_or_else(|| "unknown_ip".to_string())),
        }
    }
}
