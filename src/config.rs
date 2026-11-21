//! Configuration models, header modes, and fluent builders.

use std::sync::Arc;

pub use lagam::HeaderMode;
use lagam::{QuotaPolicy, RateLimitResult};
use router::{HttpContext, Response};

use crate::key::KeyResolver;

/// Dynamic policy derivation closure wrapped in an [`Arc`].
pub type PolicyResolver = Arc<dyn Fn(&HttpContext) -> QuotaPolicy + Send + Sync>;

/// Custom rejection response handler wrapped in an [`Arc`].
pub type RejectionHandler = Arc<dyn Fn(&HttpContext, &RateLimitResult) -> Response + Send + Sync>;

/// Comprehensive rate limiter configuration settings.
#[derive(Clone)]
pub struct LimiterConfig {
    /// Whether rate limiting middleware is actively enforced.
    pub enabled: bool,
    /// Header emission format strategy.
    pub header_mode: HeaderMode,
    /// Whether to allow requests through if the backing store encounters errors or timeouts.
    pub fail_open: bool,
    /// Whether to evaluate quotas without rejecting requests with HTTP 429.
    pub dry_run: bool,
    /// URL path prefixes or patterns where rate limiting applies (empty means all routes).
    pub paths: Vec<String>,
    /// Fingerprint key derivation strategy.
    pub key_resolver: KeyResolver,
    /// Dynamic policy resolution closure evaluated per request.
    pub policy_fn: PolicyResolver,
    /// Custom HTTP 429 response builder.
    pub rejection_handler: Option<RejectionHandler>,
}

impl Default for LimiterConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            header_mode: HeaderMode::Both,
            fail_open: true,
            dry_run: false,
            paths: Vec::new(),
            key_resolver: KeyResolver::Ip,
            policy_fn: Arc::new(|_| QuotaPolicy::per_minute(60)),
            rejection_handler: None,
        }
    }
}

impl LimiterConfig {
    /// Creates a new `LimiterConfigBuilder`.
    #[must_use]
    pub fn builder() -> LimiterConfigBuilder {
        LimiterConfigBuilder::new()
    }

    /// Evaluates whether the request path falls within the configured path patterns.
    #[must_use]
    pub fn matches_path(&self, request_path: &str) -> bool {
        if self.paths.is_empty() {
            return true;
        }

        self.paths.iter().any(|pattern| {
            if pattern == "*" || pattern == "/*" || pattern == request_path {
                true
            } else if let Some(prefix) = pattern
                .strip_suffix("/*")
                .or_else(|| pattern.strip_suffix('*'))
            {
                request_path.starts_with(prefix)
            } else {
                false
            }
        })
    }
}

/// Fluent builder for [`LimiterConfig`].
#[derive(Clone, Default)]
pub struct LimiterConfigBuilder {
    config: LimiterConfig,
}

impl LimiterConfigBuilder {
    /// Creates a builder initialized with default settings.
    #[must_use]
    pub fn new() -> Self {
        Self {
            config: LimiterConfig::default(),
        }
    }

    /// Toggles active rate limiting.
    #[must_use]
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.config.enabled = enabled;
        self
    }

    /// Sets the header emission format mode.
    #[must_use]
    pub fn header_mode(mut self, mode: HeaderMode) -> Self {
        self.config.header_mode = mode;
        self
    }

    /// Configures the fail-open resilience policy on store failures.
    #[must_use]
    pub fn fail_open(mut self, fail_open: bool) -> Self {
        self.config.fail_open = fail_open;
        self
    }

    /// Toggles dry-run audit mode (computes quotas and headers without 429 rejections).
    #[must_use]
    pub fn dry_run(mut self, dry_run: bool) -> Self {
        self.config.dry_run = dry_run;
        self
    }

    /// Scopes rate limiting to specific request path prefixes or patterns.
    #[must_use]
    pub fn path(mut self, path: impl Into<String>) -> Self {
        self.config.paths.push(path.into());
        self
    }

    /// Scopes rate limiting to multiple request path prefixes or patterns.
    #[must_use]
    pub fn paths<I, S>(mut self, paths: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.config.paths.extend(paths.into_iter().map(Into::into));
        self
    }

    /// Configures the key resolution strategy.
    #[must_use]
    pub fn key_resolver(mut self, resolver: KeyResolver) -> Self {
        self.config.key_resolver = resolver;
        self
    }

    /// Configures a static rate limit policy across all matching requests.
    #[must_use]
    pub fn policy(mut self, policy: QuotaPolicy) -> Self {
        self.config.policy_fn = Arc::new(move |_| policy.clone());
        self
    }

    /// Configures a dynamic policy derivation closure evaluated per request.
    #[must_use]
    pub fn policy_fn<F>(mut self, policy_fn: F) -> Self
    where
        F: Fn(&HttpContext) -> QuotaPolicy + Send + Sync + 'static,
    {
        self.config.policy_fn = Arc::new(policy_fn);
        self
    }

    /// Configures a custom rejection response builder when a rate limit is exceeded.
    #[must_use]
    pub fn rejection_handler<F>(mut self, handler: F) -> Self
    where
        F: Fn(&HttpContext, &RateLimitResult) -> Response + Send + Sync + 'static,
    {
        self.config.rejection_handler = Some(Arc::new(handler));
        self
    }

    /// Consumes the builder and returns the completed [`LimiterConfig`].
    #[must_use]
    pub fn build(self) -> LimiterConfig {
        self.config
    }
}
