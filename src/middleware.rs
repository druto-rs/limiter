//! Asynchronous onion middleware for HTTP request throttling and rate limiting.

use std::sync::Arc;

use dhap::{BoxFuture, Error as DhapError, Middleware, Next};
use lagam::{
    HeaderMode, LimiterStore, MemoryLimiterStore, QuotaPolicy, RateLimitResult, constants,
};
use router::{HeaderName, HttpContext, Response, ResponseBody, RouterError, StatusCode};

use crate::config::{LimiterConfig, LimiterConfigBuilder};

/// HTTP rate limiting and burst throttling middleware for the execution pipeline.
#[derive(Clone)]
pub struct LimiterMiddleware {
    config: Arc<LimiterConfig>,
    store: Arc<dyn LimiterStore>,
}

impl LimiterMiddleware {
    /// Constructs a new `LimiterMiddleware` with a chosen storage backend and configuration.
    pub fn new(store: Arc<dyn LimiterStore>, config: LimiterConfig) -> Self {
        Self {
            config: Arc::new(config),
            store,
        }
    }

    /// Constructs a `LimiterMiddleware` backed by the default in-memory store.
    #[must_use]
    pub fn memory(config: LimiterConfig) -> Self {
        Self::new(Arc::new(MemoryLimiterStore::new()), config)
    }

    /// Creates an in-memory rate limiter permitting `limit` requests per second.
    #[must_use]
    pub fn per_second(limit: u64) -> Self {
        let config = LimiterConfig::builder()
            .policy(QuotaPolicy::per_second(limit))
            .build();
        Self::memory(config)
    }

    /// Creates an in-memory rate limiter permitting `limit` requests per minute.
    #[must_use]
    pub fn per_minute(limit: u64) -> Self {
        let config = LimiterConfig::builder()
            .policy(QuotaPolicy::per_minute(limit))
            .build();
        Self::memory(config)
    }

    /// Creates an in-memory rate limiter permitting `limit` requests per hour.
    #[must_use]
    pub fn per_hour(limit: u64) -> Self {
        let config = LimiterConfig::builder()
            .policy(QuotaPolicy::per_hour(limit))
            .build();
        Self::memory(config)
    }

    /// Returns a fluent builder for configuring and creating a `LimiterMiddleware`.
    #[must_use]
    pub fn builder() -> LimiterMiddlewareBuilder {
        LimiterMiddlewareBuilder::new()
    }

    /// Returns a reference to the active limiter configuration.
    #[must_use]
    pub fn config(&self) -> &LimiterConfig {
        &self.config
    }

    /// Returns a reference to the active storage driver.
    #[must_use]
    pub fn store(&self) -> &Arc<dyn LimiterStore> {
        &self.store
    }

    fn attach_headers(
        &self,
        response: &mut Response,
        result: &RateLimitResult,
        policy: &QuotaPolicy,
    ) {
        let mode = self.config.header_mode;
        if mode == HeaderMode::None {
            return;
        }

        let limit_str = result.limit.to_string();
        let remaining_str = result.remaining.to_string();
        let reset_str = result.reset_seconds().to_string();

        if matches!(mode, HeaderMode::Both | HeaderMode::Standard) {
            response.set_header(constants::RATE_LIMIT_LIMIT, limit_str.as_str());
            response.set_header(constants::RATE_LIMIT_REMAINING, remaining_str.as_str());
            response.set_header(constants::RATE_LIMIT_RESET, reset_str.as_str());

            if let Some(first_tier) = policy.tiers().first() {
                let policy_desc = format!("{};w={}", first_tier.limit, first_tier.window.as_secs());
                response.set_header(constants::RATE_LIMIT_POLICY, policy_desc);
            }
        }

        if matches!(mode, HeaderMode::Both | HeaderMode::Legacy) {
            response.set_header(constants::X_RATE_LIMIT_LIMIT, limit_str.as_str());
            response.set_header(constants::X_RATE_LIMIT_REMAINING, remaining_str.as_str());
            response.set_header(constants::X_RATE_LIMIT_RESET, reset_str.as_str());
        }

        if result.is_exceeded() {
            let retry_secs = result.retry_after_seconds().to_string();
            response.set_header(constants::RETRY_AFTER, retry_secs.as_str());
        }
    }

    async fn execute(
        &self,
        ctx: &mut HttpContext,
        next: Next<HttpContext, Response, RouterError>,
    ) -> Result<Response, DhapError<RouterError>> {
        // 1. Bypass if rate limiting middleware is globally disabled.
        if !self.config.enabled {
            return next.run(ctx).await;
        }

        // 2. Bypass if request URI path is outside configured paths.
        let path = ctx.request.path().to_string();
        if !self.config.matches_path(&path) {
            return next.run(ctx).await;
        }

        // 3. Evaluate dynamic quota policy for the request.
        let policy = (self.config.policy_fn)(ctx);
        if policy.is_unlimited() {
            return next.run(ctx).await;
        }

        // 4. Resolve fingerprint identity key.
        let key = self.config.key_resolver.resolve(ctx);

        // 5. Evaluate all configured rate limit tiers.
        let tiers = policy.tiers();
        let mut combined_result: Option<RateLimitResult> = None;

        for (index, tier) in tiers.iter().enumerate() {
            let tier_key = if tiers.len() > 1 {
                format!("{key}:tier{index}")
            } else {
                key.clone()
            };

            let eval_result = match self
                .store
                .consume(&tier_key, 1, tier.limit, tier.window, tier.algorithm)
                .await
            {
                Ok(res) => res,
                Err(error) => {
                    if self.config.fail_open {
                        // Fail-open: bypass rate limiting and allow request downstream
                        return next.run(ctx).await;
                    }
                    return Err(DhapError::Execution(RouterError::from(error)));
                }
            };

            // If quota exhausted and penalty lockout is configured, block the key.
            if eval_result.is_exceeded()
                && let Some(penalty) = tier.block_for
            {
                let _ = self.store.block(&tier_key, penalty).await;
            }

            combined_result = match combined_result {
                Some(prev) => Some(prev.combine_most_restrictive(&eval_result)),
                None => Some(eval_result),
            };
        }

        let Some(result) = combined_result else {
            return next.run(ctx).await;
        };

        // 6. Inject evaluation result into request extensions for downstream handlers.
        ctx.extensions.insert(result.clone());

        // 7. Check if request is allowed or running in dry-run mode.
        if result.is_allowed || self.config.dry_run {
            let mut response = next.run(ctx).await?;
            self.attach_headers(&mut response, &result, &policy);
            Ok(response)
        } else {
            // 8. Rate limit exceeded: short-circuit with 429 Too Many Requests.
            let mut response = if let Some(handler) = &self.config.rejection_handler {
                handler(ctx, &result)
            } else {
                let payload = serde_json::to_vec(&serde_json::json!({
                    "message": "Too Many Requests",
                    "retry_after": result.retry_after_seconds(),
                }))
                .unwrap_or_default();

                let mut rejection = Response::new(StatusCode::TOO_MANY_REQUESTS);
                rejection.set_header(HeaderName::CONTENT_TYPE, "application/json");
                rejection.set_body(ResponseBody::Bytes(bytes::Bytes::from(payload)));
                rejection
            };

            self.attach_headers(&mut response, &result, &policy);
            Ok(response)
        }
    }
}

impl Middleware<HttpContext, Response, RouterError> for LimiterMiddleware {
    fn handle<'a>(
        &'a self,
        ctx: &'a mut HttpContext,
        next: Next<HttpContext, Response, RouterError>,
    ) -> BoxFuture<'a, Result<Response, DhapError<RouterError>>> {
        Box::pin(self.execute(ctx, next))
    }
}

/// Fluent builder for [`LimiterMiddleware`].
#[derive(Clone, Default)]
pub struct LimiterMiddlewareBuilder {
    config_builder: LimiterConfigBuilder,
    store: Option<Arc<dyn LimiterStore>>,
}

impl LimiterMiddlewareBuilder {
    /// Creates a new builder instance.
    #[must_use]
    pub fn new() -> Self {
        Self {
            config_builder: LimiterConfigBuilder::new(),
            store: None,
        }
    }

    /// Sets the underlying storage driver.
    #[must_use]
    pub fn store(mut self, store: Arc<dyn LimiterStore>) -> Self {
        self.store = Some(store);
        self
    }

    /// Configures the header emission mode.
    #[must_use]
    pub fn header_mode(mut self, mode: HeaderMode) -> Self {
        self.config_builder = self.config_builder.header_mode(mode);
        self
    }

    /// Toggles fail-open policy on storage errors.
    #[must_use]
    pub fn fail_open(mut self, fail_open: bool) -> Self {
        self.config_builder = self.config_builder.fail_open(fail_open);
        self
    }

    /// Toggles dry-run audit mode.
    #[must_use]
    pub fn dry_run(mut self, dry_run: bool) -> Self {
        self.config_builder = self.config_builder.dry_run(dry_run);
        self
    }

    /// Adds a path prefix or pattern where rate limiting applies.
    #[must_use]
    pub fn path(mut self, path: impl Into<String>) -> Self {
        self.config_builder = self.config_builder.path(path);
        self
    }

    /// Sets the quota policy.
    #[must_use]
    pub fn policy(mut self, policy: QuotaPolicy) -> Self {
        self.config_builder = self.config_builder.policy(policy);
        self
    }

    /// Sets the dynamic policy derivation closure.
    #[must_use]
    pub fn policy_fn<F>(mut self, policy_fn: F) -> Self
    where
        F: Fn(&HttpContext) -> QuotaPolicy + Send + Sync + 'static,
    {
        self.config_builder = self.config_builder.policy_fn(policy_fn);
        self
    }

    /// Builds the `LimiterMiddleware`.
    #[must_use]
    pub fn build(self) -> LimiterMiddleware {
        let store = self
            .store
            .unwrap_or_else(|| Arc::new(MemoryLimiterStore::new()));
        LimiterMiddleware::new(store, self.config_builder.build())
    }
}
