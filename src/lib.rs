//! Rate limiting, traffic throttling, burst management, and standardized HTTP response headers for Druto.
//!
//! Provides algorithmic rate limiting engines, pluggable storage backends (including hybrid memory/Redis tiers),
//! and execution middleware complying with RFC 6585 and IETF RateLimit header field specifications.

pub mod config;
pub mod key;
pub mod middleware;
#[cfg(feature = "redis")]
pub mod redis_store;

pub use config::{LimiterConfig, LimiterConfigBuilder, PolicyResolver, RejectionHandler};
pub use key::{CustomKeyResolver, KeyResolver};
#[cfg(feature = "redis")]
pub use lagam::DEFAULT_REDIS_KEY_PREFIX;
pub use lagam::{
    Algorithm, BoundLimiter, BoxFuture, HeaderMode, LimiterError, LimiterRegistry, LimiterStore,
    MemoryLimiterStore, MultiLimiter, QuotaPolicy, RateLimitResult, TierConfig, TieredLimiterStore,
    constants,
};
pub use middleware::{LimiterMiddleware, LimiterMiddlewareBuilder};
#[cfg(feature = "redis")]
pub use redis_store::{DrutoRedisClient, RedisLimiterStore};

/// Convenient prelude re-exporting core rate limiting types and builders.
pub mod prelude {
    #[cfg(feature = "redis")]
    pub use crate::RedisLimiterStore;
    pub use crate::{
        Algorithm, HeaderMode, KeyResolver, LimiterConfig, LimiterConfigBuilder, LimiterError,
        LimiterMiddleware, LimiterRegistry, LimiterStore, MemoryLimiterStore, MultiLimiter,
        QuotaPolicy, RateLimitResult, TierConfig, TieredLimiterStore,
    };
}
