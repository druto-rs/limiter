# druto-limiter

[![Crates.io](https://img.shields.io/crates/v/druto-limiter.svg)](https://crates.io/crates/druto-limiter)
[![Documentation](https://docs.rs/druto-limiter/badge.svg)](https://docs.rs/druto-limiter)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](LICENSE-MIT)

Traffic throttling, rate limiting, burst management, and standardized HTTP rate limit headers for Druto.

Operating both as an HTTP-agnostic rate limiting engine and an asynchronous onion middleware layer on top of `dhap` and `druto-router`, `druto-limiter` protects endpoints against brute-force attacks, credential stuffing, scraping, and denial-of-service conditions in compliance with [RFC 6585](https://datatracker.ietf.org/doc/html/rfc6585), [RFC 9110](https://datatracker.ietf.org/doc/html/rfc9110), and the [IETF RateLimit Header Fields Draft](https://datatracker.ietf.org/doc/html/draft-ietf-httpapi-ratelimit-headers).

---

## Features

- **Core Rate Limiting Algorithms**:
  - **Sliding Window Counter (Default)**: Approximated rolling window combining current and prior intervals ($Count = \text{current} + \text{previous} \times (1 - \frac{t}{\text{window}})$). Delivers $O(1)$ memory and smooth boundary transitions without unbounded timestamp logs.
  - **Fixed Window**: Discretizes time into fixed intervals for high-throughput, low-overhead basic quotas.
  - **Token Bucket**: Accommodates bursts up to capacity $B$ while refilling tokens continuously at rate $R$/sec.
- **Pluggable Storage Backends (`LimiterStore`)**:
  - `MemoryLimiterStore`: Lock-free, bounded in-memory store with automated eviction for node-local throttling.
  - `RedisLimiterStore`: Distributed store powered by `druto-redis` executing atomic rate evaluation and sliding calculations in single Lua script roundtrips.
  - `TieredLimiterStore`: Multi-tier L1 Memory + L2 Redis architecture with local short-circuit flood shielding and graceful offline fallback.
- **Standards-Compliant Rate Limit Headers**:
  - **IETF RateLimit Fields**: `RateLimit-Limit`, `RateLimit-Remaining`, `RateLimit-Reset`, and `RateLimit-Policy`.
  - **Legacy De Facto Headers**: `X-RateLimit-Limit`, `X-RateLimit-Remaining`, and `X-RateLimit-Reset`.
  - **RFC 9110 `Retry-After`**: Automatically emitted in delta-seconds on `429 Too Many Requests`.
- **Declarative Named Registries & Dynamic Keys**:
  - `LimiterRegistry`: Define and reuse rate limit configurations by name.
  - `KeyResolver`: Custom dynamic key extractors (e.g., client IP, authenticated user ID, API key).
- **MultiLimiter & Tiered Orchestration**:
  - Compose multi-dimensional limits (e.g. 10 requests per second burst in memory AND 500 requests per hour globally in Redis).
- **Auditing & Observability**:
  - `dry_run` mode: Observe evaluation metrics and emit headers without terminating requests.
  - `RateLimitResult` injection into `HttpContext::extensions` for downstream consumption.

---

## Installation

Add `druto-limiter` to your `Cargo.toml`:

```toml
[dependencies]
druto-limiter = "0.1.0"

# Optional distributed Redis store support
# druto-limiter = { version = "0.1.0", features = ["redis"] }
```

---

## Quick Start

```rust
use std::time::Duration;

use druto_limiter::{HeaderMode, LimiterConfig, LimiterMiddleware, MemoryLimiterStore};
use druto_router::{HttpContext, IntoResponse, Router};

async fn api_handler(ctx: &mut HttpContext) -> impl IntoResponse {
    "Hello from rate-limited API!"
}

fn app_router() -> Router {
    let store = MemoryLimiterStore::new();
    let config = LimiterConfig::builder()
        .allow_requests(100)
        .every(Duration::from_secs(60))
        .header_mode(HeaderMode::Both)
        .build();

    let mut router = Router::new();
    router.use_middleware(LimiterMiddleware::new(store, config));
    router.get("/api/data", api_handler);
    router
}
```

---

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.
