# `limiter` Agent Instructions

## Scope

This crate owns rate limiting, traffic throttling, burst management, rate limiting algorithms, pluggable storage
backends (including hybrid tiered memory+redis stores), and HTTP rate limit headers for Druto.

## Design Rules

1. **Precision & Thread Safety**:
    - Rate limit state updates must be atomic and race-condition free under high concurrency.
    - `MemoryLimiterStore` must avoid lock contention under heavy multi-threaded read/write workloads and manage
      capacity with bounded eviction.
    - `RedisLimiterStore` must execute multi-tier counter updates and time-sliding logic in a single atomic Lua script
      roundtrip.
    - `TieredLimiterStore` must coordinate L1 Memory and L2 Redis atomically, correctly propagating local in-memory
      blocks and degrading gracefully upon Redis failure.

2. **HTTP Standards & Header Compliance**:
    - Default to `HeaderMode::Both`, emitting standard IETF `RateLimit-*` and legacy `X-RateLimit-*` headers on all
      evaluated responses.
    - Emit `Retry-After` in delta-seconds on `429 Too Many Requests`.
    - Derive headers from the most constrained tier when multi-tier limits are active.
    - Never clobber headers already set by downstream route handlers or upstream middlewares.

3. **Resilience & Fault Tolerance**:
    - Default to fail-open (`fail_open = true`) on store driver errors or timeouts, logging a warning while letting the
      request proceed.
    - `TieredLimiterStore` provides fallback insurance, falling back to local memory rate limiting when Redis is
      unavailable.
    - Support explicit opt-in to fail-closed (`fail_open = false`) on security-sensitive routes.

4. **Performance & Memory Hygiene**:
    - In-memory store must automatically evict expired entries or bound maximum capacity to prevent unbounded memory
      growth.
    - Employ local in-memory short-circuit caching for blocked keys to shield remote Redis clusters during abusive
      traffic floods.
    - Avoid heap allocations in hot-path key evaluation when possible.

5. **Pipeline & Router Integration**:
    - Implement `dhap::Middleware<HttpContext, Response, RouterError>`.
    - Provide both direct route builders and `LimiterRegistry` named policies.
    - Support `MultiLimiter` combinations (e.g. node-level burst memory limit + global Redis quota).
    - Support `dry_run` mode and inject `RateLimitResult` into `HttpContext::extensions`.

## Testing Requirements

- **Algorithm Precision**: Test sliding window counter, fixed window, and token bucket calculations with deterministic
  time advancement.
- **Store Roundtrips**: Test store, consume, peek, reset, and block across `MemoryLimiterStore`, `RedisLimiterStore`
  (using `RedisFake` script simulation), and `TieredLimiterStore`.
- **Concurrent Execution**: Use multi-threaded or async task bursts to verify no double-spending or token leaks under
  race conditions.
- **Tiered Store & Flood Shielding**: Test that in `TieredLimiterStore`, blocked keys are rejected in L1 memory without
  querying Redis, and simulated Redis disconnection seamlessly triggers in-memory fallback.
- **Multi-Store / MultiLimiter**: Test combining local memory burst limits with global Redis quotas.
- **Header Emission**: Verify correct calculation and formatting of `RateLimit-*`, `X-RateLimit-*`, and `Retry-After`
  headers across both allowed and rejected responses.
- **Middleware Pipeline**: Test short-circuiting on quota exhaustion (`429 Too Many Requests`), non-blocking when
  allowed, extension injection, and dry-run execution.
