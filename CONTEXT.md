# `limiter` Crate Context & Architecture

## Vision & Scope

The `limiter` crate provides traffic throttling, rate limiting, burst management, and standardized HTTP rate limit
headers for the Druto web framework. It operates as both a standalone, HTTP-agnostic rate limiting engine and an
asynchronous onion middleware integrating with `dhap` and `router`. It protects endpoints against brute-force
attacks, credential stuffing, scraping, and denial-of-service conditions while providing fine-grained quota management
for multi-tier APIs.

## Reference Blueprint & Specification Mapping

The crate synthesizes proven rate-limiting paradigms from industry standards and framework designs:

- **Multi-Driver & Hybrid Storage Architectures**:
    - Multi-driver storage contracts (`Memory`, `Redis`, `Tiered`).
    - Fluent limiter builder with explicit time windows (`allow_requests`, `every`, `using_key`).
    - In-memory short-circuit caching to shield distributed stores from volumetric floods.
    - Multi-limiter orchestration (`MultiLimiter`) for multi-dimensional and multi-store rate limiting.
    - Dry-run / audit mode (`dry_run`) allowing observation and header calculation without rejecting requests.
    - Temporary penalization windows (`block_for`) upon quota exhaustion.
- **Declarative Registry & Multi-Tier Quota Architectures**:
    - Declarative named limiter registries (`LimiterRegistry`).
    - Granular throttling tiers (e.g. VIP vs authenticated vs anonymous visitors).
    - Multi-tier limit composition (e.g. 10 requests per second AND 500 requests per hour).
    - Dynamic key derivation (e.g. request user ID fallback to client IP).
- **IETF RFC & Internet Drafts**:
    - **RFC 6585**: Section 4 (`429 Too Many Requests`).
    - **RFC 7231 / RFC 9110**: Section 10.2.7 (`Retry-After` header in delta-seconds).
    - **IETF Draft `draft-ietf-httpapi-ratelimit-headers`**: Standardized RateLimit headers:
        - `RateLimit-Limit`: Maximum quota allocation for the window.
        - `RateLimit-Remaining`: Number of available units left in current window.
        - `RateLimit-Reset`: Number of seconds remaining until the quota resets.
        - `RateLimit-Policy`: Quota policy description (e.g. `100;w=60`).
    - **Legacy De Facto Headers**:
        - `X-RateLimit-Limit`, `X-RateLimit-Remaining`, `X-RateLimit-Reset`, `Retry-After`.

## Key Capabilities & Core Responsibilities

1. **Two-Tier Architecture**:
    - **Core Engine (HTTP-Agnostic)**: Standalone primitives including `LimiterStore`, algorithms, and evaluated
      `RateLimitResult`. Usable outside HTTP servers for background jobs, webhook throttling, or external API client
      pacing.
    - **HTTP Integration Layer**: `LimiterMiddleware` implementing
      `dhap::Middleware<HttpContext, Response, RouterError>`.

2. **Core Algorithms**:
    - **Sliding Window Counter (Default)**: Approximated rolling window combining current and prior window counts
      weighted
      by time elapsed ($Count = \text{current} + \text{previous} \times (1 - \frac{t}{\text{window}})$). Provides $O(1)$
      memory,
      atomic updates, and smooth boundary transitions without sorted-set timestamp logs.
    - **Fixed Window**: Quantizes time into discrete intervals. Zero-memory overhead and highest throughput for basic
      quotas.
    - **Token Bucket**: Allows capacity bursts up to $B$ tokens while continuously refilling tokens at rate $R$/sec.

3. **Pluggable Storage Engine (`LimiterStore`)**:
    - Abstract, object-safe, thread-safe asynchronous contract:
      `trait LimiterStore: Send + Sync + 'static`.
    - Key operations:
      -
      `consume(&self, key: &str, amount: u64, limit: u64, window: Duration) -> BoxFuture<'_, Result<RateLimitResult, LimiterError>>`
        - `peek(&self, key: &str, limit: u64, window: Duration) -> BoxFuture<'_, Result<RateLimitResult, LimiterError>>`
        - `reset(&self, key: &str) -> BoxFuture<'_, Result<(), LimiterError>>`
        - `block(&self, key: &str, duration: Duration) -> BoxFuture<'_, Result<(), LimiterError>>`
    - Concrete drivers:
        - **`MemoryLimiterStore`**: Zero-external-dependency, thread-safe in-memory store (`RwLock<HashMap>` or
          concurrent map)
          with bounded capacity and automatic lazy/periodic TTL eviction.
        - **`RedisLimiterStore`**: Distributed rate limiting backed by `crates/redis` via atomic Lua scripts (single
          network round-trip), gated behind the optional `features = ["redis"]`.

4. **Hybrid Tiered Storage (`TieredLimiterStore`) & Multi-Store Combinators**:
    - **`TieredLimiterStore` (L1 Memory + L2 Redis)**:
        - **L1 In-Memory Fast Cache**: Checks memory first. When a key is exhausted or blocked, drops subsequent
          requests
          locally in sub-microseconds without querying Redis, protecting Redis clusters from volumetric DDoS floods.
        - **L2 Distributed Redis Store**: Synchronizes shared quotas across horizontal cluster nodes via atomic Lua
          scripts.
        - **Automated Fallback Insurance**: If Redis drops connection, times out, or errors, automatically degrades to
          in-memory
          rate limiting, keeping the application protected instead of failing open unthrottled.
    - **`MultiLimiter`**: Orchestrates operations across multiple stores or multi-dimensional limits simultaneously
      (e.g., node-local memory burst limit of 20 req/s + global Redis user quota of 1,000 req/day).

5. **Multi-Tier Limit Chaining**:
    - Support combining multiple tiered limits in a single rule (e.g., burst limit of 10 req/sec AND quota limit of
      1,000 req/hour).
    - Atomically evaluated in a single pass; the most constrained tier dictates quota exhaustion and emitted response
      headers.

6. **Fault Tolerance & Resilience (Fail-Open Policy)**:
    - Defaulting to **fail-open** (`fail_open = true`): If the distributed storage driver (e.g. Redis) is down, times
      out, or
      errors, the request is allowed through with a logged warning, preventing cache outages from cascading into full
      site downtime.
    - Opt-in **fail-closed** (`fail_open = false`): Explicitly configured for security-critical routes (e.g. `/login`,
      `/auth`) to
      strictly block requests if the limiter backend fails.

7. **Penalty Duration & In-Memory Flood Shielding**:
    - Optional `block_for: Option<Duration>` locks out abusive clients for an extended duration upon quota exhaustion.
    - Local in-memory short-circuit cache drops subsequent requests from blocked keys immediately, preventing DDoS
      floods from
      exhausting Redis connection pools or CPU.

8. **Dynamic Quota Derivation & Key Resolution**:
    - Policy closures `Fn(&HttpContext) -> QuotaPolicy` allow inspecting the request context to:
        - Select fingerprint keys: Client IP (`ctx.request.ip()`), authenticated User ID, API keys, or compound keys.
        - Assign tiered quotas: E.g., 60 req/min for anonymous visitors vs 600 req/min for authenticated accounts.
        - Bypass limits entirely: Returning `QuotaPolicy::Unlimited` for internal health checks or whitelisted IPs.

9. **Dual-Mode HTTP Headers Compliance**:
    - Configurable `HeaderMode`:
        - `HeaderMode::Both` (Default): Emits both standard IETF `RateLimit-*` and legacy `X-RateLimit-*` headers, plus
          `Retry-After`.
        - `HeaderMode::Standard`: Emits strictly IETF draft headers.
        - `HeaderMode::Legacy`: Emits traditional `X-RateLimit-*` headers.
        - `HeaderMode::None`: Suppresses rate limit headers.
    - Emitted on all evaluated responses (both allowed and rejected) to enable proactive client pacing.
    - `Retry-After` emitted in delta-seconds on `429 Too Many Requests`.

10. **Observability, Dry-Run & Custom Rejection**:
    - `dry_run = true`: Evaluates quotas and injects headers/extensions without rejecting requests with 429.
    - Extension Injection: Injects `RateLimitResult` into `HttpContext::extensions` for downstream handler and logger
      inspection.
    - Custom Rejection Formatter: Configurable `rejection_handler` defaulting to standard JSON
      (`{"message": "Too Many Requests", "retry_after": ...}`).

11. **Dual Registration Model**:
    - **Direct Middleware Builders**: Quick inline helpers such as `LimiterMiddleware::per_minute(60)` or
      `LimiterMiddleware::builder()`.
    - **Centralized Named Registry (`LimiterRegistry`)**: Central catalog where named throttle profiles are registered
      during boot and resolved across route groups.

## Boundaries & Non-Responsibilities

- **Not an Authentication Provider**: Does not validate credentials or decode tokens; reads authenticated identities
  from `HttpContext`.
- **Not a WAF / Firewall**: Does not inspect payload bodies or validate SQL/HTML injection; that belongs to
  `crates/shield` and `jachai`.
- **Not a Network Socket Throttler**: Operates at Layer 7 HTTP application level, not TCP SYN/connection flood
  prevention.

## Public API Surface

- `trait LimiterStore`: Storage driver contract (`Send + Sync + 'static`).
- `struct MemoryLimiterStore`: In-memory storage engine.
- `struct RedisLimiterStore`: Redis-backed distributed storage engine (optional feature `redis`).
- `struct TieredLimiterStore`: Hybrid L1 Memory + L2 Redis store with short-circuiting and fallback insurance.
- `struct MultiLimiter`: Multi-store and multi-dimensional rate limiter combinator.
- `struct RateLimitResult`: Snapshot of rate limit evaluation (`limit`, `remaining`, `reset_after`, `is_allowed`).
- `struct Limiter`: Rate limiting coordinator executing rules against a chosen store.
- `struct LimiterConfig` & `LimiterConfigBuilder`: Configuration options for rate limiting rules and policies.
- `struct LimiterRegistry`: Central catalog for defining and looking up named limiter policies.
- `struct LimiterMiddleware`: Middleware implementation for `dhap`.
- `enum HeaderMode`: Header emission strategy (`Both`, `Standard`, `Legacy`, `None`).
- `enum Algorithm`: Rate limiting algorithm (`SlidingWindow`, `FixedWindow`, `TokenBucket`).
- `enum QuotaPolicy`: Dynamic policy result (`Limit(TierConfig)`, `Unlimited`).
- `struct LimiterError`: Strongly typed error representation.

## Invariants & Design Contracts

1. **Atomicity**: Increments, sliding-window calculations, and quota checks must be atomic under high concurrency.
2. **Zero False Positives Under Normal Load**: Sliding window approximations must remain strictly consistent across
   window transitions.
3. **Resilience & Availability**: Default fail-open posture prevents cache storage downtime from breaking web
   application availability; `TieredLimiterStore` automatically degrades to memory during Redis outages.
4. **Header Cleanliness**: Headers are calculated from the most constrained tier and must
   satisfy $0 \le \text{remaining} \le \text{limit}$.
5. **Non-Destructive Middleware Execution**: Augment response headers without removing headers set by upstream or
   downstream layers.
