//! Comprehensive test suite for the `limiter` crate.

use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
    time::Duration,
};

use dhap::{Error as DhapError, Pipeline, from_terminal};
#[cfg(feature = "redis")]
use druto_limiter::RedisLimiterStore;
use druto_limiter::{
    Algorithm, BoundLimiter, BoxFuture, HeaderMode, KeyResolver, LimiterConfig, LimiterError,
    LimiterMiddleware, LimiterRegistry, LimiterStore, MemoryLimiterStore, MultiLimiter,
    QuotaPolicy, RateLimitResult, TierConfig, TieredLimiterStore, constants,
};
use proptest::prelude::*;
#[cfg(feature = "redis")]
use redis::fake::RedisFake;
use router::{
    HeaderMap, HeaderName, HttpContext, Method, Request, Response, ResponseBody, RouterError,
    StatusCode,
};

// --- Test Utilities (Zero-Dependency Async Executor) ---

fn noop_waker() -> Waker {
    fn noop_clone(data: *const ()) -> RawWaker {
        RawWaker::new(data, &NOOP_VTABLE)
    }
    fn noop_action(data: *const ()) {
        let _ = data;
    }
    static NOOP_VTABLE: RawWakerVTable =
        RawWakerVTable::new(noop_clone, noop_action, noop_action, noop_action);

    // SAFETY: RawWaker implementation performs no operations and maintains valid null pointer state.
    unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &NOOP_VTABLE)) }
}

fn block_on<F: Future>(future: F) -> F::Output {
    let mut pinned = Pin::from(Box::new(future));
    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    match pinned.as_mut().poll(&mut context) {
        Poll::Ready(output) => output,
        Poll::Pending => {
            panic!("test future unexpectedly yielded Pending without an external driver")
        }
    }
}

fn make_context(method: Method, path: &str, headers: HeaderMap, ip: Option<&str>) -> HttpContext {
    let mut builder = Request::builder()
        .method(method)
        .path(path)
        .trust_proxy(true);
    if let Some(client_ip) = ip {
        builder = builder.header(HeaderName::X_REAL_IP, client_ip);
    }
    for (name, values) in headers.iter() {
        for value in values {
            builder = builder.header(name.clone(), value.clone());
        }
    }
    HttpContext::new(builder.build())
}

fn execute_pipeline(
    middleware: LimiterMiddleware,
    ctx: &mut HttpContext,
    called: Arc<AtomicBool>,
) -> Result<Response, DhapError<RouterError>> {
    let pipeline = Pipeline::new().pipe(middleware);
    let terminal = from_terminal(move |ctx: &mut HttpContext| {
        called.store(true, Ordering::SeqCst);
        ctx.response.set_status(StatusCode::OK);
        ctx.response.text("downstream output");
        let resp = ctx.response.clone();
        Box::pin(async move { Ok(resp) })
    });
    block_on(pipeline.run(ctx, terminal))
}

// ============================================================================
// 1. MemoryLimiterStore Algorithm Tests (Sliding Window, Fixed Window, Token Bucket)
// ============================================================================

#[test]
fn test_sliding_window_counter_precision_and_transitions() {
    let current_time = Arc::new(AtomicU64::new(600_000));
    let time_ref = Arc::clone(&current_time);
    let store = MemoryLimiterStore::with_clock(100, move || time_ref.load(Ordering::SeqCst));

    let key = "user_sliding_1";
    let window = Duration::from_secs(60);
    let limit = 5;

    // 1. Consume 3 units at t=0s into the window
    for expected_remaining in (2..=4).rev() {
        let res = block_on(store.consume(key, 1, limit, window, Algorithm::SlidingWindow)).unwrap();
        assert!(res.is_allowed());
        assert_eq!(res.remaining, expected_remaining);
    }

    // Peek status
    let peek = block_on(store.peek(key, limit, window, Algorithm::SlidingWindow)).unwrap();
    assert!(peek.is_allowed());
    assert_eq!(peek.remaining, 2);

    // 2. Consume 2 more to exhaust quota
    let res4 = block_on(store.consume(key, 1, limit, window, Algorithm::SlidingWindow)).unwrap();
    assert!(res4.is_allowed());
    assert_eq!(res4.remaining, 1);

    let res5 = block_on(store.consume(key, 1, limit, window, Algorithm::SlidingWindow)).unwrap();
    assert!(res5.is_allowed());
    assert_eq!(res5.remaining, 0);

    // 3. 6th request is rejected
    let res6 = block_on(store.consume(key, 1, limit, window, Algorithm::SlidingWindow)).unwrap();
    assert!(res6.is_exceeded());
    assert_eq!(res6.remaining, 0);
    assert!(res6.retry_after.is_some());

    // 4. Advance time by 90 seconds (window 11, 30 seconds into window 11, 50% elapsed)
    // Previous window had 5 requests; in new window at 50% elapsed, weighted past count is 5 * 0.5 = 2.5
    current_time.store(690_000, Ordering::SeqCst);
    let peek_next = block_on(store.peek(key, limit, window, Algorithm::SlidingWindow)).unwrap();
    assert!(peek_next.is_allowed());
    assert_eq!(peek_next.remaining, 3); // 5 - floor(2.5) = 3 remaining

    // 5. Advance time by 3 windows (full expiration)
    current_time.store(1_200_000, Ordering::SeqCst);
    let peek_expired = block_on(store.peek(key, limit, window, Algorithm::SlidingWindow)).unwrap();
    assert!(peek_expired.is_allowed());
    assert_eq!(peek_expired.remaining, 5);
}

#[test]
fn test_fixed_window_modulo_resets() {
    let current_time = Arc::new(AtomicU64::new(60_000));
    let time_ref = Arc::clone(&current_time);
    let store = MemoryLimiterStore::with_clock(100, move || time_ref.load(Ordering::SeqCst));

    let key = "user_fixed_1";
    let window = Duration::from_secs(60);
    let limit = 2;

    // Consume 2 requests
    let res1 = block_on(store.consume(key, 1, limit, window, Algorithm::FixedWindow)).unwrap();
    assert!(res1.is_allowed());
    assert_eq!(res1.remaining, 1);

    let res2 = block_on(store.consume(key, 1, limit, window, Algorithm::FixedWindow)).unwrap();
    assert!(res2.is_allowed());
    assert_eq!(res2.remaining, 0);

    // Exceeded in same window
    let res3 = block_on(store.consume(key, 1, limit, window, Algorithm::FixedWindow)).unwrap();
    assert!(res3.is_exceeded());

    // Advance into next discrete 60s window
    current_time.store(120_000, Ordering::SeqCst);
    let res4 = block_on(store.consume(key, 1, limit, window, Algorithm::FixedWindow)).unwrap();
    assert!(res4.is_allowed());
    assert_eq!(res4.remaining, 1);
}

#[test]
fn test_token_bucket_burst_and_refill() {
    let current_time = Arc::new(AtomicU64::new(10_000));
    let time_ref = Arc::clone(&current_time);
    let store = MemoryLimiterStore::with_clock(100, move || time_ref.load(Ordering::SeqCst));

    let key = "user_token_bucket";
    let window = Duration::from_secs(10);
    let limit = 10; // Refill rate = 1 token/sec

    // Consume entire bucket burst (10 tokens)
    let res1 = block_on(store.consume(key, 10, limit, window, Algorithm::TokenBucket)).unwrap();
    assert!(res1.is_allowed());
    assert_eq!(res1.remaining, 0);

    // Request immediately after should fail
    let res2 = block_on(store.consume(key, 1, limit, window, Algorithm::TokenBucket)).unwrap();
    assert!(res2.is_exceeded());

    // Advance by 3 seconds -> 3 tokens refilled
    current_time.fetch_add(3_000, Ordering::SeqCst);
    let peek = block_on(store.peek(key, limit, window, Algorithm::TokenBucket)).unwrap();
    assert_eq!(peek.remaining, 3);

    // Consume 2 tokens
    let res3 = block_on(store.consume(key, 2, limit, window, Algorithm::TokenBucket)).unwrap();
    assert!(res3.is_allowed());
    assert_eq!(res3.remaining, 1);
}

#[test]
fn test_memory_store_reset_and_blocking() {
    let current_time = Arc::new(AtomicU64::new(1_000_000));
    let time_ref = Arc::clone(&current_time);
    let store = MemoryLimiterStore::with_clock(100, move || time_ref.load(Ordering::SeqCst));

    let key = "block_target";
    let window = Duration::from_secs(60);

    // Block key explicitly for 30 seconds
    block_on(store.block(key, Duration::from_secs(30))).unwrap();

    let peek_blocked = block_on(store.peek(key, 10, window, Algorithm::SlidingWindow)).unwrap();
    assert!(peek_blocked.is_exceeded());
    assert_eq!(peek_blocked.remaining, 0);
    assert_eq!(peek_blocked.retry_after_seconds(), 30);

    // Attempting to consume while blocked must be rejected
    let consume_blocked =
        block_on(store.consume(key, 1, 10, window, Algorithm::SlidingWindow)).unwrap();
    assert!(consume_blocked.is_exceeded());

    // Advance time past the block penalty (35 seconds)
    current_time.fetch_add(35_000, Ordering::SeqCst);
    let consume_unblocked =
        block_on(store.consume(key, 1, 10, window, Algorithm::SlidingWindow)).unwrap();
    assert!(consume_unblocked.is_allowed());

    // Reset clears the key
    block_on(store.reset(key)).unwrap();
    let peek_fresh = block_on(store.peek(key, 10, window, Algorithm::SlidingWindow)).unwrap();
    assert_eq!(peek_fresh.remaining, 10);
}

#[test]
fn test_memory_store_capacity_eviction() {
    let current_time = Arc::new(AtomicU64::new(1_000_000));
    let time_ref = Arc::clone(&current_time);
    let store = MemoryLimiterStore::with_clock(3, move || time_ref.load(Ordering::SeqCst));

    let window = Duration::from_secs(60);

    // Insert 3 keys (reaches capacity limit of 3)
    block_on(store.consume("k1", 1, 10, window, Algorithm::FixedWindow)).unwrap();
    current_time.fetch_add(10, Ordering::SeqCst);
    block_on(store.consume("k2", 1, 10, window, Algorithm::FixedWindow)).unwrap();
    current_time.fetch_add(10, Ordering::SeqCst);
    block_on(store.consume("k3", 1, 10, window, Algorithm::FixedWindow)).unwrap();

    // Inserting 4th key should trigger eviction without panic
    current_time.fetch_add(10, Ordering::SeqCst);
    let res4 = block_on(store.consume("k4", 1, 10, window, Algorithm::FixedWindow)).unwrap();
    assert!(res4.is_allowed());
}

#[test]
fn test_memory_store_concurrent_stress() {
    let store = Arc::new(MemoryLimiterStore::new());
    let window = Duration::from_secs(60);
    let limit = 50;

    let mut handles = Vec::new();
    for thread_idx in 0..10 {
        let store_clone = Arc::clone(&store);
        handles.push(std::thread::spawn(move || {
            let key = format!("concurrent_user_{}", thread_idx % 2);
            for _ in 0..20 {
                let _ =
                    block_on(store_clone.consume(&key, 1, limit, window, Algorithm::SlidingWindow));
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let peek0 =
        block_on(store.peek("concurrent_user_0", limit, window, Algorithm::SlidingWindow)).unwrap();
    assert_eq!(peek0.limit, limit);
}

// ============================================================================
// 2. RedisLimiterStore Tests (Feature Gated)
// ============================================================================

#[cfg(feature = "redis")]
#[test]
fn test_redis_store_sliding_fixed_and_token_bucket() {
    let fake_redis = Arc::new(RedisFake::new());
    let store = RedisLimiterStore::new(fake_redis);

    let window = Duration::from_secs(60);

    // 1. Sliding Window
    let res1 =
        block_on(store.consume("redis_sliding", 2, 5, window, Algorithm::SlidingWindow)).unwrap();
    assert!(res1.is_allowed());
    assert_eq!(res1.remaining, 3);

    let peek_res =
        block_on(store.peek("redis_sliding", 5, window, Algorithm::SlidingWindow)).unwrap();
    assert!(peek_res.is_allowed());
    assert_eq!(peek_res.remaining, 3);

    // 2. Fixed Window
    let res_fw =
        block_on(store.consume("redis_fixed", 1, 2, window, Algorithm::FixedWindow)).unwrap();
    assert!(res_fw.is_allowed());
    assert_eq!(res_fw.remaining, 1);

    // 3. Token Bucket
    let res_tb =
        block_on(store.consume("redis_tb", 5, 10, window, Algorithm::TokenBucket)).unwrap();
    assert!(res_tb.is_allowed());
    assert_eq!(res_tb.remaining, 5);

    // 4. Blocking and Reset
    block_on(store.block("redis_sliding", Duration::from_secs(60))).unwrap();
    let res_blocked =
        block_on(store.consume("redis_sliding", 1, 5, window, Algorithm::SlidingWindow)).unwrap();
    assert!(res_blocked.is_exceeded());

    block_on(store.reset("redis_sliding")).unwrap();
}

#[cfg(feature = "redis")]
#[test]
fn test_redis_store_custom_prefix() {
    let fake_redis = Arc::new(RedisFake::new());
    let store = RedisLimiterStore::with_prefix(fake_redis, "custom:limit:");
    let res = block_on(store.consume(
        "prefix_test",
        1,
        5,
        Duration::from_secs(60),
        Algorithm::FixedWindow,
    ))
    .unwrap();
    assert!(res.is_allowed());
}

// ============================================================================
// 3. TieredLimiterStore (L1 Memory + L2 Distributed)
// ============================================================================

#[derive(Clone)]
struct MockFailingStore {
    fail_on_consume: Arc<AtomicBool>,
}

impl LimiterStore for MockFailingStore {
    fn consume<'a>(
        &'a self,
        key: &'a str,
        amount: u64,
        limit: u64,
        window: Duration,
        algorithm: Algorithm,
    ) -> BoxFuture<'a, Result<RateLimitResult, LimiterError>> {
        let _ = (key, amount, limit, window, algorithm);
        Box::pin(async move {
            if self.fail_on_consume.load(Ordering::SeqCst) {
                Err(LimiterError::Store("Simulated cluster failure".to_string()))
            } else {
                Ok(RateLimitResult::new(
                    10,
                    9,
                    Duration::from_secs(60),
                    true,
                    None,
                ))
            }
        })
    }

    fn peek<'a>(
        &'a self,
        key: &'a str,
        limit: u64,
        window: Duration,
        algorithm: Algorithm,
    ) -> BoxFuture<'a, Result<RateLimitResult, LimiterError>> {
        let _ = (key, limit, window, algorithm);
        Box::pin(async move {
            if self.fail_on_consume.load(Ordering::SeqCst) {
                Err(LimiterError::Store("Simulated cluster failure".to_string()))
            } else {
                Ok(RateLimitResult::new(
                    10,
                    9,
                    Duration::from_secs(60),
                    true,
                    None,
                ))
            }
        })
    }

    fn reset<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), LimiterError>> {
        let _ = key;
        Box::pin(async move { Ok(()) })
    }

    fn block<'a>(
        &'a self,
        key: &'a str,
        duration: Duration,
    ) -> BoxFuture<'a, Result<(), LimiterError>> {
        let _ = (key, duration);
        Box::pin(async move { Ok(()) })
    }
}

#[test]
fn test_tiered_limiter_store_short_circuit_and_fallback() {
    let memory = MemoryLimiterStore::new();
    let fail_flag = Arc::new(AtomicBool::new(false));
    let mock_distributed = Arc::new(MockFailingStore {
        fail_on_consume: Arc::clone(&fail_flag),
    });

    let tiered = TieredLimiterStore::new(memory, mock_distributed)
        .with_fallback_insurance(true)
        .with_short_circuit(true);

    let key = "tiered_test_client";
    let window = Duration::from_secs(60);

    // 1. Normal operation (L2 succeeds)
    let res1 = block_on(tiered.consume(key, 1, 10, window, Algorithm::FixedWindow)).unwrap();
    assert!(res1.is_allowed());

    // 2. L1 Short-Circuit: Block key directly in L1 memory
    block_on(tiered.memory().block(key, Duration::from_secs(30))).unwrap();
    let res_short = block_on(tiered.consume(key, 1, 10, window, Algorithm::FixedWindow)).unwrap();
    assert!(res_short.is_exceeded());

    // Clear L1 block
    block_on(tiered.reset(key)).unwrap();

    // 3. Fallback Insurance: L2 Distributed fails
    fail_flag.store(true, Ordering::SeqCst);
    let res_fallback =
        block_on(tiered.consume(key, 1, 10, window, Algorithm::FixedWindow)).unwrap();
    assert!(res_fallback.is_allowed()); // Degraded to L1 memory cleanly!

    // Peek with fallback
    let peek_res = block_on(tiered.peek(key, 10, window, Algorithm::FixedWindow)).unwrap();
    assert!(peek_res.is_allowed());

    // 4. Fallback Insurance Disabled -> Returns L2 error
    let tiered_no_fallback = tiered.clone().with_fallback_insurance(false);
    let res_err = block_on(tiered_no_fallback.consume(key, 1, 10, window, Algorithm::FixedWindow));
    assert!(res_err.is_err());
}

// ============================================================================
// 4. MultiLimiter Store Combinator
// ============================================================================

#[test]
fn test_multi_limiter_combinator() {
    let store1 = Arc::new(MemoryLimiterStore::new());
    let store2 = Arc::new(MemoryLimiterStore::new());

    let bound1 = BoundLimiter::new(
        "user_dim_1",
        5,
        Duration::from_secs(10),
        Algorithm::FixedWindow,
        store1,
    );
    let bound2 = BoundLimiter::new(
        "user_dim_2",
        2,
        Duration::from_secs(60),
        Algorithm::SlidingWindow,
        store2,
    );

    let multi = MultiLimiter::new()
        .push_limiter(bound1)
        .push_limiter(bound2);

    // Consume 1: both pass
    let res1 = block_on(multi.consume()).unwrap();
    assert!(res1.is_allowed());
    assert_eq!(res1.remaining, 1); // 2nd limiter had limit 2, remaining 1 (most restrictive)

    // Consume 2: exhausts 2nd limiter
    let res2 = block_on(multi.consume()).unwrap();
    assert!(res2.is_allowed());
    assert_eq!(res2.remaining, 0);

    // Consume 3: rejected by 2nd limiter
    let res3 = block_on(multi.consume()).unwrap();
    assert!(res3.is_exceeded());

    // Reset clears both
    block_on(multi.reset()).unwrap();
    let peek = block_on(multi.peek()).unwrap();
    assert_eq!(peek.remaining, 2);

    // Block both
    block_on(multi.block(Duration::from_secs(20))).unwrap();
    let res_blocked = block_on(multi.consume()).unwrap();
    assert!(res_blocked.is_exceeded());
}

// ============================================================================
// 5. QuotaPolicy & KeyResolver
// ============================================================================

#[test]
fn test_quota_policy_builders_and_chaining() {
    let p_sec = QuotaPolicy::per_second(10);
    assert_eq!(p_sec.tiers().len(), 1);
    assert_eq!(p_sec.tiers()[0].window, Duration::from_secs(1));

    let p_min = QuotaPolicy::per_minute(60);
    assert_eq!(p_min.tiers()[0].window, Duration::from_secs(60));

    let p_hour = QuotaPolicy::per_hour(1000);
    assert_eq!(p_hour.tiers()[0].window, Duration::from_secs(3600));

    let p_day = QuotaPolicy::per_day(50000);
    assert_eq!(p_day.tiers()[0].window, Duration::from_secs(86400));

    let p_unlimited = QuotaPolicy::unlimited();
    assert!(p_unlimited.is_unlimited());
    assert!(p_unlimited.tiers().is_empty());

    // Chaining multi-tier
    let multi_tier = QuotaPolicy::per_second(5)
        .and(100, Duration::from_secs(60))
        .algorithm(Algorithm::TokenBucket)
        .block_for(Duration::from_secs(120));

    assert_eq!(multi_tier.tiers().len(), 2);
    assert_eq!(multi_tier.tiers()[1].algorithm, Algorithm::TokenBucket);
    assert_eq!(
        multi_tier.tiers()[1].block_for,
        Some(Duration::from_secs(120))
    );
}

#[test]
fn test_key_resolvers() {
    let ctx = make_context(
        Method::Get,
        "/api/data",
        HeaderMap::new(),
        Some("192.168.1.50"),
    );

    // 1. IP resolver
    assert_eq!(KeyResolver::Ip.resolve(&ctx), "192.168.1.50");

    // 2. Header resolver
    let mut headers = HeaderMap::new();
    headers.insert(HeaderName::new("x-api-key"), "key_secret_123");
    let ctx_header = make_context(Method::Get, "/api/data", headers, Some("192.168.1.50"));
    assert_eq!(
        KeyResolver::Header(HeaderName::new("x-api-key")).resolve(&ctx_header),
        "key_secret_123"
    );

    // 3. RouteAndIp resolver
    assert_eq!(
        KeyResolver::RouteAndIp.resolve(&ctx),
        "/api/data:192.168.1.50"
    );

    // 4. Custom resolver
    let custom = KeyResolver::Custom(Arc::new(|c| Some(format!("custom_{}", c.request.path()))));
    assert_eq!(custom.resolve(&ctx), "custom_/api/data");
}

// ============================================================================
// 6. LimiterConfig Path Matching & Registry
// ============================================================================

#[test]
fn test_limiter_config_path_matching() {
    let config = LimiterConfig::builder()
        .paths(["/api/*", "/auth/login", "/webhook*"])
        .build();

    assert!(config.matches_path("/api/users"));
    assert!(config.matches_path("/api/products/123"));
    assert!(config.matches_path("/auth/login"));
    assert!(config.matches_path("/webhook/stripe"));

    assert!(!config.matches_path("/static/app.js"));
    assert!(!config.matches_path("/auth/signup"));
}

#[test]
fn test_limiter_registry() {
    let registry = LimiterRegistry::new();
    assert!(registry.is_empty());

    let mw1 = LimiterMiddleware::per_minute(60);
    let mw2 = LimiterMiddleware::per_hour(1000);

    registry.register("api", mw1);
    registry.register("auth", mw2);

    assert_eq!(registry.len(), 2);
    assert!(registry.get("api").is_some());
    assert!(registry.get("auth").is_some());
    assert!(registry.get("unknown").is_none());

    registry.remove("api");
    assert_eq!(registry.len(), 1);
    assert!(registry.get("api").is_none());
}

// ============================================================================
// 7. LimiterMiddleware & Pipeline Integration Tests
// ============================================================================

#[test]
fn test_limiter_middleware_allows_normal_traffic_and_sets_headers() {
    let config = LimiterConfig::builder()
        .policy(QuotaPolicy::per_minute(10))
        .header_mode(HeaderMode::Both)
        .build();

    let middleware = LimiterMiddleware::memory(config);
    let called = Arc::new(AtomicBool::new(false));
    let mut ctx = make_context(Method::Get, "/api/data", HeaderMap::new(), Some("10.0.0.1"));

    let response = execute_pipeline(middleware, &mut ctx, Arc::clone(&called)).unwrap();

    assert!(called.load(Ordering::SeqCst));
    assert_eq!(response.status(), StatusCode::OK);

    // Verify Both header modes (Standard + Legacy)
    assert_eq!(
        response.headers().get(constants::RATE_LIMIT_LIMIT),
        Some("10")
    );
    assert_eq!(
        response.headers().get(constants::RATE_LIMIT_REMAINING),
        Some("9")
    );
    assert!(
        response
            .headers()
            .get(constants::RATE_LIMIT_RESET)
            .is_some()
    );

    assert_eq!(
        response.headers().get(constants::X_RATE_LIMIT_LIMIT),
        Some("10")
    );
    assert_eq!(
        response.headers().get(constants::X_RATE_LIMIT_REMAINING),
        Some("9")
    );
    assert!(
        response
            .headers()
            .get(constants::X_RATE_LIMIT_RESET)
            .is_some()
    );

    // Verify extension injection
    let injected: Option<&RateLimitResult> = ctx.extensions.get();
    assert!(injected.is_some());
    assert_eq!(injected.unwrap().remaining, 9);
}

#[test]
fn test_limiter_middleware_short_circuits_with_429_on_exhaustion() {
    let config = LimiterConfig::builder()
        .policy(QuotaPolicy::per_minute(1))
        .header_mode(HeaderMode::Both)
        .build();

    let middleware = LimiterMiddleware::memory(config);

    // 1st request -> Allowed
    let called1 = Arc::new(AtomicBool::new(false));
    let mut ctx1 = make_context(
        Method::Get,
        "/api/resource",
        HeaderMap::new(),
        Some("10.0.0.2"),
    );
    let resp1 = execute_pipeline(middleware.clone(), &mut ctx1, called1).unwrap();
    assert_eq!(resp1.status(), StatusCode::OK);

    // 2nd request -> Exhausted! Short-circuit with 429 Too Many Requests
    let called2 = Arc::new(AtomicBool::new(false));
    let mut ctx2 = make_context(
        Method::Get,
        "/api/resource",
        HeaderMap::new(),
        Some("10.0.0.2"),
    );
    let resp2 = execute_pipeline(middleware, &mut ctx2, Arc::clone(&called2)).unwrap();

    assert!(!called2.load(Ordering::SeqCst)); // Terminal handler was NOT called!
    assert_eq!(resp2.status(), StatusCode::TOO_MANY_REQUESTS);

    // Headers verification
    assert_eq!(
        resp2.headers().get(constants::RATE_LIMIT_REMAINING),
        Some("0")
    );
    assert_eq!(
        resp2.headers().get(constants::X_RATE_LIMIT_REMAINING),
        Some("0")
    );
    assert!(resp2.headers().get(constants::RETRY_AFTER).is_some());

    // Body verification
    let body_bytes = resp2.body().as_bytes();
    let body_json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
    assert_eq!(body_json["message"], "Too Many Requests");
    assert!(body_json["retry_after"].is_number());
}

#[test]
fn test_limiter_middleware_dry_run_mode() {
    let config = LimiterConfig::builder()
        .policy(QuotaPolicy::per_minute(1))
        .dry_run(true)
        .build();

    let middleware = LimiterMiddleware::memory(config);

    let mut ctx1 = make_context(Method::Get, "/api/test", HeaderMap::new(), Some("10.0.0.3"));
    let _ = execute_pipeline(
        middleware.clone(),
        &mut ctx1,
        Arc::new(AtomicBool::new(false)),
    )
    .unwrap();

    // 2nd request in dry-run mode: quota exceeded, but allowed through with 200 OK!
    let called = Arc::new(AtomicBool::new(false));
    let mut ctx2 = make_context(Method::Get, "/api/test", HeaderMap::new(), Some("10.0.0.3"));
    let resp2 = execute_pipeline(middleware, &mut ctx2, Arc::clone(&called)).unwrap();

    assert!(called.load(Ordering::SeqCst)); // Downstream handler executed!
    assert_eq!(resp2.status(), StatusCode::OK);
    assert_eq!(
        resp2.headers().get(constants::RATE_LIMIT_REMAINING),
        Some("0")
    );
}

#[test]
fn test_limiter_middleware_custom_rejection_handler() {
    let config = LimiterConfig::builder()
        .policy(QuotaPolicy::per_minute(1))
        .rejection_handler(|ctx, result| {
            let _ = (ctx, result);
            let mut resp = Response::new(StatusCode::TOO_MANY_REQUESTS);
            resp.set_header(HeaderName::CONTENT_TYPE, "text/plain");
            resp.set_body(ResponseBody::Bytes(bytes::Bytes::from(
                "Custom rate limit exceeded message",
            )));
            resp
        })
        .build();

    let middleware = LimiterMiddleware::memory(config);

    let mut ctx1 = make_context(Method::Get, "/custom", HeaderMap::new(), Some("10.0.0.4"));
    let _ = execute_pipeline(
        middleware.clone(),
        &mut ctx1,
        Arc::new(AtomicBool::new(false)),
    )
    .unwrap();

    let mut ctx2 = make_context(Method::Get, "/custom", HeaderMap::new(), Some("10.0.0.4"));
    let resp2 = execute_pipeline(middleware, &mut ctx2, Arc::new(AtomicBool::new(false))).unwrap();

    assert_eq!(resp2.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(
        &resp2.body().as_bytes()[..],
        b"Custom rate limit exceeded message"
    );
}

#[test]
fn test_limiter_middleware_fail_open_and_fail_closed() {
    let fail_flag = Arc::new(AtomicBool::new(true));
    let failing_store = Arc::new(MockFailingStore {
        fail_on_consume: Arc::clone(&fail_flag),
    });

    // 1. Fail-open (Default): Proceeds downstream
    let config_open = LimiterConfig::builder().fail_open(true).build();
    let mw_open = LimiterMiddleware::new(failing_store.clone(), config_open);
    let called_open = Arc::new(AtomicBool::new(false));
    let mut ctx_open = make_context(Method::Get, "/api", HeaderMap::new(), Some("10.0.0.5"));

    let resp_open = execute_pipeline(mw_open, &mut ctx_open, Arc::clone(&called_open)).unwrap();
    assert!(called_open.load(Ordering::SeqCst));
    assert_eq!(resp_open.status(), StatusCode::OK);

    // 2. Fail-closed: Halts pipeline with error
    let config_closed = LimiterConfig::builder().fail_open(false).build();
    let mw_closed = LimiterMiddleware::new(failing_store, config_closed);
    let called_closed = Arc::new(AtomicBool::new(false));
    let mut ctx_closed = make_context(Method::Get, "/api", HeaderMap::new(), Some("10.0.0.5"));

    let resp_closed = execute_pipeline(mw_closed, &mut ctx_closed, Arc::clone(&called_closed));
    assert!(resp_closed.is_err());
    assert!(!called_closed.load(Ordering::SeqCst));
}

#[test]
fn test_limiter_middleware_multi_tier_burst_and_quota() {
    // 2 requests per second burst AND 5 requests per minute quota
    let config = LimiterConfig::builder()
        .policy(QuotaPolicy::per_second(2).and(5, Duration::from_secs(60)))
        .build();

    let middleware = LimiterMiddleware::memory(config);
    let ip = Some("10.0.0.6");

    // 1st request -> OK
    let mut ctx1 = make_context(Method::Get, "/api", HeaderMap::new(), ip);
    let resp1 = execute_pipeline(
        middleware.clone(),
        &mut ctx1,
        Arc::new(AtomicBool::new(false)),
    )
    .unwrap();
    assert_eq!(resp1.status(), StatusCode::OK);

    // 2nd request -> OK
    let mut ctx2 = make_context(Method::Get, "/api", HeaderMap::new(), ip);
    let resp2 = execute_pipeline(
        middleware.clone(),
        &mut ctx2,
        Arc::new(AtomicBool::new(false)),
    )
    .unwrap();
    assert_eq!(resp2.status(), StatusCode::OK);

    // 3rd request in same second -> Burst limit exhausted (429)!
    let mut ctx3 = make_context(Method::Get, "/api", HeaderMap::new(), ip);
    let resp3 = execute_pipeline(middleware, &mut ctx3, Arc::new(AtomicBool::new(false))).unwrap();
    assert_eq!(resp3.status(), StatusCode::TOO_MANY_REQUESTS);
}

#[test]
fn test_limiter_middleware_disabled_and_path_bypass() {
    let config = LimiterConfig::builder()
        .enabled(false)
        .paths(["/api/*"])
        .policy(QuotaPolicy::per_minute(1))
        .build();

    let middleware = LimiterMiddleware::memory(config);

    // When disabled, quota is never exhausted
    for _ in 0..5 {
        let called = Arc::new(AtomicBool::new(false));
        let mut ctx = make_context(Method::Get, "/api/data", HeaderMap::new(), Some("10.0.0.7"));
        let resp = execute_pipeline(middleware.clone(), &mut ctx, Arc::clone(&called)).unwrap();
        assert!(called.load(Ordering::SeqCst));
        assert_eq!(resp.status(), StatusCode::OK);
    }
}

// ============================================================================
// 8. Header Modes, Error Conversions, Edge Cases & Proptests
// ============================================================================

#[test]
fn test_header_modes_standard_legacy_and_none() {
    // 1. HeaderMode::Standard
    let config_std = LimiterConfig::builder()
        .header_mode(HeaderMode::Standard)
        .policy(QuotaPolicy::per_minute(10))
        .build();
    let mw_std = LimiterMiddleware::memory(config_std);
    let mut ctx_std = make_context(Method::Get, "/std", HeaderMap::new(), Some("10.0.0.10"));
    let resp_std =
        execute_pipeline(mw_std, &mut ctx_std, Arc::new(AtomicBool::new(false))).unwrap();

    assert!(
        resp_std
            .headers()
            .get(constants::RATE_LIMIT_LIMIT)
            .is_some()
    );
    assert!(
        resp_std
            .headers()
            .get(constants::RATE_LIMIT_REMAINING)
            .is_some()
    );
    assert!(
        resp_std
            .headers()
            .get(constants::RATE_LIMIT_RESET)
            .is_some()
    );
    assert!(
        resp_std
            .headers()
            .get(constants::RATE_LIMIT_POLICY)
            .is_some()
    );
    assert!(
        resp_std
            .headers()
            .get(constants::X_RATE_LIMIT_LIMIT)
            .is_none()
    );

    // 2. HeaderMode::Legacy
    let config_leg = LimiterConfig::builder()
        .header_mode(HeaderMode::Legacy)
        .policy(QuotaPolicy::per_minute(10))
        .build();
    let mw_leg = LimiterMiddleware::memory(config_leg);
    let mut ctx_leg = make_context(Method::Get, "/leg", HeaderMap::new(), Some("10.0.0.11"));
    let resp_leg =
        execute_pipeline(mw_leg, &mut ctx_leg, Arc::new(AtomicBool::new(false))).unwrap();

    assert!(
        resp_leg
            .headers()
            .get(constants::X_RATE_LIMIT_LIMIT)
            .is_some()
    );
    assert!(
        resp_leg
            .headers()
            .get(constants::X_RATE_LIMIT_REMAINING)
            .is_some()
    );
    assert!(
        resp_leg
            .headers()
            .get(constants::X_RATE_LIMIT_RESET)
            .is_some()
    );
    assert!(
        resp_leg
            .headers()
            .get(constants::RATE_LIMIT_LIMIT)
            .is_none()
    );

    // 3. HeaderMode::None
    let config_none = LimiterConfig::builder()
        .header_mode(HeaderMode::None)
        .policy(QuotaPolicy::per_minute(10))
        .build();
    let mw_none = LimiterMiddleware::memory(config_none);
    let mut ctx_none = make_context(Method::Get, "/none", HeaderMap::new(), Some("10.0.0.12"));
    let resp_none =
        execute_pipeline(mw_none, &mut ctx_none, Arc::new(AtomicBool::new(false))).unwrap();

    assert!(
        resp_none
            .headers()
            .get(constants::RATE_LIMIT_LIMIT)
            .is_none()
    );
    assert!(
        resp_none
            .headers()
            .get(constants::X_RATE_LIMIT_LIMIT)
            .is_none()
    );
}

#[test]
fn test_algorithm_display_and_defaults() {
    assert_eq!(Algorithm::default(), Algorithm::SlidingWindow);
    assert_eq!(Algorithm::SlidingWindow.to_string(), "sliding_window");
    assert_eq!(Algorithm::FixedWindow.to_string(), "fixed_window");
    assert_eq!(Algorithm::TokenBucket.to_string(), "token_bucket");
}

#[test]
fn test_error_display_and_router_error_conversion() {
    let err_store = LimiterError::Store("disk failure".to_string());
    assert!(err_store.to_string().contains("disk failure"));
    let r_err1: RouterError = err_store.into();
    assert!(r_err1.to_string().contains("disk failure"));

    let err_unavail = LimiterError::StoreUnavailable("connection timeout".to_string());
    assert!(err_unavail.to_string().contains("connection timeout"));
    let r_err2: RouterError = err_unavail.into();
    assert!(r_err2.to_string().contains("connection timeout"));

    let err_cfg = LimiterError::Config("bad param".to_string());
    assert!(err_cfg.to_string().contains("bad param"));
    let r_err3: RouterError = err_cfg.into();
    assert!(r_err3.to_string().contains("bad param"));

    let res = RateLimitResult::new(
        10,
        0,
        Duration::from_secs(30),
        false,
        Some(Duration::from_secs(30)),
    );
    let err_exceeded = LimiterError::RateLimitExceeded(res);
    assert!(err_exceeded.to_string().contains("Rate limit exceeded"));
    let r_err4: RouterError = err_exceeded.into();
    assert!(r_err4.to_string().contains("429 Too Many Requests"));
}

#[test]
fn test_rate_limit_result_helpers_and_nanosecond_rounding() {
    // Exact seconds
    let r1 = RateLimitResult::new(
        10,
        5,
        Duration::from_secs(3),
        true,
        Some(Duration::from_secs(4)),
    );
    assert_eq!(r1.reset_seconds(), 3);
    assert_eq!(r1.retry_after_seconds(), 4);

    // Fractional nanoseconds round up
    let r2 = RateLimitResult::new(
        10,
        0,
        Duration::from_millis(3100),
        false,
        Some(Duration::from_millis(4200)),
    );
    assert_eq!(r2.reset_seconds(), 4);
    assert_eq!(r2.retry_after_seconds(), 5);

    // No retry after
    let r3 = RateLimitResult::new(10, 10, Duration::from_millis(0), true, None);
    assert_eq!(r3.retry_after_seconds(), 0);
    assert_eq!(r3.reset_seconds(), 1); // Minimum 1 second reset report
}

#[test]
fn test_memory_store_algorithm_switch_on_existing_key() {
    let store = MemoryLimiterStore::new();
    let key = "switch_test";
    let window = Duration::from_secs(60);

    // 1. Sliding window
    let r1 = block_on(store.consume(key, 1, 10, window, Algorithm::SlidingWindow)).unwrap();
    assert!(r1.is_allowed());

    // 2. Fixed window on same key (reinitializes cleanly)
    let r2 = block_on(store.consume(key, 1, 10, window, Algorithm::FixedWindow)).unwrap();
    assert!(r2.is_allowed());

    // 3. Token bucket on same key (reinitializes cleanly)
    let r3 = block_on(store.consume(key, 1, 10, window, Algorithm::TokenBucket)).unwrap();
    assert!(r3.is_allowed());
}

#[test]
fn test_empty_multi_limiter_behavior() {
    let empty_multi = MultiLimiter::new();
    let res = block_on(empty_multi.consume()).unwrap();
    assert!(res.is_allowed());

    let peek = block_on(empty_multi.peek()).unwrap();
    assert!(peek.is_allowed());
}

#[test]
fn test_middleware_builder_and_shortcuts() {
    let mw_sec = LimiterMiddleware::per_second(5);
    assert!(mw_sec.config().enabled);

    let mw_min = LimiterMiddleware::per_minute(100);
    assert!(mw_min.config().enabled);

    let mw_hr = LimiterMiddleware::per_hour(5000);
    assert!(mw_hr.config().enabled);

    let built = LimiterMiddleware::builder()
        .header_mode(HeaderMode::Legacy)
        .fail_open(false)
        .dry_run(true)
        .path("/v1/*")
        .policy(QuotaPolicy::per_minute(20))
        .build();

    assert_eq!(built.config().header_mode, HeaderMode::Legacy);
    assert!(!built.config().fail_open);
    assert!(built.config().dry_run);
    assert!(built.config().matches_path("/v1/items"));
}

proptest::proptest! {
    #[test]
    fn prop_test_memory_limiter_invariants(
        limit in 1u64..1000u64,
        amount in 1u64..100u64,
    ) {
        let store = MemoryLimiterStore::new();
        let key = "prop_key";
        let window = Duration::from_secs(60);

        let res = block_on(store.consume(key, amount, limit, window, Algorithm::SlidingWindow)).unwrap();

        // Invariant 1: remaining <= limit
        prop_assert!(res.remaining <= limit);

        // Invariant 2: if amount <= limit, request must be allowed
        if amount <= limit {
            prop_assert!(res.is_allowed());
            prop_assert_eq!(res.remaining, limit - amount);
        } else {
            prop_assert!(res.is_exceeded());
            prop_assert_eq!(res.remaining, 0);
        }
    }
}

// ============================================================================
// 9. Exhaustive Unit & Branch Coverage Tests
// ============================================================================

#[test]
fn test_key_resolver_fallbacks_when_ip_is_missing() {
    let builder = Request::builder().method(Method::Get).path("/fallback");
    let req = builder.build();
    let ctx_no_ip = HttpContext::new(req);

    // 1. IP resolver fallback to "unknown_ip"
    assert_eq!(KeyResolver::Ip.resolve(&ctx_no_ip), "unknown_ip");

    // 2. Header resolver fallback to "unknown"
    assert_eq!(
        KeyResolver::Header(HeaderName::new("non_existent")).resolve(&ctx_no_ip),
        "unknown"
    );

    // 3. RouteAndIp fallback
    assert_eq!(
        KeyResolver::RouteAndIp.resolve(&ctx_no_ip),
        "/fallback:unknown_ip"
    );

    // 4. Custom resolver returning None fallback
    let custom_none = KeyResolver::Custom(Arc::new(|_| None));
    assert_eq!(custom_none.resolve(&ctx_no_ip), "unknown_ip");
}

#[test]
fn test_tier_config_direct_builders_and_unlimited_branches() {
    let tier = TierConfig::new(50, Duration::from_secs(30))
        .algorithm(Algorithm::FixedWindow)
        .block_for(Duration::from_secs(120));

    assert_eq!(tier.limit, 50);
    assert_eq!(tier.window, Duration::from_secs(30));
    assert_eq!(tier.algorithm, Algorithm::FixedWindow);
    assert_eq!(tier.block_for, Some(Duration::from_secs(120)));

    // Unlimited policy algorithm and block_for no-ops
    let unl = QuotaPolicy::unlimited()
        .algorithm(Algorithm::FixedWindow)
        .block_for(Duration::from_secs(60));
    assert!(unl.is_unlimited());

    // Chaining from Unlimited creates limit tier
    let chained_from_unl = QuotaPolicy::unlimited().and(20, Duration::from_secs(10));
    assert_eq!(chained_from_unl.tiers().len(), 1);
}

#[test]
fn test_config_builder_and_path_patterns_exhaustive() {
    let config = LimiterConfig::builder()
        .path("/*")
        .paths(["/exact/match", "/wildcard*"])
        .key_resolver(KeyResolver::Ip)
        .build();

    assert!(config.matches_path("/any"));
    assert!(config.matches_path("/exact/match"));
    assert!(config.matches_path("/wildcard_suffix"));

    // Empty paths matches all
    let config_empty = LimiterConfig::default();
    assert!(config_empty.matches_path("/anything"));
}

#[test]
fn test_middleware_builder_and_penalty_lockout() {
    let store_dyn: Arc<dyn LimiterStore> = Arc::new(MemoryLimiterStore::new());
    let mw = LimiterMiddleware::builder()
        .store(Arc::clone(&store_dyn))
        .policy_fn(|_| QuotaPolicy::per_minute(1).block_for(Duration::from_secs(300)))
        .build();

    assert!(Arc::ptr_eq(mw.store(), &store_dyn));

    let ip = Some("10.0.0.99");

    // 1st request consumes quota
    let mut ctx1 = make_context(Method::Get, "/test", HeaderMap::new(), ip);
    let resp1 = execute_pipeline(mw.clone(), &mut ctx1, Arc::new(AtomicBool::new(false))).unwrap();
    assert_eq!(resp1.status(), StatusCode::OK);

    // 2nd request exhausts quota and triggers block_for (5 minutes)
    let mut ctx2 = make_context(Method::Get, "/test", HeaderMap::new(), ip);
    let resp2 = execute_pipeline(mw.clone(), &mut ctx2, Arc::new(AtomicBool::new(false))).unwrap();
    assert_eq!(resp2.status(), StatusCode::TOO_MANY_REQUESTS);

    // 3rd request is blocked directly by the lockout penalty!
    let mut ctx3 = make_context(Method::Get, "/test", HeaderMap::new(), ip);
    let resp3 = execute_pipeline(mw, &mut ctx3, Arc::new(AtomicBool::new(false))).unwrap();
    assert_eq!(resp3.status(), StatusCode::TOO_MANY_REQUESTS);
    assert!(resp3.headers().get(constants::RETRY_AFTER).is_some());
}

#[test]
fn test_tiered_limiter_store_getters_and_operations() {
    let mem = MemoryLimiterStore::new();
    let mock = Arc::new(MockFailingStore {
        fail_on_consume: Arc::new(AtomicBool::new(false)),
    });
    let tiered = TieredLimiterStore::new(mem, mock);

    assert_eq!(tiered.memory().max_capacity(), 10_000);
    assert!(block_on(tiered.distributed().reset("k")).is_ok());

    // Block and reset on tiered store
    block_on(tiered.block("k_tier", Duration::from_secs(10))).unwrap();
    block_on(tiered.reset("k_tier")).unwrap();
}

#[cfg(feature = "redis")]
#[test]
fn test_redis_store_peek_and_exhaustion_branches() {
    let fake = Arc::new(RedisFake::new());
    let store = RedisLimiterStore::new(fake);
    let window = Duration::from_secs(60);

    // 1. Fixed Window peek when empty vs at limit
    let p_fw_empty = block_on(store.peek("fw_k", 2, window, Algorithm::FixedWindow)).unwrap();
    assert!(p_fw_empty.is_allowed());

    // Consume all
    block_on(store.consume("fw_k", 2, 2, window, Algorithm::FixedWindow)).unwrap();
    let p_fw_full = block_on(store.peek("fw_k", 2, window, Algorithm::FixedWindow)).unwrap();
    assert_eq!(p_fw_full.remaining, 0);

    // 2. Token Bucket peek when empty vs exhausted
    let p_tb_empty = block_on(store.peek("tb_k", 5, window, Algorithm::TokenBucket)).unwrap();
    assert!(p_tb_empty.is_allowed());

    block_on(store.consume("tb_k", 5, 5, window, Algorithm::TokenBucket)).unwrap();
    let p_tb_full = block_on(store.peek("tb_k", 5, window, Algorithm::TokenBucket)).unwrap();
    assert_eq!(p_tb_full.remaining, 0);

    let res_tb_exhausted =
        block_on(store.consume("tb_k", 1, 5, window, Algorithm::TokenBucket)).unwrap();
    assert!(res_tb_exhausted.is_exceeded());
}

#[test]
fn test_additional_branch_coverage() {
    // 1. Registry is_empty check
    let reg = LimiterRegistry::new();
    assert!(reg.is_empty());
    reg.register("t", LimiterMiddleware::per_second(1));
    assert!(!reg.is_empty());

    // 2. TieredLimiterStore peek with fallback disabled
    let mem = MemoryLimiterStore::new();
    let mock = Arc::new(MockFailingStore {
        fail_on_consume: Arc::new(AtomicBool::new(true)),
    });
    let tiered_no_fb = TieredLimiterStore::new(mem, mock).with_fallback_insurance(false);
    let peek_err =
        block_on(tiered_no_fb.peek("test", 10, Duration::from_secs(60), Algorithm::FixedWindow));
    assert!(peek_err.is_err());

    // 3. Memory store default and custom capacities
    let mem_def = MemoryLimiterStore::default();
    assert_eq!(mem_def.max_capacity(), 10_000);
}
