# Calinix Router Latency Optimization Journey

We set out to build a high-performance, cache-aware router, aiming for a routing decision latency of **< 1 ms** under high concurrency. This page documents our engineering journey, showing how we identified bottlenecks, iterated on the code, and eventually optimized the hot path.

---

## Iteration 1: The Initial Baseline (`policy-bench`)

Our first implementation of the 4-stage routing pipeline (Prepare -> Filter -> Score -> Pick) was fully functional, but it didn't scale.

![Initial Router Latency](benchmark/results/policy-bench/policy_router_latency.png)

Under a single thread, routing a request took about **28 µs**. However, as soon as we introduced concurrent traffic, latency rapidly degraded. At 128 concurrency, the routing decision took an average of **~7–8 ms**—introducing a significant and unacceptable overhead on the hot path.

---
