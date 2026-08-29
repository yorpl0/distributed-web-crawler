# Fault-Tolerant Distributed Web Crawler

A compact Python crawler built to demonstrate distributed task coordination,
asynchronous networking, deduplication, politeness, persistence, and recovery
from worker failure—without dashboards, cloud infrastructure, or unrelated data
pipelines.

## Highlights

- Multiple independent Python worker processes share a persistent Redis frontier.
- `asyncio` and `aiohttp` overlap network I/O within each process.
- Atomic Redis Lua scripts implement task claim, lease, ACK, and expired-task
  requeue transitions.
- A process-local Bloom filter and Redis `SADD` provide efficient local and
  cross-worker URL deduplication.
- `robots.txt` rules and a Redis-time-based limiter enforce one global per-host
  politeness policy across every worker.
- Worker heartbeats expose liveness without adding an observability stack.
- HTTP status, response size, and elapsed time are persisted for fetched pages.
- Reproducible benchmark and failure-injection scripts measure scaling and verify
  recovery after killing a worker.

Delivery is **at least once**: a page can be fetched twice near lease expiry, but
a crashed worker does not silently lose its task. URL-derived task IDs make page
persistence and ACK handling idempotent.

## Architecture

```text
Seed URL
   |
   v
Normalize + deduplicate
   |
   v
Redis: ready -> processing + lease -> ACK -> done
                         |
                         +-> lease expires -> ready
                                  ^             |
                                  |             v
                         Python worker processes
                          | asyncio task pools
                          +-- robots.txt
                          +-- global host delay
                          +-- aiohttp fetch + parse
```

Each worker claims a URL atomically, checks the site's crawl policy, waits for
the shared host limiter, downloads the page, extracts and normalizes links,
deduplicates newly discovered URLs, persists a response summary, and ACKs the
task. A maintenance coroutine requeues expired leases and refreshes the worker's
Redis heartbeat.

## Technical decisions

- **Redis frontier:** keeps task and result state outside worker memory and gives
  every process one coordination point.
- **Lua transitions:** update multiple Redis structures atomically, avoiding
  partially claimed or acknowledged tasks during races.
- **Async I/O plus processes:** coroutines overlap network waits inside a worker;
  independent processes provide failure isolation and distributed coordination.
- **Two-level deduplication:** the Bloom filter skips local repeats without a
  network call, while Redis atomically resolves discoveries across workers.
- **Leases rather than permanent dequeue:** abandoned work becomes detectable and
  recoverable after a worker dies.
- **Global politeness:** Redis server time coordinates request starts, preventing
  additional processes from bypassing the delay for a host.

## Measured performance

The saturation benchmark used Python 3.13.1, Docker Redis 7.4, a deterministic
local zero-latency HTTP fixture, 5,000 fetched pages per trial, 32 links per page,
four async tasks per process, and three trials per configuration. Timings include
process startup.

| Processes | Async tasks | Median pages/s | Speedup | Mean peak RAM |
|---:|---:|---:|---:|---:|
| 1 | 4 | 404.98 | 1.00x | 56.84 MB |
| 2 | 8 | 611.50 | 1.51x | 113.80 MB |
| 4 | 16 | **757.78** | **1.87x** | 227.25 MB |
| 8 | 32 | 720.01 | 1.78x | 453.88 MB |

Four processes produced the highest median throughput. Eight processes were
about 5% slower while using roughly twice the memory, showing the point where
Redis coordination, deduplication, and process scheduling outweighed additional
concurrency.

These are synthetic single-machine measurements, not public-internet throughput.
A separate polite integration run crawled exactly ten pages from the public
practice site `books.toscrape.com`; all ten returned HTTP `200`, and the crawler
discovered another 110 unique URLs before stopping at the configured limit.

## Correctness and recovery

- Nine unit and real-Redis integration tests cover URL normalization, link
  extraction, Bloom behavior, cross-worker host limiting, robots rules, lease
  recovery, empty-frontier termination, and the shared page cap.
- A failure-injection demo terminates a worker while it owns a leased task; a
  surviving worker requeues and completes it with `attempts=1`.
- Measured fixture runs returned 100% `2xx` responses and finished without leaked
  processing tasks, leases, or owner records.
- A 25 ms global per-host delay remained stable at 29.18 pages/s median with
  eight processes and 32 async tasks, confirming that more workers did not
  bypass the shared limiter.

## Scope

The crawler focuses on scheduling and retrieval infrastructure. It does not
execute JavaScript, render pages, or implement a domain-specific extraction
pipeline. Page bodies are capped at 2 MiB, and only compact response metadata is
persisted. Redis high availability and sharding are intentionally outside this
project's interview-sized scope.

## Stack

Python 3.11+ · asyncio · aiohttp · Redis · Lua · Protego · Docker Compose ·
Pytest · PowerShell

