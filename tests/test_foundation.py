from __future__ import annotations

import asyncio
import os
import time

import pytest
from redis.asyncio import Redis

from distributed_crawler.dedup import BloomFilter, Deduper
from distributed_crawler.frontier import Frontier, Task


class FakeRedis:
    def __init__(self) -> None:
        self.values: set[str] = set()
        self.calls = 0

    async def sadd(self, _: str, value: str) -> int:
        self.calls += 1
        before = len(self.values)
        self.values.add(value)
        return int(len(self.values) != before)


async def connected_redis() -> Redis:
    client = Redis.from_url(
        os.getenv("REDIS_URL", "redis://localhost:6379/0"), decode_responses=True
    )
    try:
        await client.ping()
    except Exception:
        await client.aclose()
        pytest.skip("local Redis is unavailable")
    return client


def test_bloom_filter() -> None:
    bloom = BloomFilter(100, 0.001)
    assert bloom.test_and_add(b"first") is False
    assert bloom.test_and_add(b"first") is True


@pytest.mark.asyncio
async def test_deduper_uses_bloom_fast_path() -> None:
    redis = FakeRedis()
    deduper = Deduper(redis, "seen", expected_items=100)  # type: ignore[arg-type]
    assert await deduper.add("https://example.test/") is True
    assert await deduper.add("https://example.test/") is False
    assert redis.calls == 1
    assert deduper.bloom_skips == 1
    assert deduper.redis_checks == 1


@pytest.mark.asyncio
async def test_frontier_requeues_expired_lease() -> None:
    redis = await connected_redis()
    namespace = f"faultcrawler:py:test:lease:{time.time_ns()}"
    frontier = Frontier(redis, namespace, 0.08)
    try:
        await frontier.enqueue(Task("one", "https://example.test/"))
        claimed = await frontier.claim("doomed")
        assert claimed is not None
        await asyncio.sleep(0.1)
        assert await frontier.requeue_expired() == 1
        recovered = await frontier.claim("survivor", max_tasks=1)
        assert recovered is not None and recovered.attempts == 1
        assert await frontier.ack(recovered.id)
        assert await frontier.counts() == (0, 0, 1)
    finally:
        await frontier.reset()
        await redis.aclose()


@pytest.mark.asyncio
async def test_frontier_enforces_shared_page_cap() -> None:
    redis = await connected_redis()
    namespace = f"faultcrawler:py:test:cap:{time.time_ns()}"
    frontier = Frontier(redis, namespace, 1)
    try:
        await frontier.enqueue(Task("one", "https://example.test/one"))
        await frontier.enqueue(Task("two", "https://example.test/two"))
        first = await frontier.claim("worker-one", max_tasks=1)
        assert first is not None
        assert await frontier.claim("worker-two", max_tasks=1) is None
        await redis.hset(frontier.key("pages"), first.id, "200|10|1")
        assert await frontier.ack(first.id)
        assert await frontier.claim("worker-two", max_tasks=1) is None
    finally:
        await frontier.reset()
        await redis.aclose()

