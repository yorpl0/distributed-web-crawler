from __future__ import annotations

import asyncio
import hashlib
import os
import time

import aiohttp
import pytest
from aiohttp.test_utils import TestServer
from redis.asyncio import Redis

from distributed_crawler.crawler import Crawler, extract_links, normalize_url
from distributed_crawler.dedup import Deduper
from distributed_crawler.fixture import make_app
from distributed_crawler.frontier import Frontier, Task
from distributed_crawler.politeness import DistributedLimiter, Politeness


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


def test_normalize_url() -> None:
    assert normalize_url("HTTPS://Example.COM:443#fragment") == "https://example.com/"
    assert normalize_url("http://Example.com:8080/path?q=1#fragment") == (
        "http://example.com:8080/path?q=1"
    )
    assert normalize_url("ftp://example.com/file") == ""


def test_extract_links() -> None:
    body = '<a href="/one">one</a><div><a href="two">two</a></div>'
    assert extract_links(body) == ["/one", "two"]


@pytest.mark.asyncio
async def test_distributed_limiter_across_instances() -> None:
    redis = await connected_redis()
    namespace = f"faultcrawler:py:test:limiter:{time.time_ns()}"
    first = DistributedLimiter(redis, namespace, 0.12)
    second = DistributedLimiter(redis, namespace, 0.12)
    try:
        await first.wait("example.test")
        started = time.monotonic()
        await second.wait("example.test")
        assert time.monotonic() - started >= 0.10
    finally:
        await redis.aclose()


@pytest.mark.asyncio
async def test_robots_rules() -> None:
    server = TestServer(make_app(10, 2, 0))
    await server.start_server()
    try:
        async with aiohttp.ClientSession() as session:
            politeness = Politeness(session, None, "unused", 0)  # type: ignore[arg-type]
            assert not await politeness.allow(str(server.make_url("/blocked")))
            assert await politeness.allow(str(server.make_url("/?n=1")))
    finally:
        await server.close()


@pytest.mark.asyncio
async def test_crawler_stops_on_empty_frontier() -> None:
    redis = await connected_redis()
    server = TestServer(make_app(1, 2, 0))
    await server.start_server()
    namespace = f"faultcrawler:py:test:empty:{time.time_ns()}"
    frontier = Frontier(redis, namespace, 1)
    try:
        seed = normalize_url(str(server.make_url("/")))
        deduper = Deduper(redis, frontier.key("seen"), expected_items=100)
        assert await deduper.add(seed)
        task_id = hashlib.sha256(seed.encode()).hexdigest()[:24]
        await frontier.enqueue(Task(task_id, seed))

        stop_event = asyncio.Event()
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=2)) as session:
            crawler = Crawler(
                redis,
                frontier,
                deduper,
                Politeness(session, redis, namespace, 0),
                session,
                "empty-test",
                0,
                2,
                0.15,
                stop_event,
            )
            await asyncio.wait_for(crawler.run(2), timeout=3)
        assert await frontier.counts() == (0, 0, 1)
        assert await frontier.fetched_count() == 1
    finally:
        await frontier.reset()
        await server.close()
        await redis.aclose()

