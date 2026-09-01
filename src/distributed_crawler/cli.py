from __future__ import annotations

import argparse
import asyncio
import contextlib
import hashlib
import logging
import os
import signal
import socket
import time

import aiohttp
from redis.asyncio import Redis

from distributed_crawler.crawler import Crawler, normalize_url
from distributed_crawler.dedup import Deduper
from distributed_crawler.frontier import Frontier, Task
from distributed_crawler.politeness import Politeness


logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Redis-backed distributed web crawler")
    parser.add_argument("--redis", default="redis://localhost:6379/0", help="Redis URL")
    parser.add_argument("--namespace", default="faultcrawler:demo")
    parser.add_argument("--seed", default="", help="seed URL; only one process needs it")
    parser.add_argument("--worker-id", default=f"{socket.gethostname()}-{os.getpid()}")
    parser.add_argument(
        "--concurrency", type=int, default=4, help="async fetch tasks in this process"
    )
    parser.add_argument("--max-depth", type=int, default=3)
    parser.add_argument("--max-pages", type=int, default=1000)
    parser.add_argument("--lease", type=float, default=30, help="task lease in seconds")
    parser.add_argument(
        "--delay", type=float, default=0.025, help="minimum per-host delay in seconds"
    )
    parser.add_argument(
        "--idle-timeout", type=float, default=2, help="empty-frontier exit delay in seconds"
    )
    parser.add_argument("--request-timeout", type=float, default=10)
    parser.add_argument("--reset", action="store_true")
    return parser.parse_args()


async def maintenance(
    stop_event: asyncio.Event, redis: Redis, frontier: Frontier, worker_id: str
) -> None:
    while not stop_event.is_set():
        now_ms = int(time.time() * 1000)
        await redis.zadd(frontier.key("workers"), {worker_id: now_ms})
        await redis.zremrangebyscore(frontier.key("workers"), "-inf", now_ms - 10_000)
        recovered = await frontier.requeue_expired()
        if recovered:
            logger.info("requeued %d expired task(s)", recovered)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=0.5)
        except TimeoutError:
            pass


async def run(args: argparse.Namespace) -> None:
    if args.concurrency < 1 or args.lease <= 0 or args.idle_timeout < 0:
        raise SystemExit(
            "concurrency and lease must be positive; idle timeout cannot be negative"
        )
    if args.lease <= args.request_timeout:
        logger.warning("lease should exceed request timeout plus expected politeness wait")

    redis = Redis.from_url(args.redis, decode_responses=True)
    await redis.ping()
    frontier = Frontier(redis, args.namespace, args.lease)
    if args.reset:
        await frontier.reset()

    deduper = Deduper(redis, frontier.key("seen"))
    if args.seed:
        seed = normalize_url(args.seed)
        if not seed:
            raise SystemExit("invalid seed URL")
        if await deduper.add(seed):
            task_id = hashlib.sha256(seed.encode()).hexdigest()[:24]
            await frontier.enqueue(Task(task_id, seed))

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def request_stop(*_: object) -> None:
        loop.call_soon_threadsafe(stop_event.set)

    for signal_name in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(ValueError, OSError, RuntimeError):
            signal.signal(signal_name, request_stop)

    timeout = aiohttp.ClientTimeout(total=args.request_timeout)
    connector = aiohttp.TCPConnector(limit=256, limit_per_host=32, keepalive_timeout=30)
    try:
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            crawler = Crawler(
                redis,
                frontier,
                deduper,
                Politeness(session, redis, args.namespace, args.delay),
                session,
                args.worker_id,
                args.max_depth,
                args.max_pages,
                args.idle_timeout,
                stop_event,
            )
            maintenance_task = asyncio.create_task(
                maintenance(stop_event, redis, frontier, args.worker_id)
            )
            logger.info(
                "worker=%s concurrency=%d namespace=%s",
                args.worker_id,
                args.concurrency,
                args.namespace,
            )
            try:
                await crawler.run(args.concurrency)
            finally:
                stop_event.set()
                maintenance_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await maintenance_task
    finally:
        ready, processing, done = await frontier.counts()
        print(f"ready={ready} processing={processing} done={done}")
        await redis.aclose()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    asyncio.run(run(parse_args()))


if __name__ == "__main__":
    main()

