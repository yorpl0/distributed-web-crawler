from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from html.parser import HTMLParser
from urllib.parse import urljoin, urlsplit, urlunsplit

import aiohttp
from redis.asyncio import Redis

from distributed_crawler.dedup import Deduper
from distributed_crawler.frontier import Frontier, Task
from distributed_crawler.politeness import Politeness, USER_AGENT_HEADER


MAX_BODY_BYTES = 2 << 20
logger = logging.getLogger(__name__)


class LinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.links: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        for name, value in attrs:
            if name.lower() == "href" and value:
                self.links.append(value)


def extract_links(body: str) -> list[str]:
    parser = LinkParser()
    parser.feed(body)
    return parser.links


def normalize_url(raw_url: str) -> str:
    try:
        parsed = urlsplit(raw_url)
        if parsed.scheme.lower() not in {"http", "https"} or not parsed.netloc:
            return ""
        host = (parsed.hostname or "").lower()
        if not host:
            return ""
        if ":" in host and not host.startswith("["):
            host = f"[{host}]"
        port = parsed.port
        if port is not None and not (
            parsed.scheme.lower() == "http" and port == 80
        ) and not (parsed.scheme.lower() == "https" and port == 443):
            host = f"{host}:{port}"
        path = parsed.path or "/"
        return urlunsplit((parsed.scheme.lower(), host, path, parsed.query, ""))
    except ValueError:
        return ""


async def read_limited(stream: aiohttp.StreamReader, limit: int) -> bytes:
    body = bytearray()
    async for chunk in stream.iter_chunked(64 * 1024):
        remaining = limit - len(body)
        if remaining <= 0:
            break
        body.extend(chunk[:remaining])
        if len(body) >= limit:
            break
    return bytes(body)


class Crawler:
    def __init__(
        self,
        redis: Redis,
        frontier: Frontier,
        deduper: Deduper,
        politeness: Politeness,
        session: aiohttp.ClientSession,
        worker_id: str,
        max_depth: int,
        max_pages: int,
        idle_timeout: float,
        stop_event: asyncio.Event,
    ) -> None:
        self.redis = redis
        self.frontier = frontier
        self.deduper = deduper
        self.politeness = politeness
        self.session = session
        self.worker_id = worker_id
        self.max_depth = max_depth
        self.max_pages = max_pages
        self.idle_timeout = idle_timeout
        self.stop_event = stop_event

    async def run(self, concurrency: int) -> None:
        workers = [
            asyncio.create_task(self._loop(f"{self.worker_id}-{index}"))
            for index in range(concurrency)
        ]
        await asyncio.gather(*workers)

    async def _loop(self, worker: str) -> None:
        idle_since: float | None = None
        while not self.stop_event.is_set():
            try:
                fetched = await self.frontier.fetched_count()
                if self.max_pages > 0 and fetched >= self.max_pages:
                    return

                task = await self.frontier.claim(worker, self.max_pages)
                if task is None:
                    ready, processing = await self.frontier.pending_counts()
                    if ready == 0 and processing == 0:
                        idle_since = idle_since or time.monotonic()
                        if time.monotonic() - idle_since >= self.idle_timeout:
                            return
                    else:
                        idle_since = None
                    await self._sleep_or_stop(0.05)
                    continue

                idle_since = None
                if await self.process(task):
                    await self.frontier.ack(task.id)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                logger.warning("worker=%s task retry after error: %s", worker, error)
                await self._sleep_or_stop(0.1)

    async def process(self, task: Task) -> bool:
        url = normalize_url(task.url)
        if not url:
            return True
        if not await self.politeness.allow(url):
            return True

        started = time.monotonic()
        async with self.session.get(url, headers={"User-Agent": USER_AGENT_HEADER}) as response:
            body = await read_limited(response.content, MAX_BODY_BYTES)
            elapsed_ms = round((time.monotonic() - started) * 1000)
            summary = f"{response.status}|{len(body)}|{elapsed_ms}"

            content_type = response.headers.get("Content-Type", "")
            if task.depth < self.max_depth and "text/html" in content_type.lower():
                charset = response.charset or "utf-8"
                try:
                    text = body.decode(charset, "replace")
                except LookupError:
                    text = body.decode("utf-8", "replace")
                for link in extract_links(text):
                    child = normalize_url(urljoin(url, link))
                    if not child or not await self.deduper.add(child):
                        continue
                    task_id = hashlib.sha256(child.encode()).hexdigest()[:24]
                    await self.frontier.enqueue(Task(task_id, child, task.depth + 1))

            await self.redis.hset(self.frontier.key("pages"), task.id, summary)
        return True

    async def _sleep_or_stop(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self.stop_event.wait(), timeout=seconds)
        except TimeoutError:
            pass

