from __future__ import annotations

import asyncio
import hashlib
import time
from dataclasses import dataclass
from urllib.parse import urlsplit

import aiohttp
from protego import Protego
from redis.asyncio import Redis


USER_AGENT = "faultcrawler"
USER_AGENT_HEADER = "faultcrawler/1.0"

ACQUIRE_HOST_SCRIPT = """
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local delay = tonumber(ARGV[1])
local next_at = tonumber(redis.call('GET', KEYS[1]) or '0')
if next_at <= now then
  redis.call('PSETEX', KEYS[1], math.max(delay * 2, 1), tostring(now + delay))
  return 0
end
return next_at - now
"""


class DistributedLimiter:
    """One request start per host/delay across all crawler processes."""

    def __init__(self, redis: Redis, namespace: str, delay_seconds: float) -> None:
        self.redis = redis
        self.namespace = namespace
        self.delay_seconds = delay_seconds

    def host_key(self, host: str) -> str:
        digest = hashlib.sha256(host.encode()).hexdigest()[:24]
        return f"{self.namespace}:politeness:{digest}"

    async def wait(self, host: str, delay_seconds: float | None = None) -> None:
        delay = self.delay_seconds if delay_seconds is None else delay_seconds
        if delay <= 0:
            return
        delay_ms = max(1, round(delay * 1000))
        key = self.host_key(host)
        while True:
            wait_ms = int(await self.redis.eval(ACQUIRE_HOST_SCRIPT, 1, key, delay_ms))
            if wait_ms <= 0:
                return
            await asyncio.sleep(wait_ms / 1000)


@dataclass(slots=True)
class RobotsEntry:
    fetched_at: float
    parser: Protego | None
    crawl_delay: float


class Politeness:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        redis: Redis,
        namespace: str,
        delay_seconds: float,
    ) -> None:
        self.session = session
        self.limiter = DistributedLimiter(redis, namespace, delay_seconds)
        self.cache: dict[str, RobotsEntry] = {}
        self.cache_lock = asyncio.Lock()

    async def allow(self, url: str) -> bool:
        parsed = urlsplit(url)
        origin = f"{parsed.scheme}://{parsed.netloc}"
        entry = self.cache.get(origin)
        if entry is None or time.monotonic() - entry.fetched_at > 3600:
            async with self.cache_lock:
                entry = self.cache.get(origin)
                if entry is None or time.monotonic() - entry.fetched_at > 3600:
                    await self.limiter.wait(parsed.netloc)
                    entry = await self._fetch_robots(origin)
                    self.cache[origin] = entry

        if entry.parser is not None and not entry.parser.can_fetch(url, USER_AGENT):
            return False
        await self.limiter.wait(
            parsed.netloc, max(self.limiter.delay_seconds, entry.crawl_delay)
        )
        return True

    async def _fetch_robots(self, origin: str) -> RobotsEntry:
        fetched_at = time.monotonic()
        try:
            async with self.session.get(
                f"{origin}/robots.txt", headers={"User-Agent": USER_AGENT_HEADER}
            ) as response:
                body = (await response.content.read(1 << 20)).decode("utf-8", "replace")
                if 200 <= response.status < 300:
                    parser = Protego.parse(body)
                elif 500 <= response.status < 600:
                    parser = Protego.parse("User-agent: *\nDisallow: /\n")
                else:
                    parser = None
        except (aiohttp.ClientError, asyncio.TimeoutError):
            parser = None

        crawl_delay = 0.0
        if parser is not None:
            crawl_delay = float(parser.crawl_delay(USER_AGENT) or 0)
        return RobotsEntry(fetched_at, parser, crawl_delay)

