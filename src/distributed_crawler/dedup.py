from __future__ import annotations

import asyncio
import hashlib
import math

from redis.asyncio import Redis


class BloomFilter:
    def __init__(self, expected_items: int, false_positive_rate: float) -> None:
        if expected_items <= 0 or not 0 < false_positive_rate < 1:
            raise ValueError("invalid Bloom filter estimates")
        bits = -expected_items * math.log(false_positive_rate) / math.log(2) ** 2
        self.bit_count = max(8, math.ceil(bits))
        self.hash_count = max(1, round(self.bit_count / expected_items * math.log(2)))
        self.bits = bytearray((self.bit_count + 7) // 8)

    def test_and_add(self, value: bytes) -> bool:
        digest = hashlib.sha256(value).digest()
        first = int.from_bytes(digest[:16], "big")
        second = int.from_bytes(digest[16:], "big") or 1
        positions = [
            (first + index * second) % self.bit_count
            for index in range(self.hash_count)
        ]
        seen = all(self.bits[position // 8] & (1 << position % 8) for position in positions)
        for position in positions:
            self.bits[position // 8] |= 1 << position % 8
        return seen


class Deduper:
    """Lossy process-local Bloom prefilter plus Redis cross-process authority."""

    def __init__(
        self,
        redis: Redis,
        key: str,
        expected_items: int = 1_000_000,
        false_positive_rate: float = 0.001,
    ) -> None:
        self.redis = redis
        self.key = key
        self.filter = BloomFilter(expected_items, false_positive_rate)
        self.lock = asyncio.Lock()
        self.bypass = False
        self.bloom_skips = 0
        self.redis_checks = 0

    async def add(self, normalized_url: str) -> bool:
        fingerprint = hashlib.sha256(normalized_url.encode()).digest()
        if not self.bypass:
            async with self.lock:
                seen = self.filter.test_and_add(fingerprint)
            if seen:
                self.bloom_skips += 1
                return False

        self.redis_checks += 1
        try:
            return bool(await self.redis.sadd(self.key, fingerprint.hex()))
        except Exception:
            # A Bloom entry cannot be removed. Bypass after Redis errors so a
            # retry is not suppressed by a locally poisoned bit.
            self.bypass = True
            raise

