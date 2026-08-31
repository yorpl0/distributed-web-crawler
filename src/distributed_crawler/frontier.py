from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass

from redis.asyncio import Redis


@dataclass(slots=True)
class Task:
    id: str
    url: str
    depth: int = 0
    attempts: int = 0


CLAIM_SCRIPT = """
local maximum = tonumber(ARGV[3])
if maximum > 0 then
  local completed = redis.call('HLEN', KEYS[5])
  local in_flight = redis.call('HLEN', KEYS[2])
  if completed + in_flight >= maximum then return nil end
end
local pair = redis.call('HRANDFIELD', KEYS[1], 1, 'WITHVALUES')
if #pair == 0 then return nil end
redis.call('HDEL', KEYS[1], pair[1])
redis.call('HSET', KEYS[2], pair[1], pair[2])
redis.call('ZADD', KEYS[3], ARGV[1], pair[1])
redis.call('HSET', KEYS[4], pair[1], ARGV[2])
return pair
"""

ACK_SCRIPT = """
local payload = redis.call('HGET', KEYS[1], ARGV[1])
if not payload then return 0 end
redis.call('HDEL', KEYS[1], ARGV[1])
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('HDEL', KEYS[3], ARGV[1])
redis.call('HSET', KEYS[4], ARGV[1], payload)
return 1
"""

REQUEUE_SCRIPT = """
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
local count = 0
for _, id in ipairs(ids) do
  local payload = redis.call('HGET', KEYS[2], id)
  if payload then
    local task = cjson.decode(payload)
    task.attempts = (task.attempts or 0) + 1
    redis.call('HSET', KEYS[3], id, cjson.encode(task))
    redis.call('HDEL', KEYS[2], id)
    count = count + 1
  end
  redis.call('ZREM', KEYS[1], id)
  redis.call('HDEL', KEYS[4], id)
end
return count
"""


class Frontier:
    def __init__(self, redis: Redis, namespace: str, lease_seconds: float) -> None:
        self.redis = redis
        self.namespace = namespace
        self.lease_seconds = lease_seconds

    def key(self, suffix: str) -> str:
        return f"{self.namespace}:{suffix}"

    async def enqueue(self, task: Task) -> None:
        await self.redis.hset(
            self.key("ready"), task.id, json.dumps(asdict(task), separators=(",", ":"))
        )

    async def claim(self, worker: str, max_tasks: int = 0) -> Task | None:
        expires_at = int((time.time() + self.lease_seconds) * 1000)
        result = await self.redis.eval(
            CLAIM_SCRIPT,
            5,
            self.key("ready"),
            self.key("processing"),
            self.key("leases"),
            self.key("owners"),
            self.key("pages"),
            expires_at,
            worker,
            max_tasks,
        )
        if not result:
            return None
        payload = json.loads(result[1])
        return Task(**payload)

    async def ack(self, task_id: str) -> bool:
        result = await self.redis.eval(
            ACK_SCRIPT,
            4,
            self.key("processing"),
            self.key("leases"),
            self.key("owners"),
            self.key("done"),
            task_id,
        )
        return bool(result)

    async def requeue_expired(self, limit: int = 100) -> int:
        return int(
            await self.redis.eval(
                REQUEUE_SCRIPT,
                4,
                self.key("leases"),
                self.key("processing"),
                self.key("ready"),
                self.key("owners"),
                int(time.time() * 1000),
                limit,
            )
        )

    async def pending_counts(self) -> tuple[int, int]:
        pipe = self.redis.pipeline(transaction=False)
        pipe.hlen(self.key("ready"))
        pipe.hlen(self.key("processing"))
        ready, processing = await pipe.execute()
        return int(ready), int(processing)

    async def counts(self) -> tuple[int, int, int]:
        pipe = self.redis.pipeline(transaction=False)
        pipe.hlen(self.key("ready"))
        pipe.hlen(self.key("processing"))
        pipe.hlen(self.key("done"))
        ready, processing, done = await pipe.execute()
        return int(ready), int(processing), int(done)

    async def fetched_count(self) -> int:
        return int(await self.redis.hlen(self.key("pages")))

    async def reset(self) -> None:
        await self.redis.delete(
            self.key("ready"),
            self.key("processing"),
            self.key("leases"),
            self.key("owners"),
            self.key("done"),
            self.key("seen"),
            self.key("workers"),
            self.key("pages"),
        )
