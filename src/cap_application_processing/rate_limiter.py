import asyncio
import time


class RateLimiter:
    """Async token-bucket rate limiter shared across tasks.

    Capacity equals `rate` (one second's worth of tokens), so a cold start
    can burst up to `rate` requests and then settles at `rate` req/s.
    """

    def __init__(self, rate: float):
        if rate <= 0:
            raise ValueError("rate must be > 0")
        self.rate = float(rate)
        self.capacity = float(rate)
        self._tokens = float(rate)
        self._last = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        while True:
            async with self._lock:
                now = time.monotonic()
                self._tokens = min(
                    self.capacity, self._tokens + (now - self._last) * self.rate
                )
                self._last = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                wait = (1.0 - self._tokens) / self.rate
            await asyncio.sleep(wait)
