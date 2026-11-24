"""Resilience primitives: backoff with jitter and a consecutive-failure circuit breaker."""

from __future__ import annotations

import random
from dataclasses import dataclass

from fx_sync.errors import CircuitOpenError


@dataclass(frozen=True)
class RetryPolicy:
    max_retries: int = 4
    backoff_base: float = 0.5
    backoff_max: float = 30.0
    retry_after_max: float = 120.0

    def delay(self, attempt: int, rng: random.Random, retry_after: float | None = None) -> float:
        """Seconds to wait before retry number ``attempt`` (0-based).

        A server-provided Retry-After wins (capped); otherwise full-jitter exponential backoff.
        """
        if retry_after is not None:
            return min(max(retry_after, 0.0), self.retry_after_max)
        ceiling = min(self.backoff_max, self.backoff_base * (2**attempt))
        return rng.uniform(0.0, ceiling)


class CircuitBreaker:
    """Opens after ``threshold`` consecutive failures; any success resets the count.

    Scoped to one process run, so there is deliberately no half-open timer: once open it
    stays open and the run ends, leaving the next scheduled run to probe the API again.
    """

    def __init__(self, threshold: int) -> None:
        if threshold < 1:
            raise ValueError("threshold must be >= 1")
        self.threshold = threshold
        self.consecutive_failures = 0

    @property
    def is_open(self) -> bool:
        return self.consecutive_failures >= self.threshold

    def before_call(self) -> None:
        if self.is_open:
            raise CircuitOpenError(
                f"circuit open after {self.consecutive_failures} consecutive failures"
            )

    def record_success(self) -> None:
        self.consecutive_failures = 0

    def record_failure(self) -> None:
        self.consecutive_failures += 1
