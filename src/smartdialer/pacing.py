"""Progressive pacing requests one call per eligible agent."""

from dataclasses import dataclass
from typing import Protocol


class Capacity(Protocol):
    available_agents: int | None


@dataclass(frozen=True)
class PacingRequest:
    requested: int
    reasons: tuple[str, ...]


class ProgressivePacing:
    def request(self, snap: Capacity) -> PacingRequest:
        n = snap.available_agents or 0
        return PacingRequest(n, (f"progressive: {n} available agents",))
