"""Route new calls by health and keep ambiguous placements bound to their provider."""

from collections.abc import Sequence

from smartdialer.breaker import BreakerState, CircuitBreaker, Outcome
from smartdialer.clock import Clock
from smartdialer.models import FailReason
from smartdialer.providers.base import (
    Accepted,
    PlaceResult,
    Provider,
    ProviderTimeout,
    Rejected,
)


class ProviderRouter:
    def __init__(self, providers: Sequence[Provider], clock: Clock):
        self.providers = tuple(providers)
        self.breakers = {p.name: CircuitBreaker(clock) for p in providers}
        if len(self.breakers) != len(self.providers):
            raise ValueError("provider names must be unique")
        self._bindings: dict[str, Provider] = {}

    def pick(self, excluded: frozenset[str] = frozenset()) -> Provider | None:
        def rank(provider: Provider):
            breaker = self.breakers[provider.name]
            # Give a recovering provider its single probe when the cooldown expires.
            return (breaker.state != BreakerState.HALF_OPEN, breaker.failure_rate)

        for provider in sorted(self.providers, key=rank):
            if provider.name not in excluded and self.breakers[provider.name].allows():
                return provider
        return None

    def place_call(self, call_id: str, to: str, idempotency_key: str) -> PlaceResult:
        """Retry an ambiguous placement twice. Preserve its binding across later retries."""
        provider = self._bindings.get(idempotency_key)
        bound = provider is not None
        if provider is None:
            provider = self.pick()
        excluded = set()
        result = Rejected(FailReason.PROVIDER_ERROR)
        while provider is not None:
            breaker = self.breakers[provider.name]
            for attempt in range(3):
                try:
                    result = provider.place_call(call_id, to, idempotency_key)
                except ProviderTimeout:
                    breaker.record(Outcome.TIMEOUT)
                    self._bindings[idempotency_key] = provider
                    bound = True
                    if attempt == 2:
                        raise
                    continue
                if isinstance(result, Accepted):
                    breaker.record(Outcome.SUCCESS)
                    self._bindings[idempotency_key] = provider
                    return result
                breaker.record(Outcome.REJECTED)
                if bound:
                    # A rejection of a retry cannot disprove the earlier placement.
                    raise ProviderTimeout(provider.name)
                break
            excluded.add(provider.name)
            provider = self.pick(frozenset(excluded))
        return result
