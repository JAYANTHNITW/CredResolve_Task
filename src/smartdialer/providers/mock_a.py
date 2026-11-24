from smartdialer.providers.base import MockProvider


class MockProviderA(MockProvider):
    name = "A"
    latency_ms = (50, 150)
    default_failure_rate = 0.01
