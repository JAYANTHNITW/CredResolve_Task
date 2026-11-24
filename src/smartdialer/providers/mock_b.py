from smartdialer.providers.base import MockProvider


class MockProviderB(MockProvider):
    name = "B"
    latency_ms = (500, 2000)
    default_failure_rate = 0.10
    timeout_on_failure = True
    duplicate_rate = 0.20
    reorder_rate = 0.15
    loss_rate = 0.02
