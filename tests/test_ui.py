import json
from threading import Thread
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest

from smartdialer.ui.demo import Demo
from smartdialer.ui.server import local_server


@pytest.fixture
def api():
    with local_server(0) as server:
        thread = Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01})
        thread.start()

        def request(path="/state", body=None):
            req = Request(
                f"http://127.0.0.1:{server.server_port}{path}",
                data=json.dumps(body).encode() if body is not None else None,
                headers={"Content-Type": "application/json"},
            )
            with urlopen(req, timeout=5) as response:
                return response.read()

        try:
            yield request
        finally:
            server.shutdown()
            thread.join()


def test_page_and_state(api):
    assert b"What just happened?" in api("/")
    state = json.loads(api())
    assert {
        "agents",
        "legend",
        "calls",
        "waiting",
        "finished",
        "failed",
        "safety",
        "feed",
        "metrics",
        "why",
        "running",
        "speed",
        "answer_rate",
        "talk_seconds",
    } <= state.keys()
    assert len(state["agents"]) == 10 and state["waiting"] == 400
    assert state["metrics"] == {
        "utilization": 0,
        "started": 0,
        "answered": 0,
        "connected": 0,
        "abandoned": 0,
    }


def test_all_controls(api):
    initial = json.loads(api())
    for kind, value in [
        ("start", None),
        ("pause", None),
        ("speed", 20),
        ("talk_seconds", 300),
        ("answer_rate", 100),
        ("outage", None),
        ("slow", None),
        ("leave", None),
        ("duplicate", None),
        ("reorder", None),
        ("crash", None),
    ]:
        state = json.loads(api("/action", {"type": kind, "value": value}))
        if value is not None:
            assert state[kind] == value
        if kind in ("outage", "slow", "leave"):
            assert state["left" if kind == "leave" else kind]
        if kind in ("start", "pause"):
            assert state["running"] == (kind == "start")
    for kind in ("outage", "slow"):
        assert not json.loads(api("/action", {"type": kind}))[kind]
    fresh = json.loads(api("/action", {"type": "reset"}))
    assert fresh["session"] != initial["session"] and not fresh["running"]
    assert fresh["metrics"] == initial["metrics"]


@pytest.mark.parametrize(
    "body",
    [
        {"type": "other"},
        {"type": "speed", "value": 3},
        {"type": "answer_rate", "value": 101},
        {},
    ],
)
def test_invalid_action(api, body):
    with pytest.raises(HTTPError) as error:
        api("/action", body)
    with error.value as response:
        assert response.code == 400


def test_crash_recovery_and_message_chaos():
    demo = Demo()
    try:
        demo.action("crash")
        demo.step()
        assert all(a["state"] == "RESERVED" for a in demo.state()["agents"])
        for _ in range(115):
            demo.step()
        assert any("Cleanup freed" in row["text"] for row in demo.feed)
        demo.action("duplicate")
        demo.action("reorder")
        observed = set()
        for _ in range(350):
            demo.step()
            observed.update(r["text"] for r in demo.feed)
        assert "Duplicate message from the phone company ignored." in observed
        assert "Old message arrived late and was ignored." in observed
        assert demo.state()["metrics"]["started"] > 0
        demo.action("leave")
        for _ in range(70):
            demo.step()
        assert sum(a["state"] == "OFFLINE" for a in demo.state()["agents"]) == 4
    finally:
        demo.close()
