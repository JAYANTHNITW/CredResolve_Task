"""A private virtual campaign. All mutable state belongs to the simulation thread."""

import json
from collections import deque
from datetime import UTC
from random import Random
from uuid import uuid4

from smartdialer import repository as repo
from smartdialer.clock import FakeClock
from smartdialer.db import connect, init_schema
from smartdialer.dispatcher import Dispatcher
from smartdialer.event_handler import Outcome, handle_event
from smartdialer.models import CallState
from smartdialer.providers.base import Scheduler
from smartdialer.providers.mock_a import MockProviderA
from smartdialer.providers.mock_b import MockProviderB
from smartdialer.router import ProviderRouter
from smartdialer.safety import ProviderLimit
from smartdialer.simulator import START, STEP

TEAM = 10
AGENTS = {
    "AVAILABLE": "Ready",
    "RESERVED": "Saving a spot",
    "DIALING": "Calling",
    "CONNECTED": "Talking",
    "WRAP_UP": "Finishing notes",
    "PAUSED": "On a break",
    "OFFLINE": "Away",
}
CALLS = {
    "RESERVED": "Saving a spot",
    "INITIATED": "Calling",
    "RINGING": "Ringing",
    "ANSWERED": "Answered",
    "CONNECTED": "Talking",
    "COMPLETED": "Finished",
    "FAILED": "Failed",
    "CANCELLED": "Cancelled",
}
REASONS = {
    "WITHIN_LIMITS": "The team and phone lines have room.",
    "AGENT_LIMIT": "Only free agents can take calls.",
    "CONCURRENCY_CAP": "A phone company has all its lines busy.",
    "CPS_CAP": "A phone company has started enough calls this second.",
    "PROVIDER_CAPACITY": "There are fewer phone lines ready.",
    "PROVIDER_DEGRADED": "One phone company needs a break.",
    "KILL_SWITCH": "All calling is switched off.",
    "CAMPAIGN_PAUSED": "This calling list is paused.",
    "OUTSIDE_WINDOW": "It is outside calling hours.",
    "STALE_SNAPSHOT": "The head count is too old.",
    "INCOMPLETE_SNAPSHOT": "Part of the head count is missing.",
    "NO_HEALTHY_PROVIDER": "Neither phone company can take calls.",
}
WHY = {
    "outage": "When B breaks, the other company can take new calls after a clear refusal. A lost reply stays with the same company to avoid calling twice.",
    "slow": "A slow line takes longer to ring. The agent keeps their spot while waiting.",
    "leave": "Four agents stop checking in. After five seconds their spots stop counting as free, and the cleanup worker checks their calls.",
    "duplicate": "The next phone message is delivered twice. Its message ID lets the dialer ignore the extra copy.",
    "reorder": "We hold a ringing message until a later update for that call arrives. A late message cannot move a call backward.",
    "crash": "A worker stops after saving spots and before dialing. When those spots expire, its replacement checks and releases them.",
}


def agent_name(agent_id):
    return f"Agent {int(agent_id.rsplit('a', 1)[1]) + 1}"


class Demo:
    def __init__(self):
        self.conn = connect(":memory:")
        init_schema(self.conn)
        self.clock = FakeClock(START)
        self.scheduler = Scheduler()
        self.session = uuid4().hex
        self.running, self.speed = False, 1
        self.answer_rate, self.talk_seconds = 65, 45
        self.outage = self.slow = self.left = False
        self.duplicate = self.reorder = self.crash = False
        self.held = None
        self.why = "Try a surprise to see how the dialer keeps track of everyone."
        self.feed = deque(maxlen=45)
        self.tick_count = self.talk_ms = self.logged_ms = 0
        self.event_cursor = 0
        self.last_safety = None
        self.decision = {
            "action": "WAITING",
            "requested": 0,
            "approved": 0,
            "reason": "Press Start for the first check.",
        }
        repo.seed_campaign(self.conn, "demo", TEAM, 400, START)
        repo.simulation_observers(self.conn)
        args = {
            "answer_rate_fn": lambda now: self.answer_rate / 100,
            "ring_time_dist": lambda rng: rng.randint(2000, 5500),
            "talk_time_dist": lambda rng: rng.randint(
                self.talk_seconds * 500, self.talk_seconds * 1500
            ),
        }
        self.b = MockProviderB(self.clock, Random(24), self.scheduler, **args)
        self.a = MockProviderA(self.clock, Random(9), self.scheduler, **args)
        self.worker = self.new_worker()

    def new_worker(self):
        router = ProviderRouter([self.b, self.a], self.clock)
        worker = Dispatcher(
            self.conn,
            "demo",
            self.clock,
            router,
            timezone=UTC,
            limits={p.name: ProviderLimit(6, 10) for p in router.providers},
            backoff=self.clock.advance,
        )
        for p in router.providers:
            p.set_event_sink(self.receive)
        return worker

    def close(self):
        self.conn.close()

    def tell(self, text):
        seconds = (self.clock.now_ms() - START) // 1000
        self.feed.appendleft(
            {"time": f"{seconds // 60:02}:{seconds % 60:02}", "text": text}
        )

    def deliver(self, event):
        outcome = handle_event(self.conn, event, self.clock.now_ms())
        self.read_events()
        if outcome == Outcome.DUPLICATE:
            # Duplicate IDs reuse the original audit row; the handler returns the outcome.
            self.tell("Duplicate message from the phone company ignored.")

    def receive(self, event):
        if self.reorder and self.held is None and event.type == CallState.RINGING:
            self.held = event
            self.reorder = False
            return
        self.deliver(event)
        if self.duplicate:
            self.duplicate = False
            self.deliver(event)
        if (
            self.held
            and event.call_id == self.held.call_id
            and event.type != CallState.RINGING
        ):
            held, self.held = self.held, None
            self.deliver(held)

    def read_events(self):
        for row in self.conn.execute(
            """SELECT e.id,e.type,e.outcome,c.agent_id,c.fail_reason FROM call_events e
                JOIN calls c ON c.id=e.call_id WHERE e.id>? ORDER BY e.id""",
            (self.event_cursor,),
        ):
            self.event_cursor = row["id"]
            name = agent_name(row["agent_id"])
            if row["outcome"] == "STALE":
                self.tell("Old message arrived late and was ignored.")
            elif row["outcome"] == "APPLIED":
                if row["fail_reason"] == "AGENT_LOST":
                    if row["type"] == "CANCELLED":
                        self.tell(
                            f"{name} disappeared before connecting. Call cancelled; borrower back in line."
                        )
                    else:
                        self.tell(
                            f"{name} disappeared during a conversation. The borrower went back in line."
                        )
                elif row["fail_reason"] == "RESERVATION_EXPIRED":
                    self.tell(
                        f"Cleanup freed {name}'s unused spot and put the borrower back in line."
                    )
                else:
                    self.tell(
                        f"{name} · {CALLS.get(row['type'], row['type']).lower()}."
                    )

    def read_safety(self):
        r = self.conn.execute(
            "SELECT * FROM safety_decisions ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if not r:
            return
        reasons = tuple(json.loads(r["reason_codes"]))
        key = (r["action"], r["requested"], r["approved"], reasons)
        if key != self.last_safety:
            self.last_safety = key
            reason = " ".join(REASONS[code] for code in reasons)
            self.decision = {
                "action": r["action"],
                "requested": r["requested"],
                "approved": r["approved"],
                "reason": reason,
            }
            free = json.loads(r["snapshot"])["available_agents"]
            self.tell(
                f"Safety checked: {free} agents free, {r['approved']} calls allowed. {reason}"
            )

    def step(self):
        now = self.clock.now_ms()
        repo.simulation_heartbeats(self.conn, now)
        self.scheduler.run_until(now)
        if self.tick_count % 2 == 0:
            if self.crash and self.worker.snapshot().available_agents:
                snap = self.worker.snapshot()
                decision = self.worker.safety.evaluate(
                    self.worker.pacing.request(snap), snap, now
                )
                if decision.permit and self.worker.allocator.reserve(decision.permit):
                    self.crash = False
                    self.worker = self.new_worker()
                    self.tell(
                        "Worker stopped after saving spots. A replacement will check the expired spots."
                    )
                else:
                    self.worker.recovery.sweep()
            else:
                self.worker.tick()
            self.read_safety()
        self.read_events()
        counts = repo.simulation_agent_counts(self.conn)
        dt = max(STEP, self.clock.now_ms() - now)
        self.talk_ms += dt * counts.get("CONNECTED", 0)
        self.logged_ms += dt * (TEAM - counts.get("OFFLINE", 0))
        self.clock.advance(now + dt - self.clock.now_ms())
        self.tick_count += 1

    def action(self, kind, value=None):
        if kind in ("speed", "answer_rate", "talk_seconds"):
            valid = {
                "speed": (1, 5, 20),
                "answer_rate": range(101),
                "talk_seconds": range(30, 301),
            }
            if type(value) is not int or value not in valid[kind]:
                raise ValueError("Choose a value within the control's range.")
            setattr(self, kind, value)
        elif kind in ("start", "pause"):
            self.running = kind == "start"
        elif kind in WHY:
            self.why = WHY[kind]
            if kind == "outage":
                self.outage = not self.outage
                self.b.set_outage(self.outage)
                self.tell(
                    "Phone company B broke."
                    if self.outage
                    else "Phone company B is back."
                )
            elif kind == "slow":
                self.slow = not self.slow
                for p in (self.a, self.b):
                    p.set_extra_latency(3500 if self.slow else 0)
                self.tell(
                    "New calls will ring more slowly."
                    if self.slow
                    else "Phone lines are back to normal speed."
                )
            elif kind == "leave":
                if not self.left:
                    repo.simulation_drop(self.conn, 4)
                    self.left = True
                    self.tell("Four agents stopped checking in.")
            else:
                setattr(self, kind, True)
                self.tell(
                    {
                        "duplicate": "The next phone message will arrive twice.",
                        "reorder": "The next ringing message will be held back.",
                        "crash": "A worker will stop at its next reservation.",
                    }[kind]
                )
        else:
            raise ValueError("Unknown action.")

    def state(self):
        agents = [
            {
                "id": r["id"],
                "name": agent_name(r["id"]),
                "state": r["state"],
                "label": AGENTS[r["state"]],
            }
            for r in self.conn.execute("SELECT id,state FROM agents ORDER BY id")
        ]
        calls = [
            {
                "id": r["id"],
                "agent": agent_name(r["agent_id"]),
                "label": CALLS[r["state"]],
            }
            for r in self.conn.execute("""SELECT id,agent_id,state FROM calls WHERE state NOT IN
               ('COMPLETED','FAILED','CANCELLED') ORDER BY created_at DESC,agent_id""")
        ]
        counts = repo.campaign_snapshot(self.conn, "demo").calls_by_state
        (started, answered, connected), missing, lost = repo.simulation_totals(
            self.conn
        )
        waiting = self.conn.execute(
            "SELECT COUNT(*) FROM borrowers WHERE status='PENDING'"
        ).fetchone()[0]
        return {
            "session": self.session,
            "running": self.running,
            "speed": self.speed,
            "answer_rate": self.answer_rate,
            "talk_seconds": self.talk_seconds,
            "outage": self.outage,
            "slow": self.slow,
            "left": self.left,
            "agents": agents,
            "legend": AGENTS,
            "calls": calls,
            "waiting": waiting,
            "finished": counts["COMPLETED"],
            "failed": counts["FAILED"],
            "safety": self.decision,
            "why": self.why,
            "feed": list(self.feed),
            "metrics": {
                "utilization": round(100 * self.talk_ms / self.logged_ms, 1)
                if self.logged_ms
                else 0,
                "started": started,
                "answered": answered,
                "connected": connected,
                "abandoned": missing + lost,
            },
        }
