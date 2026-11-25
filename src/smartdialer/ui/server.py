"""Local HTTP adapter. Only the simulation thread touches the campaign or SQLite."""

import json
from concurrent.futures import Future
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib.resources import files
from queue import Empty, Queue
from threading import Thread
from time import monotonic

from smartdialer.ui.demo import Demo


class Simulation:
    def __init__(self):
        self.commands = Queue()
        self.thread = Thread(target=self.run, name="demo-clock", daemon=True)
        self.thread.start()

    def request(self, kind, body=None):
        future = Future()
        self.commands.put((kind, body, future))
        return future.result(timeout=10)

    def run(self):
        demo = Demo()
        due = monotonic()
        try:
            while True:
                if demo.running and monotonic() >= due:
                    demo.step()
                    due += 0.1 / demo.speed
                try:
                    kind, body, future = self.commands.get(
                        timeout=max(0, due - monotonic()) if demo.running else None
                    )
                except Empty:
                    continue
                try:
                    if kind == "stop":
                        future.set_result(None)
                        return
                    if kind == "action":
                        if body["type"] == "reset":
                            demo.close()
                            demo = Demo()
                        else:
                            demo.action(body["type"], body.get("value"))
                        if body["type"] in ("start", "reset", "speed"):
                            due = monotonic()
                    future.set_result(demo.state())
                except Exception as exc:  # noqa: BLE001 - propagate command failures to the HTTP thread
                    future.set_exception(exc)
        finally:
            demo.close()

    def close(self):
        self.request("stop")
        self.thread.join()


class Handler(BaseHTTPRequestHandler):
    def reply(self, status, data, content_type="application/json"):
        raw = data if isinstance(data, bytes) else json.dumps(data).encode()
        self.send_response(status)
        self.send_header("Content-Type", content_type + "; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        if self.path == "/":
            self.reply(
                200,
                files("smartdialer.ui").joinpath("index.html").read_bytes(),
                "text/html",
            )
        elif self.path == "/state":
            self.reply(200, self.server.simulation.request("state"))
        else:
            self.reply(404, {"error": "Unknown page."})

    def do_POST(self):
        if self.path != "/action":
            self.reply(404, {"error": "Unknown endpoint."})
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if (
                self.headers.get_content_type() != "application/json"
                or not 0 < length <= 4096
            ):
                raise ValueError("Send a small JSON action.")
            body = json.loads(self.rfile.read(length))
            if not isinstance(body, dict) or not isinstance(body.get("type"), str):
                raise TypeError("An action needs a type.")
            self.reply(200, self.server.simulation.request("action", body))
        except (ValueError, TypeError) as exc:
            self.reply(400, {"error": str(exc)})

    def log_message(self, fmt, *args):
        pass  # Polling should not fill the terminal.


@contextmanager
def local_server(port=8000):
    with ThreadingHTTPServer(("127.0.0.1", port), Handler) as server:
        server.simulation = Simulation()
        try:
            yield server
        finally:
            server.simulation.close()


def main():
    with local_server() as server:
        print("SmartDialer demo: http://localhost:8000", flush=True)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
