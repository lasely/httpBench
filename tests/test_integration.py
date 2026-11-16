"""Exercise real HTTP streaming and the CLI against a local threaded server.

The server binds to a free loopback port. Events control response completion
without timing assumptions, and bounded waits prevent a stalled test from
running indefinitely. Subprocess checks capture stdout and stderr separately.
"""

import queue
import subprocess
import sys
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import httpx
import pytest

import benchmark

WAIT_SECONDS = 5
FIRST_CHUNK = b"a" * benchmark.CHUNK_SIZE
LAST_CHUNK = b"last chunk"


class ServerState:
    """Share request observations and response gates between test and handlers."""

    def __init__(self):
        self.lock = threading.Lock()
        self.paths = []
        self.slow_count = 0
        self.started = [threading.Event() for _ in range(10)]
        self.release = [threading.Event() for _ in range(10)]
        self.errors = []


@pytest.fixture
def local_server():
    """Provide a local HTTP/1.1 server with controlled bodies and failure routes.

    Each slow response sends a full initial chunk, then waits for its release
    event before sending the final bytes. Teardown releases every gate, shuts
    down the listener, and joins the server thread with a bounded wait.
    """
    state = ServerState()

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, format, *args):
            pass

        def do_GET(self):
            with state.lock:
                state.paths.append(self.path)
            if self.path == "/redirect":
                self.send_response(302)
                self.send_header("Location", "/empty")
                self.send_header("Content-Length", "8")
                self.end_headers()
                self.wfile.write(b"redirect")
                self.wfile.flush()
                return
            if self.path == "/empty":
                self.send_response(200)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            if self.path == "/invalid-redirect-port":
                self.send_response(302)
                self.send_header(
                    "Location",
                    f"http://127.0.0.1:{self.server.server_port + 65536}/empty",
                )
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            if self.path == "/broken":
                self.send_response(200)
                self.send_header("Content-Length", "100")
                self.end_headers()
                self.wfile.write(b"cut")
                self.wfile.flush()
                self.close_connection = True
                return

            with state.lock:
                number = state.slow_count
                state.slow_count += 1
            if number >= 10:
                self.send_error(500)
                return
            self.send_response(200)
            self.send_header("Content-Length", str(len(FIRST_CHUNK) + len(LAST_CHUNK)))
            self.end_headers()
            try:
                self.wfile.write(FIRST_CHUNK)
                self.wfile.flush()
                state.started[number].set()
                if not state.release[number].wait(WAIT_SECONDS):
                    with state.lock:
                        state.errors.append(
                            f"Timed out releasing response {number + 1}"
                        )
                    self.close_connection = True
                    return
                self.wfile.write(LAST_CHUNK)
                self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError) as exc:
                with state.lock:
                    state.errors.append(str(exc))

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    host, port = server.server_address
    try:
        yield f"http://{host}:{port}", state
    finally:
        for release in state.release:
            release.set()
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=WAIT_SECONDS)
        assert not server_thread.is_alive()


def test_next_request_waits_for_client_eof_and_close_for_all_ten_downloads(
    local_server, monkeypatch
):
    """Verify that client EOF and closure precede every following request.

    A wrapper around the real HTTP transport records sends, iterator exhaustion,
    and closure. Before releasing each final body segment, the test confirms
    that its stream has not reached EOF and the next response has not started.
    The final event order and byte totals cover the complete series of ten.
    """
    url, state = local_server
    order = []
    eof = [threading.Event() for _ in range(10)]
    measurements = []

    class ObservedStream(httpx.SyncByteStream):
        def __init__(self, stream, number):
            self.stream = stream
            self.number = number
            self.closed = False

        def __iter__(self) -> Iterator[bytes]:
            yield from self.stream
            order.append(("eof", self.number))
            eof[self.number].set()

        def close(self):
            self.stream.close()
            if not self.closed:
                self.closed = True
                order.append(("close", self.number))

    class ObservedTransport(httpx.BaseTransport):
        def __init__(self):
            self.inner = httpx.HTTPTransport()
            self.count = 0
            self.closed = False

        def handle_request(self, request):
            number = self.count
            self.count += 1
            order.append(("send", number))
            response = self.inner.handle_request(request)
            response.stream = ObservedStream(response.stream, number)
            return response

        def close(self):
            self.inner.close()
            self.closed = True

    transport = ObservedTransport()
    real_client = httpx.Client

    def client_factory(**kwargs):
        return real_client(transport=transport, trust_env=False, **kwargs)

    monkeypatch.setattr(benchmark.httpx, "Client", client_factory)
    outcome = queue.Queue()

    def run():
        try:
            result = benchmark.run_benchmark(
                f"{url}/slow", timeout=WAIT_SECONDS, on_result=measurements.append
            )
        except BaseException as exc:
            outcome.put(exc)
        else:
            outcome.put(result)

    worker = threading.Thread(target=run, daemon=True)
    worker.start()
    try:
        for number in range(10):
            assert state.started[number].wait(WAIT_SECONDS)
            assert not eof[number].is_set(), "Client reached EOF before final bytes"
            if number < 9:
                assert not state.started[number + 1].is_set()
            state.release[number].set()
            assert eof[number].wait(WAIT_SECONDS)
        worker.join(timeout=WAIT_SECONDS)
        assert not worker.is_alive()
        result = outcome.get(timeout=WAIT_SECONDS)
        if isinstance(result, BaseException):
            raise result
    finally:
        for release in state.release:
            release.set()
        worker.join(timeout=WAIT_SECONDS)

    assert state.errors == []
    assert state.paths == ["/slow"] * 10
    assert transport.closed
    assert order == [
        (event, number) for number in range(10) for event in ("send", "eof", "close")
    ]
    body_length = len(FIRST_CHUNK) + len(LAST_CHUNK)
    assert len(measurements) == 10
    assert all(
        measurement.bytes_received == body_length for measurement in measurements
    )
    assert result.total_bytes == body_length * 10
    assert result.average_duration_ms > 0


def run_cli(url, *flags):
    """Run the script with the active interpreter and capture both output streams.

    A 15-second subprocess deadline bounds failure cases. The completed process
    is returned without converting a nonzero exit status into an exception.
    """
    script = Path(benchmark.__file__).resolve()
    return subprocess.run(
        [sys.executable, str(script), url, *flags],
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )


def test_cli_subprocess_follows_redirects_and_reports_ten_empty_downloads(local_server):
    url, state = local_server

    completed = run_cli(f"{url}/redirect", "--plain", "--animate")

    assert completed.returncode == 0, completed.stderr
    assert completed.stderr == ""
    assert state.paths == ["/redirect", "/empty"] * 10
    assert completed.stdout.count("Request ") == 10
    assert "Requests: 10/10" in completed.stdout
    assert "Total downloaded: 0.000 MiB (0 bytes)" in completed.stdout
    assert "Download speed: 0.000 MiB/s" in completed.stdout
    assert "\x1b" not in completed.stdout


def test_cli_subprocess_reports_truncated_body_to_stderr(local_server):
    url, state = local_server

    completed = run_cli(f"{url}/broken")

    assert completed.returncode == 1
    assert state.paths == ["/broken"]
    assert completed.stdout == ""
    assert "Request 1/10 failed:" in completed.stderr
    assert "Traceback" not in completed.stderr


def test_cli_rejects_wrapping_port_without_contacting_server(local_server):
    url, state = local_server
    actual = httpx.URL(url)
    invalid_url = str(actual.copy_with(port=actual.port + 65536, path="/empty"))

    completed = run_cli(invalid_url, "--plain")

    assert completed.returncode == 2
    assert state.paths == []
    assert completed.stdout == ""
    assert "port" in completed.stderr.lower()
    assert "Traceback" not in completed.stderr


def test_cli_rejects_redirect_port_before_sending_next_request(local_server):
    url, state = local_server

    completed = run_cli(f"{url}/invalid-redirect-port", "--plain")

    assert completed.returncode == 1
    assert state.paths == ["/invalid-redirect-port"]
    assert completed.stdout == ""
    assert "Request 1/10 failed:" in completed.stderr
    assert "port" in completed.stderr.lower()
    assert "Traceback" not in completed.stderr


def test_cli_rejects_excessive_timeout_without_traceback_or_requests(local_server):
    url, state = local_server

    completed = run_cli(f"{url}/empty", "--timeout", "1e300", "--plain")

    assert completed.returncode == 2
    assert state.paths == []
    assert completed.stdout == ""
    assert "timeout" in completed.stderr.lower()
    assert "Traceback" not in completed.stderr
