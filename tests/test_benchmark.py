"""Verify calculations, streaming, resource cleanup, CLI validation, and rendering.

Mock transports expose body consumption and closure without external network
access. Controlled clocks keep numerical expectations deterministic, while a
gated response verifies that terminal animation advances during a body read.
"""

import gzip
import io
import threading
from collections.abc import Iterator

import httpx
import pytest
from rich.console import Console

import benchmark


class TrackedStream(httpx.SyncByteStream):
    """Expose stream exhaustion and closure, with an optional injected failure."""

    def __init__(self, chunks: list[bytes], error: Exception | None = None):
        self.chunks = chunks
        self.error = error
        self.consumed = False
        self.closed = False

    def __iter__(self) -> Iterator[bytes]:
        yield from self.chunks
        if self.error is not None:
            raise self.error
        self.consumed = True

    def close(self) -> None:
        self.closed = True


class TrackedTransport(httpx.MockTransport):
    """Record transport closure to verify the owning client's cleanup path."""

    closed = False

    def close(self) -> None:
        self.closed = True


@pytest.fixture
def install_transport(monkeypatch):
    """Inject a mock transport into real clients and return the created instances.

    The runner still constructs and owns its client. Tests can inspect client
    configuration and closure after success or a propagated request failure.
    """
    real_client = httpx.Client

    def install(transport):
        clients = []

        def client_factory(**kwargs):
            client = real_client(transport=transport, **kwargs)
            clients.append(client)
            return client

        monkeypatch.setattr(benchmark.httpx, "Client", client_factory)
        return clients

    return install


def test_statistics_use_total_bytes_over_total_time_without_rounding():
    results = [
        benchmark.RequestResult(i, i * i * benchmark.NS_PER_SECOND, i * benchmark.MIB)
        for i in range(1, 11)
    ]

    summary = benchmark.calculate_statistics(results)

    assert summary.average_duration_ms == 38_500
    assert summary.total_bytes == 55 * benchmark.MIB
    assert summary.throughput_mib_s == pytest.approx(1 / 7)
    individual_mean = sum(result.speed_mib_s for result in results) / 10
    assert summary.throughput_mib_s != pytest.approx(individual_mean)


def test_statistics_require_a_complete_run():
    with pytest.raises(ValueError, match="10 complete downloads"):
        benchmark.calculate_statistics([benchmark.RequestResult(1, 1, 1)])


@pytest.mark.parametrize("byte_count", [0, 100])
def test_zero_clock_interval_has_no_defined_speed(byte_count):
    results = [benchmark.RequestResult(i, 0, byte_count) for i in range(1, 11)]

    assert results[0].speed_mib_s is None
    assert benchmark.calculate_statistics(results).throughput_mib_s is None
    assert benchmark.format_speed(None) == "n/a"


def test_measurement_includes_headers_body_and_cleanup_but_not_reporting(monkeypatch):
    events = []
    ticks = iter([100, 2_000_100])

    def clock():
        events.append("clock")
        return next(ticks)

    class Stream(httpx.SyncByteStream):
        def __iter__(self):
            events.append("body")
            yield b"first"
            yield b"last"
            events.append("eof")

        def close(self):
            events.append("close")

    def handle(request):
        events.append("request")
        return httpx.Response(200, stream=Stream())

    monkeypatch.setattr(benchmark, "perf_counter_ns", clock)
    with httpx.Client(transport=httpx.MockTransport(handle)) as client:
        result = benchmark.measure_request(client, "https://example.test/file", 1)

    assert result.duration_ns == 2_000_000
    assert result.bytes_received == 9
    assert events == ["clock", "request", "body", "eof", "close", "clock"]


def test_ten_gets_use_one_client_and_fully_consume_raw_bodies(install_transport):
    streams = []
    payload = gzip.compress(b"payload" * 100)
    callbacks = []

    def handle(request):
        assert request.method == "GET"
        assert str(request.url) == "https://example.test/file"
        assert request.headers["Accept-Encoding"] == "identity"
        if streams:
            assert streams[-1].consumed
            assert streams[-1].closed
        stream = TrackedStream([payload[:5], payload[5:]])
        streams.append(stream)
        return httpx.Response(200, headers={"Content-Encoding": "gzip"}, stream=stream)

    transport = TrackedTransport(handle)
    clients = install_transport(transport)

    def on_start(number):
        if streams:
            assert streams[-1].closed
        callbacks.append(("start", number))

    def on_result(result):
        assert streams[-1].closed
        callbacks.append(("result", result.number))

    result = benchmark.run_benchmark(
        "https://example.test/file", on_start=on_start, on_result=on_result
    )

    assert len(clients) == 1
    assert clients[0].is_closed
    assert transport.closed
    assert len(streams) == 10
    assert all(stream.consumed and stream.closed for stream in streams)
    assert result.total_bytes == 10 * len(payload)
    assert callbacks == [
        (event, number) for number in range(1, 11) for event in ("start", "result")
    ]


def test_ten_empty_successful_responses_have_zero_speed(install_transport, monkeypatch):
    transport = TrackedTransport(
        lambda request: httpx.Response(204, stream=TrackedStream([]))
    )
    install_transport(transport)
    ticks = iter(range(20))
    monkeypatch.setattr(benchmark, "perf_counter_ns", lambda: next(ticks))

    result = benchmark.run_benchmark("https://example.test/empty")

    assert result.total_bytes == 0
    assert result.throughput_mib_s == 0
    assert result.average_duration_ms == 0.000001


def test_redirects_restart_from_original_url_and_only_count_final_body(
    install_transport,
):
    urls = []
    streams = []

    def handle(request):
        urls.append(request.url.path)
        if request.url.path == "/start":
            stream = TrackedStream([b"intermediate response"])
            streams.append(stream)
            return httpx.Response(302, headers={"Location": "/file"}, stream=stream)
        stream = TrackedStream([b"done"])
        streams.append(stream)
        return httpx.Response(200, stream=stream)

    clients = install_transport(TrackedTransport(handle))

    result = benchmark.run_benchmark("https://example.test/start")

    assert urls == ["/start", "/file"] * 10
    assert result.total_bytes == 40
    assert clients[0].max_redirects == 5
    assert all(stream.consumed and stream.closed for stream in streams)


def test_redirect_loop_is_bounded_and_closes_client(install_transport):
    requests = []
    streams = []

    def handle(request):
        requests.append(request)
        stream = TrackedStream([b"redirect"])
        streams.append(stream)
        return httpx.Response(302, headers={"Location": "/loop"}, stream=stream)

    clients = install_transport(TrackedTransport(handle))

    with pytest.raises(benchmark.BenchmarkError, match="Request 1/10") as error:
        benchmark.run_benchmark("https://example.test/loop")

    assert isinstance(error.value.__cause__, httpx.TooManyRedirects)
    assert len(requests) == 6
    assert all(stream.closed for stream in streams)
    assert clients[0].is_closed


def test_five_redirects_are_allowed_for_each_logical_download(install_transport):
    paths = []

    def handle(request):
        paths.append(request.url.path)
        hop = int(request.url.path.removeprefix("/"))
        if hop < 5:
            return httpx.Response(
                302,
                headers={"Location": f"/{hop + 1}"},
                stream=TrackedStream([b"redirect body"]),
            )
        return httpx.Response(200, stream=TrackedStream([b"done"]))

    install_transport(httpx.MockTransport(handle))

    result = benchmark.run_benchmark("https://example.test/0")

    assert paths == [f"/{hop}" for hop in range(6)] * 10
    assert result.total_bytes == 40


def test_large_redirect_body_is_consumed_and_closed_without_buffering():
    """Reject eager redirect buffering while allowing full streamed consumption.

    A large generated body must be exhausted and closed without materializing
    ``Response.content``. Only the final response contributes downloaded bytes.
    """

    class LargeStream(httpx.SyncByteStream):
        consumed = False
        closed = False

        def __iter__(self):
            for _ in range(320):
                yield b"x" * benchmark.CHUNK_SIZE
            self.consumed = True

        def close(self):
            self.closed = True

    body = LargeStream()
    redirect = httpx.Response(302, headers={"Location": "/final"}, stream=body)
    final = httpx.Response(200, stream=TrackedStream([b"done"]))

    def handle(request):
        if request.url.path == "/start":
            return redirect
        assert body.consumed and body.closed
        return final

    with httpx.Client(
        transport=httpx.MockTransport(handle), follow_redirects=True
    ) as client:
        result = benchmark.measure_request(client, "https://example.test/start", 1)

    assert result.bytes_received == 4
    assert body.consumed and body.closed
    for response in (redirect, final):
        with pytest.raises(httpx.ResponseNotRead):
            _ = response.content


@pytest.mark.parametrize("status", [404, 500])
def test_http_failure_stops_run_and_cleans_up(install_transport, status):
    streams = []

    def handle(request):
        stream = TrackedStream([b"error"])
        streams.append(stream)
        return httpx.Response(status, stream=stream)

    transport = TrackedTransport(handle)
    clients = install_transport(transport)
    results = []

    with pytest.raises(benchmark.BenchmarkError, match=f"Request 1/10.*{status}"):
        benchmark.run_benchmark("https://example.test/file", on_result=results.append)

    assert results == []
    assert len(streams) == 1
    assert streams[0].closed
    assert clients[0].is_closed
    assert transport.closed


@pytest.mark.parametrize(
    "error_type", [httpx.ReadTimeout, httpx.ConnectError, httpx.RemoteProtocolError]
)
def test_partial_body_or_network_error_is_not_a_measurement(
    install_transport, error_type
):
    stream = TrackedStream([b"x" * benchmark.CHUNK_SIZE], error_type("network failed"))
    transport = TrackedTransport(lambda request: httpx.Response(200, stream=stream))
    clients = install_transport(transport)
    results = []

    with pytest.raises(benchmark.BenchmarkError, match="Request 1/10.*network failed"):
        benchmark.run_benchmark("https://example.test/file", on_result=results.append)

    assert results == []
    assert not stream.consumed
    assert stream.closed
    assert clients[0].is_closed
    assert transport.closed


@pytest.mark.parametrize(
    "flags",
    [
        [],
        ["--plain"],
        ["--plain", "--animate"],
        ["--animate"],
        ["--no-animate"],
        ["--plain", "--no-animate"],
    ],
)
def test_redirected_stdout_is_plain_and_errors_are_empty(
    install_transport, monkeypatch, capsys, flags
):
    install_transport(
        httpx.MockTransport(
            lambda request: httpx.Response(200, stream=TrackedStream([b"abc"]))
        )
    )

    def unexpected_figlet(*args, **kwargs):
        pytest.fail("Plain mode must not render a Figlet title")

    monkeypatch.setattr(benchmark, "figlet_format", unexpected_figlet)

    assert benchmark.main(["https://example.test/file", *flags]) == 0
    captured = capsys.readouterr()
    assert captured.err == ""
    assert captured.out.count("Request ") == 10
    assert "Requests: 10/10" in captured.out
    assert "(30 bytes)" in captured.out
    assert "Download speed:" in captured.out
    assert "\x1b" not in captured.out
    assert "\r" not in captured.out


def test_cli_stops_on_fourth_failure_and_keeps_errors_in_stderr(
    install_transport, capsys
):
    requests = []

    def handle(request):
        requests.append(request)
        return httpx.Response(
            500 if len(requests) == 4 else 200, stream=TrackedStream([b"abc"])
        )

    clients = install_transport(httpx.MockTransport(handle))

    assert benchmark.main(["https://example.test/file", "--plain"]) == 1
    captured = capsys.readouterr()
    assert len(requests) == 4
    assert captured.out.count("Request ") == 3
    assert "Requests: 10/10" not in captured.out
    assert "Download speed:" not in captured.out
    assert "Error" not in captured.out
    assert "Request 4/10 failed:" in captured.err
    assert "500" in captured.err
    assert "Traceback" not in captured.err
    assert clients[0].is_closed


def test_keyboard_interrupt_returns_130_and_cleans_up(install_transport, capsys):
    def handle(request):
        raise KeyboardInterrupt

    transport = TrackedTransport(handle)
    clients = install_transport(transport)

    assert benchmark.main(["https://example.test/file"]) == 130
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == "Interrupted by user.\n"
    assert clients[0].is_closed
    assert transport.closed


@pytest.mark.parametrize(
    "url",
    ["", "example.com/file", "ftp://example.com/file", "http://", "http://host:bad"],
)
def test_invalid_urls_exit_two_without_stdout(url, capsys):
    with pytest.raises(SystemExit) as error:
        benchmark.main([url])

    assert error.value.code == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "error:" in captured.err


@pytest.mark.parametrize("port", [65536, 127119, 999999999999999999999])
def test_out_of_range_ports_are_rejected_before_running_benchmark(
    monkeypatch, capsys, port
):
    def unexpected_run(*args, **kwargs):
        pytest.fail("An invalid port must be rejected before any network requests")

    monkeypatch.setattr(benchmark, "run_benchmark", unexpected_run)
    with pytest.raises(SystemExit) as error:
        benchmark.main([f"http://127.0.0.1:{port}/", "--plain"])

    assert error.value.code == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "port" in captured.err.lower()


@pytest.mark.parametrize("port", [0, 65535])
def test_boundary_ports_are_valid(port):
    value = f"http://127.0.0.1:{port}/"
    assert benchmark.validate_url(value) == value


@pytest.mark.parametrize("timeout", ["0", "-1", "nan", "inf", "-inf", "wrong"])
def test_invalid_timeouts_exit_two_without_stdout(timeout, capsys):
    with pytest.raises(SystemExit) as error:
        benchmark.main(["https://example.test/file", "--timeout", timeout])

    assert error.value.code == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "error:" in captured.err


@pytest.mark.parametrize("flags", [[], ["--animate"]])
def test_interactive_cli_animates_while_waiting_for_body(
    install_transport, monkeypatch, flags
):
    pending_body = threading.Event()
    changing_frames = threading.Event()

    class Terminal(io.StringIO):
        def __init__(self):
            super().__init__()
            self.frames = set()

        def isatty(self):
            return True

        def write(self, value):
            written = super().write(value)
            if pending_body.is_set() and "Downloading request 1/10" in value:
                self.frames.add(value)
                if len(self.frames) >= 2:
                    changing_frames.set()
            return written

    class WaitingStream(httpx.SyncByteStream):
        def __iter__(self):
            pending_body.set()
            try:
                assert changing_frames.wait(3), (
                    "Spinner did not change while downloading"
                )
            finally:
                pending_body.clear()
            yield b"done"

    terminal = Terminal()
    first_request = True

    def handle(request):
        nonlocal first_request
        if first_request:
            first_request = False
            return httpx.Response(200, stream=WaitingStream())
        return httpx.Response(200, stream=TrackedStream([b"done"]))

    monkeypatch.setattr(benchmark.sys, "stdout", terminal)
    monkeypatch.setenv("TERM", "xterm-256color")
    monkeypatch.setenv("TTY_INTERACTIVE", "1")
    monkeypatch.delenv("TTY_COMPATIBLE", raising=False)
    monkeypatch.delenv("FORCE_COLOR", raising=False)
    install_transport(httpx.MockTransport(handle))

    assert benchmark.main(["https://example.test/file", *flags]) == 0
    assert changing_frames.is_set()
    assert "Results" in terminal.getvalue()


@pytest.mark.parametrize("width", [30, 100])
@pytest.mark.parametrize("animate", [False, True])
def test_interactive_report_renders_and_stops_live_updates(width, animate):
    output = io.StringIO()
    console = Console(
        file=output,
        force_terminal=True,
        force_interactive=True,
        legacy_windows=False,
        width=width,
        height=40,
    )
    report = benchmark.ConsoleReport(console, plain=False, animate=animate)
    measurements = [benchmark.RequestResult(i, 1_000_000, 0) for i in range(1, 11)]

    with report:
        for result in measurements:
            report.start_request(result.number)
            report.record_result(result)
    report.print_summary(benchmark.calculate_statistics(measurements))

    rendered = output.getvalue()
    assert "Results" in rendered
    assert "10/10" in rendered
    assert "0.000 MiB/s" in rendered
    assert not report.live.is_started
    if width == 30:
        assert "HTTP Download Benchmark" in rendered
