"""Measure ten sequential HTTP downloads and report their aggregate throughput.

The synchronous measurement functions consume response bodies without retaining
them in memory. Immutable result objects carry unrounded measurements into the
statistics and presentation functions. Terminal rendering is managed separately
so that explicit report updates happen outside the measured intervals.

Run this module as a script or call ``main`` with a sequence of CLI arguments.
The examples in the calculation and validation docstrings require no network.
"""

import argparse
import math
import socket
import sys
from collections.abc import Callable, Sequence
from contextlib import closing
from dataclasses import dataclass
from time import perf_counter_ns

import httpx
from pyfiglet import figlet_format
from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.progress_bar import ProgressBar
from rich.spinner import Spinner
from rich.table import Table
from rich.text import Text

REQUEST_COUNT = 10
CHUNK_SIZE = 64 * 1024
MIB = 1024 * 1024
NS_PER_SECOND = 1_000_000_000
DEFAULT_TIMEOUT = 30.0
MAX_REDIRECTS = 5


def throughput_mib_s(byte_count: int, duration_ns: int) -> float | None:
    """Convert a body size and elapsed nanoseconds to mebibytes per second.

    Args:
        byte_count: Nonnegative number of raw response body bytes received.
        duration_ns: Nonnegative elapsed interval from the monotonic clock.

    Returns:
        The unrounded rate using 1 MiB = 1,048,576 bytes, or ``None`` when
        the interval is zero. An empty body with a positive interval has
        a rate of zero.

    Examples:
        >>> throughput_mib_s(1_048_576, 500_000_000)
        2.0
        >>> throughput_mib_s(0, 500_000_000)
        0.0
        >>> throughput_mib_s(100, 0) is None
        True
    """
    if duration_ns == 0:
        return None
    return byte_count * NS_PER_SECOND / duration_ns / MIB


@dataclass(frozen=True)
class RequestResult:
    """Store the complete measurement of one successful logical download.

    Attributes:
        number: One-based position within the series of ten downloads.
        duration_ns: Elapsed nanoseconds including redirects and body reads.
        bytes_received: Raw body bytes from the final response only.

    Examples:
        >>> result = RequestResult(1, 250_000_000, 1_048_576)
        >>> result.duration_ms
        250.0
        >>> result.speed_mib_s
        4.0
    """

    number: int
    duration_ns: int
    bytes_received: int

    @property
    def duration_ms(self) -> float:
        """Return the measured interval in milliseconds without rounding."""
        return self.duration_ns / 1_000_000

    @property
    def speed_mib_s(self) -> float | None:
        """Return body throughput, or ``None`` for a zero measured interval."""
        return throughput_mib_s(self.bytes_received, self.duration_ns)


@dataclass(frozen=True)
class BenchmarkResult:
    """Store the aggregate metrics of ten completed downloads.

    Attributes:
        average_duration_ms: Arithmetic mean of the ten elapsed intervals.
        total_bytes: Sum of raw body bytes from the ten final responses.
        throughput_mib_s: Total mebibytes divided by total elapsed seconds,
            or ``None`` when the combined measured interval is zero.

    Values retain their calculation precision until the report formats them.
    """

    average_duration_ms: float
    total_bytes: int
    throughput_mib_s: float | None


class BenchmarkError(Exception):
    """Identify a failed download and provide its position and reason.

    The runner chains the original HTTPX exception when raising this error.
    The message is suitable for stderr and includes the one-based request
    number. An incomplete download never becomes a ``RequestResult``.
    """

    def __init__(self, number: int, reason: str) -> None:
        """Build an error message from the download number and failure reason."""
        self.number = number
        super().__init__(f"Request {number}/{REQUEST_COUNT} failed: {reason}")


def measure_request(client: httpx.Client, url: str, number: int) -> RequestResult:
    """Stream one logical GET download and measure its complete response time.

    Timing starts after building and validating the initial request, immediately
    before sending it. It ends after the final raw body iterator is exhausted.
    Redirect requests and their body reads are included in the interval. Each
    response is closed before this function returns or propagates an exception.

    Redirects are followed explicitly through HTTPX's ``next_request``. This
    avoids the intermediate body buffering performed by automatic redirects.
    Intermediate bodies are consumed and discarded in chunks of 64 KiB. Only
    raw bytes from the successful final response contribute to the result.

    Args:
        client: Open synchronous client owned and closed by the caller.
        url: Original HTTP(S) URL for this logical download.
        number: One-based position used in the returned measurement.

    Returns:
        A complete measurement with unrounded nanoseconds and byte count.

    Raises:
        httpx.HTTPError: A request, body read, status check, or redirect limit
            fails. At most five redirect transitions are followed.
        httpx.InvalidURL: The original URL or a redirect target is invalid.
    """
    byte_count = 0
    request = client.build_request("GET", url)
    check_http_url(request.url)
    redirects = 0
    started_ns = perf_counter_ns()
    while True:
        with closing(
            client.send(request, stream=True, follow_redirects=False)
        ) as response:
            next_request = response.next_request
            if next_request is None:
                response.raise_for_status()
            else:
                check_http_url(next_request.url)
            for chunk in response.iter_raw(chunk_size=CHUNK_SIZE):
                if next_request is None:
                    byte_count += len(chunk)
            if next_request is None:
                duration_ns = perf_counter_ns() - started_ns
                return RequestResult(number, duration_ns, byte_count)
            if redirects == MAX_REDIRECTS:
                raise httpx.TooManyRedirects(
                    "Exceeded maximum allowed redirects.", request=request
                )
            redirects += 1
            request = next_request


def calculate_statistics(results: Sequence[RequestResult]) -> BenchmarkResult:
    """Calculate aggregate metrics from exactly ten successful measurements.

    Average duration is the sum of nanoseconds divided by ten and converted
    to milliseconds. Throughput uses total bytes divided by total elapsed
    time, rather than averaging the individual request speeds. No values
    are rounded during calculation.

    Args:
        results: The ten complete measurements from a benchmark run.

    Returns:
        Mean duration, total bytes, and aggregate throughput in MiB/s.

    Raises:
        ValueError: The sequence does not contain exactly ten measurements.

    Examples:
        >>> measurements = [
        ...     RequestResult(i, 500_000_000, 1_048_576)
        ...     for i in range(1, 11)
        ... ]
        >>> summary = calculate_statistics(measurements)
        >>> summary.average_duration_ms
        500.0
        >>> summary.total_bytes
        10485760
        >>> summary.throughput_mib_s
        2.0
    """
    if len(results) != REQUEST_COUNT:
        raise ValueError(f"Statistics require {REQUEST_COUNT} complete downloads.")
    duration_ns = sum(result.duration_ns for result in results)
    byte_count = sum(result.bytes_received for result in results)
    return BenchmarkResult(
        average_duration_ms=duration_ns / REQUEST_COUNT / 1_000_000,
        total_bytes=byte_count,
        throughput_mib_s=throughput_mib_s(byte_count, duration_ns),
    )


def run_benchmark(
    url: str,
    timeout: float = DEFAULT_TIMEOUT,
    *,
    on_start: Callable[[int], None] | None = None,
    on_result: Callable[[RequestResult], None] | None = None,
) -> BenchmarkResult:
    """Complete ten sequential downloads using one synchronous HTTPX client.

    Each download starts from the original URL. There are no warmup requests,
    automatic retries, or parallel downloads. The next request starts only
    after the preceding response has been fully read and closed. The client
    sends ``Accept-Encoding: identity`` and closes on success or failure.

    Args:
        url: Validated HTTP(S) address of the resource to download.
        timeout: Per-operation network timeout in seconds, including waits
            for incoming body data. This is not a total download deadline.
        on_start: Optional callback receiving the one-based request number
            before measurement begins.
        on_result: Optional callback receiving a complete measurement after
            its response is closed and timing has ended.

    Returns:
        Aggregate statistics after all ten downloads complete successfully.

    Raises:
        BenchmarkError: A download fails. The error identifies its number
            and preserves the original HTTPX exception as its cause.

    Callback exceptions and user interruptions propagate to the caller.
    """
    results = []
    with httpx.Client(
        timeout=timeout,
        follow_redirects=False,
        max_redirects=MAX_REDIRECTS,
        headers={"Accept-Encoding": "identity"},
    ) as client:
        for number in range(1, REQUEST_COUNT + 1):
            if on_start is not None:
                on_start(number)
            try:
                result = measure_request(client, url, number)
            except (httpx.HTTPError, httpx.InvalidURL) as exc:
                reason = str(exc) or type(exc).__name__
                raise BenchmarkError(number, reason) from exc
            results.append(result)
            if on_result is not None:
                on_result(result)
    return calculate_statistics(results)


def format_speed(speed: float | None) -> str:
    """Format a rate to three decimal places, including its MiB/s unit.

    An undefined rate is represented by ``n/a``. Rounding is confined to this
    display string and does not modify the stored measurement.

    Examples:
        >>> format_speed(1.23456)
        '1.235 MiB/s'
        >>> format_speed(0.0)
        '0.000 MiB/s'
        >>> format_speed(None)
        'n/a'
    """
    return "n/a" if speed is None else f"{speed:.3f} MiB/s"


class ConsoleReport:
    """Render completed measurements as plain text or a Rich terminal report.

    The context manager owns the live display and stops it even when a download
    fails. Plain mode prints permanent lines without decoration. In terminal
    mode, explicit refreshes happen in the start and result callbacks. Optional
    background animation refreshes the spinner four times per second and may
    affect the measured throughput through terminal work during downloads.

    The report stores at most the ten small result objects. It performs no
    network operations and does not calculate the benchmark summary.
    """

    def __init__(self, console: Console, *, plain: bool, animate: bool) -> None:
        """Configure the output destination and disable animation in plain mode."""
        self.console = console
        self.plain = plain
        self.animate = animate and not plain
        self.results: list[RequestResult] = []
        self.current: int | None = None
        self.live: Live | None = None

    def __enter__(self) -> "ConsoleReport":
        """Print the heading and start the live display when decoration is enabled.

        A plain heading replaces the PyFiglet banner when the terminal is too
        narrow. Plain mode creates no live display or background refresh task.
        """
        if not self.plain:
            title = figlet_format("HTTP Bench", font="small", width=200).rstrip()
            if max(map(len, title.splitlines()), default=0) > self.console.width:
                title = "HTTP Download Benchmark"
            self.console.print(Text(title, style="bold cyan"))
            self.live = Live(
                self.render(),
                console=self.console,
                auto_refresh=self.animate,
                refresh_per_second=4,
                redirect_stdout=False,
                redirect_stderr=False,
            )
            self.live.start(refresh=True)
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        """Finish the display with completed rows and propagate any active error."""
        if self.live is not None:
            self.current = None
            self.live.update(self.render(), refresh=True)
            self.live.stop()

    def render(self) -> Group:
        """Build the status, completed-download progress, and measurement table.

        This method constructs renderables without writing to the console.
        Progress advances only after a complete response has been recorded.
        """
        table = Table(expand=True)
        table.add_column("#", justify="right")
        table.add_column("Time (ms)", justify="right")
        table.add_column("Size (MiB)", justify="right")
        table.add_column("Speed", justify="right")
        for result in self.results:
            table.add_row(
                str(result.number),
                f"{result.duration_ms:.3f}",
                f"{result.bytes_received / MIB:.3f}",
                format_speed(result.speed_mib_s),
            )
        if self.current is not None:
            status = Text(f"Downloading request {self.current}/{REQUEST_COUNT}")
            indicator = Spinner("dots", text=status) if self.animate else status
        else:
            indicator = Text(f"Completed {len(self.results)}/{REQUEST_COUNT} downloads")
        return Group(
            indicator,
            ProgressBar(total=REQUEST_COUNT, completed=len(self.results)),
            table,
        )

    def start_request(self, number: int) -> None:
        """Show the active download before its measured interval begins."""
        self.current = number
        if self.live is not None:
            self.live.update(self.render(), refresh=True)

    def record_result(self, result: RequestResult) -> None:
        """Append and display a measurement after its response has been closed.

        Plain output includes exact bytes and is flushed after each result.
        Styled output refreshes the completed rows and progress in place.
        """
        self.results.append(result)
        self.current = None
        if self.plain:
            print(
                f"Request {result.number}/{REQUEST_COUNT}: "
                f"time={result.duration_ms:.3f} ms, "
                f"downloaded={result.bytes_received} bytes, "
                f"speed={format_speed(result.speed_mib_s)}",
                file=self.console.file,
                flush=True,
            )
        elif self.live is not None:
            self.live.update(self.render(), refresh=True)

    def print_summary(self, result: BenchmarkResult) -> None:
        """Print aggregate metrics supplied by a successfully completed run.

        The caller invokes this after the live display stops. Plain output
        uses labelled lines, while styled output places the metrics in a panel.
        Both include the exact total byte count alongside its MiB value.
        """
        metrics = (
            ("Requests", f"{REQUEST_COUNT}/{REQUEST_COUNT}"),
            ("Average time", f"{result.average_duration_ms:.3f} ms"),
            (
                "Total downloaded",
                f"{result.total_bytes / MIB:.3f} MiB ({result.total_bytes} bytes)",
            ),
            ("Download speed", format_speed(result.throughput_mib_s)),
        )
        if self.plain:
            for label, value in metrics:
                print(f"{label}: {value}", file=self.console.file)
        else:
            table = Table.grid(padding=(0, 2))
            for label, value in metrics:
                table.add_row(Text(label), Text(value, style="bold"))
            self.console.print(Panel(table, title="Results", border_style="green"))


def check_http_url(url: httpx.URL) -> None:
    """Check the scheme, host, and port of an already parsed HTTPX URL.

    Both initial URLs and redirect targets must use HTTP or HTTPS and contain
    a host. Explicit ports must be within 0 through 65535 to prevent platform
    socket behavior from accepting an unintended port. No connection is opened.

    Raises:
        httpx.InvalidURL: A URL component violates these requirements.
    """
    if url.scheme not in {"http", "https"} or not url.host:
        raise httpx.InvalidURL("URL must use http:// or https:// with a host.")
    if url.port is not None and not 0 <= url.port <= 65535:
        raise httpx.InvalidURL("URL port must be between 0 and 65535.")


def validate_url(value: str) -> str:
    """Parse a CLI URL and return HTTPX's normalized string representation.

    Invalid syntax, unsupported schemes, missing hosts, and invalid port ranges
    become ``argparse.ArgumentTypeError`` for standard CLI argument reporting.
    Validation performs no network requests.

    Examples:
        >>> validate_url('https://example.com/file.bin')
        'https://example.com/file.bin'
    """
    try:
        url = httpx.URL(value)
        check_http_url(url)
        return str(url)
    except httpx.InvalidURL as exc:
        raise argparse.ArgumentTypeError(f"Invalid URL: {exc}") from exc


def validate_timeout(value: str) -> float:
    """Convert a CLI timeout to a positive finite value supported by sockets.

    An unconnected temporary socket checks whether the platform can represent
    the value and is immediately closed. Invalid input raises
    ``argparse.ArgumentTypeError`` before any benchmark request is sent.

    Examples:
        >>> validate_timeout('2.5')
        2.5
    """
    try:
        timeout = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Timeout must be a positive number.") from exc
    if not math.isfinite(timeout) or timeout <= 0:
        raise argparse.ArgumentTypeError("Timeout must be a positive finite number.")
    try:
        with socket.socket() as probe:
            probe.settimeout(timeout)
    except OverflowError as exc:
        raise argparse.ArgumentTypeError(
            "Timeout is too large for this platform."
        ) from exc
    return timeout


def build_parser() -> argparse.ArgumentParser:
    """Define the URL, operation timeout, output mode, and animation options.

    The timeout defaults to 30 seconds. Animation is requested by default, but
    ``main`` selects plain output for redirected stdout, which disables it.
    Argument type validators run during parsing before a client is created.
    """
    parser = argparse.ArgumentParser(
        description="Measure ten sequential, fully consumed HTTP downloads."
    )
    parser.add_argument("url", type=validate_url, metavar="URL")
    parser.add_argument(
        "--timeout",
        type=validate_timeout,
        default=DEFAULT_TIMEOUT,
        metavar="SECONDS",
        help="network operation timeout in seconds (default: 30, no total deadline)",
    )
    parser.add_argument("--plain", action="store_true", help="print undecorated text")
    parser.add_argument(
        "--animate",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="animate by default, --no-animate refreshes only between requests",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Parse arguments, run the benchmark, and report its process exit status.

    Args:
        argv: CLI arguments excluding the executable name, or ``None`` to use
            ``sys.argv``. Stdout receives results and stderr receives errors.

    Returns:
        Zero after ten successful downloads, one on a handled execution error,
        or 130 after a user interruption. Failed runs retain completed rows but
        do not print a successful summary.

    Raises:
        SystemExit: Argument parsing exits with zero for help or two for invalid
            arguments, following argparse's standard behavior.
    """
    args = build_parser().parse_args(argv)
    console = Console(file=sys.stdout)
    report = ConsoleReport(
        console,
        plain=args.plain or not console.is_terminal,
        animate=args.animate,
    )
    try:
        with report:
            result = run_benchmark(
                args.url,
                args.timeout,
                on_start=report.start_request,
                on_result=report.record_result,
            )
        report.print_summary(result)
    except BenchmarkError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("Interrupted by user.", file=sys.stderr)
        return 130
    except OSError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
