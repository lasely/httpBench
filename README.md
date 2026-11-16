# HTTP Bench

A Python command-line tool that downloads an HTTP(S) resource ten times in
sequence and reports average request time, total downloaded bytes, and aggregate
download throughput. Each response is fully read and closed before the next
request starts. Response bodies are streamed without saving files or retaining
complete downloads in memory.

Interactive output includes a PyFiglet heading, an animated Rich spinner,
progress, a table of individual measurements, and a final summary. Plain output
is available for logs and automated runs.

## Requirements

- Python 3.12 or newer.
- [uv](https://docs.astral.sh/uv/) for the recommended setup, or Python's pip and
  venv modules for a standard installation.
- Network access to the resource being measured.

Run the following commands from the repository root.

## Installation with uv

```console
uv sync --locked
uv run --locked python benchmark.py "https://proof.ovh.net/files/10Mb.dat"
```

`uv sync --locked` creates `.venv` and installs the versions recorded in
`uv.lock`, including development tools. uv can download a compatible Python
interpreter when one is not installed.

The example resource contains 10 MiB. A successful run downloads it ten times,
transferring 100 MiB of final response bodies.

## Installation with pip

Create a virtual environment using Python 3.12 or newer:

```console
python -m venv .venv
```

On Windows PowerShell, install and run with the environment's interpreter:

```powershell
.\.venv\Scripts\python.exe -m pip install .
.\.venv\Scripts\python.exe benchmark.py "https://proof.ovh.net/files/10Mb.dat"
```

On Linux or macOS:

```sh
.venv/bin/python -m pip install .
.venv/bin/python benchmark.py "https://proof.ovh.net/files/10Mb.dat"
```

These commands do not require environment activation. pip installs compatible
dependency versions from `pyproject.toml`. Use uv for the versions pinned in
`uv.lock`.

Installation also provides the `http-download-benchmark` entry point inside the
virtual environment. After activating that environment, it accepts the same
arguments as `python benchmark.py`.

## Usage

```text
python benchmark.py URL [--timeout SECONDS] [--plain] [--animate | --no-animate]
```

| Argument | Description |
| --- | --- |
| `URL` | Absolute HTTP or HTTPS URL with a host. An explicit port must be between 0 and 65535. |
| `--timeout SECONDS` | Positive, finite network operation timeout supported by the platform. Defaults to 30 seconds. |
| `--plain` | Print ordinary text without the heading, colors, borders, or live updates. |
| `--animate` | Enable the spinner during downloads. Enabled by default in an interactive terminal. |
| `--no-animate` | Disable background animation and refresh the styled report only between downloads. |
| `-h`, `--help` | Show command-line help and exit. |

The number of logical downloads is fixed at ten. Each starts from the original
URL, including any redirects encountered on earlier downloads.

### Output modes

The default interactive report animates while waiting for each download. Its
table and completed-download progress advance after the full response arrives.
A narrow terminal uses a text heading in place of the ASCII banner.

Use `--no-animate` to keep the styled table while avoiding background terminal
refreshes during measurement:

```console
uv run --locked python benchmark.py "https://proof.ovh.net/files/10Mb.dat" --no-animate
```

Use `--plain` for ordinary text:

```console
uv run --locked python benchmark.py "https://proof.ovh.net/files/10Mb.dat" --plain --timeout 15
```

When stdout is redirected, plain mode is selected automatically. `--plain`
always disables animation, including when `--animate` is supplied. Results go to
stdout and errors go to stderr:

```console
uv run --locked python benchmark.py "https://proof.ovh.net/files/10Mb.dat" > results.txt 2> errors.txt
```

An abbreviated plain report with illustrative measurements:

```text
Request 1/10: time=500.000 ms, downloaded=1048576 bytes, speed=2.000 MiB/s
...
Request 10/10: time=500.000 ms, downloaded=1048576 bytes, speed=2.000 MiB/s
Requests: 10/10
Average time: 500.000 ms
Total downloaded: 10.000 MiB (10485760 bytes)
Download speed: 2.000 MiB/s
```

## Units and calculations

Time is displayed in milliseconds, size in mebibytes (MiB), and throughput in
mebibytes per second (MiB/s). **1 MiB = 1,048,576 bytes.** The summary also shows
the exact total byte count.

For download `i`, let `b_i` be the final response body size in bytes and `t_i`
the measured duration in seconds:

```text
Average time (ms) = sum(t_i) / 10 * 1000
Total downloaded (bytes) = sum(b_i)
Download speed (MiB/s) = sum(b_i) / sum(t_i) / 1,048,576
```

The summary uses total bytes divided by total time. It does not average the
individual request speeds. Values are rounded to three decimal places only
when displayed.

An empty successful response counts as a completed download and has zero
throughput when its duration is positive. If a measured interval is zero, the
corresponding speed is displayed as `n/a`. The same rule applies to the summary
when the sum of all measured intervals is zero.

To compare the result with decimal megabits per second, use:

```text
Mbps = MiB/s * 8.388608
```

## Measurement method

- One synchronous `httpx.Client` handles all ten downloads. There are no warmup
  requests, automatic retries, or concurrent downloads.
- The client can reuse a connection when the server permits it. The first
  download can include DNS lookup, TCP connection, and TLS negotiation costs
  that subsequent downloads avoid. The first measurement remains in the result.
- Each logical download follows up to five redirects. Ten logical downloads
  can therefore produce more than ten HTTP requests. Redirect responses are
  streamed and discarded. Their bodies do not count toward downloaded bytes.
- `perf_counter_ns()` measures from immediately before the initial request is
  sent until the final body has been fully read. The interval includes response
  latency, redirect requests, and reading intermediate response bodies.
- Bodies are read through `iter_raw()` in chunks of 64 KiB. The client requests
  `Accept-Encoding: identity`. If a server still applies content encoding, the
  count reflects the encoded body bytes received, without decompression.
- Only final response bodies count toward volume. HTTP headers, transfer
  framing, intermediate bodies, and TCP/TLS overhead are excluded.
- The next download starts after the previous response has been fully read and
  closed. Responses and the client are also closed when a download fails.
- Explicit report updates occur outside the measured intervals. Default
  background animation performs terminal work during downloads and can affect
  the result. Use `--no-animate` or `--plain` to disable that work.
- `--timeout` limits individual network operations, including waiting for the
  next incoming body data. It is not a total deadline for a download. A server
  can continue sending data for longer than the configured value while each
  operation remains within its timeout.

The result describes downloading a particular resource from a particular
server. Response latency, resource size, server limits, CDN behavior, caching,
and current network load all affect it. A small resource can spend much of its
measured interval waiting for a response. A test using multiple simultaneous
connections can produce a different result from these sequential downloads.

## Errors and exit codes

A failed HTTP status, network error, timeout, incomplete body, invalid redirect
target, or redirect limit stops the run at that download. stderr identifies the
request number and reason. Completed rows remain in stdout, but an incomplete
run does not print a successful summary. Partial bodies are not reported as
successful measurements.

| Exit code | Meaning |
| --- | --- |
| `0` | All ten downloads completed successfully. |
| `1` | Execution failed. |
| `2` | Command-line arguments are invalid. |
| `130` | The user interrupted the run with `Ctrl+C`. |

## Development and verification

```console
uv sync --locked
uv run --locked pytest -q
uv run --locked ruff check .
uv run --locked ruff format --check .
```

pytest runs both the test suite and executable docstring examples in
`benchmark.py`. Tests use mock transports and a local HTTP server. They do not
download public resources or require internet access once dependencies are
installed.

The streaming integration test starts a threaded HTTP server on `127.0.0.1`
with a free port. For every response, the server sends an initial chunk and
gates the final bytes with a `threading.Event`. The test observes client stream
exhaustion and closure, verifies that both precede the next request, and checks
all ten downloads and their exact total size. Waits are bounded and the server
is closed during fixture teardown.

Additional checks cover aggregate calculations, empty bodies, redirect limits,
streaming of large redirect bodies, HTTP and network failures, timeouts,
truncated bodies, resource cleanup, argument validation, animation, plain
output, and separation of stdout and stderr.

## Project files

| File | Purpose |
| --- | --- |
| `benchmark.py` | CLI, streamed measurements, statistics, and terminal reporting in separate functions and classes. |
| `tests/test_benchmark.py` | Deterministic calculations, HTTP behavior, validation, cleanup, and rendering tests. |
| `tests/test_integration.py` | Real local HTTP streaming and CLI subprocess tests. |
| `pyproject.toml` | Package metadata, dependencies, entry point, pytest, and Ruff configuration. |
| `uv.lock` | Pinned dependency versions for uv. |
| `.gitignore` | Excludes virtual environments, caches, bytecode, and build artifacts. |

Runtime dependencies are HTTPX, Rich, and PyFiglet. Development uses pytest,
Ruff, and uv.
