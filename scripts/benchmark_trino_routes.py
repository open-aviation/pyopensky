"""Compare separate route queries at a Git revision with one routes query.

Run from the repository root, for example:
    uv run --env-file .secrets scripts/benchmark_trino_routes.py \
        --baseline-ref origin/master --output /tmp/trino-routes-benchmark.json

This runs live Trino queries. Every query uses cached=False and an isolated,
temporary result-cache directory. Server and storage caches are not controlled.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
import subprocess
import time
import types
from contextlib import contextmanager
from importlib.metadata import version
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Iterable, Iterator
from unittest.mock import patch

from sqlalchemy import CursorResult

import pandas as pd
import pyopensky.trino as current
from pyopensky.trino import Trino

COLUMNS = ("time", "icao24", "callsign", "lat", "lon", "baroaltitude")
DEFAULT_ROUTES = [("EGLL", "KJFK"), ("LFPG", "KJFK"), ("EHAM", "KJFK")]


@contextmanager
def instrument(client: Trino, records: list[dict[str, Any]]) -> Iterator[None]:
    process_result = client.process_result

    def record(
        result: CursorResult[Any], batch_size: int = 50_000
    ) -> Iterable[pd.DataFrame]:
        cursor = result.cursor
        yield from process_result(result, batch_size)
        assert cursor is not None
        records.append(dict(cursor.stats))

    with patch.object(client, "process_result", record):
        yield


def run(
    client: Trino,
    mode: str,
    start: str,
    stop: str,
    routes: list[tuple[str, str]],
) -> tuple[pd.DataFrame, float]:
    frames = []
    begin = time.perf_counter()
    if mode == "before":
        for departure, arrival in routes:
            frame = client.history(
                start,
                stop,
                departure_airport=departure,
                arrival_airport=arrival,
                selected_columns=COLUMNS,
                cached=False,
            )
            if frame is not None:
                frames.append(frame)
    else:
        frame = client.history(
            start, stop, routes=routes, selected_columns=COLUMNS, cached=False
        )
        if frame is not None:
            frames.append(frame)
    result = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    return result, time.perf_counter() - begin


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-ref", required=True)
    parser.add_argument("--start", default="2025-06-01 12:00")
    parser.add_argument("--stop", default="2025-06-01 13:00")
    parser.add_argument("--route", action="append", help="DEPARTURE:ARRIVAL")
    parser.add_argument("--rounds", type=int, default=2)
    parser.add_argument(
        "--after-session-property",
        action="append",
        default=[],
        help="NAME=VALUE; configure the batched client only (repeatable)",
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.rounds < 1:
        parser.error("--rounds must be at least 1")
    session_properties = {}
    for setting in args.after_session_property:
        name, separator, value = setting.partition("=")
        if not separator or not name or not value:
            parser.error(
                "--after-session-property must have the form NAME=VALUE"
            )
        session_properties[name] = value
    routes = DEFAULT_ROUTES
    if args.route:
        routes = []
        for route in args.route:
            parts = route.split(":")
            if len(parts) != 2 or not all(parts):
                parser.error("--route must have the form DEPARTURE:ARRIVAL")
            routes.append((parts[0], parts[1]))
    routes = list(dict.fromkeys(routes))

    baseline_commit = subprocess.check_output(
        ["git", "rev-parse", "--verify", f"{args.baseline_ref}^{{commit}}"],
        text=True,
    ).strip()
    source = subprocess.check_output(
        ["git", "show", f"{baseline_commit}:src/pyopensky/trino.py"], text=True
    )
    baseline = types.ModuleType("pyopensky._routes_benchmark_before")
    baseline.__package__ = "pyopensky"
    exec(
        compile(source, f"{baseline_commit}:trino.py", "exec"),
        baseline.__dict__,
    )

    report: dict[str, Any] = {
        "baseline_commit": baseline_commit,
        "after_source_sha256": hashlib.sha256(
            Path(current.__file__).read_bytes()
        ).hexdigest(),
        "started_at_utc": pd.Timestamp.now(tz="UTC").isoformat(),
        "start": args.start,
        "stop": args.stop,
        "timezone": "UTC",
        "routes": routes,
        "selected_columns": COLUMNS,
        "cached": False,
        "after_session_properties": session_properties,
        "cache_directory": "isolated temporary directory",
        "server_and_storage_caches": "not controlled",
        "python": platform.python_version(),
        "packages": {
            name: version(name) for name in ("pyopensky", "trino", "sqlalchemy")
        },
        "runs": [],
    }
    with TemporaryDirectory(prefix="pyopensky-routes-benchmark-") as directory:
        current.cache_path = Path(directory)
        setattr(baseline, "cache_path", Path(directory))
        after = Trino(session_properties=session_properties)
        after.token()  # Authenticate before timing either workload.
        before = baseline.Trino()
        before._token = after._token
        clients = {"before": before, "after": after}
        reference = None
        for round_number in range(args.rounds):
            order = (
                ("before", "after")
                if round_number % 2 == 0
                else ("after", "before")
            )
            for mode in order:
                records: list[dict[str, Any]] = []
                client = clients[mode]
                with instrument(client, records):
                    frame, seconds = run(
                        client, mode, args.start, args.stop, routes
                    )
                canonical = (
                    frame.sort_values(list(COLUMNS)).reset_index(drop=True)
                    if len(frame)
                    else frame
                )
                if reference is None:
                    reference = canonical
                else:
                    pd.testing.assert_frame_equal(reference, canonical)
                assert not list(Path(directory).iterdir()), "Cache was written"
                run_report = {
                    "round": round_number + 1,
                    "mode": mode,
                    "rows": len(frame),
                    "seconds": seconds,
                    "query_count": len(records),
                    "physical_input_bytes": sum(
                        record["physicalInputBytes"] for record in records
                    ),
                    "cpu_ms": sum(
                        record["cpuTimeMillis"] for record in records
                    ),
                    "queries": records,
                }
                report["runs"].append(run_report)
                args.output.write_text(json.dumps(report, indent=2))
                print(
                    json.dumps(
                        {k: v for k, v in run_report.items() if k != "queries"}
                    ),
                    flush=True,
                )
    report["identical_results"] = True
    report["result_cache_files_written"] = 0
    args.output.write_text(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
