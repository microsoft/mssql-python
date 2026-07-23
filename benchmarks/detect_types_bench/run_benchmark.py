"""Benchmark: run the three DetectParamTypes implementations on realistic
parameter mixes and report ns-per-parameter statistics for each.

Prerequisite: run `build.py` first to produce the .so files.

Usage:
    /usr/bin/python run_benchmark.py                # default: median of 5 runs
    /usr/bin/python run_benchmark.py --runs 9       # more samples
    /usr/bin/python run_benchmark.py --seconds 2.0  # longer per-run window

Key metrics reported per (workload, variant):
    median ns/param  — primary point estimate
    stddev           — run-to-run variance (want this < 5% of median)
    min .. max       — full range across runs
"""

from __future__ import annotations

import argparse
import datetime
import decimal
import gc
import statistics
import sys
import time
import uuid
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import detect_cpython
import detect_nanobind
import detect_pybind11

VARIANTS = {
    "pybind11 (baseline)": detect_pybind11.detect,
    "raw CPython (this PR)": detect_cpython.detect,
    "nanobind (proposed)": detect_nanobind.detect,
}


# ---------------------------------------------------------------------------
# Parameter mixes — scale up to real bulk workload sizes.
# ---------------------------------------------------------------------------

def mix_oltp(n_rows: int = 1) -> list:
    """A typical OLTP row of 6 columns, repeated to reach the desired size."""
    row = [
        42,                                       # int -> TINYINT/INT
        "alice@example.com",                      # str ASCII
        "Alice Anderson",                         # str ASCII
        datetime.datetime(2026, 7, 23, 12, 34, 56),  # datetime
        True,                                     # bool
        decimal.Decimal("199.99"),                # decimal in MONEY range
    ]
    out = []
    for _ in range(n_rows):
        out.extend(row)
    return out


def mix_analytics(n_rows: int = 1) -> list:
    """A wider analytics-style row with more types."""
    row = [
        1234567,
        3.14159,
        "orders",
        b"\x00\x01\x02\x03",
        datetime.date(2026, 7, 23),
        datetime.time(12, 34, 56, 789012),
        None,
        uuid.UUID("12345678-1234-5678-1234-567812345678"),
        decimal.Decimal("922337203685477.5807"),   # MONEY_MAX
        "Ünïcödé strîng with émoji-free Latin-1",  # non-ASCII
    ]
    out = []
    for _ in range(n_rows):
        out.extend(row)
    return out


def mix_int_heavy(n: int) -> list:
    return [i % 100000 for i in range(n)]


def mix_str_heavy(n: int) -> list:
    return [f"row-{i:06d}-value-abcdef" for i in range(n)]


def mix_decimal_heavy(n: int) -> list:
    return [decimal.Decimal(f"{i}.{(i * 7) % 100:02d}") for i in range(n)]


def mix_dae_heavy(n: int) -> list:
    """Large strings and bytes that trigger the DAE (data-at-execution) path."""
    out = []
    for i in range(n):
        if i % 2 == 0:
            out.append("x" * 5000)          # DAE string (>4000 UTF-16 code units)
        else:
            out.append(b"\xab" * 10000)     # DAE bytes (>8000 bytes)
    return out


# workload_name -> (build_fn, params_per_call)
# The size counts represent realistic "one execute() call" scales:
#   1-column single-row execute: 6 params
#   10-row executemany:          60-100 params
#   100-row executemany:         600-1000 params
#   1000-row bulk insert:        6000-10000 params
WORKLOADS = {
    "OLTP (6 params, 1 row)":            (lambda: mix_oltp(1),              6),
    "OLTP (60 params, 10 rows)":         (lambda: mix_oltp(10),             60),
    "OLTP (600 params, 100 rows)":       (lambda: mix_oltp(100),            600),
    "OLTP (6000 params, 1000 rows)":     (lambda: mix_oltp(1000),           6000),
    "Analytics (10 params, 1 row)":      (lambda: mix_analytics(1),         10),
    "Analytics (100 params, 10 rows)":   (lambda: mix_analytics(10),        100),
    "Analytics (1000 params, 100 rows)": (lambda: mix_analytics(100),       1000),
    "Int-heavy (100 params)":            (lambda: mix_int_heavy(100),       100),
    "Int-heavy (1000 params)":           (lambda: mix_int_heavy(1000),      1000),
    "Str-heavy (100 params)":            (lambda: mix_str_heavy(100),       100),
    "Str-heavy (1000 params)":           (lambda: mix_str_heavy(1000),      1000),
    "Decimal-heavy (100 params)":        (lambda: mix_decimal_heavy(100),   100),
    "Decimal-heavy (1000 params)":       (lambda: mix_decimal_heavy(1000),  1000),
    "DAE-heavy (100 large params)":      (lambda: mix_dae_heavy(100),       100),
    "DAE-heavy (1000 large params)":     (lambda: mix_dae_heavy(1000),      1000),
}


# ---------------------------------------------------------------------------
# Timing loop
# ---------------------------------------------------------------------------

def one_run(fn, params: list, iters: int) -> float:
    """Return wall-clock seconds for `iters` calls with GC disabled."""
    gc.collect()
    gc.disable()
    try:
        t0 = time.perf_counter_ns()
        for _ in range(iters):
            fn(params)
        t1 = time.perf_counter_ns()
    finally:
        gc.enable()
    return (t1 - t0) / 1e9


def calibrate_iters(fn, params: list, target_seconds: float) -> int:
    """Adaptively pick iteration count so a run takes ~target_seconds."""
    n = 100
    while True:
        t0 = time.perf_counter_ns()
        for _ in range(n):
            fn(params)
        elapsed = (time.perf_counter_ns() - t0) / 1e9
        if elapsed >= 0.05:
            iters = int(n * target_seconds / max(elapsed, 1e-9))
            return max(iters, 3)
        n *= 10
        if n > 200_000_000:
            return n


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

def run(runs: int, target_seconds: float) -> None:
    # Warm up caches for every variant on every workload.
    for _, fn in VARIANTS.items():
        for build_fn, _ in WORKLOADS.values():
            fn(build_fn())

    print()
    print("=" * 96)
    print(f" DetectParamTypes microbenchmark — median of {runs} runs, "
          f"~{target_seconds:.1f}s per run")
    print("=" * 96)
    print(f" Python: {sys.version.split()[0]}")
    print()

    # Header
    hdr = (f" {'variant':<24} {'median ns/param':>16} "
           f"{'stddev':>10} {'min':>10} {'max':>10} {'vs pybind11':>14}")
    for workload_name, (build_fn, n_params) in WORKLOADS.items():
        params = build_fn()
        # Calibrate on raw CPython as the timing anchor so iteration counts
        # are the same across variants.
        iters = calibrate_iters(detect_cpython.detect, params, target_seconds)

        print(f" Workload: {workload_name}   "
              f"(params/call = {n_params}, calls/run = {iters:,})")
        print(" " + "-" * 94)
        print(hdr)

        variant_medians: dict[str, float] = {}
        for name, fn in VARIANTS.items():
            samples = []
            for _ in range(runs):
                secs = one_run(fn, params, iters)
                ns_per_param = secs * 1e9 / iters / n_params
                samples.append(ns_per_param)
            med = statistics.median(samples)
            stdev = statistics.stdev(samples) if len(samples) > 1 else 0.0
            mn = min(samples)
            mx = max(samples)
            variant_medians[name] = med
            baseline_med = variant_medians.get("pybind11 (baseline)", med)
            speedup = baseline_med / med if med > 0 else 1.0
            print(f" {name:<24} {med:>16.2f} {stdev:>10.2f} "
                  f"{mn:>10.2f} {mx:>10.2f} {speedup:>13.2f}x")
        print()

    print("=" * 96)
    print(" Binary sizes")
    print("=" * 96)
    for name in ["detect_pybind11", "detect_cpython", "detect_nanobind"]:
        so_files = list(HERE.glob(f"{name}*.so"))
        if so_files:
            sz = so_files[0].stat().st_size
            print(f"  {name:<24} {sz:>10,} bytes  ({sz/1024:>6.1f} KB)")
    print()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=5,
                    help="samples per (workload, variant) — default 5")
    ap.add_argument("--seconds", type=float, default=1.0,
                    help="approximate wall-clock target per run — default 1.0s")
    args = ap.parse_args()
    run(runs=args.runs, target_seconds=args.seconds)


if __name__ == "__main__":
    main()
