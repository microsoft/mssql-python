"""Benchmark: run the three DetectParamTypes implementations on realistic
parameter mixes and report ns-per-parameter for each.

Usage:  /usr/bin/python run_benchmark.py

Prerequisite: run `build.py` first to produce the .so files.
"""

from __future__ import annotations

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
# Realistic parameter mixes (matching typical mssql-python workloads)
# ---------------------------------------------------------------------------

def mix_oltp() -> list:
    """A typical OLTP row: (id, name, email, created_at, active, amount)."""
    return [
        42,                                       # int -> TINYINT/INT
        "alice@example.com",                      # str ASCII
        "Alice Anderson",                         # str ASCII
        datetime.datetime(2026, 7, 23, 12, 34, 56),  # datetime
        True,                                     # bool
        decimal.Decimal("199.99"),                # decimal in MONEY range
    ]


def mix_wide_analytics() -> list:
    """A wider analytics-style row with more types."""
    return [
        1234567,                                  # int -> INTEGER
        3.14159,                                  # float
        "orders",                                 # str
        b"\x00\x01\x02\x03",                      # bytes
        datetime.date(2026, 7, 23),               # date
        datetime.time(12, 34, 56, 789012),        # time
        None,                                     # NULL
        uuid.UUID("12345678-1234-5678-1234-567812345678"),  # UUID
        decimal.Decimal("922337203685477.5807"),  # decimal at MONEY_MAX
        "Ünïcödé strîng with émoji-free Latin-1",  # str non-ASCII
    ]


def mix_int_heavy() -> list:
    """Numeric-heavy workload: metrics, counters, IDs."""
    return [i for i in range(20)]


def mix_str_heavy() -> list:
    """String-heavy: log ingestion style."""
    return [f"row-{i:04d}-value" for i in range(20)]


def mix_decimal_heavy() -> list:
    """Financial workload: many Decimals."""
    return [decimal.Decimal(f"{i}.{i:02d}") for i in range(20)]


MIXES = {
    "OLTP (6 params, mixed)":    mix_oltp,
    "Analytics (10 params)":     mix_wide_analytics,
    "Int-heavy (20 params)":     mix_int_heavy,
    "Str-heavy (20 params)":     mix_str_heavy,
    "Decimal-heavy (20 params)": mix_decimal_heavy,
}


# ---------------------------------------------------------------------------
# Timing loop
# ---------------------------------------------------------------------------

def time_variant(fn, params: list, iters: int) -> float:
    """Return best-of-3 wall-clock seconds for `iters` calls."""
    best = float("inf")
    for _ in range(3):
        gc.collect()
        gc.disable()
        try:
            t0 = time.perf_counter_ns()
            for _ in range(iters):
                fn(params)
            t1 = time.perf_counter_ns()
        finally:
            gc.enable()
        best = min(best, (t1 - t0) / 1e9)
    return best


def calibrate_iters(fn, params: list, target_seconds: float = 0.5) -> int:
    """Adaptively pick iteration count so a run takes ~target_seconds."""
    n = 1000
    while True:
        t0 = time.perf_counter_ns()
        for _ in range(n):
            fn(params)
        elapsed = (time.perf_counter_ns() - t0) / 1e9
        if elapsed >= 0.05:
            return int(n * target_seconds / elapsed)
        n *= 10
        if n > 100_000_000:
            return n


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

def run() -> None:
    # Warm up caches
    for name, fn in VARIANTS.items():
        for params in [m() for m in MIXES.values()]:
            fn(params)

    print()
    print("=" * 90)
    print(" DetectParamTypes microbenchmark — pybind11 vs raw CPython vs nanobind")
    print("=" * 90)
    print(f" Python: {sys.version.split()[0]}   ")
    print()

    for mix_name, make_mix in MIXES.items():
        params = make_mix()
        n_params = len(params)
        iters = calibrate_iters(detect_cpython.detect, params, target_seconds=0.4)

        print(f" Workload: {mix_name}    (params/call = {n_params}, calls = {iters:,})")
        print(" " + "-" * 88)
        print(f" {'variant':<24} {'total (ms)':>12} {'ns/call':>12} {'ns/param':>12} {'vs pybind11':>14}")

        results = {}
        for name, fn in VARIANTS.items():
            secs = time_variant(fn, params, iters)
            ns_per_call = secs * 1e9 / iters
            ns_per_param = ns_per_call / n_params
            results[name] = ns_per_param
            baseline = results.get("pybind11 (baseline)", ns_per_param)
            speedup = baseline / ns_per_param if ns_per_param > 0 else 1.0
            print(f" {name:<24} {secs*1000:>12.1f} {ns_per_call:>12.1f} "
                  f"{ns_per_param:>12.1f} {speedup:>13.2f}x")
        print()

    # Also report binary sizes.
    print("=" * 90)
    print(" Binary sizes")
    print("=" * 90)
    for name in ["detect_pybind11", "detect_cpython", "detect_nanobind"]:
        so_files = list(HERE.glob(f"{name}*.so"))
        if so_files:
            sz = so_files[0].stat().st_size
            print(f"  {name:<24} {sz:>10,} bytes  ({sz/1024:>6.1f} KB)")
    print()


if __name__ == "__main__":
    run()
