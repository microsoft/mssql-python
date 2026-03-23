"""
Benchmark: Credential Instance Caching for Azure AD Authentication

Measures the performance difference between:
  1. Creating a new DefaultAzureCredential + get_token() each call (old behavior)
  2. Reusing a cached DefaultAzureCredential instance (new behavior)

Prerequisites:
  - pip install azure-identity azure-core
  - az login  (for AzureCliCredential to work)

Usage:
  python benchmarks/bench_credential_cache.py
"""

from __future__ import annotations

import time
import statistics


def bench_no_cache(n: int) -> list[float]:
    """Simulate the OLD behavior: new credential per call."""
    from azure.identity import DefaultAzureCredential

    times = []
    for _ in range(n):
        start = time.perf_counter()
        cred = DefaultAzureCredential()
        cred.get_token("https://database.windows.net/.default")
        times.append(time.perf_counter() - start)
    return times


def bench_with_cache(n: int) -> list[float]:
    """Simulate the NEW behavior: reuse a single credential instance."""
    from azure.identity import DefaultAzureCredential

    cred = DefaultAzureCredential()
    times = []
    for _ in range(n):
        start = time.perf_counter()
        cred.get_token("https://database.windows.net/.default")
        times.append(time.perf_counter() - start)
    return times


def report(label: str, times: list[float]) -> None:
    print(f"\n{'=' * 50}")
    print(f"  {label}")
    print(f"{'=' * 50}")
    print(f"  Calls:   {len(times)}")
    print(f"  Total:   {sum(times):.3f}s")
    print(f"  Mean:    {statistics.mean(times) * 1000:.1f}ms")
    print(f"  Median:  {statistics.median(times) * 1000:.1f}ms")
    print(f"  Stdev:   {statistics.stdev(times) * 1000:.1f}ms" if len(times) > 1 else "")
    print(f"  Min:     {min(times) * 1000:.1f}ms")
    print(f"  Max:     {max(times) * 1000:.1f}ms")


def main() -> None:
    N = 10  # number of calls to benchmark

    print("Credential Instance Cache Benchmark")
    print(f"Running {N} sequential token acquisitions for each scenario...\n")

    try:
        print(">>> Without cache (new credential each call)...")
        no_cache_times = bench_no_cache(N)
        report("WITHOUT credential cache (old behavior)", no_cache_times)

        print("\n>>> With cache (reuse credential instance)...")
        cache_times = bench_with_cache(N)
        report("WITH credential cache (new behavior)", cache_times)

        speedup = statistics.mean(no_cache_times) / statistics.mean(cache_times)
        saved = (statistics.mean(no_cache_times) - statistics.mean(cache_times)) * 1000
        print(f"\n{'=' * 50}")
        print(f"  SPEEDUP: {speedup:.1f}x  ({saved:.0f}ms saved per call)")
        print(f"{'=' * 50}")
    except Exception as e:
        print(f"\nBenchmark failed: {e}")
        print("Make sure you are logged in via 'az login' and have azure-identity installed.")


if __name__ == "__main__":
    main()
