# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.
"""
CLI entry point: python -m profiler [options]
"""

import argparse
import sys

from profiler import Profiler
from profiler.scenarios import SCENARIOS


def main():
    parser = argparse.ArgumentParser(
        prog="python -m profiler",
        description="mssql-python performance profiler — Python + C++ instrumentation",
    )
    parser.add_argument(
        "--conn-str",
        help="Connection string (or set DB_CONNECTION_STRING env var)",
    )
    parser.add_argument(
        "--scenarios",
        nargs="+",
        metavar="NAME",
        choices=list(SCENARIOS.keys()),
        help=f"Scenarios to run (default: all). Choices: {', '.join(SCENARIOS.keys())}",
    )
    parser.add_argument(
        "--script",
        metavar="FILE",
        help="Run a custom .py script. The script gets `conn` and `cursor` injected.",
    )
    parser.add_argument(
        "--timeline",
        action="store_true",
        help="Show chronological timeline instead of aggregate stats",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List available scenarios and exit",
    )
    args = parser.parse_args()

    if args.list:
        print("Available scenarios:")
        for name in SCENARIOS:
            print(f"  {name}")
        return

    try:
        with Profiler(args.conn_str, timeline=args.timeline) as p:
            if args.script:
                p.run_script(args.script)
            elif args.scenarios:
                p.run(*args.scenarios)
            else:
                p.run()
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
