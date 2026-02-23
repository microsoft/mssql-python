#!/usr/bin/env python
"""Resolve the PackageBaseAddress URL from a NuGet v3 feed index.

Reads NuGet v3 service index JSON from stdin and prints the
PackageBaseAddress endpoint URL to stdout.

Usage:
    curl -sS "$FEED_URL" | python resolve_nuget_feed.py
"""
import json
import sys


def main() -> None:
    data = json.load(sys.stdin)
    for resource in data["resources"]:
        if "PackageBaseAddress" in resource.get("@type", ""):
            print(resource["@id"])
            return
    print("ERROR: No PackageBaseAddress found in feed index", file=sys.stderr)
    sys.exit(1)


if __name__ == "__main__":
    main()
