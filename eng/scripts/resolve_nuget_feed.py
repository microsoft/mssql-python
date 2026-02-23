#!/usr/bin/env python
"""Resolve the PackageBaseAddress URL from a NuGet v3 feed index.

Fetches the NuGet v3 service index from the given feed URL and prints
the PackageBaseAddress endpoint URL to stdout.

Usage:
    python resolve_nuget_feed.py <feed_url>
"""
import json
import sys
import urllib.request


def resolve(feed_url: str) -> str:
    """Fetch the feed index and return the PackageBaseAddress URL.

    NuGet v3 feeds expose a service index (index.json) listing available
    API resources. The JSON looks like:

        {
          "version": "3.0.0",
          "resources": [
            {
              "@id": "https://pkgs.dev.azure.com/.../nuget/v3/flat2/",
              "@type": "PackageBaseAddress/3.0.0"
            },
            {
              "@id": "https://pkgs.dev.azure.com/.../query",
              "@type": "SearchQueryService"
            }
            ...
          ]
        }

    The PackageBaseAddress resource provides a flat container URL for
    downloading .nupkg files by convention:
        {base}/{id}/{version}/{id}.{version}.nupkg

    We need this base URL because we download the mssql-py-core-wheels
    nupkg directly via HTTP rather than using the NuGet CLI.
    """
    with urllib.request.urlopen(feed_url) as resp:
        data = json.loads(resp.read())
    for resource in data["resources"]:
        if "PackageBaseAddress" in resource.get("@type", ""):
            return resource["@id"]
    raise ValueError("No PackageBaseAddress found in feed index")


def main() -> None:
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <feed_url>", file=sys.stderr)
        sys.exit(2)
    try:
        print(resolve(sys.argv[1]))
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
