#!/usr/bin/env python3
"""Backward-compatible shim for the packaged CLI."""

from jvlink_raw_fetcher.cli import main


if __name__ == "__main__":
    raise SystemExit(main())
