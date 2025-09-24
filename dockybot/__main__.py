#!/usr/bin/env python3
"""
DockyBot package main entry point

Allows running: python -m dockybot
"""

from .cli import app

if __name__ == "__main__":
    app()