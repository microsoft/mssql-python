#!/usr/bin/env python3
"""
Setup script for DockyBot package
"""

from setuptools import setup, find_packages
from pathlib import Path

# Read README for long description
readme_path = Path(__file__).parent / "dockybot" / "README.md"
long_description = ""
if readme_path.exists():
    long_description = readme_path.read_text(encoding="utf-8")

setup(
    name="dockybot",
    version="0.1.0",
    description="DevOps-as-Code CLI for cross-platform testing with Dagger",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Microsoft",
    packages=find_packages(),
    python_requires=">=3.8",
    install_requires=[
        "typer[all]>=0.9.0",
        "dagger-io>=0.9.0", 
        "rich>=13.0.0",
    ],
    entry_points={
        "console_scripts": [
            "dockybot=dockybot.cli:main",
        ],
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Software Development :: Testing",
        "Topic :: System :: Systems Administration",
    ],
)