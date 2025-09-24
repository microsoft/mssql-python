#!/usr/bin/env python3
"""
DockyBot CLI - Minimal Docker abstraction for multi-platform testing.
"""

import typer
from rich.console import Console
from rich.table import Table
from .docker_runner import DockerRunner
from .platforms import PLATFORMS, list_platforms

app = typer.Typer(help="DockyBot - Run tests across platforms with Docker")
console = Console()

@app.command()
def test(
    platform: str = typer.Argument(..., help="Platform to test on"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show detailed output")
):
    """Run tests on specified platform."""
    if platform not in PLATFORMS:
        console.print(f"[red]Error:[/red] Unknown platform '{platform}'")
        console.print(f"Available platforms: {', '.join(PLATFORMS.keys())}")
        raise typer.Exit(1)
    
    runner = DockerRunner()
    success = runner.run_platform_test(platform, verbose=verbose)
    
    if not success:
        raise typer.Exit(1)

@app.command()
def platforms():
    """List available platforms."""
    table = Table(title="Available Platforms")
    table.add_column("Platform", style="cyan")
    table.add_column("Name", style="green")
    table.add_column("Description", style="white")
    
    for platform_id, config in list_platforms().items():
        table.add_row(platform_id, config['name'], config['description'])
    
    console.print(table)

@app.command()
def test_all(
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show detailed output")
):
    """Run tests on all platforms."""
    runner = DockerRunner()
    results = {}
    
    for platform in PLATFORMS.keys():
        console.print(f"\n[cyan]Testing {platform}...[/cyan]")
        results[platform] = runner.run_platform_test(platform, verbose=verbose)
    
    # Summary
    console.print("\n[bold]Test Results Summary:[/bold]")
    for platform, success in results.items():
        status = "[green]✓ PASSED[/green]" if success else "[red]✗ FAILED[/red]"
        console.print(f"  {platform}: {status}")
    
    failed_count = sum(1 for success in results.values() if not success)
    if failed_count > 0:
        console.print(f"\n[red]{failed_count} platform(s) failed[/red]")
        raise typer.Exit(1)
    else:
        console.print("\n[green]All platforms passed![/green]")

if __name__ == "__main__":
    app()

import typer
from typing import Optional
from rich.console import Console
from rich.panel import Panel
from pathlib import Path

from .docker_client import DockerClient

app = typer.Typer(
    name="dockybot",
    help="🤖 DockyBot - DevOps-as-Code testing tool using Docker",
    no_args_is_help=True,
    rich_markup_mode="rich"
)

console = Console()


@app.command()
def test(
    cache: bool = typer.Option(
        True,
        "--cache/--no-cache", 
        help="Enable/disable pip and build artifact caching"
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Enable verbose output"
    )
):
    """
    Run tests on Ubuntu using Docker (replicates exact PR pipeline steps).
    """
    console.print(Panel.fit(
        f"🚀 [bold blue]DockyBot Ubuntu Test Runner[/bold blue]\n"
        f"Platform: Ubuntu 22.04\n"
        f"Caching: {'✅ Enabled' if cache else '❌ Disabled'}",
        border_style="blue"
    ))
    
    console.print("🔄 [bold yellow]Testing on Ubuntu...[/bold yellow]")
    
    # Create Docker client and run tests
    try:
        client = DockerClient(verbose=verbose, cache_enabled=cache)
        success = client.run_tests(platform="ubuntu")
        
        if success:
            console.print("[bold green]✅ All tests passed![/bold green]")
        else:
            console.print("[bold red]❌ Tests failed![/bold red]")
            raise typer.Exit(1)
            
    except Exception as e:
        console.print(f"[red]❌ Error: {e}[/red]")
        raise typer.Exit(1)


@app.command()
def clean():
    """
    Clean up Docker cache and containers.
    """
    console.print("🧹 [bold yellow]Cleaning Docker cache...[/bold yellow]")
    
    try:
        client = DockerClient()
        client.clean_cache()
    except Exception as e:
        console.print(f"[red]❌ Error cleaning cache: {e}[/red]")
        raise typer.Exit(1)


@app.command()
def version():
    """
    Show DockyBot version information.
    """
    try:
        from . import __version__
        console.print(f"DockyBot v{__version__} (Docker-based)")
    except ImportError:
        console.print("DockyBot v1.0.0 (Docker-based)")


def main():
    """Entry point for the CLI."""
    app()


if __name__ == "__main__":
    main()