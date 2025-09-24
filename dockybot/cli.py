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
def bash(
    platform: str = typer.Argument(..., help="Platform to bash into"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show detailed output")
):
    """Open interactive bash session in platform container with dependencies installed."""
    if platform not in PLATFORMS:
        console.print(f"[red]Error:[/red] Unknown platform '{platform}'")
        console.print(f"Available platforms: {', '.join(PLATFORMS.keys())}")
        raise typer.Exit(1)
    
    runner = DockerRunner()
    success = runner.run_platform_bash(platform, verbose=verbose)
    
    if not success:
        raise typer.Exit(1)

@app.command()
def images():
    """List DockyBot cached images."""
    runner = DockerRunner()
    runner.list_cached_images()

@app.command()  
def clean(
    platform: str = typer.Argument(None, help="Platform to clean (optional - cleans all if not specified)"),
    force: bool = typer.Option(False, "--force", "-f", help="Force removal without confirmation")
):
    """Clean DockyBot cached images."""
    runner = DockerRunner()
    runner.clean_cached_images(platform, force)

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
