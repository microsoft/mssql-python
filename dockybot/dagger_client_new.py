"""
DockyBot Dagger Client - Ubuntu test pipeline

Clean implementation that uses localhost SQL Server.
"""

import asyncio
import dagger
from rich.console import Console
from rich.panel import Panel

console = Console()


class DaggerClient:
    """Dagger client for running Ubuntu test pipeline."""

    def __init__(self, verbose: bool = False, cache_enabled: bool = True):
        self.verbose = verbose
        self.cache_enabled = cache_enabled

    def run_tests(self, platform: str = "ubuntu") -> bool:
        """Entry point for running tests on a given platform (only ubuntu supported)."""
        if platform != "ubuntu":
            raise ValueError("Only Ubuntu platform is supported for now")
        return asyncio.run(self._run_tests_async())

    async def _run_tests_async(self) -> bool:
        """Run the Ubuntu PR pipeline inside Dagger with localhost SQL Server."""

        async with dagger.Connection(dagger.Config(log_output=None)) as client:
            try:
                console.print("[bold green]🐳 Starting Dagger engine...[/bold green]")

                # Workspace
                workspace = client.host().directory(
                    ".",
                    exclude=[".git/", "__pycache__/", "*.pyc", "venv/", "build/", "dist/"],
                )

                # Create Ubuntu container for testing
                console.print("🐧 Creating Ubuntu test container...")
                container = (
                    client.container()
                    .from_("ubuntu:22.04")
                    .with_workdir("/workspace")
                    .with_directory("/workspace", workspace)
                )

                # Run complete pipeline in one step
                console.print("🔄 Running complete pipeline...")
                
                pipeline_script = """
set -euxo pipefail

# Install system dependencies
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y python3 python3-pip python3-venv cmake build-essential curl wget gnupg python3-dev pybind11-dev

# Install ODBC driver
curl -sSL -O https://packages.microsoft.com/config/ubuntu/22.04/packages-microsoft-prod.deb
dpkg -i packages-microsoft-prod.deb || true
apt-get update
ACCEPT_EULA=Y apt-get install -y msodbcsql18 mssql-tools18 unixodbc-dev

# Setup Python environment
python3 -m venv /opt/venv
source /opt/venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

# Build C++ extensions
cd mssql_python/pybind
chmod +x build.sh
./build.sh
cd /workspace

# Run tests with localhost SQL Server
export DB_CONNECTION_STRING="Driver=ODBC Driver 18 for SQL Server;Server=host.docker.internal,1433;Database=master;UID=sa;Pwd=Str0ng@Passw0rd123;Encrypt=no;TrustServerCertificate=yes;"
python -m pytest -v --tb=short --cache-clear
"""

                result = await container.with_exec(["bash", "-c", pipeline_script]).sync()

                console.print(
                    Panel.fit(
                        "[bold green]🎉 All tests completed successfully![/bold green]",
                        border_style="green",
                        title="Success",
                    )
                )
                return True

            except dagger.ExecError as e:
                console.print(f"[red]❌ Pipeline failed[/red]")
                console.print(f"[yellow]STDOUT:[/yellow]\n{e.stdout}")
                console.print(f"[red]STDERR:[/red]\n{e.stderr}")
                return False
            except Exception as e:
                console.print(f"[red]❌ Unexpected error: {e}[/red]")
                return False

    def clean_cache(self):
        """Placeholder for cleaning caches (Dagger manages volumes internally)."""
        console.print("🧹 Cache cleanup not implemented — use `dagger engine reset` if needed.")