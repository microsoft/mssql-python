"""
DockyBot Dagger Client - Ubuntu test pipeline

Robust implementation with proper timeout handling.
"""

import asyncio
import signal
import sys
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
        
        # Set up signal handler for graceful termination
        def signal_handler(signum, frame):
            console.print("\n[yellow]⚠️ Received interrupt signal. Cleaning up...[/yellow]")
            sys.exit(1)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        try:
            return asyncio.run(asyncio.wait_for(self._run_tests_async(), timeout=1800))  # 30 min timeout
        except asyncio.TimeoutError:
            console.print("[red]❌ Pipeline timed out after 30 minutes[/red]")
            return False
        except KeyboardInterrupt:
            console.print("\n[yellow]⚠️ Operation cancelled by user[/yellow]")
            return False

    async def _run_tests_async(self) -> bool:
        """Run the Ubuntu PR pipeline inside Dagger with localhost SQL Server."""
        
        # Simple non-hanging approach - no persistent connections
        try:
            console.print("[bold green]🐳 Starting Dagger engine...[/bold green]")
            
            # Create connection with shorter timeout
            config = dagger.Config(log_output=sys.stderr, timeout=600)  # 10 min timeout
            
            async with dagger.Connection(config) as client:
                # Workspace
                workspace = client.host().directory(
                    ".",
                    exclude=[".git/", "__pycache__/", "*.pyc", "venv/", "build/", "dist/"],
                )

                # Create and run container in one shot
                console.print("🐧 Running Ubuntu pipeline...")
                
                # Simplified pipeline script
                pipeline_script = '''
#!/bin/bash
set -euo pipefail

echo "🔧 Installing system dependencies..."
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq python3 python3-pip python3-venv cmake build-essential curl wget gnupg python3-dev

echo "📀 Installing ODBC driver..."
curl -sSL https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl -sSL https://packages.microsoft.com/config/ubuntu/22.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
apt-get update -qq
ACCEPT_EULA=Y apt-get install -y -qq msodbcsql18 mssql-tools18 unixodbc-dev

echo "🐍 Setting up Python environment..."
python3 -m venv /opt/venv
source /opt/venv/bin/activate
pip install --upgrade pip -q
pip install -r requirements.txt -q

echo "🔨 Building C++ extensions..."
cd mssql_python/pybind
chmod +x build.sh
./build.sh
cd /workspace

echo "🧪 Running tests..."
export DB_CONNECTION_STRING="Driver=ODBC Driver 18 for SQL Server;Server=host.docker.internal,1433;Database=master;UID=sa;Pwd=Str0ng@Passw0rd123;Encrypt=no;TrustServerCertificate=yes;"
python -m pytest tests/ -v --tb=short --maxfail=5 -x

echo "✅ Pipeline completed successfully!"
'''

                # Execute with timeout
                result = await asyncio.wait_for(
                    client.container()
                    .from_("ubuntu:22.04")
                    .with_workdir("/workspace")
                    .with_directory("/workspace", workspace)
                    .with_exec(["bash", "-c", pipeline_script])
                    .sync(),
                    timeout=1200  # 20 minute timeout for the container execution
                )

                console.print(
                    Panel.fit(
                        "[bold green]🎉 All tests completed successfully![/bold green]",
                        border_style="green",
                        title="Success",
                    )
                )
                return True

        except asyncio.TimeoutError:
            console.print("[red]❌ Container execution timed out[/red]")
            return False
        except dagger.ExecError as e:
            console.print(f"[red]❌ Pipeline failed[/red]")
            if self.verbose and e.stdout:
                console.print(f"[yellow]STDOUT:[/yellow]\n{e.stdout[:2000]}...")
            if e.stderr:
                console.print(f"[red]STDERR:[/red]\n{e.stderr[:1000]}...")
            return False
        except Exception as e:
            console.print(f"[red]❌ Unexpected error: {e}[/red]")
            return False

    def clean_cache(self):
        """Clean Dagger cache."""
        console.print("🧹 To clean Dagger cache, run: dagger engine reset")