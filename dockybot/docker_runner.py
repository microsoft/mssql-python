"""
Minimal Docker abstraction f        # Print welcome message
        console.print(f"\n🚀 [bold green]Starting DockyBot Test Suite[/bold green]")
        console.print(f"🐳 [bold blue]Platform:[/bold blue] {platfo        elif "requirement        elif "building" in line_lower and ("c++" in line_lower or "extensions" in line_lower or "pybind" in line_lower):
            return "🔨 Building C++ extensions", 5
        elif ("running tests" in line_lower or "pytest" in line_lower or
              "tests/test_" in line_lower or "passed" in line_lower or
              "failed" in line_lower or "skipped" in line_lower or
              "test session starts" in line_lower or "collected" in line_lower):
            return "🧪 Running tests", 6t" in line_lower or "pip install" in line_lower:
            return "📦 Installing Python packages", 4
        elif "building" in line_lower and ("c++" in line_lower or "extensions" in line_lower or "pybind" in line_lower):
            return "🔨 Building C++ extensions", 5
        elif any(test_indicator in line_lower for test_indicator in [
            "running tests", "pytest", "test session starts", "collected", 
            "tests/test_", "passed", "failed", "skipped", "::test_"
        ]):
            return "🧪 Running tests", 6
        elif "completed successfully" in line_lower or "test pipeline completed" in line_lower:
            return "✅ Tests completed", 7title()}")
        console.print(f"⏰ [bold yellow]Starting at:[/bold yellow] {console._environ.get('TZ', 'UTC')} time")
        console.print(f"🔗 [bold cyan]Database:[/bold cyan] SQL Server via host.docker.internal")
        console.print()nning tests across platforms.
"""

import docker
import tempfile
import os
from pathlib import Path
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.text import Text
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn
from rich.rule import Rule
from rich.columns import Columns
from .platforms import get_platform_script
import time

console = Console()


class DockerRunner:
    """Minimal Docker abstraction for cross-platform testing."""
    
    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.client = docker.from_env()
        self.workspace = Path.cwd()

    def run_platform(self, platform_name: str, script_content: str) -> bool:
        """Run a platform test script in Docker."""
        
        # Print welcome message
        console.print(f"\n� [bold green]Starting DockyBot Test Suite[/bold green]")
        console.print(f"🐳 [bold blue]Platform:[/bold blue] {platform_name.title()}")
        console.print()
        
        # Create temp script
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sh', delete=False) as f:
            f.write(script_content)
            script_path = f.name
        
        try:
            os.chmod(script_path, 0o755)
            
            # Get platform config
            platform_config = self._get_platform_config(platform_name)
            
            # Prepare environment variables
            environment = {
                'DB_CONNECTION_STRING': 'Driver=ODBC Driver 18 for SQL Server;Server=host.docker.internal,1433;Database=master;UID=sa;Pwd=Str0ng@Passw0rd123;Encrypt=no;TrustServerCertificate=yes;',
                'PYTHONPATH': '/workspace',
                'DEBIAN_FRONTEND': 'noninteractive',
                'TZ': 'UTC'
            }
            
            # Override with any environment variables from host if they exist
            if 'DB_CONNECTION_STRING' in os.environ:
                environment['DB_CONNECTION_STRING'] = os.environ['DB_CONNECTION_STRING']
            
            # Run container
            container = self.client.containers.run(
                platform_config['image'],
                command=platform_config['command'] + [f"/tmp/test.sh"],
                volumes={
                    str(self.workspace): {"bind": "/workspace", "mode": "rw"},
                    script_path: {"bind": "/tmp/test.sh", "mode": "ro"}
                },
                working_dir="/workspace",
                environment=environment,
                remove=True,
                detach=True,
                **platform_config.get('docker_options', {})
            )
            
            # Stream output
            return self._stream_container_output(container, platform_name)
            
        finally:
            if os.path.exists(script_path):
                os.unlink(script_path)

    def _get_platform_config(self, platform_name: str) -> dict:
        """Get Docker configuration for platform."""
        configs = {
            'ubuntu': {
                'image': 'ubuntu:22.04',
                'command': ['bash'],
                'docker_options': {
                    'extra_hosts': {"host.docker.internal": "host-gateway"}
                }
            },
            'alpine': {
                'image': 'alpine:3.18',
                'command': ['sh'],
                'docker_options': {
                    'extra_hosts': {"host.docker.internal": "host-gateway"}
                }
            },
            'centos': {
                'image': 'centos:7',
                'command': ['bash'],
                'docker_options': {
                    'extra_hosts': {"host.docker.internal": "host-gateway"}
                }
            },
            'debian': {
                'image': 'debian:11',
                'command': ['bash'],
                'docker_options': {
                    'extra_hosts': {"host.docker.internal": "host-gateway"}
                }
            }
        }
        
        if platform_name not in configs:
            raise ValueError(f"Unknown platform: {platform_name}")
            
        return configs[platform_name]

    def _stream_container_output(self, container, platform_name: str) -> bool:
        """Stream container output with live updates and beautiful formatting."""
        
        output_lines = []
        current_step = "Starting"
        step_count = 0
        total_steps = 8  # Updated number of major steps including DB verification
        start_time = time.time()
        
        # Print header
        console.print(Rule(f"[bold blue]🐳 Running {platform_name.title()} Tests[/bold blue]"))
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}", justify="left"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%", justify="center"),
            TextColumn("[dim]{task.fields[elapsed]}", justify="right"),
            console=console,
            expand=True
        ) as progress:
            
            task = progress.add_task(
                f"[cyan]Initializing {platform_name}...", 
                total=total_steps,
                elapsed="0:00"
            )
            
            try:
                for log_line in container.logs(stream=True, follow=True):
                    line = log_line.decode('utf-8').strip()
                    if line:
                        output_lines.append(line)
                        
                        # Update progress based on content
                        new_step, step_progress = self._extract_step_info(line)
                        if new_step != current_step and step_progress > 0:
                            current_step = new_step
                            step_count = step_progress  # Use the actual step number, not increment
                        
                        # Calculate elapsed time
                        elapsed_seconds = int(time.time() - start_time)
                        elapsed_str = f"{elapsed_seconds // 60}:{elapsed_seconds % 60:02d}"
                        
                        progress.update(
                            task, 
                            completed=step_count, 
                            description=f"[cyan]{current_step}",
                            elapsed=elapsed_str
                        )
                        
                        # Format and display output
                        self._display_formatted_line(line)
                
                # Check result
                result = container.wait()
                success = result['StatusCode'] == 0
                
                # Final elapsed time
                elapsed_seconds = int(time.time() - start_time)
                elapsed_str = f"{elapsed_seconds // 60}:{elapsed_seconds % 60:02d}"
                
                # Complete progress
                progress.update(
                    task, 
                    completed=total_steps, 
                    description="[green]✅ Completed" if success else "[red]❌ Failed",
                    elapsed=elapsed_str
                )
                
            except Exception as e:
                elapsed_seconds = int(time.time() - start_time)
                elapsed_str = f"{elapsed_seconds // 60}:{elapsed_seconds % 60:02d}"
                progress.update(task, description="[red]❌ Error occurred", elapsed=elapsed_str)
                console.print(f"[red]💥 Error streaming logs: {e}[/red]")
                return False
        
        # Print summary (outside the progress context to avoid overlap)
        elapsed_seconds = int(time.time() - start_time)
        self._print_test_summary(platform_name, success, len(output_lines), elapsed_seconds)
        
        return success

    def _create_status_panel(self, platform_name: str, status: str) -> Panel:
        """Create status panel for live updates."""
        content = Text()
        content.append(f"Platform: ", style="bold")
        content.append(f"{platform_name}\n", style="blue")
        content.append(f"Status: ", style="bold")
        content.append(status, style="green" if "✅" in status else "yellow" if "..." in status else "red")
        
        return Panel(content, title="DockyBot", border_style="blue")

    def _extract_step_info(self, line: str) -> tuple[str, int]:
        """Extract current step and progress from log line."""
        line_lower = line.lower()
        
        if ("apt-get update" in line_lower or 
            "installing system dependencies" in line_lower or
            ("install" in line_lower and "dependencies" in line_lower)):
            return "📦 Installing system dependencies", 1
        elif ("microsoft odbc" in line_lower or "msodbcsql" in line_lower or
              "packages-microsoft-prod" in line_lower):
            return "📀 Installing Microsoft ODBC Driver", 2
        elif "python" in line_lower and ("setup" in line_lower or "environment" in line_lower or "venv" in line_lower):
            return "🐍 Setting up Python environment", 3
        elif "requirements.txt" in line_lower or "pip install" in line_lower:
            return "� Installing Python packages", 4
        elif "building" in line_lower and ("c++" in line_lower or "extensions" in line_lower or "pybind" in line_lower):
            return "� Building C++ extensions", 5
        elif "running tests" in line_lower or "pytest" in line_lower:
            return "🧪 Running tests", 6
        elif "completed successfully" in line_lower or "test pipeline completed" in line_lower:
            return "✅ Tests completed", 7
        else:
            return "🔄 Processing", 0
    
    def _extract_status(self, line: str) -> str:
        """Extract current status from log line (legacy method for compatibility)."""
        step, _ = self._extract_step_info(line)
        return step

    def _display_formatted_line(self, line: str) -> None:
        """Display a formatted log line with appropriate styling."""
        line_lower = line.lower()
        line_stripped = line.strip()
        
        # Skip empty lines and less important output
        if not line_stripped or self._should_skip_line(line):
            return
            
        # Test results (highest priority - check these first)
        if " passed " in line_lower or line_lower.endswith(" passed"):
            console.print(f"[green]✅ {line}[/green]")
        elif " failed " in line_lower or line_lower.endswith(" failed"):
            console.print(f"[red]❌ {line}[/red]")
        elif " skipped " in line_lower or line_lower.endswith(" skipped"):
            console.print(f"[yellow]⚠️  {line}[/yellow]")
        
        # Database connection messages
        elif any(word in line_lower for word in ['db_connection_string', 'database connection', 'connection target']):
            console.print(f"[cyan]🔗 {line}[/cyan]")
        
        # Installation success messages (check before error patterns)
        elif any(phrase in line_lower for phrase in [
            'successfully installed', 'successfully uninstalled', 'successfully',
            'setting up', 'processing triggers', 'created symlink',
            'collected packages', 'downloading', 'requirement already satisfied'
        ]):
            console.print(f"[green]✅ {line}[/green]")
        
        # Package installation messages
        elif any(phrase in line_lower for phrase in [
            'installing collected packages', 'collecting', 'unpacking', 'preparing to unpack',
            'selecting previously unselected', 'installing system dependencies',
            'installing microsoft odbc', 'installing python packages'
        ]):
            console.print(f"[cyan]📦 {line}[/cyan]")
        
        # Actual error conditions (very specific patterns)
        elif any(pattern in line_lower for pattern in [
            'fatal error', 'compilation failed', 'build failed', 'installation failed',
            'command not found', 'no such file or directory', 'permission denied',
            'connection refused', 'timeout error', 'cannot connect'
        ]) and not any(success_word in line_lower for success_word in ['successfully', 'completed']):
            console.print(f"[red]❌ {line}[/red]")
        
        # Warning messages (but not system messages)
        elif line_lower.startswith('warning:') or 'update-alternatives: warning' in line_lower:
            console.print(f"[yellow]⚠️  {line}[/yellow]")
        
        # Test execution messages
        elif any(phrase in line_lower for phrase in ['running tests', 'pytest']) and 'passed' not in line_lower:
            console.print(f"[magenta]🧪 {line}[/magenta]")
        
        # Building/compilation messages
        elif any(phrase in line_lower for phrase in ['building', 'compiling', 'linking']) and 'failed' not in line_lower:
            console.print(f"[cyan]🔨 {line}[/cyan]")
        
        # Important system messages
        elif self._is_important_line(line):
            console.print(f"[blue]ℹ️  {line}[/blue]")
        
        # Verbose-only messages
        elif self.verbose:
            console.print(f"[dim]   {line}[/dim]")
    
    def _should_skip_line(self, line: str) -> bool:
        """Check if line should be skipped entirely."""
        line_lower = line.lower()
        
        # Skip very verbose package management output
        skip_patterns = [
            'debconf:', 'unable to initialize frontend:', 'dpkg-preconfigure:',
            'reading package lists', 'building dependency tree',
            'reading state information', 'the following new packages',
            'get:', 'hit:', 'ign:', 'reading changelogs',
            'reading database ...', '(reading database', 'files and directories currently installed',
            'preparing to unpack', 'unpacking', 'selecting previously unselected',
            'processing triggers for', 'invoke-rc.d:', 'created symlink',
            'update-alternatives: using', 'no schema files found',
            'first installation detected', 'checking nss setup',
            'current default time zone'
        ]
        
        # Skip if line matches any skip pattern
        if any(pattern in line_lower for pattern in skip_patterns):
            return True
            
        # Skip very short or empty lines
        if len(line.strip()) < 3:
            return True
            
        # Skip lines that are just progress indicators
        if line.strip().startswith('━') or line.strip().startswith('│'):
            return True
            
        return False
    
    def _is_important_line(self, line: str) -> bool:
        """Check if line should be shown in non-verbose mode."""
        important_keywords = [
            "Installing", "Building", "Testing", "Running", "✅", "❌", 
            "ERROR", "FAILED", "SUCCESS", "Started", "Finished",
            "===", "collected", "warnings summary"
        ]
        return any(keyword in line for keyword in important_keywords)

    def _print_test_summary(self, platform_name: str, success: bool, log_lines: int, elapsed_seconds: int) -> None:
        """Print a formatted test summary."""
        console.print()
        console.print(Rule("[bold]Test Summary[/bold]"))
        
        status_color = "green" if success else "red"
        status_icon = "✅" if success else "❌"
        status_text = "PASSED" if success else "FAILED"
        
        elapsed_str = f"{elapsed_seconds // 60}:{elapsed_seconds % 60:02d}"
        
        summary_panel = Panel(
            f"[bold]Platform:[/bold] {platform_name.title()}\n"
            f"[bold]Status:[/bold] [{status_color}]{status_icon} {status_text}[/{status_color}]\n"
            f"[bold]Duration:[/bold] {elapsed_str}\n"
            f"[bold]Log Lines:[/bold] {log_lines}",
            title="🤖 DockyBot Results",
            border_style=status_color
        )
        
        console.print(summary_panel)
        console.print()
    
    def run_platform_test(self, platform: str, verbose: bool = False) -> bool:
        """Run platform test using the predefined test script."""
        self.verbose = verbose
        
        try:
            # Get the test script for this platform
            script_content = get_platform_script(platform)
            
            # Run the platform test
            return self.run_platform(platform, script_content)
            
        except Exception as e:
            console.print(f"[red]💥 Error running {platform} test: {e}[/red]")
            return False

    def list_platforms(self) -> list:
        """List available platforms."""
        return ['ubuntu', 'alpine', 'centos', 'debian']