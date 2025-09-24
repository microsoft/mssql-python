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
        elif " failed " in line_lower or line_lower.endswith(" failed") or " error " in line_lower or line_lower.endswith(" failed"):
            console.print(f"[red]❌ {line}[/red]")
        elif " skipped " in line_lower or line_lower.endswith(" skipped"):
            console.print(f"[yellow]⚠️  {line}[/yellow]")
        
        # Database connection messages
        elif any(word in line_lower for word in ['db_connection_string', 'database connection', 'connection target']):
            console.print(f"[cyan]🔗 {line}[/cyan]")
        
        # Installation success messages (check before error patterns)
        elif any(phrase in line_lower for phrase in [
            'successfully installed', 'successfully uninstalled', 'successfully',
            'created symlink',
            'collected packages', 'requirement already satisfied'
        ]):
            console.print(f"[green]✅ {line}[/green]")
        
        # Package installation messages
        elif any(phrase in line_lower for phrase in [
            'installing collected packages', 'collecting', 'unpacking', 'preparing to unpack',
            'setting up', 'selecting previously unselected', 'installing system dependencies',
            'installing microsoft odbc', 'installing python packages', 'downloading', 'processing triggers'
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

    def run_platform_bash(self, platform: str, verbose: bool = False) -> bool:
        """Run interactive bash session in platform container with dependencies installed."""
        self.verbose = verbose
        
        try:
            # Check if we have a cached image, if not build it
            image_name = f"dockybot/{platform}:latest"
            
            if not self._image_exists(image_name):
                console.print(f"🏗️  [bold yellow]Building cached image for {platform}...[/bold yellow]")
                console.print("⚙️  [dim]This will take a few minutes but only happens once![/dim]")
                console.print(f"💾 [dim]Image will be saved as: {image_name}[/dim]")
                
                # Build the image with all dependencies
                success = self._build_platform_image(platform, image_name)
                if not success:
                    return False
                    
                console.print(f"✅ [bold green]Image built and cached![/bold green]")
                console.print(f"🔍 [dim]You can see it with: docker images | grep dockybot[/dim]")
            else:
                console.print(f"🚀 [bold green]Using cached image:[/bold green] {image_name}")
                console.print(f"⚡ [dim]No rebuild needed - dependencies already installed![/dim]")
            
            # Run interactive session using the cached image
            return self._run_interactive_session(platform, image_name)
            
        except Exception as e:
            console.print(f"[red]💥 Error running {platform} bash session: {e}[/red]")
            return False

    def _image_exists(self, image_name: str) -> bool:
        """Check if Docker image exists locally."""
        try:
            self.client.images.get(image_name)
            return True
        except:
            return False

    def _build_platform_image(self, platform: str, image_name: str) -> bool:
        """Build a cached Docker image with all dependencies installed."""
        try:
            # Get the setup script
            script_content = get_platform_script(platform)
            setup_script = self._create_build_script(script_content)
            
            # Create Dockerfile
            dockerfile_content = self._create_dockerfile(platform, setup_script)
            
            # Build image
            console.print(f"📦 [cyan]Building image {image_name}...[/cyan]")
            
            # Create build context
            import tempfile
            import tarfile
            import io
            
            # Create tar archive with Dockerfile and setup script
            tar_buffer = io.BytesIO()
            with tarfile.open(fileobj=tar_buffer, mode='w') as tar:
                # Add Dockerfile
                dockerfile_info = tarfile.TarInfo(name='Dockerfile')
                dockerfile_info.size = len(dockerfile_content.encode())
                tar.addfile(dockerfile_info, io.BytesIO(dockerfile_content.encode()))
                
                # Add setup script
                script_info = tarfile.TarInfo(name='setup.sh')
                script_info.size = len(setup_script.encode())
                script_info.mode = 0o755
                tar.addfile(script_info, io.BytesIO(setup_script.encode()))
            
            tar_buffer.seek(0)
            
            # Build the image
            self.client.images.build(
                fileobj=tar_buffer,
                tag=image_name,
                custom_context=True,
                rm=True
            )
            
            console.print(f"✅ [bold green]Successfully built image:[/bold green] {image_name}")
            return True
            
        except Exception as e:
            console.print(f"[red]❌ Error building image: {e}[/red]")
            return False

    def _create_dockerfile(self, platform: str, setup_script: str) -> str:
        """Create Dockerfile for the platform."""
        platform_config = self._get_platform_config(platform)
        base_image = platform_config['image']
        
        dockerfile = f"""FROM {base_image}

# Set environment variables
ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=UTC
ENV PYTHONPATH=/workspace
ENV DB_CONNECTION_STRING="Driver=ODBC Driver 18 for SQL Server;Server=host.docker.internal,1433;Database=master;UID=sa;Pwd=Str0ng@Passw0rd123;Encrypt=no;TrustServerCertificate=yes;"

# Copy and run setup script
COPY setup.sh /tmp/setup.sh
RUN chmod +x /tmp/setup.sh && /tmp/setup.sh

# Set working directory
WORKDIR /workspace

# Default command
CMD ["bash"]
"""
        return dockerfile

    def _create_build_script(self, original_script: str) -> str:
        """Create build script that installs dependencies but doesn't run tests."""
        lines = original_script.split('\n')
        build_lines = []
        
        for line in lines:
            # Skip the shebang and set -euo pipefail for build
            if line.startswith('#!') or 'set -euo pipefail' in line:
                continue
            # Stop before running tests
            if any(test_indicator in line.lower() for test_indicator in 
                   ['python -m pytest', 'pytest', 'running tests']):
                break
            build_lines.append(line)
        
        # Add final steps for image
        build_lines.extend([
            '',
            'echo "🎉 DockyBot image build complete!"',
            'echo "✅ All dependencies installed and ready to use"'
        ])
        
        return '\n'.join(build_lines)

    def _run_interactive_session(self, platform: str, image_name: str) -> bool:
        """Run interactive bash session using cached image."""
        
        console.print(f"\n🚀 [bold green]Starting DockyBot Interactive Session[/bold green]")
        console.print(f"🐳 [bold blue]Platform:[/bold blue] {platform.title()}")
        console.print(f"🖼️  [bold cyan]Image:[/bold cyan] {image_name}")
        console.print()
        
        try:
            # Create or reuse named container
            container_name = f"dockybot-{platform}-session"
            
            try:
                # Try to remove existing container if it exists
                existing = self.client.containers.get(container_name)
                existing.remove(force=True)
            except:
                pass  # Container doesn't exist, which is fine
            
            console.print(f"🎯 [bold green]Starting container...[/bold green]")
            console.print(f"📝 [dim]Type 'exit' to leave the container[/dim]")
            console.print()
            
            # Run interactive container
            import subprocess
            import warnings
            
            # Suppress urllib3 warnings that occur during container cleanup
            warnings.filterwarnings("ignore", category=ResourceWarning)
            
            try:
                result = subprocess.run([
                    'docker', 'run', '--rm', '-it',
                    '--name', container_name,
                    '-v', f'{str(self.workspace)}:/workspace',
                    '-w', '/workspace',
                    '--add-host', 'host.docker.internal:host-gateway',
                    '-e', 'DB_CONNECTION_STRING=Driver=ODBC Driver 18 for SQL Server;Server=host.docker.internal,1433;Database=master;UID=sa;Pwd=Str0ng@Passw0rd123;Encrypt=no;TrustServerCertificate=yes;',
                    image_name,
                    'bash', '-c', 'source /opt/venv/bin/activate 2>/dev/null || true; exec bash'
                ], cwd=str(self.workspace))
                
                console.print(f"\n👋 [dim]Session ended. Container cleaned up.[/dim]")
                return result.returncode == 0
                
            except KeyboardInterrupt:
                console.print(f"\n⚠️  [yellow]Session interrupted. Cleaning up...[/yellow]")
                # Try to stop the container gracefully
                try:
                    running_container = self.client.containers.get(container_name)
                    running_container.stop(timeout=5)
                except:
                    pass
                return False
            
        except Exception as e:
            console.print(f"[red]💥 Error running interactive session: {e}[/red]")
            return False

    def _create_setup_script(self, original_script: str) -> str:
        """Create setup script that stops before running tests and keeps container alive."""
        lines = original_script.split('\n')
        setup_lines = []
        
        for line in lines:
            # Stop before running tests
            if any(test_indicator in line.lower() for test_indicator in 
                   ['python -m pytest', 'pytest', 'running tests']):
                break
            setup_lines.append(line)
        
        # Add keeping container alive instead of exec bash
        setup_lines.extend([
            '',
            'echo "🎉 Setup complete! Container ready for interactive session..."',
            'echo "💡 You can now run tests manually with: python -m pytest tests/ -v"',
            'echo "📁 Workspace is mounted at: /workspace"',
            'echo "🐍 Python environment is activated"',
            'echo "🔗 DB_CONNECTION_STRING is configured"',
            'echo ""',
            'cd /workspace',
            'source /opt/venv/bin/activate',
            '# Keep container running',
            'tail -f /dev/null'
        ])
        
        return '\n'.join(setup_lines)

    def run_platform_interactive(self, platform_name: str, script_content: str) -> bool:
        """Run platform in interactive mode."""
        
        # Print welcome message
        console.print(f"\n🚀 [bold green]Starting DockyBot Interactive Session[/bold green]")
        console.print(f"🐳 [bold blue]Platform:[/bold blue] {platform_name.title()}")
        console.print(f"🔧 [bold yellow]Mode:[/bold yellow] Interactive Bash")
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
            
            # Create or reuse named container
            container_name = f"dockybot-{platform_name}"
            
            console.print(f"🏗️  [bold cyan]Setting up container:[/bold cyan] {container_name}")
            console.print("⚙️  [dim]Installing dependencies... This may take a few minutes on first run.[/dim]")
            console.print()
            
            try:
                # Try to remove existing container if it exists
                existing = self.client.containers.get(container_name)
                existing.remove(force=True)
            except:
                pass  # Container doesn't exist, which is fine
            
            # Run container interactively
            container = self.client.containers.run(
                platform_config['image'],
                command=platform_config['command'] + [f"/tmp/setup.sh"],
                name=container_name,
                volumes={
                    str(self.workspace): {"bind": "/workspace", "mode": "rw"},
                    script_path: {"bind": "/tmp/setup.sh", "mode": "ro"}
                },
                working_dir="/workspace",
                environment=environment,
                detach=True,
                **platform_config.get('docker_options', {})
            )
            
            # Wait for setup to complete
            console.print("⏳ [yellow]Waiting for setup to complete...[/yellow]")
            
            # Stream setup logs to show progress
            for log_line in container.logs(stream=True, follow=True):
                line = log_line.decode('utf-8').strip()
                if line:
                    self._display_formatted_line(line)
                    # Break when we see the completion message
                    if "Setup complete! Container ready for interactive session" in line:
                        break
            
            console.print(f"🎯 [bold green]Container ready![/bold green] Attaching to interactive session...")
            console.print(f"📝 [dim]Type 'exit' to leave the container[/dim]")
            console.print()
            
            # Attach to container for interactive session with proper environment
            import subprocess
            result = subprocess.run([
                'docker', 'exec', '-it', '-e', 'DB_CONNECTION_STRING=' + environment['DB_CONNECTION_STRING'],
                container_name, 'bash', '-c', 
                'cd /workspace && source /opt/venv/bin/activate && exec bash'
            ], cwd=str(self.workspace))
            
            # Clean up
            try:
                container.remove(force=True)
            except:
                pass
                
            return result.returncode == 0
            
        finally:
            if os.path.exists(script_path):
                os.unlink(script_path)

    def list_platforms(self) -> list:
        """List available platforms."""
        return ['ubuntu', 'alpine', 'centos', 'debian']
    
    def list_cached_images(self) -> None:
        """List DockyBot cached images."""
        console.print("\n🖼️  [bold blue]DockyBot Cached Images[/bold blue]")
        console.print()
        
        images = self.client.images.list()
        dockybot_images = [img for img in images if any('dockybot/' in tag for tag in img.tags)]
        
        if not dockybot_images:
            console.print("📭 [dim]No cached images found[/dim]")
            console.print("💡 [dim]Run 'python -m dockybot bash <platform>' to create one[/dim]")
            return
            
        from rich.table import Table
        table = Table()
        table.add_column("Image", style="cyan")
        table.add_column("Size", style="green")
        table.add_column("Created", style="yellow")
        
        for image in dockybot_images:
            for tag in image.tags:
                if 'dockybot/' in tag:
                    size_mb = round(image.attrs['Size'] / (1024 * 1024), 1)
                    created = image.attrs['Created'][:19].replace('T', ' ')
                    table.add_row(tag, f"{size_mb} MB", created)
        
        console.print(table)
        console.print()
        console.print("💡 [dim]Use 'python -m dockybot clean' to remove cached images[/dim]")
    
    def clean_cached_images(self, platform: str = None, force: bool = False) -> None:
        """Clean DockyBot cached images."""
        if platform:
            image_name = f"dockybot/{platform}:latest"
            if not self._image_exists(image_name):
                console.print(f"📭 [yellow]No cached image found for {platform}[/yellow]")
                return
                
            if not force:
                import typer
                confirm = typer.confirm(f"Remove cached image for {platform}?")
                if not confirm:
                    console.print("❌ [dim]Cancelled[/dim]")
                    return
            
            try:
                self.client.images.remove(image_name, force=True)
                console.print(f"🗑️  [green]Removed cached image: {image_name}[/green]")
            except Exception as e:
                console.print(f"[red]Error removing image: {e}[/red]")
        else:
            # Clean all dockybot images
            images = self.client.images.list()
            dockybot_images = []
            for img in images:
                for tag in img.tags:
                    if 'dockybot/' in tag:
                        dockybot_images.append(tag)
                        break
            
            if not dockybot_images:
                console.print("📭 [dim]No cached images to clean[/dim]")
                return
            
            if not force:
                console.print(f"Found {len(dockybot_images)} cached images:")
                for img in dockybot_images:
                    console.print(f"  - {img}")
                console.print()
                    
                import typer
                confirm = typer.confirm("Remove all DockyBot cached images?")
                if not confirm:
                    console.print("❌ [dim]Cancelled[/dim]")
                    return
            
            removed_count = 0
            for img_tag in dockybot_images:
                try:
                    self.client.images.remove(img_tag, force=True)
                    console.print(f"🗑️  [green]Removed: {img_tag}[/green]")
                    removed_count += 1
                except Exception as e:
                    console.print(f"[red]Error removing {img_tag}: {e}[/red]")
            
            console.print(f"\n✅ [green]Cleaned {removed_count} cached images[/green]")