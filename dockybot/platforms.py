"""
Platform definitions and test scripts for DockyBot.
"""

from pathlib import Path

SCRIPT_DIR = Path(__file__).parent / "scripts"

# Supported platforms
PLATFORMS = {
    'ubuntu': {
        'name': 'Ubuntu 22.04',
        'script': 'ubuntu.sh',
        'description': 'Standard Ubuntu with apt package manager'
    },
    'alpine': {
        'name': 'Alpine Linux 3.18',
        'script': 'alpine.sh', 
        'description': 'Lightweight Alpine with apk package manager'
    },
    'centos': {
        'name': 'CentOS 7',
        'script': 'centos.sh',
        'description': 'Enterprise CentOS with yum package manager'
    },
    'debian': {
        'name': 'Debian 11',
        'script': 'debian.sh',
        'description': 'Debian stable with apt package manager'
    }
}

def get_platform_script(platform_name: str) -> str:
    """Get the test script content for a platform."""
    if platform_name not in PLATFORMS:
        raise ValueError(f"Unknown platform: {platform_name}. Available: {list(PLATFORMS.keys())}")
    
    script_file = SCRIPT_DIR / PLATFORMS[platform_name]['script']
    
    if not script_file.exists():
        raise FileNotFoundError(f"Script not found: {script_file}")
    
    return script_file.read_text()

def list_platforms() -> dict:
    """List all available platforms."""
    return PLATFORMS