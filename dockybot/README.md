# 🤖 DockyBot

<div align="center">

![DockyBot](https://img.shields.io/badge/DockyBot-🐳_Powered-blue?style=for-the-badge)
![Python](https://img.shields.io/badge/Python-3.10+-brightgreen?style=for-the-badge&logo=python)
![Docker](https://img.shields.io/badge/Docker-Required-2496ED?style=for-the-badge&logo=docker)
![Platform](https://img.shields.io/badge/Platforms-4_Supported-orange?style=for-the-badge)

**🚀 DevOps-as-Code testing tool using Docker for cross-platform testing**

*Test your mssql-python library across multiple Linux distributions with beautiful, interactive output*

</div>

---

## 🌟 Features

### 🧪 **Automated Testing**
- 🐳 **Multi-Platform Support**: Ubuntu, Alpine, CentOS, Debian
- 📊 **Beautiful Progress Bars**: Real-time progress tracking with Rich UI
- 🎨 **Colored Output**: Smart log formatting with emoji indicators
- ⚡ **Smart Caching**: Build once, run instantly

### 🛠️ **Interactive Development** 
- 🖥️ **Bash Sessions**: Drop into fully-configured containers
- 📦 **Pre-installed Dependencies**: System packages, ODBC drivers, Python libs
- 🔗 **Database Ready**: Pre-configured SQL Server connection
- 💾 **Persistent Images**: No rebuild needed between sessions

### 📈 **Advanced Monitoring**
- 📊 **Step-by-Step Progress**: Track installation phases
- ⏱️ **Elapsed Time Tracking**: See exactly how long each phase takes
- 🎯 **Smart Status Detection**: Automatically detects build phases
- 📝 **Detailed Summaries**: Complete test reports with metrics

---

## 🚀 Quick Start

### Prerequisites
- 🐳 **Docker** (with daemon running)
- 🐍 **Python 3.10+**
- 📦 **Dependencies**: `pip install docker typer rich`

### Installation
```bash
# Navigate to mssql-python directory
cd /path/to/mssql-python

# Install dependencies (if not already installed)
pip install docker typer rich

# You're ready to go! 🎉
```

---

## 📖 Usage Guide

### 🧪 **Running Tests**

#### Test Single Platform
```bash
# Run tests on Ubuntu (recommended)
python -m dockybot test ubuntu

# Run with verbose output
python -m dockybot test ubuntu -v

# Test other platforms
python -m dockybot test alpine
python -m dockybot test centos  
python -m dockybot test debian
```

#### Test All Platforms
```bash
# Run tests across all supported platforms
python -m dockybot test-all

# With verbose output
python -m dockybot test-all -v
```

### 🖥️ **Interactive Development**

#### Bash Into Container
```bash
# Open interactive bash session (Ubuntu)
python -m dockybot bash ubuntu

# With verbose setup output
python -m dockybot bash ubuntu -v
```

**What you get in the bash session:**
- ✅ **All system dependencies** installed
- ✅ **Microsoft ODBC Driver 18** configured  
- ✅ **Python virtual environment** activated
- ✅ **All requirements.txt** packages installed
- ✅ **C++ extensions** built and ready
- ✅ **Database connection** pre-configured
- ✅ **Workspace mounted** at `/workspace`

#### Manual Testing Inside Container
```bash
# Inside the container, you can:
python -m pytest tests/ -v          # Run all tests
python -m pytest tests/test_003* -v # Run specific test
python main.py                      # Run your scripts
pip list                            # Check installed packages
echo $DB_CONNECTION_STRING          # Check DB config
```

### 🖼️ **Image Management**

#### List Cached Images
```bash
# See all DockyBot cached images
python -m dockybot images
```

#### Clean Cached Images
```bash
# Clean specific platform
python -m dockybot clean ubuntu

# Clean all cached images
python -m dockybot clean

# Force clean without confirmation
python -m dockybot clean --force
```

### 📋 **Platform Information**
```bash
# List all supported platforms
python -m dockybot platforms
```

---

## 🎨 Output Examples

### 🧪 **Test Output**
```
🚀 Starting DockyBot Test Suite
🐳 Platform: Ubuntu
⏰ Starting at: UTC time
🔗 Database: SQL Server via host.docker.internal

──────────────────────── 🐳 Running Ubuntu Tests ────────────────────────

🔄 Installing system dependencies    ━━━━━━━━░░░░░░░░░░░░    25%    0:01:23

📦 Setting up tzdata (2025b-0ubuntu0.22.04.1) ...
✅ Successfully installed docker-7.1.0 typer-0.19.1 rich-14.1.0
🧪 tests/test_003_connection.py::test_connection_close PASSED     [45%]
✅ tests/test_007_logging.py::test_setup_logging PASSED           [89%]

───────────────────────── Test Summary ──────────────────────────
╭─────────────────── 🤖 DockyBot Results ───────────────────╮
│ Platform: Ubuntu                                          │
│ Status: ✅ PASSED                                         │
│ Duration: 2:34                                            │
│ Log Lines: 1247                                           │
╰───────────────────────────────────────────────────────────╯
```

### 🖥️ **Interactive Session**
```
🚀 Starting DockyBot Interactive Session
🐳 Platform: Ubuntu
🖼️  Image: dockybot/ubuntu:latest

🎯 Container ready! Attaching to interactive session...
📝 Type 'exit' to leave the container

root@container:/workspace# python -m pytest tests/ -v
root@container:/workspace# pip list
root@container:/workspace# python main.py
```

---

## 🏗️ Architecture

### 📂 **File Structure**
```
dockybot/
├── README.md              # 📖 This documentation
├── __init__.py            # 📦 Package initialization  
├── __main__.py            # 🚀 Entry point
├── cli.py                 # 🖥️ Command-line interface
├── docker_runner.py       # 🐳 Docker abstraction layer
├── platforms.py           # 🌍 Platform configurations
└── scripts/               # 📜 Platform-specific setup scripts
    ├── ubuntu.sh          # 🟠 Ubuntu 22.04 setup
    ├── alpine.sh          # 🔵 Alpine 3.18 setup  
    ├── centos.sh          # 🔴 CentOS 7 setup
    └── debian.sh          # 🟣 Debian 11 setup
```

### 🐳 **Docker Strategy**

#### **Smart Image Caching**
- **First Run**: Builds `dockybot/ubuntu:latest` with all dependencies
- **Subsequent Runs**: Reuses cached image for instant startup
- **Containers**: Removed after each session (`--rm`)
- **Images**: Persist until manually cleaned

#### **Platform Configurations**
| Platform | Base Image | Package Manager | Shell |
|----------|------------|----------------|-------|
| 🟠 Ubuntu | `ubuntu:22.04` | `apt` | `bash` |
| 🔵 Alpine | `alpine:3.18` | `apk` | `sh` |
| 🔴 CentOS | `centos:7` | `yum` | `bash` |
| 🟣 Debian | `debian:11` | `apt` | `bash` |

---

## 🎯 Advanced Usage

### 🔧 **Custom Environment Variables**
```bash
# Override default database connection
export DB_CONNECTION_STRING="your_custom_connection_string"
python -m dockybot test ubuntu
```

### 🐳 **Direct Docker Commands**
```bash
# Check DockyBot images
docker images | grep dockybot

# Run container manually
docker run -it --rm -v $(pwd):/workspace dockybot/ubuntu:latest bash

# Clean up everything
docker system prune -a
```

### 📊 **Debugging & Logs**
```bash
# Enable verbose output for detailed logging
python -m dockybot test ubuntu -v

# Check Docker daemon logs
docker system events

# Container resource usage
docker stats
```

---

## 🏃‍♂️ Performance Tips

### ⚡ **Speed Optimizations**
- **Use cached images**: First run takes 3-5 minutes, subsequent runs are instant
- **Keep images**: Only clean when needed (`python -m dockybot clean`)
- **Use Ubuntu**: Fastest platform for most testing scenarios
- **Verbose mode**: Only use `-v` when debugging

### 💾 **Storage Management**
```bash
# Check image sizes
python -m dockybot images

# Clean specific platforms you don't use
python -m dockybot clean alpine centos debian

# Full cleanup (forces rebuild next time)
docker system prune -a
```

---

## 🐛 Troubleshooting

### Common Issues

#### 🐳 **Docker Not Running**
```bash
# Error: Cannot connect to the Docker daemon
# Solution: Start Docker Desktop or daemon
systemctl start docker  # Linux
open -a Docker          # macOS
```

#### 🔒 **Permission Issues**
```bash
# Error: Permission denied accessing Docker
# Solution: Add user to docker group (Linux)
sudo usermod -aG docker $USER
# Then logout and login again
```

#### 💾 **Disk Space**
```bash
# Error: No space left on device
# Solution: Clean Docker system
docker system prune -a
python -m dockybot clean --force
```

#### 🌐 **Network Issues**
```bash
# Error: Cannot connect to SQL Server
# Check: host.docker.internal resolves correctly
docker run --rm alpine ping -c 1 host.docker.internal
```

### 📞 **Getting Help**
- 📖 **Documentation**: Check this README
- 🐛 **Issues**: Report bugs in the main repository
- 💬 **Discussions**: Use GitHub Discussions for questions
- 📧 **Contact**: Reach out to the mssql-python team

---

## 📈 Roadmap

### 🚀 **Coming Soon**
- 🍎 **macOS Support**: ARM64 and Intel testing
- 🪟 **Windows Containers**: Native Windows testing
- 🧪 **Test Parallelization**: Run multiple platforms simultaneously  
- 📊 **HTML Reports**: Beautiful test result dashboards
- 🔄 **CI Integration**: GitHub Actions workflows
- 🐍 **Python Version Matrix**: Test across Python versions

### 💡 **Ideas & Suggestions**
We're always looking for ways to improve DockyBot! Feel free to:
- 🌟 Star the repository
- 🐛 Report issues
- 💡 Suggest features
- 🤝 Contribute code

---

<div align="center">

## 🎉 Happy Testing with DockyBot! 

**Made with ❤️ by the mssql-python team**

*Empowering developers to test across platforms effortlessly* 🚀

---

![Footer](https://img.shields.io/badge/🤖_DockyBot-Powered_by_Docker-blue?style=for-the-badge)

</div>