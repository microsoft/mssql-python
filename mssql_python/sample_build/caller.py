import os
import sys
import importlib.util
import platform

def get_python_info():
    # Get current Python version and architecture
    ver = f"{sys.version_info.major}{sys.version_info.minor}{sys.version_info.micro}"
    
    # Check for ARM64 specifically
    machine = platform.machine().lower()
    if machine == 'arm64':
        arch = 'arm64'
    else:
        arch = "x64" if sys.maxsize > 2**32 else "x86"
    
    return ver, arch

def load_extension():
    ver, arch = get_python_info()
    print(f"Current Python: {ver}, {arch}")
    
    # Find the matching extension
    current_dir = os.path.dirname(os.path.abspath(__file__))
    extension_name = f"sample_extension.cp{ver}-{arch}.pyd"
    extension_path = os.path.join(current_dir, extension_name)
    
    if not os.path.exists(extension_path):
        raise ImportError(f"No extension found for Python {ver} {arch} at {extension_path}")
    
    # Load the extension module using importlib
    spec = importlib.util.spec_from_file_location("sample_extension", extension_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

try:
    # Load the appropriate extension
    sample_extension = load_extension()
    
    # Test the get_architecture function
    ext_arch = sample_extension.get_architecture()
    print(f"Extension architecture: {ext_arch}")
    
    # Test the add function
    result = sample_extension.add(5, 3)
    print(f"5 + 3 = {result}")
    
except ImportError as e:
    print(f"Error importing the extension: {e}")
    print("\nLooking for extension in:", os.path.dirname(os.path.abspath(__file__)))
    files = [f for f in os.listdir(os.path.dirname(os.path.abspath(__file__))) 
             if f.startswith('sample_extension')]
    print("Available extension files:", files)