from setuptools import setup, find_packages, find_namespace_packages
from pybind11.setup_helpers import Pybind11Extension, build_ext
import sys
import platform
import os
import sysconfig
import subprocess
import glob

def get_platform_architecture():
    arch = platform.machine().lower()
    if arch == 'amd64' or arch == 'x86_64':
        return 'win64'
    elif arch == 'x86' or arch == 'i386':
        return 'win32'
    elif arch == 'arm64' or arch == 'aarch64':
        return 'winarm64'
    else:
        raise ValueError(f"Unsupported architecture: {arch}")

# Detect host architecture (the machine we're building on)
def get_host_architecture():
    arch = platform.machine().lower()
    if arch == 'amd64' or arch == 'x86_64':
        return 'x64'
    elif arch == 'x86' or arch == 'i386':
        return 'x86'
    elif arch == 'arm64' or arch == 'aarch64':
        return 'arm64'
    else:
        return 'unknown'

arch_folder = get_platform_architecture()
host_arch = get_host_architecture()
arch_define = f'"{arch_folder}"'

print(f"Target architecture: {arch_folder}")
print(f"Host architecture: {host_arch}")

# Get Python version details
python_version = sysconfig.get_config_var('py_version_nodot')
if not python_version:
    python_version = ''.join(platform.python_version_tuple()[:2])

# For Python 3.13, use 313 as the library version
if python_version.startswith('3') and len(python_version) >= 3:
    python_lib = f'python{python_version}'
else:
    python_lib = f'python{python_version}'

# Get Python library directory
python_lib_dir = sysconfig.get_config_var('LIBDIR')
if not python_lib_dir:
    python_lib_dir = os.path.join(sys.base_prefix, 'libs')

# Set up compiler flags
extra_compile_args = [
    '/std:c++14',
    '/EHsc',
    '/bigobj',
]

if '--debug' in sys.argv:
    extra_compile_args.extend(['/Zi', '/Od'])

# Set up linker flags and compiler settings
extra_link_args = []
os.environ['DISTUTILS_USE_SDK'] = '1'  # Force use of correct SDK

# Known compiler locations
KNOWN_ARM64_CL_PATH = r"C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools\VC\Tools\MSVC\14.43.34808\bin\Hostarm64\arm64\cl.exe"
KNOWN_X64_CL_PATH = r"C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools\VC\Tools\MSVC\14.43.34808\bin\Hostarm64\x64\cl.exe"

# Automatically determine target machine type based on compiler location
def determine_machine_type(cl_path):
    """Determine the machine type (x64, ARM64, etc.) based on compiler path."""
    if cl_path:
        cl_path_lower = cl_path.lower()
        if 'hostarm64' in cl_path_lower:
            # We're on ARM64 host
            if '\\x64\\' in cl_path:
                # Using x64 cross compiler - always build x64
                print("Detected x64 target compiler on ARM64 host - forcing x64 build")
                return 'x64'
            elif '\\arm64\\' in cl_path:
                print("Detected ARM64 target compiler on ARM64 host")
                return 'ARM64'
        elif 'hostx64' in cl_path_lower:
            if '\\x64\\' in cl_path:
                print("Detected x64 target compiler on x64 host")
                return 'x64'
            elif '\\arm64\\' in cl_path:
                print("Detected ARM64 target compiler on x64 host")
                return 'ARM64'
    
    # Default to the platform architecture
    if arch_folder == 'winarm64':
        print("No specific compiler detected, defaulting to ARM64 based on platform")
        return 'ARM64'
    elif arch_folder == 'win64':
        print("No specific compiler detected, defaulting to x64 based on platform")
        return 'x64'
    else:
        print("No specific compiler detected, defaulting to x86 based on platform")
        return 'x86'

# Find Visual Studio installations
def find_vs_installations():
    program_files_paths = [
        os.environ.get('ProgramFiles(x86)', 'C:\\Program Files (x86)'),
        os.environ.get('ProgramFiles', 'C:\\Program Files'),
    ]
    
    vs_installations = []
    for program_files in program_files_paths:
        vs_path = os.path.join(program_files, 'Microsoft Visual Studio')
        if not os.path.exists(vs_path):
            continue
            
        # Find all Visual Studio versions
        for year in ['2022', '2019', '2017']:
            for edition in ['BuildTools', 'Community', 'Professional', 'Enterprise']:
                edition_path = os.path.join(vs_path, year, edition)
                if os.path.exists(edition_path):
                    vs_installations.append((year, edition, edition_path))
    
    return vs_installations

# Find cl.exe for specific architecture
def find_cl_exe(target_arch, host_arch=None):
    """Find cl.exe for a specific architecture by searching multiple possible paths."""
    if host_arch is None:
        host_arch = get_host_architecture()
        
    print(f"Searching for {target_arch} compiler (host: {host_arch})...")
    
    # Try common Visual Studio installation paths
    vs_base_paths = [
        r"C:\Program Files (x86)\Microsoft Visual Studio",
        r"C:\Program Files\Microsoft Visual Studio"
    ]
    
    vs_years = ["2022", "2019", "2017"]
    vs_editions = ["BuildTools", "Community", "Professional", "Enterprise"]
    
    # For ARM64 target, prioritize native ARM64 compiler first
    if target_arch == 'winarm64' and host_arch == 'arm64':
        # Check for native ARM64 compiler first
        for base_path in vs_base_paths:
            for year in vs_years:
                for edition in vs_editions:
                    native_arm64_cl = os.path.join(
                        base_path, year, edition,
                        r"VC\Tools\MSVC", "*", r"bin\Hostarm64\arm64\cl.exe"
                    )
                    # Use glob to handle any MSVC version
                    matches = glob.glob(native_arm64_cl)
                    if matches:
                        cl_path = matches[0]
                        print(f"Found native ARM64 compiler at: {cl_path}")
                        return cl_path
        
        # Only if native ARM64 compiler is not found, try x64 cross-compiler
        for base_path in vs_base_paths:
            for year in vs_years:
                for edition in vs_editions:
                    x64_cl = os.path.join(
                        base_path, year, edition,
                        r"VC\Tools\MSVC", "*", r"bin\Hostarm64\x64\cl.exe"
                    )
                    matches = glob.glob(x64_cl)
                    if matches:
                        cl_path = matches[0]
                        print(f"Native ARM64 compiler not found, using x64 cross-compiler at: {cl_path}")
                        return cl_path
    
    print(f"Could not find a {target_arch} compiler! You may need to install Visual Studio with ARM64 components.")
    return None

# Find link.exe to match cl.exe
def find_link_exe(cl_path):
    if (cl_path and os.path.exists(cl_path)):
        cl_dir = os.path.dirname(cl_path)
        link_path = os.path.join(cl_dir, 'link.exe')
        if (os.path.exists(link_path)):
            return link_path
    return None

# Setup Visual Studio environment
def setup_msvc():
    vs_installations = find_vs_installations()
    
    for year, edition, vs_path in vs_installations:
        vcvarsall_path = os.path.join(vs_path, 'VC', 'Auxiliary', 'Build', 'vcvarsall.bat')
        if os.path.exists(vcvarsall_path):
            # Determine architecture argument for vcvarsall
            if arch_folder == 'win32':
                arch_arg = 'x86'
            elif arch_folder == 'win64':
                arch_arg = 'x64'
            elif arch_folder == 'winarm64':
                # For ARM64, we want native compilation on ARM64 host
                if host_arch == 'arm64':
                    arch_arg = 'arm64'  # Use native ARM64 toolchain
                else:
                    # If we're on x64 trying to build for ARM64, use cross tools
                    arch_arg = 'x64_arm64'
                    if host_arch == 'x86':
                        # x86 host to ARM64 target
                        arch_arg = 'x86_arm64'
            
            # Execute vcvarsall.bat and get environment variables
            cmd = f'"{vcvarsall_path}" {arch_arg} && set'
            try:
                print(f"Running: {vcvarsall_path} {arch_arg}")
                result = subprocess.check_output(cmd, shell=True, text=True)
                for line in result.splitlines():
                    if '=' in line:
                        name, value = line.split('=', 1)
                        os.environ[name] = value
                print(f"Successfully set up MSVC environment using {vcvarsall_path} {arch_arg}")
                
                # Set environment variables for native ARM64 compilation
                if arch_folder == 'winarm64' and host_arch == 'arm64':
                    os.environ['VSCMD_ARG_TGT_ARCH'] = 'arm64'
                    os.environ['Platform'] = 'ARM64'
                    os.environ['PreferredToolArchitecture'] = 'arm64'
                
                return True
            except subprocess.CalledProcessError as e:
                print(f"Error running vcvarsall: {e}")
                continue
    
    return False

# Set up MSVC environment
setup_msvc()

# Get compiler first to determine true target architecture
main_cl_path = find_cl_exe('winarm64', host_arch) if arch_folder == 'winarm64' else None
machine_type = determine_machine_type(main_cl_path)

# Override package architecture based on compiler
if arch_folder == 'winarm64' and machine_type == 'x64':
    print("Found x64 compiler for ARM64 host - building x64 package instead")
    # Change the arch_folder to match the compiler target
    # Note: We keep arch_define as "winarm64" for proper runtime detection
    arch_folder_actual = 'win64'  # Use x64 arch for packaging/building
else:
    arch_folder_actual = arch_folder

# Architecture-specific settings
if arch_folder == 'winarm64':
    # Specific ARM64 environment variables
    if machine_type == 'x64':
        os.environ['VSCMD_ARG_TGT_ARCH'] = 'x64'
        os.environ['Platform'] = 'X64'
        # When using x64 target compiler on ARM64 host, add x64-specific library paths
        msvc_lib_path = os.path.dirname(os.path.dirname(main_cl_path))
        msvc_x64_lib_paths = [
            os.path.join(msvc_lib_path, 'lib', 'x64'),
            r"C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools\VC\Tools\MSVC\14.43.34808\lib\x64\uwp",
            r"C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools\VC\Tools\MSVC\14.43.34808\lib\x64",
            r"C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools\VC\Tools\MSVC\14.43.34808\lib\x64\uwp\legacy",
            r"C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools\VC\Tools\MSVC\14.43.34808\lib\x64\legacy"
        ]
        sdk_lib_path = r"C:\Program Files (x86)\Windows Kits\10\lib\10.0.22621.0"
        
        # Add required runtime libraries
        extra_compile_args.extend([])
        extra_link_args.extend([f'/LIBPATH:{python_lib_dir}'])
        
        # Add all MSVC x64 library paths
        for lib_path in msvc_x64_lib_paths:
            if os.path.exists(lib_path):
                extra_link_args.append(f'/LIBPATH:{lib_path}')
        
        # Add SDK library paths
        extra_link_args.extend([
            f'/LIBPATH:{os.path.join(sdk_lib_path, "ucrt", "x64")}',
            f'/LIBPATH:{os.path.join(sdk_lib_path, "um", "x64")}',
            '/machine:X64',
            '/SUBSYSTEM:CONSOLE'
        ])
        
        # Add required runtime libraries
        extra_link_args.extend([
            'msvcrt.lib',
            'vcruntime.lib',
            'ucrt.lib',
            'kernel32.lib',
            'user32.lib',
            'gdi32.lib',
            'winspool.lib',
            'comdlg32.lib',
            'advapi32.lib',
            'shell32.lib',
            'ole32.lib',
            'oleaut32.lib',
            'uuid.lib',
            'odbc32.lib',
            'odbccp32.lib',
            'msvcprt.lib'  # C++ Runtime
        ])
    else:
        os.environ['VSCMD_ARG_TGT_ARCH'] = 'arm64'
        os.environ['Platform'] = 'ARM64'
        extra_compile_args.extend([
            '/arch:arm64',  # Compile for ARM64
        ])
        extra_link_args.extend([
            f'/LIBPATH:{python_lib_dir}',
            '/machine:ARM64',  # Use ARM64 machine type for link.exe
            '/SUBSYSTEM:CONSOLE',
        ])
    
    # Update PATH to prioritize the appropriate compiler
    if main_cl_path:
        compiler_dir = os.path.dirname(main_cl_path)
        if compiler_dir not in os.environ['PATH'].split(os.pathsep):
            os.environ['PATH'] = compiler_dir + os.pathsep + os.environ['PATH']

# Python include dirs and library
python_include = sysconfig.get_path('include')
python_include_config = sysconfig.get_path('platinclude')

# Define the extension module
ext_modules = [
    Pybind11Extension(
        "mssql_python.ddbc_bindings",
        ["mssql_python/pybind/ddbc_bindings.cpp"],
        define_macros=[
            ('ARCHITECTURE', arch_define),  # Keep using winarm64 for runtime detection
            ('_DEBUG', 1) if '--debug' in sys.argv else ('NDEBUG', 1)
        ],
        include_dirs=[
            python_include,
            python_include_config,
        ],
        extra_compile_args=extra_compile_args,
        extra_link_args=extra_link_args,
        libraries=['shlwapi', python_lib],
        library_dirs=[python_lib_dir],
    ),
]

# Include specific packages to avoid warnings
all_packages = [
    'mssql_python', 
    'mssql_python.pybind',  # Added pybind package explicitly 
    'mssql_python.libs'
]

# Add architecture-specific packages
for arch in ['win32', 'win64', 'winarm64']:
    all_packages.append(f'mssql_python.libs.{arch}')
    if os.path.exists(f'mssql_python/libs/{arch}/1033'):
        all_packages.append(f'mssql_python.libs.{arch}.1033')

class CustomBuildExt(build_ext):
    def initialize_options(self):
        super().initialize_options()
        self.plat_name = None  # Will be set by build_ext base class
        self._cl_path = None
    
    def finalize_options(self):
        super().finalize_options()
        
        # Special handling for ARM64
        if arch_folder == 'winarm64':
            # Use the global compiler path or find it if not already found
            if main_cl_path is not None:
                self._cl_path = main_cl_path
            else:
                self._cl_path = find_cl_exe('winarm64', host_arch)
                
            machine_type = determine_machine_type(self._cl_path)
            
            if self._cl_path:
                print(f"Using compiler: {self._cl_path} (machine type: {machine_type})")
                link_path = find_link_exe(self._cl_path)
                if link_path:
                    print(f"Using linker: {link_path}")
                
                # Add compiler to PATH
                compiler_dir = os.path.dirname(self._cl_path)
                os.environ['PATH'] = compiler_dir + os.pathsep + os.environ['PATH']
                
                # Try to set compiler executable directly if possible
                if hasattr(self, 'compiler') and self.compiler:
                    self.compiler.cc = self._cl_path
                    self.compiler.linker = link_path
            
            # Update extension settings
            for ext in self.extensions:
                ext.py_limited_api = False
                
                # Always use the system's actual Python location for library/include
                # paths rather than relying on potentially incorrect auto-detection
                if machine_type == 'x64':
                    # For x64, explicitly set libraries and paths for x64 architecture
                    # Clean up library list first to avoid duplicates
                    ext.libraries = [lib for lib in ext.libraries if not lib.startswith('python')]
                    
                    # For cross-compilation: If we're building for x64 but running on ARM64,
                    # we need to use the x64 python libraries from a different location
                    # This is the critical fix for the linking errors
                    if host_arch == 'arm64':
                        # First try to find x64 Python libraries
                        x64_python_lib_candidates = [
                            # Common locations for x64 Python libraries
                            r"C:\Program Files\Python313\libs",  # Standard x64 Python location
                            r"C:\Python313\libs",
                            r"C:\Program Files\Python313-64\libs",
                            # Additional locations to check
                            r"C:\Program Files (x86)\Python313-64\libs",
                            # New paths to check
                            r"C:\Program Files\Python\Python313\libs",
                            r"C:\Program Files\Python\Python313-64\libs",
                            # Program Files (x86) paths
                            r"C:\Program Files (x86)\Python\Python313\libs",
                            r"C:\Program Files (x86)\Python313-64\libs",
                            # Python.org default paths
                            r"C:\Users\mssql-python\AppData\Local\Programs\Python\Python313\libs",
                            r"C:\Python313-64\libs",
                            # Visual Studio Python installations
                            r"C:\Program Files\Microsoft Visual Studio\Shared\Python313_64\libs",
                            r"C:\Program Files (x86)\Microsoft Visual Studio\Shared\Python313_64\libs",
                        ]
                        
                        x64_python_lib_dir = None
                        for lib_path in x64_python_lib_candidates:
                            potential_lib = os.path.join(lib_path, 'python313.lib')
                            if os.path.exists(potential_lib):
                                x64_python_lib_dir = lib_path
                                print(f"Found x64 Python library at: {x64_python_lib_dir}")
                                break
                        
                        if x64_python_lib_dir:
                            # Replace library dirs with x64 Python path and keep it at the front
                            ext.library_dirs = [x64_python_lib_dir] + [lib_dir for lib_dir in ext.library_dirs if lib_dir != python_lib_dir]
                            # We need to explicitly add this to the linker args if not already present
                            libpath = f'/LIBPATH:{x64_python_lib_dir}'
                            if libpath not in ext.extra_link_args:
                                ext.extra_link_args.append(libpath)
                            print(f"Using x64 Python library directory: {x64_python_lib_dir}")
                        else:
                            # If we still can't find x64 Python, we need to verify if the ARM64 Python actually has x64 libraries
                            # Sometimes Python installations include both architectures
                            arm64_python_dir = os.path.dirname(python_lib_dir)
                            potential_x64_lib = os.path.join(arm64_python_dir, 'PCbuild', 'amd64', 'python313.lib')
                            if os.path.exists(potential_x64_lib):
                                x64_python_lib_dir = os.path.dirname(potential_x64_lib)
                                print(f"Found x64 Python library in ARM64 installation: {x64_python_lib_dir}")
                                ext.library_dirs = [x64_python_lib_dir] + [lib_dir for lib_dir in ext.library_dirs if lib_dir != python_lib_dir]
                                libpath = f'/LIBPATH:{x64_python_lib_dir}'
                                if libpath not in ext.extra_link_args:
                                    ext.extra_link_args.append(libpath)
                            else:
                                # Final fallback - try to locate the Python installation using registry
                                try:
                                    import winreg
                                    with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\Python\PythonCore\3.13\InstallPath") as key:
                                        py_install_dir = winreg.QueryValue(key, None)
                                        x64_lib_path = os.path.join(py_install_dir, 'libs')
                                        if os.path.exists(os.path.join(x64_lib_path, 'python313.lib')):
                                            x64_python_lib_dir = x64_lib_path
                                            print(f"Found x64 Python library through registry: {x64_python_lib_dir}")
                                            ext.library_dirs = [x64_python_lib_dir] + [lib_dir for lib_dir in ext.library_dirs if lib_dir != python_lib_dir]
                                            libpath = f'/LIBPATH:{x64_python_lib_dir}'
                                            if libpath not in ext.extra_link_args:
                                                ext.extra_link_args.append(libpath)
                                except:
                                    pass
                    
                    # Now determine the correct Python library to use
                    # Always use python313 for Python 3.13
                    ext.libraries.append('python313')
                    
                    # Make sure we're using the correct Python library path
                    if python_lib_dir not in ext.library_dirs:
                        ext.library_dirs.append(python_lib_dir)
                    
                    print(f"Python library dirs: {ext.library_dirs}")
                    print(f"Python libraries: {ext.libraries}")
                
                # Update include and library directories for all cases
                if python_include not in ext.include_dirs:
                    ext.include_dirs.append(python_include)
                if python_include_config not in ext.include_dirs:
                    ext.include_dirs.append(python_include_config)
                
                # Update machine type in link args based on compiler
                for i, arg in enumerate(ext.extra_link_args):
                    if arg.startswith('/machine:'):
                        if machine_type == 'x64':
                            ext.extra_link_args[i] = '/machine:X64'
                        else:
                            ext.extra_link_args[i] = '/machine:ARM64'
    
    def build_extension(self, ext):
        """Build a single extension."""
        if arch_folder == 'winarm64':
            # Find the compiler again if needed
            if self._cl_path is None and main_cl_path is not None:
                self._cl_path = main_cl_path
            elif self._cl_path is None:
                self._cl_path = find_cl_exe('winarm64', host_arch)
            
            # Get machine type
            machine_type = determine_machine_type(self._cl_path)
                
            # Try to build if we have a compiler
            if self._cl_path:
                output_dir = os.path.join(self.build_lib, *ext.name.split('.')[:-1])
                
                print(f"Building extension {ext.name} with compiler {self._cl_path} (machine type: {machine_type})...")
                
                # Update compiler path in environment
                compiler_dir = os.path.dirname(self._cl_path)
                os.environ['PATH'] = compiler_dir + os.pathsep + os.environ['PATH']
                
                # Update compiler flags through environment variables instead of directly
                # modifying compiler attributes
                if machine_type == 'x64':
                    # Remove ARM64 flag from extension if present
                    updated_args = []
                    for flag in ext.extra_compile_args:
                        if '/arch:arm64' not in flag:
                            updated_args.append(flag)
                    ext.extra_compile_args = updated_args
                    
                    # Set correct machine type for link in extension
                    for i, arg in enumerate(ext.extra_link_args):
                        if '/machine:ARM64' in arg:
                            ext.extra_link_args[i] = '/machine:X64'

                # Set target architecture in environment variables
                if machine_type == 'x64':
                    os.environ['VSCMD_ARG_TGT_ARCH'] = 'x64'
                    os.environ['Platform'] = 'X64'
                    
                    # Fix the output filename to use win_amd64 suffix instead of win_arm64
                    # This is the critical part that was missing
                    # Create the extension filename with proper architecture name
                    if hasattr(ext, '_full_name'):
                        module_name = ext._full_name
                        ext_path = ext.name.split('.')
                        ext_suffix = f".cp{sys.version_info.major}{sys.version_info.minor}-win_amd64.pyd"
                        filename = os.path.join(*ext_path[:-1], f"{ext_path[-1]}{ext_suffix}")
                        setattr(ext, '_file_name', filename)
                        print(f"Set extension filename to: {filename}")
                else:
                    os.environ['VSCMD_ARG_TGT_ARCH'] = 'arm64'
                    os.environ['Platform'] = 'ARM64'
                
                # Update the build output folder if needed
                if machine_type == 'x64':
                    # Change build directory structure to use amd64 instead of arm64
                    self.build_lib = self.build_lib.replace('arm64', 'amd64')
        
        # Use standard build method
        super().build_extension(ext)
    
    def get_ext_filename(self, ext_name):
        """Override to ensure extensions get the correct architecture suffix."""
        filename = super().get_ext_filename(ext_name)
        
        # For ARM64 host but x64 target, use win_amd64 suffix
        if arch_folder == 'winarm64' and machine_type == 'x64':
            filename = filename.replace('win_arm64', 'win_amd64')
            print(f"Modified extension filename: {filename}")
        
        return filename

# Use our custom build extension class
setup(
    name="mssql-python",
    version="0.1.0",
    packages=all_packages,
    package_data={
        'mssql_python': ['*.pyi', 'mssql_python_trace.log', '*.dll'],
        'mssql_python.libs.win32': ['*.dll', '1033/*.rll'],
        'mssql_python.libs.win64': ['*.dll', '1033/*.rll'],
        'mssql_python.libs.winarm64': ['*.dll', '1033/*.rll'],
        'mssql_python.pybind': ['*.cpp', '*.h'],
    },
    ext_modules=ext_modules,
    cmdclass={"build_ext": CustomBuildExt},
    include_package_data=True,
)