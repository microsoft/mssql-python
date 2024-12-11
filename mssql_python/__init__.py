from .utils import connect
from .constants import (
    ConstantsODBC
)
import sys
from utils import get_odbc_dll_path
import ctypes

# GLOBALS
# Read-Only
apilevel = '2.0'
paramstyle = 'pyformat'
threadsafety = 1

# Exceptions
# https://www.python.org/dev/peps/pep-0249/#exceptions
from .exceptions import (
    Warning, Error, InterfaceError, DatabaseError, DataError, OperationalError,
    IntegrityError, InternalError, ProgrammingError, NotSupportedError
)

# Type Objects
from .type import (
    Date, Time, Timestamp, DateFromTicks, TimeFromTicks, TimestampFromTicks, Binary,
    STRING, BINARY, NUMBER, DATETIME, ROWID
)

# Connection Objects
from .connection import Connection

# Cursor Objects
from .cursor import Cursor

# Loading ODBC DLL: to be changed post pybind11 integration
if sys.platform == 'win32':
    odbc = ctypes.windll.LoadLibrary(get_odbc_dll_path("msodbcsql18.dll"))
elif sys.platform == 'darwin':
    pass
elif sys.platform == 'linux':
    pass