#pragma once

#if defined(__linux__) && defined(__OPTIMIZE__) && !defined(_FORTIFY_SOURCE)
#define _FORTIFY_SOURCE 2
#endif
