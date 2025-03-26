#include <pybind11/pybind11.h>
#include <string>

namespace py = pybind11;

// A simple function that returns the target architecture
std::string get_architecture() {
#ifdef TARGET_ARM64
    return "ARM64";
#elif defined(TARGET_X64)
    return "AMD64 (x64)";
#elif defined(TARGET_X86)
    return "x86";
#else
    return "Unknown architecture";
#endif
}

// A simple function that adds two numbers
int add(int a, int b) {
    return a + b;
}

// The module definition - keep this minimal for cross-compilation
PYBIND11_MODULE(sample_extension, m) {
    m.doc() = "Sample extension module built for different architectures";
    
    // Add functions
    m.def("get_architecture", &get_architecture, "Returns the target architecture of the extension");
    m.def("add", &add, "Add two numbers");
}