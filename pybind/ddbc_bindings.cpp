std::wstring LoadDriverOrThrowException(const std::wstring& modulePath = L"") {
    std::wstring ddbcModulePath = modulePath;
    if (ddbcModulePath.empty()) {
        // Get the module path if not provided
        std::string path = GetModuleDirectory();
        ddbcModulePath = std::wstring(path.begin(), path.end());
    }

    std::wstring dllDir = ddbcModulePath + L"\\libs\\win\\1033\\msodbcsql18.dll";

    LOG("Attempting to load driver from - {}", std::string(dllDir.begin(), dllDir.end()));

    HMODULE hModule = LoadLibraryW(dllDir.c_str());
    if (!hModule) {
        DWORD error = GetLastError();
        char* messageBuffer = nullptr;
        size_t size = FormatMessageA(
            FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
            NULL,
            error,
            MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
            (LPSTR)&messageBuffer,
            0,
            NULL
        );
        std::string errorMessage = messageBuffer ? std::string(messageBuffer, size) : "Unknown error";
        LocalFree(messageBuffer);

        LOG("Failed to load the driver with error code: {} - {}", error, errorMessage);
        throw std::runtime_error("Failed to load ODBC driver, error = " + std::to_string(error));
    }

    LOG("Successfully loaded driver from - {}", std::string(dllDir.begin(), dllDir.end()));

    return dllDir;
}