#define NOMINMAX
#include <windows.h>
#include <dbghelp.h>
#include <cstdio>
#include <cwchar>
#include <map>
#include <string>
#include <vector>

static void logModule(DWORD pid, void* base, const wchar_t* path) {
    const wchar_t* name = wcsrchr(path, L'\\');
    name = name ? name + 1 : path;
    DWORD unused = 0;
    DWORD size = GetFileVersionInfoSizeW(path, &unused);
    DWORD ms = 0, ls = 0;
    if (size) {
        std::vector<unsigned char> bytes(size);
        VS_FIXEDFILEINFO* info = nullptr;
        UINT length = 0;
        if (GetFileVersionInfoW(path, 0, size, bytes.data()) &&
            VerQueryValueW(bytes.data(), L"\\", reinterpret_cast<void**>(&info), &length) &&
            length >= sizeof(VS_FIXEDFILEINFO)) {
            ms = info->dwFileVersionMS;
            ls = info->dwFileVersionLS;
        }
    }
    fwprintf(stderr, L"MODULE pid=%lu base=%p name=%ls version=%lu.%lu.%lu.%lu\n",
             pid, base, name, ms >> 16, ms & 65535, ls >> 16, ls & 65535);
}

static std::wstring quote(const wchar_t* text) {
    std::wstring result = L"\"";
    size_t slashes = 0;
    for (const wchar_t* p = text; *p; ++p) {
        if (*p == L'\\') {
            ++slashes;
        } else {
            result.append(slashes * (*p == L'"' ? 2 : 1), L'\\');
            slashes = 0;
            if (*p == L'"') result += L'\\';
            result += *p;
        }
    }
    result.append(slashes * 2, L'\\');
    return result + L'"';
}

static void saveCrash(HANDLE process, HANDLE thread, DWORD pid, DWORD tid,
                      EXCEPTION_RECORD record, const std::wstring& prefix) {
    CONTEXT context{};
    context.ContextFlags = CONTEXT_ALL;
    if (!GetThreadContext(thread, &context)) {
        fprintf(stderr, "GET_CONTEXT_FAILED %lu\n", GetLastError());
        return;
    }
    fprintf(stderr, "SECOND_CHANCE pid=%lu tid=%lu code=%08lx pc=%p address=%p\n",
            pid, tid, record.ExceptionCode, (void*)context.Rip,
            record.NumberParameters >= 2 ? (void*)record.ExceptionInformation[1] : nullptr);
    fprintf(stderr, "REGISTERS rax=%016llx rbx=%016llx rcx=%016llx rdx=%016llx rsp=%016llx\n",
            context.Rax, context.Rbx, context.Rcx, context.Rdx, context.Rsp);
    std::wstring name = prefix + L"-" + std::to_wstring(pid) + L".dmp";
    HANDLE file = prefix == L"-" ? INVALID_HANDLE_VALUE :
        CreateFileW(name.c_str(), GENERIC_WRITE, 0, nullptr, CREATE_NEW,
                    FILE_ATTRIBUTE_NORMAL, nullptr);
    if (file != INVALID_HANDLE_VALUE) {
        EXCEPTION_POINTERS pointers{&record, &context};
        MINIDUMP_EXCEPTION_INFORMATION info{tid, &pointers, FALSE};
        bool ok = MiniDumpWriteDump(
            process, pid, file,
            (MINIDUMP_TYPE)(MiniDumpWithFullMemory | MiniDumpWithThreadInfo |
                            MiniDumpWithUnloadedModules),
            &info, nullptr, nullptr) != 0;
        fprintf(stderr, "DUMP saved=%d error=%lu\n", ok, ok ? 0 : GetLastError());
        CloseHandle(file);
    }
    SymSetOptions(SYMOPT_DEFERRED_LOADS | SYMOPT_UNDNAME | SYMOPT_LOAD_LINES |
                  SYMOPT_FAIL_CRITICAL_ERRORS | SYMOPT_NO_PROMPTS);
    if (!SymInitialize(process, ".", TRUE)) return;
    STACKFRAME64 frame{};
    frame.AddrPC = {context.Rip, 0, AddrModeFlat};
    frame.AddrStack = {context.Rsp, 0, AddrModeFlat};
    frame.AddrFrame = {context.Rbp, 0, AddrModeFlat};
    for (int i = 0; i < 64 && frame.AddrPC.Offset; ++i) {
        DWORD64 address = frame.AddrPC.Offset, displacement = 0;
        alignas(SYMBOL_INFO) char buffer[sizeof(SYMBOL_INFO) + MAX_SYM_NAME]{};
        auto symbol = reinterpret_cast<SYMBOL_INFO*>(buffer);
        symbol->SizeOfStruct = sizeof(SYMBOL_INFO);
        symbol->MaxNameLen = MAX_SYM_NAME;
        IMAGEHLP_MODULE64 module{};
        module.SizeOfStruct = sizeof(module);
        SymGetModuleInfo64(process, address, &module);
        bool found = SymFromAddr(process, address, &displacement, symbol) != 0;
        fprintf(stderr, "FRAME %02d %016llx %s!%s+0x%llx\n", i, address,
                module.ModuleName, found ? symbol->Name : "<no-symbol>",
                found ? displacement : address - module.BaseOfImage);
        if (!StackWalk64(IMAGE_FILE_MACHINE_AMD64, process, thread, &frame, &context,
                         nullptr, SymFunctionTableAccess64, SymGetModuleBase64, nullptr)) break;
    }
    SymCleanup(process);
}

int wmain(int argc, wchar_t** argv) {
    if (argc < 5) {
        fprintf(stderr, "usage: native_probe seconds dump-prefix executable [args...]\n");
        return 2;
    }
    std::wstring command;
    for (int i = 3; i < argc; ++i) {
        if (i > 3) command += L' ';
        command += quote(argv[i]);
    }
    STARTUPINFOW startup{};
    startup.cb = sizeof(startup);
    PROCESS_INFORMATION initial{};
    if (!CreateProcessW(nullptr, command.data(), nullptr, nullptr, TRUE,
                        DEBUG_PROCESS, nullptr, nullptr, &startup, &initial)) {
        fprintf(stderr, "CREATE_PROCESS_FAILED %lu\n", GetLastError());
        return 3;
    }
    CloseHandle(initial.hProcess);
    CloseHandle(initial.hThread);
    std::map<DWORD, HANDLE> processes;
    std::map<DWORD, HANDLE> threads;
    DWORD rootExit = 0;
    bool captured = false;
    ULONGLONG deadline = GetTickCount64() + wcstoul(argv[1], nullptr, 10) * 1000ULL;
    do {
        DEBUG_EVENT event{};
        if (!WaitForDebugEvent(&event, 1000)) {
            if (GetLastError() != ERROR_SEM_TIMEOUT) return 4;
            if (GetTickCount64() > deadline) {
                for (auto& entry : processes) TerminateProcess(entry.second, 124);
                deadline = GetTickCount64() + 10000;
                fprintf(stderr, "BOUNDED_TIMEOUT\n");
            }
            continue;
        }
        DWORD status = DBG_CONTINUE;
        switch (event.dwDebugEventCode) {
            case CREATE_PROCESS_DEBUG_EVENT:
                processes[event.dwProcessId] = event.u.CreateProcessInfo.hProcess;
                threads[event.dwThreadId] = event.u.CreateProcessInfo.hThread;
                if (event.u.CreateProcessInfo.hFile) CloseHandle(event.u.CreateProcessInfo.hFile);
                fprintf(stderr, "CREATE pid=%lu\n", event.dwProcessId);
                break;
            case CREATE_THREAD_DEBUG_EVENT:
                threads[event.dwThreadId] = event.u.CreateThread.hThread;
                break;
            case LOAD_DLL_DEBUG_EVENT:
                if (event.u.LoadDll.hFile) {
                    wchar_t path[32768]{};
                    GetFinalPathNameByHandleW(event.u.LoadDll.hFile, path, 32768, 0);
                    logModule(event.dwProcessId, event.u.LoadDll.lpBaseOfDll, path);
                    CloseHandle(event.u.LoadDll.hFile);
                }
                break;
            case UNLOAD_DLL_DEBUG_EVENT:
                fprintf(stderr, "UNLOAD pid=%lu base=%p\n", event.dwProcessId,
                        event.u.UnloadDll.lpBaseOfDll);
                break;
            case EXCEPTION_DEBUG_EVENT: {
                const auto& exception = event.u.Exception;
                if (!exception.dwFirstChance && !captured) {
                    captured = true;
                    saveCrash(processes.at(event.dwProcessId), threads.at(event.dwThreadId),
                              event.dwProcessId, event.dwThreadId,
                              exception.ExceptionRecord, argv[2]);
                }
                if (exception.ExceptionRecord.ExceptionCode != EXCEPTION_BREAKPOINT)
                    status = DBG_EXCEPTION_NOT_HANDLED;
                break;
            }
            case EXIT_THREAD_DEBUG_EVENT:
                CloseHandle(threads.at(event.dwThreadId));
                threads.erase(event.dwThreadId);
                break;
            case EXIT_PROCESS_DEBUG_EVENT:
                fprintf(stderr, "EXIT pid=%lu code=%08lx\n", event.dwProcessId,
                        event.u.ExitProcess.dwExitCode);
                if (event.dwProcessId == initial.dwProcessId) rootExit = event.u.ExitProcess.dwExitCode;
                CloseHandle(processes.at(event.dwProcessId));
                processes.erase(event.dwProcessId);
                break;
        }
        ContinueDebugEvent(event.dwProcessId, event.dwThreadId, status);
    } while (!processes.empty());
    for (auto& entry : threads) CloseHandle(entry.second);
    return static_cast<int>(rootExit);
}
