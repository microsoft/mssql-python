#define NOMINMAX
#include <windows.h>
#include <dbghelp.h>
#include <cstdio>
#include <cwchar>
#include <cstring>
#include <map>
#include <string>
#include <vector>

#ifndef _M_X64
#error This diagnostic collector requires Windows x64.
#endif

static ULONGLONG unixMilliseconds() {
    FILETIME time{};
    GetSystemTimeAsFileTime(&time);
    ULARGE_INTEGER value{};
    value.LowPart = time.dwLowDateTime;
    value.HighPart = time.dwHighDateTime;
    return value.QuadPart / 10000ULL - 11644473600000ULL;
}

__declspec(noinline) static void hosted_probe_symbol_smoke() {
    volatile ULONG_PTR address = 0;
    *reinterpret_cast<volatile int*>(address) = 1;
}

static BOOL CALLBACK findFreeSymbol(PSYMBOL_INFO symbol, ULONG, PVOID context) {
    if (strstr(symbol->Name, "SqlHandle::free"))
        *static_cast<bool*>(context) = true;
    return TRUE;
}

static int verifyPdb(const wchar_t* image) {
    HANDLE process = GetCurrentProcess();
    SymSetOptions(SYMOPT_DEFERRED_LOADS | SYMOPT_UNDNAME |
                  SYMOPT_FAIL_CRITICAL_ERRORS | SYMOPT_NO_PROMPTS);
    if (!SymInitialize(process, ".", FALSE)) return 20;
    DWORD64 base = SymLoadModuleExW(process, nullptr, image, nullptr, 0, 0, nullptr, 0);
    IMAGEHLP_MODULE64 module{};
    module.SizeOfStruct = sizeof(module);
    bool found = false;
    bool ok = base && SymEnumSymbols(process, base, nullptr, findFreeSymbol, &found) &&
              SymGetModuleInfo64(process, base, &module) &&
              found && module.SymType == SymPdb && !module.PdbUnmatched;
    printf("PDB_VERIFY matched=%d SqlHandle_free=%d age=%lu guid=%08lx-%04x-%04x-"
           "%02x%02x-%02x%02x%02x%02x%02x%02x\n", ok, found, module.PdbAge,
           module.PdbSig70.Data1, module.PdbSig70.Data2, module.PdbSig70.Data3,
           module.PdbSig70.Data4[0], module.PdbSig70.Data4[1],
           module.PdbSig70.Data4[2], module.PdbSig70.Data4[3],
           module.PdbSig70.Data4[4], module.PdbSig70.Data4[5],
           module.PdbSig70.Data4[6], module.PdbSig70.Data4[7]);
    SymCleanup(process);
    return ok ? 0 : 21;
}

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

static void logProcess(DWORD pid, HANDLE file, bool root) {
    wchar_t path[32768]{};
    if (file) GetFinalPathNameByHandleW(file, path, 32768, 0);
    const wchar_t* name = wcsrchr(path, L'\\');
    name = name ? name + 1 : path;
    fwprintf(stderr, L"PROCESS pid=%lu root=%d image=%ls\n", pid, root, name);
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

static void logCrash(HANDLE process, HANDLE thread, DWORD pid, DWORD tid,
                     EXCEPTION_RECORD record) {
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
    SymSetOptions(SYMOPT_DEFERRED_LOADS | SYMOPT_UNDNAME | SYMOPT_LOAD_LINES |
                  SYMOPT_FAIL_CRITICAL_ERRORS | SYMOPT_NO_PROMPTS);
    if (!SymInitialize(process, ".", TRUE)) {
        fprintf(stderr, "SYMBOL_INIT_FAILED %lu\n", GetLastError());
        return;
    }
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

static bool activeProcesses(HANDLE job, DWORD& active) {
    JOBOBJECT_BASIC_ACCOUNTING_INFORMATION info{};
    if (!QueryInformationJobObject(job, JobObjectBasicAccountingInformation,
                                   &info, sizeof(info), nullptr)) {
        fprintf(stderr, "TREE_QUERY_FAILED error=%lu\n", GetLastError());
        return false;
    }
    active = info.ActiveProcesses;
    return true;
}

static bool drainJob(HANDLE job, HANDLE root, ULONGLONG deadline) {
    for (;;) {
        DWORD state = WaitForSingleObject(root, 0);
        if (state != WAIT_OBJECT_0 && state != WAIT_TIMEOUT) {
            fprintf(stderr, "TREE_DRAIN_INCOMPLETE reason=root_wait error=%lu\n", GetLastError());
            return false;
        }
        DWORD active = 0;
        if (!activeProcesses(job, active)) {
            fprintf(stderr, "TREE_DRAIN_INCOMPLETE reason=accounting\n");
            return false;
        }
        if (state == WAIT_OBJECT_0 && active == 0) {
            fprintf(stderr, "TREE_DRAIN_COMPLETE active=0 root_signaled=1\n");
            return true;
        }
        ULONGLONG now = GetTickCount64();
        if (now >= deadline) {
            fprintf(stderr, "TREE_DRAIN_INCOMPLETE reason=deadline active=%lu root_signaled=%d\n",
                    active, state == WAIT_OBJECT_0);
            return false;
        }
        Sleep(static_cast<DWORD>((deadline - now) < 10 ? deadline - now : 10));
    }
}

static bool terminateAndDrain(HANDLE job, HANDLE root, DWORD code, ULONGLONG deadline) {
    bool terminated = TerminateJobObject(job, code) != FALSE;
    if (!terminated) {
        fprintf(stderr, "TERMINATE_JOB_FAILED error=%lu\n", GetLastError());
    }
    bool drained = drainJob(job, root, deadline);
    if (!terminated || !drained) {
        fprintf(stderr, "TREE_CLEANUP_INCOMPLETE termination_confirmed=%d drained=%d\n",
                terminated, drained);
    }
    return terminated && drained;
}

int wmain(int argc, wchar_t** argv) {
    SetErrorMode(SEM_FAILCRITICALERRORS | SEM_NOGPFAULTERRORBOX);
    if (argc == 2 && wcscmp(argv[1], L"--symbol-smoke") == 0) {
        if (!IsDebuggerPresent()) return 22;
        hosted_probe_symbol_smoke();
        return 23;
    }
    if (argc == 3 && wcscmp(argv[1], L"--verify-pdb") == 0) return verifyPdb(argv[2]);
    bool plain = argc > 1 && wcscmp(argv[1], L"--plain") == 0;
    int deadlineIndex = plain ? 2 : 1;
    int executableIndex = deadlineIndex + 1;
    if (argc <= executableIndex) {
        fprintf(stderr, "usage: native_probe [--plain] deadline-unix-ms executable [args...]\n");
        return 2;
    }
    wchar_t* end = nullptr;
    ULONGLONG absoluteDeadline = _wcstoui64(argv[deadlineIndex], &end, 10);
    ULONGLONG now = unixMilliseconds();
    if (*end || absoluteDeadline <= now || absoluteDeadline - now > 3600000ULL) return 124;
    ULONGLONG deadline = GetTickCount64() + absoluteDeadline - now;
    std::wstring command;
    for (int i = executableIndex; i < argc; ++i) {
        if (i > executableIndex) command += L' ';
        command += quote(argv[i]);
    }
    HANDLE job = CreateJobObjectW(nullptr, nullptr);
    JOBOBJECT_EXTENDED_LIMIT_INFORMATION limits{};
    limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
    if (!job || !SetInformationJobObject(job, JobObjectExtendedLimitInformation,
                                         &limits, sizeof(limits))) {
        fprintf(stderr, "JOB_CONFIGURATION_FAILED error=%lu\n", GetLastError());
        if (job) CloseHandle(job);
        return 6;
    }
    STARTUPINFOW startup{};
    startup.cb = sizeof(startup);
    PROCESS_INFORMATION initial{};
    if (!CreateProcessW(nullptr, command.data(), nullptr, nullptr, TRUE,
                        plain ? CREATE_SUSPENDED : DEBUG_PROCESS,
                        nullptr, nullptr, &startup, &initial)) {
        fprintf(stderr, "CREATE_PROCESS_FAILED %lu\n", GetLastError());
        CloseHandle(job);
        return 3;
    }
    if (!AssignProcessToJobObject(job, initial.hProcess)) {
        fprintf(stderr, "ASSIGN_JOB_FAILED %lu\n", GetLastError());
        bool terminated = TerminateProcess(initial.hProcess, 6) != FALSE;
        if (!terminated) {
            fprintf(stderr, "TERMINATE_PROCESS_FAILED error=%lu\n", GetLastError());
        }
        DWORD state = WaitForSingleObject(initial.hProcess, 5000);
        bool drained = state == WAIT_OBJECT_0;
        if (!drained) {
            fprintf(stderr, "ROOT_TERMINATION_WAIT_FAILED state=%lu error=%lu\n",
                    state, state == WAIT_FAILED ? GetLastError() : 0);
        }
        if (!terminated || !drained) {
            fprintf(stderr, "TREE_CLEANUP_INCOMPLETE termination_confirmed=%d drained=%d\n",
                    terminated, drained);
        }
        CloseHandle(initial.hThread);
        CloseHandle(initial.hProcess);
        CloseHandle(job);
        return terminated && drained ? 6 : 125;
    }
    if (plain) {
        DWORD code = 126;
        bool terminate = true;
        if (ResumeThread(initial.hThread) == static_cast<DWORD>(-1)) {
            fprintf(stderr, "RESUME_THREAD_FAILED error=%lu\n", GetLastError());
        } else {
            ULONGLONG tick = GetTickCount64();
            DWORD remaining = static_cast<DWORD>(deadline > tick ? deadline - tick : 0);
            DWORD state = WaitForSingleObject(initial.hProcess, remaining);
            if (state == WAIT_TIMEOUT) {
                code = 124;
                fprintf(stderr, "BOUNDED_TIMEOUT\n");
            } else if (state != WAIT_OBJECT_0) {
                fprintf(stderr, "ROOT_WAIT_FAILED error=%lu\n", GetLastError());
            } else if (!GetExitCodeProcess(initial.hProcess, &code)) {
                fprintf(stderr, "ROOT_EXIT_CODE_FAILED error=%lu\n", GetLastError());
                code = 126;
            } else {
                DWORD active = 0;
                if (!activeProcesses(job, active)) {
                    code = 126;
                } else if (active != 0) {
                    fprintf(stderr, "UNFINISHED_DESCENDANTS active=%lu root_code=%08lx\n",
                            active, code);
                    if (code == 0) code = 125;
                } else {
                    terminate = false;
                }
            }
        }
        ULONGLONG cleanupDeadline = GetTickCount64() + 5000;
        bool drained = terminate ?
            terminateAndDrain(job, initial.hProcess, code, cleanupDeadline) :
            drainJob(job, initial.hProcess, cleanupDeadline);
        if (!drained) code = 125;
        fprintf(stderr, "PLAIN_EXIT pid=%lu code=%08lx cleanup_complete=%d\n",
                initial.dwProcessId, code, drained);
        CloseHandle(initial.hThread);
        CloseHandle(initial.hProcess);
        CloseHandle(job);
        return static_cast<int>(code);
    }
    CloseHandle(initial.hThread);
    std::map<DWORD, HANDLE> processes;
    std::map<DWORD, HANDLE> threads;
    DWORD rootExit = 0;
    bool rootExited = false;
    bool timedOut = false;
    bool cleanupFailed = false;
    do {
        // Check on every iteration, including when debug events arrive continuously.
        if (GetTickCount64() >= deadline) {
            if (timedOut) break;
            timedOut = true;
            if (!TerminateJobObject(job, 124)) {
                fprintf(stderr, "TERMINATE_JOB_FAILED error=%lu\n", GetLastError());
                cleanupFailed = true;
            }
            deadline = GetTickCount64() + 5000;
            fprintf(stderr, "BOUNDED_TIMEOUT\n");
        }
        DEBUG_EVENT event{};
        ULONGLONG tick = GetTickCount64();
        ULONGLONG remaining = deadline > tick ? deadline - tick : 0;
        if (!WaitForDebugEvent(&event, static_cast<DWORD>(remaining < 100 ? remaining : 100))) {
            if (GetLastError() != ERROR_SEM_TIMEOUT) {
                fprintf(stderr, "WAIT_DEBUG_FAILED %lu\n", GetLastError());
                bool drained = terminateAndDrain(
                    job, initial.hProcess, 4, timedOut ? deadline : GetTickCount64() + 5000);
                for (auto& entry : processes) CloseHandle(entry.second);
                for (auto& entry : threads) CloseHandle(entry.second);
                CloseHandle(initial.hProcess);
                CloseHandle(job);
                return drained ? 4 : 125;
            }
            continue;
        }
        DWORD status = DBG_CONTINUE;
        switch (event.dwDebugEventCode) {
            case CREATE_PROCESS_DEBUG_EVENT:
                processes[event.dwProcessId] = event.u.CreateProcessInfo.hProcess;
                threads[event.dwThreadId] = event.u.CreateProcessInfo.hThread;
                logProcess(event.dwProcessId, event.u.CreateProcessInfo.hFile,
                           event.dwProcessId == initial.dwProcessId);
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
                if (!exception.dwFirstChance) {
                    logCrash(processes.at(event.dwProcessId), threads.at(event.dwThreadId),
                             event.dwProcessId, event.dwThreadId, exception.ExceptionRecord);
                    // End the stopped faulting process here, without entering its WER path.
                    if (!TerminateProcess(processes.at(event.dwProcessId),
                                          exception.ExceptionRecord.ExceptionCode)) {
                        fprintf(stderr, "TERMINATE_PROCESS_FAILED error=%lu\n", GetLastError());
                        cleanupFailed = true;
                    }
                } else if (exception.ExceptionRecord.ExceptionCode != EXCEPTION_BREAKPOINT) {
                    status = DBG_EXCEPTION_NOT_HANDLED;
                }
                break;
            }
            case EXIT_THREAD_DEBUG_EVENT:
                CloseHandle(threads.at(event.dwThreadId));
                threads.erase(event.dwThreadId);
                break;
            case EXIT_PROCESS_DEBUG_EVENT:
                fprintf(stderr, "EXIT pid=%lu code=%08lx\n", event.dwProcessId,
                        event.u.ExitProcess.dwExitCode);
                if (event.dwProcessId == initial.dwProcessId) {
                    rootExit = event.u.ExitProcess.dwExitCode;
                    rootExited = true;
                }
                CloseHandle(processes.at(event.dwProcessId));
                processes.erase(event.dwProcessId);
                break;
        }
        if (!ContinueDebugEvent(event.dwProcessId, event.dwThreadId, status)) {
            fprintf(stderr, "CONTINUE_DEBUG_FAILED error=%lu\n", GetLastError());
            cleanupFailed = true;
        }
    } while (!processes.empty());
    bool drained = drainJob(job, initial.hProcess, timedOut ? deadline : GetTickCount64() + 5000);
    if (cleanupFailed || !drained) {
        fprintf(stderr, "TREE_CLEANUP_INCOMPLETE termination_confirmed=%d drained=%d\n",
                !cleanupFailed, drained);
    }
    for (auto& entry : processes) CloseHandle(entry.second);
    for (auto& entry : threads) CloseHandle(entry.second);
    CloseHandle(initial.hProcess);
    CloseHandle(job);
    return cleanupFailed || !drained ? 125 : timedOut ? 124 : rootExited ? static_cast<int>(rootExit) : 5;
}
