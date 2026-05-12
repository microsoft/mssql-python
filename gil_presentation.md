---
marp: true
theme: default
paginate: true
backgroundColor: #1a1a2e
color: #eaeaea
style: |
  section {
    font-family: 'Segoe UI', Arial, sans-serif;
  }
  h1 {
    color: #e94560;
  }
  h2 {
    color: #0f3460;
    background: #e94560;
    padding: 4px 16px;
    border-radius: 8px;
    display: inline-block;
  }
  code {
    background: #16213e;
    color: #e94560;
  }
  pre {
    background: #16213e !important;
  }
  strong {
    color: #e94560;
  }
  table th {
    background: #0f3460;
  }
---

# 🐍 The Global Interpreter Lock (GIL)

### What it does, why it exists, and how to work around it

<br>

> *"The GIL is a mutex that protects access to Python objects, preventing multiple threads from executing Python bytecodes at once."*
> — Python Documentation

---

## 🔒 What Is the GIL?

- A **mutex** (lock) inside CPython that allows **only one thread** to execute Python bytecode at any given time
- Even on multi-core machines, Python threads **cannot run in parallel** for CPU-bound work
- It protects CPython's **reference-counting** memory management from race conditions

```
Thread 1  ██████░░░░░░██████░░░░░░██████
Thread 2  ░░░░░░██████░░░░░░██████░░░░░░
                 ↑ only one thread runs at a time
```

- The GIL is **released** during I/O operations (network, disk, sleep)
- It is specific to **CPython** — Jython and IronPython do not have one

---

## ⚙️ How the GIL Works

1. A thread **acquires the GIL** before executing any Python bytecode
2. After a fixed interval (~5 ms in Python 3.2+), the interpreter **forces a GIL release**
3. Other waiting threads **compete** to acquire it next

```python
import threading, time

counter = 0

def increment():
    global counter
    for _ in range(1_000_000):
        counter += 1          # NOT thread-safe despite the GIL!

t1 = threading.Thread(target=increment)
t2 = threading.Thread(target=increment)
t1.start(); t2.start()
t1.join();  t2.join()

print(counter)  # Often < 2,000,000 — bytecode ops interleave
```

| Scenario | GIL Impact |
|---|---|
| **CPU-bound** threads | Serialized — no parallel speedup |
| **I/O-bound** threads | GIL released during wait — concurrency works |
| **C extensions** | Can manually release GIL for true parallelism |

---

## 😤 Problems & Why It Still Exists

### Problems
- **CPU-bound multi-threading is effectively single-threaded**
- Threads contend for the GIL → overhead with no parallel gain
- Makes Python slower than expected for compute-heavy workloads

### Why it still exists
- Simplifies CPython internals and **C extension development**
- Removing it would break the **reference-counting garbage collector**
- Single-threaded programs would get **slower** without it
- Decades of C extensions **depend** on GIL guarantees

### 🚀 Python 3.13+ (PEP 703)
- Experimental **free-threaded mode** (`--disable-gil` build)
- Aims to remove the GIL while maintaining compatibility
- Still **opt-in and experimental** — the GIL isn't gone yet

---

## ⚠️ Bad GIL Handling in C Extensions Can Stall Your Entire App

A C extension that **forgets to release the GIL** during native work blocks **all** Python threads:

```
 C extension holds GIL for 500ms of number crunching...
 ┌──────────────────────────────────────────────────┐
 │  Thread 1 (C ext)  ██████████████████████████████ │ ← holds GIL in C code
 │  Thread 2 (Python)  ░░░░░░░░░░░░░░░░░░░░░░░░░░░░ │ ← blocked, waiting for GIL
 │  Thread 3 (I/O)     ░░░░░░░░░░░░░░░░░░░░░░░░░░░░ │ ← even I/O thread is frozen
 └──────────────────────────────────────────────────┘
```

**How it happens in C extension code:**
```c
// ❌ BAD — holds the GIL during expensive native work
static PyObject* slow_transform(PyObject* self, PyObject* args) {
    // GIL is held the entire time — all other threads are frozen
    heavy_computation(data, len);       // 500ms of blocking
    return PyLong_FromLong(result);
}

// ✅ GOOD — releases the GIL so other threads can run
static PyObject* fast_transform(PyObject* self, PyObject* args) {
    Py_BEGIN_ALLOW_THREADS              // release GIL
    heavy_computation(data, len);       // other threads run freely
    Py_END_ALLOW_THREADS                // re-acquire GIL
    return PyLong_FromLong(result);
}
```

**Real-world symptoms:** web server stops responding, async event loops freeze, logging stalls — all because **one library** monopolized the GIL.

---

## 🛠️ Working Around the GIL

| Approach | Best For | How |
|---|---|---|
| `multiprocessing` | CPU-bound work | Separate processes, each with its own GIL |
| `concurrent.futures` | Mixed workloads | `ProcessPoolExecutor` for CPU, `ThreadPoolExecutor` for I/O |
| `asyncio` | I/O-bound work | Single-threaded event loop, no GIL contention |
| **C extensions** | Performance-critical code | Release GIL with `Py_BEGIN_ALLOW_THREADS` |
| **NumPy / Pandas** | Numeric computation | Internal C code releases the GIL |

```python
from concurrent.futures import ProcessPoolExecutor
import math

def heavy_computation(n):
    return sum(math.sqrt(i) for i in range(n))

# True parallelism — each process has its own interpreter & GIL
with ProcessPoolExecutor(max_workers=4) as pool:
    results = list(pool.map(heavy_computation, [10**7]*4))
```

**Key takeaway:** Use **threads for I/O**, **processes for CPU**, and the GIL won't hold you back.
