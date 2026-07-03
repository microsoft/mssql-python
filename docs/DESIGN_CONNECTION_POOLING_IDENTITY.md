# Design: Identity-Aware Connection Pooling for Access Tokens

**Status:** Design / Planning
**Related issues:** #651 (identity-aware pooling); #659 (skip token acquisition on pool hits)
**Discussion:** Internal cross-driver design discussion

---

## Executive Summary

`mssql-python` ships an in-driver connection pool that is keyed **only** on the
connection string. For most auth methods the identity is part of that string, so
different identities naturally land in different pools. But for **Entra ID access-token**
authentication the identity is *not* in the connection string — and for those methods
`mssql-python` even strips `UID`/`PWD`/`Authentication` before the string becomes the
pool key. The result is a **privilege-escalation hole**: User B can be handed User A's
still-authenticated pooled connection because both share the same connection-string key.

This document proposes an **identity-aware "security context key"** — a small,
deterministic identity component appended to the pool key whenever a token is present.
The core invariant is simple: **when a token is present, the pool key is never the bare
connection string.** Each auth source contributes a precise, first-class identity signal
(Managed Identity → client/object id, with system-assigned needing none; Service Principal
→ `client_id`; Interactive / Device-code → `home_account_id` from `AuthenticationRecord`;
`DefaultAzureCredential` and raw tokens → token hash), so same-identity reuse stays fast
while cross-identity reuse is eliminated. The design also captures token `expires_on`
(currently discarded) and refreshes-or-discards near-expiry tokens on a **single
on-checkout threshold**, with **token-provider factories** as the clean long-term path
that removes expiry handling from our side entirely.

**Key guarantees:**

- **No breaking change for existing users.** Non-token connections (SQL auth, Windows
  integrated) and single-identity token apps see byte-for-byte identical behavior. Only
  cross-identity reuse — the vulnerability — changes.
- **Security fix on by default.** Identity-aware keying is a correctness fix and a no-op
  for single-identity apps, so it requires no opt-in.
- **Scoped delivery.** The work is split into two tracked enhancements: identity-aware
  partitioning for access tokens **now**, and Windows/Kerberos partitioning (safe today,
  documentation-only) as a **follow-up**.

---

## 1. Overview

`mssql-python` provides an in-driver connection pool (custom C++/pybind11, not an
external pool like HikariCP). Today the pool is keyed **only** on the connection
string. That works for auth methods where the identity is part of the connection
string, but it creates a **privilege-escalation hole** for Entra ID access-token
authentication, where the identity is *not* in the connection string.

This document captures:

- How pooling works today.
- Why token auth breaks the current model (the security problem).
- The current behavior matrix (where connStr-keying is safe vs. a known limitation).
- The proposed fix (an identity-aware "security context key").
- A related performance win it unlocks: skipping token acquisition on a pool hit (#659).
- All code changes required (Python + native).
- Feedback received from reviewers and how it shaped the design.
- Backward-compatibility / breaking-change analysis.
- Future scope, what ships now vs. later, and which scenarios are covered.

> **Two independent enhancements.** Per reviewer feedback, this work is deliberately
> split into two efforts that are being tracked separately:
> 1. **Identity-aware partitioning for access tokens** — *this release*.
> 2. **Windows/Kerberos and other Entra methods** — *follow-up* (mostly documentation;
>    see §11).

---

## 2. How pooling works today

### 2.1 Native structure

The native `ConnectionPoolManager` singleton holds a single shared map keyed by
connection string only:

```
_pools : map< connectionString , ConnectionPool >
```

- `mssql_python/pybind/connection/connection_pool.cpp` / `.h`
- `_default_max_size = 10` and `_default_idle_secs = 300` are **uninitialized native
  fallbacks**. On first connect, Python's `PoolingManager.enable()` overrides them
  with **100 / 600s** (see §2.3), so the effective per-pool defaults are 100 / 600.

### 2.2 Connection flow

1. Python builds the connection string and calls
   `ddbc_bindings.Connection(connStr, usePool, attrs_before)`
   (`mssql_python/connection.py`, ~line 405).
2. Native `acquireConnection(connStr, attrs)` looks up `_pools[connStr]`.
3. **Reuse path:** if a live idle connection exists in that pool, it is validated
   (`isAlive()` + `reset()`) and handed back — a fast reuse.
   - **Important:** on reuse, the caller's incoming `attrs_before` (including any new
     token) is **ignored**. Only `reset()` + liveness run.
4. **Create path:** otherwise a new physical `Connection(connStr, fromPool=true)` is
   opened via `connect(attrs_before)`.
5. On close, the connection is returned to `_pools[connStr]` for the next caller.

### 2.3 Python pooling manager

- `mssql_python/pooling.py` — `PoolingManager` singleton.
- `_config = {"max_size": 100, "idle_timeout": 600}`.
- `enable(max_size=100, idle_timeout=600)` calls `ddbc_bindings.enable_pooling(...)`,
  which configures the native manager (`configure(maxSize, idleTimeout)`).
- Pooling **auto-enables** on first connect if the app never called it explicitly
  (`connection.py`: `if not PoolingManager.is_initialized(): PoolingManager.enable()`).

**Net:** effective per-pool max size is **100 and configurable** (not hardcoded to 10).
The `10` in the C++ header is only the pre-configuration fallback.

---

## 3. The security problem (token auth)

An Entra access token is passed as a **deferred ODBC attribute**
(`SQL_COPT_SS_ACCESS_TOKEN`) that is consumed **once, at login**. After login, the
open connection is already authenticated as whoever logged in. On reuse, the pool's
`reset()`:

- issues `SQL_ATTR_RESET_CONNECTION` and resets session state (e.g. isolation level),
- **intentionally does not tear down the auth context** and **does not clear the token
  buffers** (this is correct for same-identity reuse; see `connection.cpp` `reset()`).

Because the pool key is the connection string only — and for token auth the identity
is **not** in the connection string — the following happens:

> User **A** connects (as themselves) → closes → the connection goes into
> `_pools[connStr]`. User **B** connects to the same server → same connection string →
> same pool → is handed **User A's still-authenticated connection**.

That is the **privilege-escalation hole**.

### 3.1 Contributing code facts (verified)

- For a **token-bearing** `Authentication=` (Entra) connection — the types
  `process_auth_parameters` returns non-`None` for (Default / DeviceCode / MSI /
  non-Windows Interactive) — `mssql-python` **strips** `UID` / `PWD` / `Authentication` /
  `Trusted_Connection` and rebuilds the connection string **before** it becomes the pool
  key (`connection.py`), and that sanitized string is what is passed to the native pool.
- **ServicePrincipal is the exception:** `process_auth_parameters` returns `None` for it
  (msodbcsql 17.3+ handles it natively), so it is **not** stripped — its `UID`(client_id)
  and `PWD` stay in the connection string, which already isolates distinct SPs. This is
  pre-existing behavior, not part of this work.
- Non-Entra connection strings are **not** stripped, so `UID`/`PWD` remain in the key.
- The access token's `expires_on` is currently **discarded** in
  `auth.py::_acquire_token` (`raw_token = credential.get_token(scope).token`), so expiry
  never reaches the native layer.
- Token attribute buffers are retained for the connection lifetime and reused on
  internal reconnect (ICR) — see issue #594.

---

## 4. Current behavior matrix (what overlaps with the connection-string key)

This answers the question "where does connStr-keying already partition identity
correctly, and where is it a known limitation?"

| Auth method | Identity in connStr key? | Pool behavior today | Status |
|---|---|---|---|
| **SQL auth** (UID/PWD, no `Authentication=`) | Yes (UID/PWD kept in key) | Different users → different pools | ✅ Safe today |
| **Windows Integrated / Kerberos / NTLM** | N/A — fixed process identity | Always same security context | ✅ Safe by design |
| **System-assigned Managed Identity** | N/A — one fixed identity | Single identity | ✅ Safe today |
| **Entra Service Principal** | Yes — handled natively by ODBC; UID(client_id)/PWD kept in key | Different SPs → different pools | ✅ Safe today |
| **User-assigned Managed Identity** | No — UID(client_id) stripped | Different UAMIs collide | ❌ Known limitation |
| **Interactive / Device-code / Default** | No — no user id in connStr | All identities collide | ❌ Known limitation |
| **Raw access token / `token_provider`** | No — not in connStr | All identities collide | ❌ Known limitation |

**Framing:** intelligent pool partitioning was never a stated goal. Partitioning by
connection string was a *side effect* that happened to isolate identities when the user
id was in the connection string. Publicly we document the ❌ rows as a **known
limitation** (comparable to many OSS pools that are "just a pool" or require the user to
partition manually) and update the docs to advertise smart partitioning as each row is
lifted.

---

## 5. Proposed fix — identity-aware "security context key"

### 5.1 Terminology

The **security context key** is a *subset* of the full pool key. The rest of the pool
key (database, app name, options) is already carried in the connection string, so the
security context is the only new component we add.

### 5.2 Core invariant

> **When a token is present, the pool key is NEVER the bare connection string.**

The composite key is:

```
key = connStr                                  (no token — unchanged behavior)
key = connStr + "\0" + <security-context>      (token present)
```

Fail-safe: if we cannot compute a specific security context, fall back to hashing the
token — **never** silently collapse to the bare connection string.

### 5.3 Security context per auth source

| Auth source | Security context component | Reuse behavior | Notes |
|---|---|---|---|
| **Managed Identity** | System-assigned: none. User-assigned: `client_id`/object id (from `UID`) | Full reuse | Provably fixed; **not** Python `id()` |
| **Service Principal** | `client_id` | Full reuse | Identity provably fixed |
| **Interactive / Device-code** | `home_account_id` (from `AuthenticationRecord`) | Reuse across refreshes; new pool on account change | See §5.4 |
| **DefaultAzureCredential** | Per-token isolation | New pool when token changes | Documented "not recommended for multi-user pooling" |
| **Raw token / token_provider** | `sha256(token_struct)` | Pool by token hash | See §5.5 |

### 5.4 Interactive / Device-code — the `home_account_id` approach

Originally we worried this required decoding the opaque access token to detect a user
change. That is **not** necessary. `azure-identity` provides documented, first-class
signals:

- `credential.authenticate()` returns an `AuthenticationRecord` with a public
  **`home_account_id`** ("a unique identifier of the account"). Same user → same id;
  different user → different id. No token decoding.
- `disable_automatic_authentication=True` makes `get_token()` raise
  `AuthenticationRequiredError` when interactive re-auth is required — an explicit
  "silent refresh vs. re-prompt" signal.

Plan: key on `home_account_id`, and rotate the pool key only when a new
`AuthenticationRecord` shows a different account. This requires reworking the current
bare `get_token()` flow to adopt the `authenticate()` / `AuthenticationRecord` pattern.

**Confirmed industry pattern.** The standard flow other drivers use is:

1. Try to acquire the token **silently** — from the cached account for the specified
   user id, or (if no user id) from the current cached account.
2. If that raises the "interaction required" signal (`MsalUiRequiredException` in .NET;
   `AuthenticationRequiredError` in `azure-identity` when
   `disable_automatic_authentication=True`), fall back to **interactive** acquisition.

Our approach maps directly onto this: silent `get_token()` first, and on
`AuthenticationRequiredError` call `authenticate()`. The matured `azure-identity`/MSAL
libraries additionally give us the `AuthenticationRecord.home_account_id`, which earlier
driver implementations did not have — so we can key precisely on the account without
decoding the token.

> **Behavior note (confirmed acceptable):** setting
> `disable_automatic_authentication=True` changes *when* the interactive prompt fires
> (we trigger it ourselves on `AuthenticationRequiredError`). Current prompt UX is
> preserved; this is a deliberate internal change and matches the silent-first pattern
> above.

### 5.5 Raw token / token_provider

Pool these **by token hash**. Do **not** refuse to pool them (an earlier idea that was
dropped). Document the contract: the app must never open a connection with an expired
token; ICR-with-expired-token is the **app's responsibility**. This matches other
drivers.

### 5.6 Token expiry handling

Capture `expires_on` from `credential.get_token(scope)` (an `AccessToken` with `.token`
and `.expires_on`) and thread it to native. Enforce expiry with a **single on-checkout
threshold** (not a multi-tier background scheme):

- On a pool checkout, if the pool's current token is within a small window of expiry
  (**~5 minutes; never above 10**, to avoid being handed back the identical cached
  token), ask the provider for a fresh token.
- **Same token returned** (SDK cache still valid) → reuse a pooled connection as normal.
- **Different token returned** → the pool's existing connections are **discarded** and
  new connections are opened with the new token (we do **not** try to update the token
  already inside `msodbcsql`; see §5.7).

This replaces the earlier JDBC two-tier (10–45 min proactive + < 10 min synchronous)
model. Because the refresh happens only on checkout and the `azure-identity` cache
returns a valid-or-refreshed token, a separate proactive/background "single-flight
refresher" tier is **not needed** — only a lightweight per-pool guard so concurrent
checkouts don't each fire a redundant `get_token()`.

**Static-token paths** (a raw token, or a `token_provider` that returns a fixed token)
have no SDK cache to refresh from, so there is nothing to ask: not opening with an
expired token, and ICR-with-expired-token, remain the **app's responsibility** (§5.5).

**Clean long-term path.** If an underlying provider (`msodbcsql`, `mssql-odbc`, or
`mssql-py-conn`) supports **token-provider factories**, expiry handling disappears from
our side entirely — the provider asks for a token when it needs one (on open or ICR).
This is the preferred end state (§5.7, §11).

### 5.7 Expiry enforcement on the native side — two options

Native cannot fetch a fresh token by itself; it only holds the `attrs_before` captured
at first open, and the reuse path currently ignores new attrs. So:

- **Option 1 — discard-and-reopen (chosen first, for correctness):** plumb the freshly
  re-acquired token (Python re-computes it cheaply when the SDK cache is warm) into the
  native reuse path so the reopen uses the new token, not the stale one.
- **Option 2 — overwrite the token buffer (deprioritized):** our `Connection` retains the
  token attr buffers for its lifetime and reuses them on ICR (issue #594); in principle we
  could overwrite that buffer so `msodbcsql` picks up a new token on the next reconnect.
  Per reviewer guidance we are **not** trying to update the token already inside
  `msodbcsql`, so this is deprioritized in favor of discard-and-reopen (Option 1) now and
  token-provider factories (below) long term.

Long term, the clean answer is **token-provider / callback factories** in the underlying
provider (`msodbcsql` / `mssql-odbc` / `mssql-py-conn`; aligns with mssql-tds supporting
registered providers; a `token_provider=` surface is already designed — see
`docs/DESIGN_CUSTOM_CREDENTIAL_SUPPORT.md`). With a token-provider factory **we stop
caring about expiry at all** — the provider requests a token when it needs one (on open
or ICR). Multi-user apps should bring their own token provider rather than rely on the
built-ins.

### 5.8 Pool lifecycle changes

- **`returnConnection` when the pool is missing:** today it does nothing (would leak once
  eviction exists). Fix: disconnect the connection if its pool key is gone. Safe today
  because the key is always present; only matters once eviction is added.
- **Lazy pool eviction:** today pool teardown is all-or-nothing — `closePools()` closes
  every pool and clears the entire map; there is no per-entry eviction of individual idle
  pools. (Individual *connections* are still pruned on idle timeout inside
  `ConnectionPool::acquire`.) Add per-entry eviction of **empty + idle** pool entries.
  **No eviction grace** — evict purely on idle timeout. Never evict a pool with
  checked-out connections.
- **Pool-full behavior:** today we **throw immediately** (`"pool size limit reached"`).
  SqlClient waits up to the login timeout, then errors. Changing this would alter
  observable behavior, so we **keep throw-immediately as the default** and offer
  wait-then-error as **opt-in** only (see §9).
- **Global cross-pool cap:** **dropped.** It is non-standard (other drivers cap per
  pool). Lazy eviction of empty/idle pools addresses the "many identities × per-pool max"
  concern without a global ceiling.

### 5.9 Performance — skip token acquisition on a pool hit (#659)

Because the pool is keyed only on the connection string today, `connection.py` acquires
an access token on **every** `connect()`, *before* the pool is consulted — and on a pool
hit that token is thrown away (the reused physical connection is already authenticated).
For token-auth workloads this defeats part of the point of pooling: a token is
materialized per connection even though most connects are reuses (a reporter measured a
1:1 token-acquisition-to-connect ratio despite ~200 pool reuses).

Once the pool is identity-aware (§5.1–5.3) this becomes fixable: the security-context key
can be computed **without** a fresh token for the provably-keyable auth types (MSI, SP,
interactive, device-code), so Python can **consult the pool first and acquire only on a
miss or near-expiry** (§14.4). This is a pure performance improvement — no observable
behavior change beyond eliminating the wasted work (and the misleading per-connect
"token acquired" log). It is tracked as **issue #659** and delivered as part of this work
because it is unblocked by the identity-aware key.

---

## 6. Required code changes

### 6.1 Python (`mssql_python/`)

| File | Change |
|---|---|
| `auth.py` | Return `expires_on` alongside the token from `_acquire_token` (keep public `get_token` / `get_token_struct` signatures unchanged). Adopt `authenticate()` / `AuthenticationRecord` for interactive/device-code; capture `home_account_id`. |
| `connection.py` | Compute the security-context component per auth source; build the composite key; pass token expiry to native. Non-token path unchanged. |
| `pooling.py` | (If needed) expose opt-in pool-full timeout config. |

### 6.2 Native (`mssql_python/pybind/connection/`)

| File | Change |
|---|---|
| `connection_pool.cpp` / `.h` | Composite key (connStr + `\0` + security context); store per-connection token expiry; expiry-aware checkout gate; `returnConnection` disconnect-on-missing; lazy per-entry eviction (empty + idle, no grace). |
| `connection.cpp` / `.h` | Optional ctor params `pool_key` and `token_expiry_epoch`; (Option 2, future) token-buffer overwrite on reuse. |
| `ddbc_bindings.cpp` | Thread the new optional ctor args through the pybind binding. |

---

## 7. Reviewer feedback incorporated

Feedback gathered from the internal cross-driver design discussion shaped the design as
follows:

- Use the term **"security context key"** (a subset of the pool key). ✔ adopted.
- **Do not** disable pooling for unknown-expiry raw tokens — **pool by hash** and
  document ICR as the app's responsibility. ✔ adopted (dropped the no-pool special case).
- Managed identity → credential object + client id. ✔ adopted.
- Device-code / interactive → key on the credential account and rotate only when re-auth
  happens (not per token). ✔ adopted via `home_account_id` / `AuthenticationRecord`.
- Token expiry: (1) discard connections with an expired key, or (2) overwrite the token
  pointer. ✔ Option 1 first, Option 2 as a later optimization.
- Refresh-on-reconnect is best solved with token providers / callbacks. ✔ captured as the
  long-term path.
- **Windows integrated auth is always the current process identity** — connStr-keying is
  safe there; nothing to partition. ✔ reframed from "unsolvable" to "safe by design."
- Cap connections **per pool**, not globally — a cross-pool cap is non-standard; the
  effective per-pool default is already 100 and configurable. ✔ dropped the global cap.
- Token expiry: **simplified to a single on-checkout threshold** (~5 min, ≤ 10) — ask the
  provider only when near expiry, reuse if the token is unchanged, discard-and-reopen if
  different. The earlier JDBC multi-tier model was dropped as unnecessary given the SDK
  cache; token-provider factories are the clean end state. ✔ adopted (see §5.6).
- **No eviction grace** — evict purely on idle timeout. ✔ adopted.
- Frame current behavior as a **known limitation**, not a bug (partitioning was never a
  goal; connStr partitioning is a side effect). ✔ adopted (§4).
- **Split into two enhancements** (access token now; Windows/Kerberos later). ✔ adopted.

---

## 8. Backward compatibility / breaking-change analysis

| Change | Breaking? | Rationale |
|---|---|---|
| Non-token path (key = connStr) | No | Byte-for-byte identical: same key, same pool, same throw-on-full. |
| Single-identity token apps | No | Security context is constant → still one pool → same reuse behavior, now correct. |
| Multi-identity token apps | Behavior changes (intended) | This is the security fix; the old cross-identity reuse was a bug, not contracted behavior. |
| Identity-aware keying | On by default | It is a security correctness fix and a no-op for single-identity apps. |
| Capture `expires_on` | No | Internal `_acquire_token` return only; public signatures unchanged. |
| Expiry discard-and-reopen | No | Only active on the token path. |
| Lazy eviction (empty + idle) | No | Active pools untouched; guarded against evicting checked-out connections. |
| `returnConnection` disconnect-on-missing | No | Branch never fires today (key always present); only relevant after eviction. |
| **Pool-full wait-then-error** | **Would be breaking** | Apps catching `"pool size limit reached"` would block. → **Kept opt-in; default stays throw-immediately.** |
| Drop global cross-pool cap | No | Never existed in code. |

**Guarantee:** non-token users and single-identity token users see **no change**. Only
cross-identity reuse changes — which is the vulnerability being fixed.

---

## 9. Opt-in behavior (non-breaking, still gets the win)

- **Identity-aware keying** — on by default (security fix, no-op for single identity).
- **Wait-then-error on pool-full** — opt-in (e.g. a pool-timeout setting); default stays
  throw-immediately.
- **Expiry discard-and-reopen** — only on the token path; non-token pooling unchanged.

---

## 10. Testing plan

- **Non-token regression:** SQL auth and Windows integrated pool exactly as before
  (same key, same reuse, same throw-on-full).
- **Cross-identity isolation:** two different service principals / UAMIs / interactive
  users to the same server get separate pools (no cross-identity reuse).
- **Same-identity reuse:** repeated connects for one identity reuse a single pool.
- **Expiry:** near-expiry (< 10 min) token forces a fresh token on checkout; expired
  connection is never handed out.
- **Eviction:** empty + idle pools evicted on idle timeout; pools with checked-out
  connections are never evicted.
- **`returnConnection`:** returning to a missing pool disconnects (no leak).
- **`home_account_id` rotation:** re-auth as a different account creates a new pool.

---

## 11. Future scope (deferred, not in this release)

| Item | Notes |
|---|---|
| **Windows / Kerberos / NTLM** | Safe by design (fixed process identity). **Documentation only** — no code change needed. Second enhancement track. |
| **Option 2 — token-buffer overwrite** | Perf optimization over discard-and-reopen; leverages existing ICR buffer retention (#594). |
| **Token-provider / callback refresh** | The clean long-term refresh-on-ICR path; aligns with mssql-tds registered providers and the designed `token_provider=` surface. |
| **Wait-then-error on pool-full** | Opt-in now; could become default in a future major version if desired. |
| **Other Entra methods** | Advertise smart partitioning in public docs as each is lifted. |

---

## 12. Scenarios covered by this release

> **Implementation status (PR #660).** The list below is the *full-effort* target across
> the staged rollout (§14.8). PR #660 delivers a subset; the rest are tracked follow-ups.
>
> **Wired in PR #660:**
> - ✅ Identity-aware key for **Managed Identity** — `msi:<client_id>` (user-assigned) /
>   `msi:system` (system-assigned), derived without a token.
> - ✅ **Fail-safe token-hash key** — `tok:<sha256>` for DefaultAzureCredential, raw
>   token, and (for now) Interactive / Device-code, so the core invariant holds.
> - ✅ **Lazy token acquisition (#659)** — the native pool invokes a token factory only
>   on a pool miss / non-pooled connect; same-identity hits acquire nothing.
> - ✅ **Expiry capture foundation** — `_acquire_token` surfaces `expires_on`;
>   `get_auth_token_info` / `is_token_near_expiry` / `TokenInfo` exist as the foundation
>   for expiry-aware checkout. *Not yet consumed on checkout — see deferred below.*
>
> **Deferred (designed here, not in PR #660):**
> - ⏭️ **Interactive / Device-code `home_account_id`** keying (§5.4) — currently falls
>   back to the safe `tok:<hash>` key (new pool per token, no reuse across refreshes).
> - ⏭️ **Expiry-aware checkout** (§14.6) — expiry is captured but not enforced; a pooled
>   token is not yet refreshed/discarded near expiry.
> - ⏭️ **`returnConnection` disconnect-on-missing** (§5.8) — still a no-op; harmless until
>   eviction lands.
> - ⏭️ **Lazy eviction of empty/idle pools** (§14.7) — not implemented; many distinct
>   identities can accumulate one pool each.
> - ⏭️ **DefaultAzureCredential** multi-user warning/doc note.
>
> **Not needed (clarification):** a dedicated **ServicePrincipal `sp:` key** is *not*
> required — SP is handled natively by ODBC with `UID`/`PWD` retained in the connection
> string, so distinct SPs are already isolated by the connStr key.

**Fixed / covered now (full-effort target):**

- ✅ Entra Service Principal — already isolated via the connection string (ODBC-native;
  no dedicated key needed).
- ✅ User-assigned Managed Identity — isolated by the `client_id`/object id supplied in
  `UID` (system-assigned needs no extra context).
- ✅ Interactive / Device-code — target: isolated by `home_account_id` with reuse across
  refreshes; **PR #660 ships the interim `tok:<hash>` isolation** (safe, no cross-identity
  reuse).
- ✅ Raw token / `token_provider` — pooled by token hash (ICR-expiry is app's responsibility).
- ⏭️ Token expiry — near-expiry tokens refreshed/discarded on checkout *(deferred; capture
  foundation only in PR #660)*.
- ✅ Performance — token acquisition is skipped on a same-identity pool hit; a token is
  materialized only on a pool miss or near-expiry refresh (#659).
- ⏭️ Pool lifecycle — lazy eviction of empty/idle pools; no connection leak on return
  *(deferred)*.

**Safe already (no change needed):**

- ✅ SQL auth (UID/PWD in connStr).
- ✅ Windows Integrated / Kerberos / NTLM (fixed process identity).
- ✅ System-assigned Managed Identity (one fixed identity).

**Documented as fuzzy / not recommended:**

- ⚠️ DefaultAzureCredential — per-token isolation; documented as not recommended for
  multi-user pooling.

**Deferred to a follow-up:**

- ⏭️ Windows/Kerberos identity partitioning docs (safe today, documentation track).
- ⏭️ Token-buffer overwrite (Option 2) and token-provider callback refresh.

---

## 13. Follow-up questions — resolved

| # | Question | Resolution |
|---|---|---|
| 1 | Does `disable_automatic_authentication=True` + trigger-on-`AuthenticationRequiredError` match how other drivers detect the account? | **Yes.** The standard flow is silent-first (from the cached account for the user id, or the current cached account) → on "interaction required" → interactive. Our approach maps onto this, and matured `azure-identity` additionally gives us `home_account_id` for precise keying. |
| 2 | What pre-expiry margin before a pooled token is "too close" to hand out? | **Single on-checkout threshold** (~5 min, ≤ 10) — ask the provider only when near expiry; reuse if the token is unchanged, discard-and-reopen if different. (Simplified from the earlier two-tier model.) |
| 3 | OK to document DefaultAzureCredential as not-recommended for multi-user pooling, kept on per-token isolation? | **Yes.** |
| 4 | Evict empty/idle pools purely on idle timeout, or keep a grace window? | **No grace** — evict purely on idle timeout. |
| 5 | Hand-roll a single-flight background refresher for the raw-token path, or lean on the Identity SDK's silent refresh? | **Neither a multi-tier scheme.** Use a **single on-checkout threshold** (~5 min, ≤ 10): ask the provider for a token, reuse if it's the same, discard-and-reopen if different. The SDK cache covers refresh; a lightweight per-pool guard avoids redundant concurrent `get_token()` calls. Static-token paths can't refresh (app's responsibility). Token-provider factories remove the concern entirely. |
| 6 | MSI security-context key — "credential instance / object id" vs. Python's `id()`? | **Neither Python `id()`.** System-assigned MSI needs no extra context (single identity). User-assigned MSI is keyed on the **client/object id** supplied in `UID` (msodbcsql uses the *object* id by convention). Since mssql-python acquires its own token and passes it (not msodbcsql's built-in `ActiveDirectoryMSI`), the security context = managed-identity provider + that id. Client-id-vs-object-id alignment is tracked in §15. |

---

## 14. Implementation design (detailed)

### 14.1 Python — security-context computation

Add a helper (e.g. in `auth.py` or a small `pool_key.py`) that returns the security
context string for the current connection, given the auth type, credential kwargs, the
credential instance, and (for interactive/device-code) the `AuthenticationRecord`:

```
def compute_security_context(auth_type, credential, credential_kwargs, token_struct):
    if auth_type in (MSI,):
        # user-assigned: discriminate by the client/object id supplied in UID.
        # system-assigned: no id -> single fixed identity, no extra context.
        msi_id = (credential_kwargs or {}).get("client_id", "")   # object id today (msodbcsql convention)
        return f"msi:{msi_id}" if msi_id else "msi:system-assigned"
    if auth_type in (SERVICE_PRINCIPAL,):
        return f"sp:{credential_kwargs.get('client_id','')}"
    if auth_type in (INTERACTIVE, DEVICE_CODE):
        # auth_record captured from credential.authenticate()
        return f"acct:{auth_record.home_account_id}"
    if auth_type in (DEFAULT,):
        # per-token isolation (documented not-recommended for multi-user pooling)
        return f"at:{sha256(token_struct)}"
    # raw token / token_provider, or any unknown source → isolate by token hash
    return f"at:{sha256(token_struct)}"
```

- The MSI identity is fully determined by the **client/object id** the user supplies in
  `UID` (plus the fact it is a managed-identity provider) — we deliberately do **not** use
  Python's `id(credential)`, which is fragile and can alias a freed object. System-assigned
  MSI has no id and is a single fixed identity, so no extra context is added.
- The composite pool key handed to native becomes:
  `connStr` when no token, else `connStr + "\0" + security_context`.
- **Fail-safe:** if the security context cannot be computed, fall back to the token hash
  — never the bare connection string. This preserves the §5.2 invariant.

### 14.2 Python — interactive/device-code flow rework (`auth.py`)

For `INTERACTIVE` / `DEVICE_CODE`:

1. Construct the credential with `disable_automatic_authentication=True` and, if we have
   a cached `AuthenticationRecord`, pass `authentication_record=` to enable silent reuse.
2. Call `get_token(scope)`:
   - success → use the token; the account is unchanged.
   - `AuthenticationRequiredError` → call `authenticate()`, capture the new
     `AuthenticationRecord`, then `get_token(scope)` again.
3. Cache the `AuthenticationRecord` keyed by the credential-cache key so subsequent
   connects reuse it silently.
4. The security context is `home_account_id` from the current `AuthenticationRecord`.

Public `get_token` / `get_token_struct` signatures are unchanged; expiry and the record
are returned via the internal `_acquire_token` only.

### 14.3 Python — capture expiry (`auth.py`)

Change `_acquire_token` to keep `expires_on`:

```
result = credential.get_token(scope)      # AccessToken(token, expires_on)
raw_token = result.token
expires_on = result.expires_on            # epoch seconds
```

Return `(token_struct, raw_token, expires_on)` internally; keep the public wrappers
returning what they do today.

### 14.4 Python → native handoff (`connection.py`) — lazy, pool-key-first

Today `connection.py` acquires the token **unconditionally, before** the pool is consulted
(`get_auth_token(...)` runs on every `connect()`), so on a pool hit the freshly minted
token is discarded — the per-connect encode + `struct.pack` is wasted work (issue #659).
The revised flow is **lazy and pool-key-first**:

1. **Compute the security-context key *without* acquiring a token** wherever possible:
   - **MSI / Service Principal** — `client_id`/object id comes from the connection params.
   - **Interactive / Device-code** — `home_account_id` from the **cached**
     `AuthenticationRecord` (no fresh `get_token()`).
   - **DefaultAzureCredential / raw token / `token_provider`** — the key is the token
     hash, so these still need the token materialized (raw tokens are already supplied for
     free; Default is the documented "not recommended for multi-user" case).
2. **Consult the pool** with that key, passing `token_expiry_epoch` where relevant.
3. **Pool hit, token not near expiry** → reuse and **acquire nothing** (the #659 win).
4. **Pool hit, token near expiry** (~5 min) → acquire a fresh token, compare, and reuse or
   discard-and-reopen (§5.6 / §14.6).
5. **Pool miss** → acquire the token and open a new physical connection.

The composite key and (optional) `token_expiry_epoch` are still passed to the native
`Connection` constructor with defaults so the non-token path and external callers are
unaffected.

### 14.5 Native — constructor + storage

- `Connection(connStr, fromPool, pool_key="", token_expiry_epoch=0)` — new optional
  params; store `_pool_key` and `_token_expiry_epoch` on the connection.
- `ConnectionPoolManager` and `ConnectionPool` key on `pool_key` when provided, else on
  `connStr` (backward compatible).
- Bind the new args through `ddbc_bindings.cpp` with defaults.

### 14.6 Native — expiry-aware checkout

On a pool checkout, before handing back a live candidate, apply the single expiry
threshold (Python has already re-acquired the token when near expiry — see §5.6):

```
now = epoch_now()
THRESHOLD = 300   # ~5 min (never above 600)
if candidate.token_expiry_epoch != 0 and (candidate.token_expiry_epoch - now) < THRESHOLD:
    fresh = python_reacquire_token()          # SDK cache returns valid-or-refreshed
    if fresh == candidate.current_token:
        reuse candidate                        # token still valid; normal reuse
    else:
        discard this pool's connections
        open a fresh connection with `fresh`   # we do NOT update msodbcsql's token
```

- There is **no** separate 10–45 min proactive/background tier; the SDK cache makes the
  on-checkout refresh sufficient (§5.6). A lightweight per-pool guard prevents concurrent
  checkouts from each firing a redundant `get_token()`.
- This is **Option 1** (discard-and-reopen). **Option 2** (buffer overwrite) is
  deprioritized — we do not update msodbcsql's token; **token-provider factories** are the
  clean end state that removes this logic entirely (§5.7).

### 14.7 Native — pool lifecycle

- **`returnConnection`:** if the pool key is not found in `_pools`, **disconnect** the
  connection instead of doing nothing (prevents a leak once eviction exists).
- **Lazy eviction:** add a step (e.g. during `acquireConnection` / a periodic sweep) that
  removes pool *entries* whose `ConnectionPool` is **empty** and whose last activity is
  older than the idle timeout. **No grace.** Never evict a pool with checked-out
  connections (track outstanding count).
- **Pool-full:** keep the current immediate `throw`. Add an **opt-in** wait-then-error
  path (bounded by a configurable timeout) only when the app enables it.

### 14.8 Rollout order (low-risk first)

1. Capture `expires_on` (internal only, no behavior change).
2. Add optional native `pool_key` / `token_expiry_epoch` params (defaulted; no-op unless
   supplied).
3. Python composite key for MSI / SP (provably-fixed identities) — lowest risk — and make
   token acquisition **lazy / pool-key-first** for these types so a same-identity hit
   skips acquisition (#659).
4. Interactive / device-code `home_account_id` rework (extends the lazy path).
5. Expiry-aware checkout (Option 1).
6. `returnConnection` disconnect-on-missing + lazy eviction.
7. Docs (gotchas matrix) + tests.
8. (Later) Option 2 buffer overwrite, opt-in wait-then-error, token-provider callback.

---

## 15. Remaining open questions

1. **`home_account_id` availability for device-code in headless/CI** — confirm the
   `authenticate()` / `AuthenticationRecord` pattern behaves correctly (and prompt UX is
   preserved) across our supported platforms.
2. **Opt-in surface for wait-then-error** — decide the public API/setting name and
   default timeout (login timeout vs. a dedicated pool-acquire timeout).
3. **Outstanding-connection tracking for eviction** — the current `ConnectionPool`
   tracks `_current_size` (reserved capacity) but not a distinct checked-out count; decide
   whether eviction keys off "empty `_pool` and `_current_size == 0`" or a new counter.
4. **User-assigned MSI: align on client id vs. object id?** — `msodbcsql`'s
   `ActiveDirectoryMSI` uses the **object id** in `UID`, whereas nearly all other drivers
   use the **client id**. Since mssql-python acquires its own token and passes it, the
   pooling security context is keyed on whatever id the user supplies in `UID` (object id
   today). Decide whether to align mssql-python on the **client id** for consistency —
   this would be a **breaking change** for existing user-assigned-MI users and should be
   evaluated on its own, independent of pooling.

(Two earlier questions are now resolved and moved to §13: proactive-refresh ownership —
resolved to a single on-checkout threshold with no background tier; and the MSI key —
resolved to the client/object id, not Python's `id()`.)

---

## 16. Discussion history

This design was shaped by an internal cross-driver design discussion. The account below
traces how the proposal evolved from the first framing to the current plan (reviewer
names omitted).

### 16.1 Starting point

The initial framing described the symptom — the connection-string-only pool key lets a
later connection reuse an earlier connection that is still authenticated as a different
Entra identity — and proposed partitioning the pool by identity. An early instinct was
that interactive / device-code auth would require **decoding the opaque access token**
to detect a change of principal, and that raw tokens of unknown expiry might have to be
**excluded from pooling** entirely.

### 16.2 Key pivots from review

- **"Security context key" terminology.** Reviewers preferred naming the new identity
  component a *security context key* — explicitly a **subset** of the full pool key —
  rather than overloading "pool key." Adopted (§5.1).
- **Don't refuse to pool raw tokens.** The idea of disabling pooling for unknown-expiry
  raw tokens was dropped in favor of **pooling by token hash**, with the contract that
  the application must not open a connection with an expired token and that
  ICR-with-expired-token is the app's responsibility. This matches other drivers
  (§5.5).
- **No token decoding for interactive / device-code.** The token-decoding worry was
  resolved: `azure-identity` exposes `AuthenticationRecord.home_account_id` and
  `disable_automatic_authentication=True`, giving clean, documented signals for account
  identity and "silent vs. re-prompt." Keying on `home_account_id` — the confirmed
  industry silent-first→interactive pattern — replaced token decoding (§5.4).
- **Per-pool cap, not a global cap.** An early proposal for a global cross-pool
  connection ceiling was dropped as non-standard; other drivers cap **per pool**. The
  effective per-pool default is already 100 and configurable, and lazy eviction of
  empty/idle pools addresses the "many identities × per-pool max" concern (§5.8).
- **Frame current behavior as a known limitation, not a bug.** Intelligent partitioning
  was never a stated goal; connection-string partitioning was a *side effect* that
  happened to isolate identities when the user id was in the string. The affected rows
  are documented as a **known limitation**, lifted (and advertised) as each is fixed
  (§4).
- **Windows integrated auth is safe by design.** Reviewers reframed Windows / Kerberos /
  NTLM from "unsolvable" to "safe by design" — the identity is always the current
  process identity, so connection-string keying is already correct. This became a
  documentation-only follow-up (§11).
- **Expiry tiers, then simplified.** Expiry handling first adopted a JDBC two-tier model
  (< 10 min synchronous; 10–45 min proactive single-flight; > 45 min nothing). It was
  **later simplified to a single on-checkout threshold** once it was clear the SDK cache
  makes a background refresher unnecessary — see §16.4 and §5.6.
- **No eviction grace.** Empty/idle pools are evicted purely on idle timeout, with no
  grace window (§5.8).
- **Split into two enhancements.** The work was deliberately split: identity-aware
  partitioning for access tokens now, Windows/Kerberos partitioning (documentation) as a
  separate follow-up track.

### 16.3 Resolved follow-up questions

The outstanding review questions were closed as recorded in §13: the
`disable_automatic_authentication` + trigger-on-`AuthenticationRequiredError` flow
matches how other drivers detect the account; the pre-expiry margin uses a single
on-checkout threshold; `DefaultAzureCredential` is documented as not recommended for
multi-user pooling (kept on per-token isolation); and empty/idle pools are evicted
purely on idle timeout with no grace window.

### 16.4 Latest clarifications

Two follow-up points were resolved in the most recent review round:

- **Token refresh.** The multi-tier (10–45 min proactive + < 10 min synchronous) scheme is
  more than we need. The agreed model is a **single on-checkout threshold** (~5 min, never
  above 10): ask the provider for a token only when the pool's current token is near
  expiry; reuse if the returned token is unchanged, otherwise **discard the pool's
  connections and reopen** with the new token (we do not update the token inside
  `msodbcsql`). The SDK cache covers refresh, so a separate background "single-flight
  refresher" tier is unnecessary — only a small per-pool guard against redundant concurrent
  `get_token()` calls is warranted. Static-token paths cannot refresh, so that stays the
  app's responsibility. The clean long-term answer is **token-provider factories**, after
  which expiry handling disappears from our side entirely (§5.6, §5.7).
- **MSI security context.** Terminology was clarified. **System-assigned** MSI needs no
  extra security context (single fixed identity). **User-assigned** MSI is identified by a
  **client id or object id**; most drivers use the client id, but `msodbcsql`'s
  `ActiveDirectoryMSI` uses the **object id** (supplied in `UID`). Because mssql-python
  acquires its own token and passes it (rather than using msodbcsql's built-in MSI), the
  security context is the **managed-identity provider + that client/object id** — **not**
  Python's `id(credential)`. Whether to align mssql-python on the client id (a breaking
  change) is tracked separately (§15).

---

## 17. Conclusion

The connection-string-only pool key is safe for auth methods that carry the identity in
the connection string, but it is a **privilege-escalation hole** for Entra access-token
authentication, where the identity is deliberately stripped from the key. The
**identity-aware security context key** closes that hole with a single invariant —
*when a token is present, the pool key is never the bare connection string* — while
preserving fast same-identity reuse and leaving the non-token path byte-for-byte
unchanged.

The design is intentionally conservative and incremental:

- It is a **security correctness fix**, on by default, and a **no-op** for non-token and
  single-identity token applications — so it ships without a breaking change.
- It follows a **low-risk rollout order** (§14.8): capture expiry internally, add
  defaulted native params, partition the provably-fixed identities (MSI / SP) first,
  then interactive/device-code, then expiry-aware checkout and lifecycle fixes.
- It defers non-essential work — Option 2 token-buffer overwrite, token-provider
  callback refresh, opt-in wait-then-error on pool-full, and Windows/Kerberos
  documentation — to clearly scoped follow-ups.

Before implementation begins, the remaining open questions in §15 (`home_account_id`
behavior in headless/CI, the opt-in wait-then-error surface, outstanding-connection
tracking for eviction, and whether to align user-assigned MSI on the client id vs. the
object id) should be validated. With those confirmed, the plan is ready to implement in
the sequence above.
