"""Conda build and audit commands, human-readable reports and exit codes."""

from __future__ import annotations

import argparse
import sys

from . import audit, build
from .contracts import Format

_ELF_DESCRIPTION = """Masking-immune audit of the vendored Linux ODBC binaries in built conda packages.

The #563 reachability fix lives in the ELF RUNPATH of the vendored driver, not in a
comment or a runtime probe. A runtime ``ldd``/import check can PASS on any host that
happens to carry a system ``krb5``/``libltdl`` -- the driver silently binds the
system copy and a wrong/missing conda climb goes unnoticed (exactly what the full CI
agents hide). This audit is immune to that masking: it reads the ELF bytes straight
out of each built ``.conda`` payload -- via the ``PT_DYNAMIC`` program header the
*loader itself* uses -- and asserts, statically and exactly:

  * each driver/manager ELF's ``e_machine`` MATCHES the package's conda subdir
    (``linux-64`` == x86_64, ``linux-aarch64`` == aarch64), so a wrong-arch or
    mislabeled ``.so`` is caught statically (the Linux twin of the PE audit);
  * ``libmsodbcsql*`` and ``libodbcinst.so.2`` carry the EXACT relative ``$ORIGIN``
    climb that lands on the package-root ``lib`` (== ``$PREFIX/lib``), computed from
    each binary's own location -- not a substring, not "any ``..``". A too-short,
    overshooting, or ``$ORIGINATOR`` climb FAILS;
  * that climb entry appears in the EFFECTIVE RUNPATH: the loader honours
    ``DT_RUNPATH`` and IGNORES ``DT_RPATH`` when ``DT_RUNPATH`` is present, so a good
    ``DT_RPATH`` decoy behind a bad ``DT_RUNPATH`` FAILS;
  * no absolute RPATH entry exists (stay relocatable);
  * the run deps that SERVICE the driver -- ``krb5``, ``libtool`` (libltdl provider),
    ``openssl`` -- are DECLARED in ``info/index.json`` ``depends`` (deleting a dep
    from ``meta.yaml`` must fail here, not just be masked at runtime), and the driver
    still ``DT_NEEDED``s ``libkrb5``/``libgssapi_krb5``/``libodbcinst`` (and
    ``libodbcinst`` still needs ``libltdl``) so a driver that stopped needing krb5 is
    caught too;
  * no ``krb5``/``openssl``/``libltdl`` is VENDORED inside the payload (they are
    serviced by conda, never bundled).
    * the complete supported driver inventory is present: alpine/debian_ubuntu/rhel/suse
        for x86_64 and alpine/debian_ubuntu/rhel for arm64 (no SUSE ARM64 driver is shipped).

Non-Linux packages (``win-*`` / ``osx-*``) have no such ELF payload and are skipped.
An unreadable/malformed package FAILS (it is never silently treated as non-Linux).

Exit code 0 = every Linux package is exactly self-contained; non-zero = a violation
was found (blocks the build/release)."""

_PE_DESCRIPTION = """Assert the vendored Windows PE binaries in a built conda package match its arch.

The win-arm64 conda package is CROSS-built on an x64 agent, where the arm64 Python
cannot execute -- so the build-time runtime import is skipped and the package's
architecture would otherwise be trusted purely from the wheel filename. A mislabeled
or mis-built wheel could therefore ship x64 (.pyd/.dll) binaries inside a win-arm64
package and nothing would catch it before publish.

This is the Windows twin of the ELF audit (which audits the
Linux ELF payload): it reads the PE COFF Machine field straight out of every
.pyd/.dll in the built .conda payload and asserts it matches the package's subdir
(win-arm64 -> ARM64, win-64 -> AMD64). A Windows package missing EITHER the binding
(ddbc_bindings*.pyd) OR the core ODBC driver (msodbcsql18*.dll) FAILS.

Exit 0 = every checked package's PE binaries match; non-zero = a mismatch/violation."""

_MACHO_DESCRIPTION = """Assert macOS Mach-O binaries in a built conda package match their intended architectures.

The macOS conda packages are repackaged from a UNIVERSAL2 wheel, so a package's architecture
is otherwise trusted purely from the wheel filename -- and osx-arm64 is CROSS-built on an
Intel agent, where the arm64 slice cannot execute, so the build-time runtime import is skipped.
A mislabeled or thin (single-arch) wheel could therefore ship an x86_64-only binary inside an
osx-arm64 package and nothing would catch it before publish.

This is the macOS twin of the PE machine and ELF RUNPATH audits: it reads the Mach-O cputype(s)
straight out of the binding and vendored driver files in the built .conda payload. The binding
must contain the package's arch slice (osx-arm64 -> arm64, osx-64 -> x86_64). The ODBC wheel
deliberately bundles separate macos/arm64 and macos/x86_64 driver trees, so each tree is checked
against its directory arch and the package target's runtime tree must contain all four required
dylibs. FAT/universal binaries are validated like ``lipo -archs``, including complete tables and
valid slice ranges.

Exit 0 = every checked package satisfies the binding/driver architecture contract; non-zero =
a mismatch/violation."""

_BUILD_DESCRIPTION = """One cross-platform orchestrator for the conda build+validate leg.

Replaces build-conda-packages.ps1 + build-conda-packages.sh (the same 7-step pipeline
written twice, which had already drifted). conda is Python and every agent has a bootstrap
interpreter, so ONE orchestrator runs on every leg; the platform differences (the Miniforge
installer, the win-arm64 channel profile, the Linux-only reachability gate) are
a handful of branches, not a second 360-line script. Running as a NORMAL process also means
the caller reads the exit code directly -- so the PowerShell ErrorActionPreference flips, the
`2>$null` swallows, and the `cmd /c "exit 0"` reset all disappear.

Pipeline: gather this leg's wheels into a find-links dir -> locate/install Miniforge ->
create a dedicated conda-build env -> build the self-contained mssql-python package (which
VENDORS the ODBC Driver 18 payload) per Python version -> masking-immune platform binary
audit -> solve a fresh env from the freshly built local channel and import + driver-load +
(opt-in) reachability gate -> stage the packages onto the leg artifact.

Cross-builds (CONDA_SUBDIR): linux-aarch64 executes under QEMU binfmt; osx-arm64 and
win-arm64 cannot execute the target Python on their x64 build hosts, so static platform
audits enforce architecture and the runtime import auto-skips. osx-64 runs natively on
the Intel macOS agent."""


def _build_arguments(ap: argparse.ArgumentParser) -> None:
    ap.add_argument("--mssql-wheel-dir", required=True)
    ap.add_argument("--mssql-wheel-glob", default="mssql_python-*.whl")
    ap.add_argument("--odbc-wheel-dir", required=True)
    ap.add_argument("--odbc-wheel-filter", required=True)
    ap.add_argument(
        "--rs-wheel-dir", help="Actual RS wheel inputs; required by RS-dependent bindings."
    )
    ap.add_argument(
        "--rs-version-file", help="Assert the RS distribution version from the selected source."
    )
    ap.add_argument("--recipe-root", required=True)
    ap.add_argument("--output-dir", required=True)
    ap.add_argument("--stage-dir", required=True)
    ap.add_argument("--conda-subdir", required=True, help="This leg's subdir (staging + display).")
    ap.add_argument("--conda-target-subdir", default="", help="Cross-target via CONDA_SUBDIR.")
    ap.add_argument("--python-versions", default="")


def _audit_arguments(parser: argparse.ArgumentParser, kind: Format) -> None:
    if kind == "elf":
        parser.add_argument("--root", help="Directory to scan recursively for *.conda / *.tar.bz2.")
        parser.add_argument("packages", nargs="*", help="Explicit package paths to audit.")
    else:
        parser.add_argument("--root", required=True, help="Directory to scan recursively.")
        example, family = ("win-arm64", "win") if kind == "pe" else ("osx-arm64", "osx")
        parser.add_argument(
            "--subdir",
            default="",
            help=f"Only audit packages of this subdir (e.g. {example}). Empty = all {family}-* packages.",
        )


def _audit(args: argparse.Namespace, kind: Format) -> int:
    paths = audit.discover_packages(args.root, args.packages if kind == "elf" else ())
    if not paths:
        message = (
            "ERROR: no conda packages to audit (pass --root DIR or package paths)."
            if kind == "elf"
            else f"ERROR: no conda packages found under {args.root}."
        )
        print(message, file=sys.stderr)
        return 1
    if kind == "elf":
        print(f"Auditing RUNPATH self-containment of {len(paths)} conda package(s):")
    selected = "" if kind == "elf" else args.subdir
    result = audit.audit_packages(paths, kind, selected)
    for detail in result.details:
        print(detail)
    if selected and result.checked == 0:
        print(f"ERROR: no '{selected}' packages found under {args.root}.", file=sys.stderr)
        return 1
    if result.violations:
        title = {
            "elf": "RUNPATH audit",
            "pe": "PE machine-type assert",
            "macho": "Mach-O arch-slice assert",
        }[kind]
        print(f"\n{title} FAILED:", file=sys.stderr)
        for error in result.violations:
            print(f"  - {error}", file=sys.stderr)
        return 1
    if kind == "pe":
        print(f"\nOK: all {result.checked} checked package(s) carry the expected PE machine type.")
    elif kind == "macho":
        print(
            f"\nOK: all {result.checked} checked package(s) satisfy the Mach-O architecture contract."
        )
    elif result.checked == 0:
        print("\nOK: no Linux packages present; nothing to audit (win/osx have no ELF payload).")
    else:
        print(
            f"\nOK: all {result.checked} Linux package(s) match their subdir arch, carry the "
            f"EXACT $ORIGIN climb, keep their krb5/gssapi/libltdl NEEDEDs, declare "
            f"krb5/libtool/openssl, and vendor no crypto (conda services them)."
        )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m eng.conda_tools",
        description="Build or audit Conda packages from the repository root.",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    _build_arguments(
        commands.add_parser(
            "build", help="Build and validate packages.", description=_BUILD_DESCRIPTION
        )
    )
    descriptions: dict[Format, str] = {
        "elf": _ELF_DESCRIPTION,
        "pe": _PE_DESCRIPTION,
        "macho": _MACHO_DESCRIPTION,
    }
    for kind, description in descriptions.items():
        _audit_arguments(
            commands.add_parser(
                kind, help=f"Audit {kind.upper()} packages.", description=description
            ),
            kind,
        )
    args = parser.parse_args(argv)
    if args.command == "build":
        return build.execute(args)
    return _audit(args, args.command)


if __name__ == "__main__":
    sys.exit(main())
