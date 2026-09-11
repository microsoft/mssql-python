"""Validate local inputs and promote or recover exact staged Conda archives.

Use restricted publishing credentials and coordinate one publication or recovery at
a time for the same owner/package/target label. This helper does not prevent concurrent
writers; overlapping runs can interfere with labels and rollback. --check-local-only
needs neither credentials nor an API client.

Uploads happen before this helper under a build-unique staging label. This module
verifies every uploaded distribution against the local artifact, adds the public
label to the complete set, and removes the staging label only after all target-label
operations succeed.
Rollback is compensating, not atomic: only attempted additions absent from the
initial snapshot are removed. An interrupted invocation resumes from verified labels.
Local archives must use the metadata-derived canonical basename that the pinned
upload client sends; a renamed file is rejected before any upload.

Failed uploads/promotions invoke --cleanup-staging for their attempted
archives. After a hard interruption, run this same option with the original exact
staging label and retained archives, with no overlapping publisher or recovery. Cleanup
verifies identity/SHA-256 and removes only that staging label, never public labels
or files. It is bounded, compensating recovery, not guaranteed cleanup after a kill.
"""

from __future__ import annotations

import argparse
import hashlib
import re
import sys
import time
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Any, Callable

from validate_conda_release import _required_index_string, read_index_json

_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_ANACONDA_API_URL = "https://api.anaconda.org"


@dataclass(frozen=True)
class Distribution:
    path: Path
    package: str
    version: str
    basename: str
    sha256: str


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def distribution_from_path(path: str | Path) -> Distribution:
    package_path = Path(path).resolve()
    if not package_path.is_file():
        raise ValueError(f"Conda package does not exist: {package_path}")

    index = read_index_json(str(package_path))
    package = str(index.get("name", "")).strip()
    version = str(index.get("version", "")).strip()
    subdir = str(index.get("subdir", "")).strip()
    if package != "mssql-python":
        raise ValueError(f"Unexpected package '{package}' in {package_path.name}.")
    if not version:
        raise ValueError(f"Package version is missing in {package_path.name}.")
    if not subdir or package_path.parent.name != subdir:
        raise ValueError(
            f"Package {package_path.name} is staged under '{package_path.parent.name}' "
            f"but metadata subdir is '{subdir or '<missing>'}'."
        )
    build = _required_index_string(index, "build", str(package_path))
    extension = ".conda" if package_path.name.endswith(".conda") else ".tar.bz2"
    canonical_name = f"{package}-{version}-{build}{extension}"
    if package_path.name != canonical_name:
        raise ValueError(
            f"Package {package_path.name} must use canonical basename '{canonical_name}': "
            "anaconda-client normalizes upload names from metadata."
        )

    return Distribution(
        path=package_path,
        package=package,
        version=version,
        basename=f"{subdir}/{package_path.name}",
        sha256=_sha256(package_path),
    )


def _metadata_labels(metadata: dict[str, Any], distribution: Distribution) -> set[str]:
    labels = metadata.get("labels")
    if not isinstance(labels, list) or not all(isinstance(label, str) for label in labels):
        raise RuntimeError(
            f"Anaconda metadata for '{distribution.basename}' has no valid labels list."
        )
    return set(labels)


def verify_distribution(
    api: Any,
    owner: str,
    distribution: Distribution,
    *,
    required_label: str | None = None,
    forbidden_label: str | None = None,
) -> set[str]:
    metadata = api.distribution(
        owner,
        distribution.package,
        distribution.version,
        distribution.basename,
    )
    if metadata.get("basename") != distribution.basename:
        raise RuntimeError(
            f"Anaconda returned basename '{metadata.get('basename')}' for "
            f"'{distribution.basename}'."
        )

    remote_sha = str(metadata.get("sha256", "")).lower()
    if not _SHA256_RE.fullmatch(remote_sha):
        raise RuntimeError(f"Anaconda metadata for '{distribution.basename}' has no valid SHA-256.")
    if remote_sha != distribution.sha256:
        raise RuntimeError(
            f"SHA-256 mismatch for '{distribution.basename}': "
            f"local {distribution.sha256} != remote {remote_sha}."
        )

    labels = _metadata_labels(metadata, distribution)
    if required_label is not None and required_label not in labels:
        raise RuntimeError(
            f"Distribution '{distribution.basename}' is missing required label "
            f"'{required_label}' (labels={sorted(labels)})."
        )
    if forbidden_label is not None and forbidden_label in labels:
        raise RuntimeError(
            f"Distribution '{distribution.basename}' still has forbidden label "
            f"'{forbidden_label}' (labels={sorted(labels)})."
        )
    return labels


def _verify_with_retry(
    verify: Callable[[], set[str]],
    description: str,
    *,
    attempts: int,
    delay_seconds: float,
) -> set[str]:
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return verify()
        except Exception as exc:  # API metadata can be briefly eventually consistent.
            last_error = exc
            if attempt < attempts:
                print(f"{description} attempt {attempt} failed: {exc}; retrying...", flush=True)
                time.sleep(delay_seconds)
    assert last_error is not None
    raise last_error


def validate_release_input(expected_version: str, distributions: list[Distribution]) -> None:
    if not expected_version:
        raise ValueError("Expected mssql-python version is required for promotion.")
    if not distributions:
        raise ValueError("No Conda distributions were supplied for promotion.")

    basenames = [distribution.basename for distribution in distributions]
    if len(basenames) != len(set(basenames)):
        raise ValueError(f"Duplicate distribution basenames: {basenames}")
    wrong_versions = sorted(
        {
            distribution.version
            for distribution in distributions
            if distribution.version != expected_version
        }
    )
    if wrong_versions:
        raise ValueError(
            f"Distribution versions {wrong_versions} do not match expected "
            f"mssql-python version '{expected_version}'."
        )


def _require_publication(
    owner: str,
    staging_label: str,
    target_label: str,
    expected_version: str,
    distributions: list[Distribution],
) -> None:
    if not _IDENTIFIER_RE.fullmatch(owner):
        raise ValueError(f"Invalid Anaconda owner/channel: {owner!r}")
    for label_name, label in (("staging", staging_label), ("target", target_label)):
        if not _IDENTIFIER_RE.fullmatch(label):
            raise ValueError(f"Invalid {label_name} label: {label!r}")
    if staging_label == target_label:
        raise ValueError("Staging and target labels must be different.")
    validate_release_input(expected_version, distributions)


def _remove_staging_label(
    api: Any,
    owner: str,
    distribution: Distribution,
    staging_label: str,
    labels: set[str],
    *,
    required_target: str | None,
    verify_attempts: int,
    delay_seconds: float,
) -> None:
    cleanup_error: Exception | None = None
    if staging_label in labels:
        try:
            api.remove_channel(
                staging_label,
                owner,
                package=distribution.package,
                version=distribution.version,
                filename=distribution.basename,
            )
        except Exception as exc:
            cleanup_error = exc
    try:
        _verify_with_retry(
            partial(
                verify_distribution,
                api,
                owner,
                distribution,
                required_label=required_target,
                forbidden_label=staging_label,
            ),
            f"Verify staging cleanup {distribution.basename}",
            attempts=verify_attempts,
            delay_seconds=delay_seconds,
        )
    except Exception as verification_error:
        raise RuntimeError(
            f"Failed to remove staging label from '{distribution.basename}': "
            f"remove={cleanup_error}; verify={verification_error}"
        ) from verification_error
    if cleanup_error is not None:
        print(
            f"Staging cleanup API reported an error but '{distribution.basename}' "
            f"verified clean: {cleanup_error}",
            flush=True,
        )


def cleanup_staging(
    api: Any,
    owner: str,
    staging_label: str,
    target_label: str,
    expected_version: str,
    distributions: list[Distribution],
    *,
    verify_attempts: int = 3,
    delay_seconds: float = 5,
) -> None:
    """Remove only the specified staging label from verified attempted uploads."""
    _require_publication(owner, staging_label, target_label, expected_version, distributions)
    from binstar_client.errors import NotFound  # type: ignore[import-not-found]

    errors: list[str] = []
    for distribution in distributions:
        try:
            try:
                labels = _verify_with_retry(
                    partial(verify_distribution, api, owner, distribution),
                    f"Verify cleanup input {distribution.basename}",
                    attempts=verify_attempts,
                    delay_seconds=delay_seconds,
                )
            except NotFound:
                print(f"Cleanup: '{distribution.basename}' is not present on the server.")
                continue
            _remove_staging_label(
                api,
                owner,
                distribution,
                staging_label,
                labels,
                required_target=None,
                verify_attempts=verify_attempts,
                delay_seconds=delay_seconds,
            )
        except Exception as exc:
            # Continue compensating other attempted uploads, but report every failure.
            errors.append(f"{distribution.basename}: {exc}")
    if errors:
        raise RuntimeError(f"Staging cleanup incomplete for '{staging_label}': {errors}")


def promote(
    api: Any,
    owner: str,
    staging_label: str,
    target_label: str,
    expected_version: str,
    distributions: list[Distribution],
    *,
    verify_attempts: int = 3,
    delay_seconds: float = 5,
) -> None:
    _require_publication(owner, staging_label, target_label, expected_version, distributions)

    def verifier(
        distribution: Distribution,
        *,
        required_label: str | None = None,
        forbidden_label: str | None = None,
    ) -> Callable[[], set[str]]:
        def run_verification() -> set[str]:
            return verify_distribution(
                api,
                owner,
                distribution,
                required_label=required_label,
                forbidden_label=forbidden_label,
            )

        return run_verification

    initial_labels: dict[str, set[str]] = {}
    for distribution in distributions:
        labels = _verify_with_retry(
            verifier(distribution),
            f"Verify staged {distribution.basename}",
            attempts=verify_attempts,
            delay_seconds=delay_seconds,
        )
        if staging_label not in labels and target_label not in labels:
            stage_error: Exception | None = None
            try:
                # --skip-existing does not attach a new label. An exact matching file
                # left on an older staging label is recoverable: attach this build's
                # staging label only after checksum/identity verification above.
                api.add_channel(
                    staging_label,
                    owner,
                    package=distribution.package,
                    version=distribution.version,
                    filename=distribution.basename,
                )
            except Exception as exc:
                stage_error = exc
            labels = _verify_with_retry(
                verifier(distribution, required_label=staging_label),
                f"Verify current staging label for {distribution.basename}",
                attempts=verify_attempts,
                delay_seconds=delay_seconds,
            )
            if stage_error is not None:
                print(
                    f"Staging-label API reported an error but '{distribution.basename}' "
                    f"verified on '{staging_label}': {stage_error}",
                    flush=True,
                )
        initial_labels[distribution.basename] = labels

    attempted_additions: list[Distribution] = []
    try:
        for distribution in distributions:
            add_error: Exception | None = None
            if target_label not in initial_labels[distribution.basename]:
                attempted_additions.append(distribution)
                try:
                    api.add_channel(
                        target_label,
                        owner,
                        package=distribution.package,
                        version=distribution.version,
                        filename=distribution.basename,
                    )
                except Exception as exc:
                    # The request may have reached the server before the client observed
                    # failure. Verify metadata below; rollback covers any label that landed.
                    add_error = exc
            _verify_with_retry(
                verifier(distribution, required_label=target_label),
                f"Verify promoted {distribution.basename}",
                attempts=verify_attempts,
                delay_seconds=delay_seconds,
            )
            if add_error is not None:
                print(
                    f"Target-label API reported an error but '{distribution.basename}' "
                    f"verified on '{target_label}': {add_error}",
                    flush=True,
                )
    except Exception as exc:
        rollback_errors: list[str] = []
        for distribution in reversed(attempted_additions):
            remove_error: Exception | None = None
            try:
                api.remove_channel(
                    target_label,
                    owner,
                    package=distribution.package,
                    version=distribution.version,
                    filename=distribution.basename,
                )
            except Exception as rollback_error:
                # Verify the result even when the request reported an error; it may
                # have reached the server before the client observed the failure.
                remove_error = rollback_error
            try:
                _verify_with_retry(
                    verifier(distribution, forbidden_label=target_label),
                    f"Verify rollback {distribution.basename}",
                    attempts=verify_attempts,
                    delay_seconds=delay_seconds,
                )
                if remove_error is not None:
                    print(
                        f"Rollback API reported an error but '{distribution.basename}' "
                        f"verified without '{target_label}': {remove_error}",
                        flush=True,
                    )
            except Exception as verification_error:
                rollback_errors.append(
                    f"{distribution.basename}: remove={remove_error}; "
                    f"verify={verification_error}"
                )
        detail = f" Rollback errors: {rollback_errors}" if rollback_errors else ""
        raise RuntimeError(
            f"Promotion failed; rollback of newly added target labels was attempted: "
            f"{exc}.{detail}"
        ) from exc

    for distribution in distributions:
        _remove_staging_label(
            api,
            owner,
            distribution,
            staging_label,
            initial_labels[distribution.basename],
            required_target=target_label,
            verify_attempts=verify_attempts,
            delay_seconds=delay_seconds,
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--owner", required=True)
    parser.add_argument("--staging-label", required=True)
    parser.add_argument("--target-label", required=True)
    parser.add_argument("--expected-version", required=True)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--check-local-only", action="store_true")
    mode.add_argument(
        "--cleanup-staging",
        action="store_true",
        help="Recover attempted uploads: verify exact files and remove only this staging label.",
    )
    parser.add_argument("packages", nargs="+")
    args = parser.parse_args(argv)

    distributions = [distribution_from_path(path) for path in args.packages]
    validate_release_input(args.expected_version, distributions)
    if args.check_local_only:
        print(
            f"LOCAL_RELEASE_INPUT_OK: verified {len(distributions)} distribution(s) "
            f"for mssql-python {args.expected_version}."
        )
        return 0

    _require_publication(
        args.owner, args.staging_label, args.target_label, args.expected_version, distributions
    )
    from binstar_client.utils import get_server_api  # type: ignore[import-not-found]

    api = get_server_api(config={"url": _ANACONDA_API_URL, "ssl_verify": True})
    # anaconda-client 1.14.1 does not set timeouts on its metadata/label requests.
    api.session.request = partial(api.session.request, timeout=(15, 60))
    operation = cleanup_staging if args.cleanup_staging else promote
    operation(
        api,
        args.owner,
        args.staging_label,
        args.target_label,
        args.expected_version,
        distributions,
    )
    if args.cleanup_staging:
        print(f"STAGING_CLEANUP_OK: verified cleanup of label '{args.staging_label}'.")
    else:
        print(
            f"PROMOTION_OK: verified and promoted {len(distributions)} distribution(s) "
            f"to {args.owner}/{args.target_label}."
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
