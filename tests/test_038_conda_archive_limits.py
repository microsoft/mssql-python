"""Small boundary fixtures for bounded archive reads; no native extension or database."""

import io
import json
import tarfile
import zipfile
from pathlib import Path

import pytest

if not (Path(__file__).resolve().parent.parent / "eng/conda_tools/archive.py").is_file():
    pytest.skip("Conda release sources are not shipped in wheels.", allow_module_level=True)

from eng.conda_tools import archive


def _compress(data, *, unknown_size=False):
    try:
        from compression import zstd
    except ImportError:
        import zstandard

        return zstandard.ZstdCompressor(
            write_content_size=not unknown_size, write_checksum=True
        ).compress(data)
    return zstd.compress(
        data,
        options={
            zstd.CompressionParameter.content_size_flag: int(not unknown_size),
            zstd.CompressionParameter.checksum_flag: 1,
        },
    )


def _tar(files):
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w", format=tarfile.USTAR_FORMAT) as contents:
        for name, data in files:
            member = tarfile.TarInfo(name)
            member.size = len(data)
            contents.addfile(member, io.BytesIO(data))
    return buffer.getvalue()


def _package(tmp_path, extension=".conda", *, files=None, unknown_size=False):
    index = json.dumps(
        {"name": "mssql-python", "version": "1.15.0", "build": "py313_0", "subdir": "win-64"}
    ).encode()
    files = [("payload.bin", b"x" * 128)] if files is None else files
    path = tmp_path / ("mssql-python-1.15.0-py313_0" + extension)
    if extension == ".conda":
        with zipfile.ZipFile(path, "w") as container:
            container.writestr("metadata.json", json.dumps({"conda_pkg_format_version": 2}))
            container.writestr(
                f"info-{path.stem}.tar.zst",
                _compress(_tar([("info/index.json", index)]), unknown_size=unknown_size),
            )
            container.writestr(
                f"pkg-{path.stem}.tar.zst", _compress(_tar(files), unknown_size=unknown_size)
            )
    else:
        with tarfile.open(path, "w:bz2", format=tarfile.USTAR_FORMAT) as contents:
            for name, data in [("info/index.json", index), *files]:
                member = tarfile.TarInfo(name)
                member.size = len(data)
                contents.addfile(member, io.BytesIO(data))
    return path, index


@pytest.mark.parametrize("extension", [".conda", ".tar.bz2"])
@pytest.mark.parametrize("over", [False, True])
def test_archive_file_size_boundary(tmp_path, monkeypatch, extension, over):
    path, _ = _package(tmp_path, extension)
    monkeypatch.setattr(archive, "_MAX_ARCHIVE_BYTES", path.stat().st_size - over, raising=False)
    if over:
        with pytest.raises(ValueError, match="archive bytes"):
            archive.read_release_index(str(path))
    else:
        assert archive.read_release_index(str(path))["name"] == "mssql-python"


@pytest.mark.parametrize("extension", [".conda", ".tar.bz2"])
@pytest.mark.parametrize("over", [False, True])
def test_member_size_boundary(tmp_path, monkeypatch, extension, over):
    path, _ = _package(tmp_path, extension)
    monkeypatch.setattr(archive, "_MAX_MEMBER_BYTES", 128 - over, raising=False)
    if over:
        with pytest.raises(ValueError, match="member bytes"):
            list(archive.iter_payload_members(str(path)))
    else:
        assert dict(archive.iter_payload_members(str(path)))["payload.bin"] == b"x" * 128


@pytest.mark.parametrize("extension", [".conda", ".tar.bz2"])
@pytest.mark.parametrize("over", [False, True])
def test_metadata_size_boundary(tmp_path, monkeypatch, extension, over):
    path, index = _package(tmp_path, extension)
    monkeypatch.setattr(archive, "_MAX_METADATA_BYTES", len(index) - over, raising=False)
    if over:
        with pytest.raises(ValueError, match="metadata bytes"):
            archive.read_release_index(str(path))
    else:
        assert archive.read_release_index(str(path))["name"] == "mssql-python"


@pytest.mark.parametrize("over", [False, True])
def test_member_count_boundary(tmp_path, monkeypatch, over):
    path, _ = _package(tmp_path, files=[("a", b""), ("b", b"")])
    monkeypatch.setattr(archive, "_MAX_MEMBERS", 2 - over, raising=False)
    if over:
        with pytest.raises(ValueError, match="member count"):
            list(archive.iter_payload_members(str(path)))
    else:
        assert len(list(archive.iter_payload_members(str(path)))) == 2


@pytest.mark.parametrize("over", [False, True])
def test_cumulative_member_bytes_boundary(tmp_path, monkeypatch, over):
    path, _ = _package(tmp_path, files=[("a", b"x" * 128), ("b", b"x" * 128)])
    monkeypatch.setattr(archive, "_MAX_PAYLOAD_BYTES", 256 - over, raising=False)
    if over:
        with pytest.raises(ValueError, match="cumulative member bytes"):
            list(archive.iter_payload_members(str(path)))
    else:
        assert len(list(archive.iter_payload_members(str(path)))) == 2


@pytest.mark.parametrize("unknown_size", [False, True])
@pytest.mark.parametrize("over", [False, True])
def test_expanded_size_includes_tar_padding(tmp_path, monkeypatch, unknown_size, over):
    path, _ = _package(tmp_path, unknown_size=unknown_size)
    monkeypatch.setattr(archive, "_MAX_EXPANDED_BYTES", 10240 - over, raising=False)
    if over:
        with pytest.raises(ValueError, match="expanded bytes"):
            list(archive.iter_payload_members(str(path)))
    else:
        assert len(list(archive.iter_payload_members(str(path)))) == 1


def test_readers_do_not_materialize_components_or_member_lists(tmp_path, monkeypatch):
    path, _ = _package(tmp_path)

    def forbidden(*args, **kwargs):
        raise AssertionError("whole ZIP member read or complete TAR member-list materialization")

    monkeypatch.setattr(zipfile.ZipFile, "read", forbidden)
    monkeypatch.setattr(tarfile.TarFile, "getmembers", forbidden)
    assert archive.read_release_index(str(path))["name"] == "mssql-python"
    assert archive.read_index(str(path))["name"] == "mssql-python"
    assert dict(archive.iter_payload_members(str(path)))["payload.bin"] == b"x" * 128


@pytest.mark.parametrize("over", [False, True])
def test_zip_directory_allocation_boundary(tmp_path, monkeypatch, over):
    path, _ = _package(tmp_path)
    with zipfile.ZipFile(path) as container:
        directory_size = path.stat().st_size - 22 - container.start_dir
    monkeypatch.setattr(archive, "_MAX_ZIP_DIRECTORY_BYTES", directory_size - over)
    if over:
        with pytest.raises(ValueError, match="ZIP directory/read bytes"):
            archive.read_release_index(str(path))
    else:
        assert archive.read_release_index(str(path))["version"] == "1.15.0"


@pytest.mark.parametrize("over", [False, True])
def test_zip_member_count_boundary(tmp_path, monkeypatch, over):
    path, _ = _package(tmp_path)
    monkeypatch.setattr(archive, "_MAX_ZIP_MEMBERS", 3 - over)
    if over:
        with pytest.raises(ValueError, match="ZIP member count"):
            archive.read_release_index(str(path))
    else:
        assert archive.read_release_index(str(path))["version"] == "1.15.0"


@pytest.mark.parametrize("over", [False, True])
def test_compressed_component_boundary(tmp_path, monkeypatch, over):
    path, _ = _package(tmp_path)
    with zipfile.ZipFile(path) as container:
        limit = max(info.file_size for info in container.infolist())
    monkeypatch.setattr(archive, "_MAX_COMPRESSED_BYTES", limit - over)
    if over:
        with pytest.raises(ValueError, match="ZIP member bytes|compressed bytes"):
            archive.read_release_index(str(path))
    else:
        assert archive.read_release_index(str(path))["version"] == "1.15.0"


def test_zip_expansion_checked_before_component_decoder(tmp_path, monkeypatch):
    path, _ = _package(tmp_path)
    with zipfile.ZipFile(path) as container:
        members = {name: container.read(name) for name in container.namelist()}
    members[f"pkg-{path.stem}.tar.zst"] = b"x" * 4096
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as container:
        for name, data in members.items():
            container.writestr(name, data)
    monkeypatch.setattr(archive, "_MAX_COMPRESSED_BYTES", 4095)
    with pytest.raises(ValueError, match="ZIP member bytes"):
        list(archive.iter_payload_members(str(path)))


@pytest.mark.parametrize("over", [False, True])
def test_cumulative_metadata_boundary(tmp_path, monkeypatch, over):
    path, _ = _package(
        tmp_path,
        files=[("a.dist-info/METADATA", b"x" * 128), ("a.dist-info/RECORD", b"x" * 128)],
    )
    monkeypatch.setattr(archive, "_MAX_METADATA_TOTAL", 256 - over)
    if over:
        with pytest.raises(ValueError, match="cumulative metadata bytes"):
            list(archive.iter_payload_members(str(path)))
    else:
        assert len(list(archive.iter_payload_members(str(path)))) == 2


@pytest.mark.parametrize("unknown_size", [False, True])
@pytest.mark.parametrize("reader", [archive.zstd_decompress, archive.decompress_index])
def test_truncated_zstd_footer_is_rejected(reader, unknown_size):
    raw = _compress(b"readable data", unknown_size=unknown_size)
    with pytest.raises(archive.READ_ERRORS):
        reader(raw[:-1])


@pytest.mark.parametrize("extension", [".conda", ".tar.bz2"])
def test_expanded_limit_covers_metadata_stream_padding(tmp_path, monkeypatch, extension):
    path, _ = _package(tmp_path, extension)
    monkeypatch.setattr(archive, "_MAX_EXPANDED_BYTES", 10239)
    with pytest.raises(ValueError, match="expanded bytes"):
        archive.read_release_index(str(path))


def test_zstd_window_limit_is_enforced(tmp_path, monkeypatch):
    path, _ = _package(tmp_path, unknown_size=True)
    monkeypatch.setattr(archive, "_MAX_ZSTD_WINDOW_BYTES", 1024)
    with pytest.raises(ValueError, match="(?i)window|memory"):
        list(archive.iter_payload_members(str(path)))


def test_normal_larger_decoder_window_is_not_accidentally_limited_to_kibibytes(tmp_path):
    path, _ = _package(tmp_path, files=[("payload.bin", b"x" * 256 * 1024)], unknown_size=True)
    assert len(dict(archive.iter_payload_members(str(path)))["payload.bin"]) == 256 * 1024


@pytest.mark.parametrize("unknown_size", [False, True])
def test_concatenated_zstd_frames_share_expanded_budget(monkeypatch, unknown_size):
    raw = _compress(b"a" * 100, unknown_size=unknown_size) + _compress(
        b"b" * 100, unknown_size=unknown_size
    )
    monkeypatch.setattr(archive, "_MAX_EXPANDED_BYTES", 200)
    assert archive.zstd_decompress(raw) == b"a" * 100 + b"b" * 100
    monkeypatch.setattr(archive, "_MAX_EXPANDED_BYTES", 199)
    with pytest.raises(ValueError, match="expanded bytes"):
        archive.zstd_decompress(raw)


def test_pax_size_checked_before_extended_header_processing(tmp_path, monkeypatch):
    member = tarfile.TarInfo("PaxHeaders/oversized")
    member.type = tarfile.XHDTYPE
    member.size = 4096
    path, _ = _package(tmp_path)
    with zipfile.ZipFile(path) as container:
        members = {name: container.read(name) for name in container.namelist()}
    members[f"pkg-{path.stem}.tar.zst"] = _compress(member.tobuf() + b"\0" * 10240)
    with zipfile.ZipFile(path, "w") as container:
        for name, data in members.items():
            container.writestr(name, data)
    monkeypatch.setattr(archive, "_MAX_METADATA_BYTES", 4095)

    def forbidden(*args):
        raise AssertionError("oversized PAX must fail before extension parsing")

    monkeypatch.setattr(tarfile.TarInfo, "_proc_pax", forbidden)
    with pytest.raises(ValueError, match="metadata bytes"):
        list(archive.iter_payload_members(str(path)))


@pytest.mark.parametrize("pax", [False, True])
def test_sparse_maps_rejected_before_sparse_processing(tmp_path, pax):
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w") as contents:
        member = tarfile.TarInfo("sparse")
        if pax:
            member.pax_headers = {
                "GNU.sparse.major": "1",
                "GNU.sparse.minor": "0",
                "GNU.sparse.realsize": "128",
                "GNU.sparse.name": "sparse",
            }
        else:
            member.type = tarfile.GNUTYPE_SPARSE
        member.size = 2
        contents.addfile(member, io.BytesIO(b"0\n"))
    path, _ = _package(tmp_path)
    with zipfile.ZipFile(path) as container:
        members = {name: container.read(name) for name in container.namelist()}
    members[f"pkg-{path.stem}.tar.zst"] = _compress(buffer.getvalue())
    with zipfile.ZipFile(path, "w") as container:
        for name, data in members.items():
            container.writestr(name, data)
    with pytest.raises(ValueError, match="Sparse TAR"):
        list(archive.iter_payload_members(str(path)))
