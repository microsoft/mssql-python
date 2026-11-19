"""Read ELF header and dynamic-segment facts without loading native code."""

from __future__ import annotations

import re
import struct
from dataclasses import dataclass
from typing import TypedDict

# --- ELF constants ---------------------------------------------------------
_DT_NEEDED = 1
_DT_STRTAB = 5
_DT_STRSZ = 10
_DT_RPATH = 15
_DT_RUNPATH = 29
_DT_VERNEED = 0x6FFFFFFE
_DT_VERNEEDNUM = 0x6FFFFFFF
_PT_LOAD = 1
_PT_DYNAMIC = 2


class ElfDynamicInfo(TypedDict):
    runpath: str | None
    rpath: str | None
    needed: list[str]
    glibc_required: list[tuple[int, ...]]


def _is_elf(data: bytes) -> bool:
    return len(data) >= 64 and data[:4] == b"\x7fELF"


def elf_machine(data: bytes) -> int | None:
    """Return the ELF ``e_machine`` architecture id (header offset 0x12), or None.

    Endianness comes from ``e_ident[5]`` (the shipped drivers are ELF64-LE).
    """
    if not _is_elf(data):
        return None
    en = "<" if data[5] == 1 else ">"
    return struct.unpack_from(en + "H", data, 0x12)[0]


def elf_dynamic(data: bytes) -> ElfDynamicInfo:
    """Return ``{'runpath': str|None, 'rpath': str|None, 'needed': [str]}``.

    Parses the ``PT_DYNAMIC`` program header -- the segment the LOADER actually uses
    -- and maps ``DT_STRTAB``'s virtual address to a file offset through the
    ``PT_LOAD`` segments, so this matches the loader's own view rather than a section
    table that a stripped/rewritten binary might not carry. Handles ELF32/ELF64 and
    both endiannesses; the shipped drivers are ELF64-LE.
    """
    out: ElfDynamicInfo = {"runpath": None, "rpath": None, "needed": [], "glibc_required": []}
    if not _is_elf(data):
        raise ValueError("not a complete ELF header")
    if data[4] not in (1, 2) or data[5] not in (1, 2):
        raise ValueError("invalid ELF class or endianness")
    is64 = data[4] == 2
    en = "<" if data[5] == 1 else ">"

    if is64:
        e_phoff = struct.unpack_from(en + "Q", data, 0x20)[0]
        e_phentsize = struct.unpack_from(en + "H", data, 0x36)[0]
        e_phnum = struct.unpack_from(en + "H", data, 0x38)[0]
    else:
        e_phoff = struct.unpack_from(en + "I", data, 0x1C)[0]
        e_phentsize = struct.unpack_from(en + "H", data, 0x2A)[0]
        e_phnum = struct.unpack_from(en + "H", data, 0x2C)[0]
    if (
        not e_phoff
        or not e_phnum
        or e_phentsize < (56 if is64 else 32)
        or e_phoff + e_phnum * e_phentsize > len(data)
    ):
        raise ValueError("invalid or truncated ELF program headers")

    loads = []  # (p_vaddr, p_offset, p_filesz)
    dyn = None  # (p_offset, p_filesz)
    for i in range(e_phnum):
        off = e_phoff + i * e_phentsize
        if off + e_phentsize > len(data):
            raise ValueError("truncated ELF program header")
        p_type = struct.unpack_from(en + "I", data, off)[0]
        if is64:
            p_offset = struct.unpack_from(en + "Q", data, off + 8)[0]
            p_vaddr = struct.unpack_from(en + "Q", data, off + 16)[0]
            p_filesz = struct.unpack_from(en + "Q", data, off + 32)[0]
        else:
            p_offset = struct.unpack_from(en + "I", data, off + 4)[0]
            p_vaddr = struct.unpack_from(en + "I", data, off + 8)[0]
            p_filesz = struct.unpack_from(en + "I", data, off + 16)[0]
        if p_offset + p_filesz > len(data):
            raise ValueError("ELF segment extends beyond the file")
        if p_type == _PT_LOAD:
            loads.append((p_vaddr, p_offset, p_filesz))
        elif p_type == _PT_DYNAMIC:
            dyn = (p_offset, p_filesz)
    if dyn is None:
        raise ValueError("ELF has no PT_DYNAMIC segment")
    dyn_off, dyn_size = dyn

    def vaddr_to_off(vaddr: int, size: int = 1) -> int:
        for v, o, sz in loads:
            if v <= vaddr and vaddr + size <= v + sz:
                return vaddr - v + o
        raise ValueError("ELF dynamic address is outside a file-backed PT_LOAD segment")

    strtab_vaddr = None
    strtab_size = None
    verneed_vaddr = None
    verneed_num = None
    runpath_rel = None
    rpath_rel = None
    needed_rel: list[int] = []
    entsize = 16 if is64 else 8
    terminated = False
    if dyn_size % entsize:
        raise ValueError("ELF dynamic segment has a partial entry")
    for off in range(dyn_off, dyn_off + dyn_size, entsize):
        if off + entsize > len(data):
            break
        if is64:
            d_tag = struct.unpack_from(en + "q", data, off)[0]
            d_val = struct.unpack_from(en + "Q", data, off + 8)[0]
        else:
            d_tag = struct.unpack_from(en + "i", data, off)[0]
            d_val = struct.unpack_from(en + "I", data, off + 4)[0]
        if d_tag == 0:  # DT_NULL terminates the array
            terminated = True
            break
        if d_tag == _DT_STRTAB:
            strtab_vaddr = d_val
        elif d_tag == _DT_RUNPATH:
            runpath_rel = d_val
        elif d_tag == _DT_RPATH:
            rpath_rel = d_val
        elif d_tag == _DT_NEEDED:
            needed_rel.append(d_val)
        elif d_tag == _DT_STRSZ:
            strtab_size = d_val
        elif d_tag == _DT_VERNEED:
            verneed_vaddr = d_val
        elif d_tag == _DT_VERNEEDNUM:
            verneed_num = d_val
    if not terminated or strtab_vaddr is None or not strtab_size:
        raise ValueError("ELF dynamic segment lacks DT_NULL, DT_STRTAB or DT_STRSZ")
    strtab_off = vaddr_to_off(strtab_vaddr, strtab_size)

    def read_str(rel: int) -> str:
        if not 0 <= rel < strtab_size:
            raise ValueError("ELF string offset is outside DT_STRTAB")
        pos = strtab_off + rel
        end = data.find(b"\x00", pos, strtab_off + strtab_size)
        if end < 0:
            raise ValueError("unterminated ELF dynamic string")
        return data[pos:end].decode("utf-8", "strict")

    if runpath_rel is not None:
        out["runpath"] = read_str(runpath_rel)
    if rpath_rel is not None:
        out["rpath"] = read_str(rpath_rel)
    out["needed"] = [read_str(n) for n in needed_rel]
    if (verneed_vaddr is None) != (verneed_num is None):
        raise ValueError("ELF version requirements need both DT_VERNEED and DT_VERNEEDNUM")
    if verneed_vaddr is not None:
        if not verneed_num or verneed_num > len(data) // 16:
            raise ValueError("invalid ELF version requirement count")
        current = verneed_vaddr
        for number in range(verneed_num):
            offset = vaddr_to_off(current, 16)
            version, count, library, aux, next_need = struct.unpack_from(en + "HHIII", data, offset)
            if version != 1 or not count or count > len(data) // 16 or aux < 16:
                raise ValueError("invalid ELF version requirement record")
            read_str(library)
            auxiliary = current + aux
            for item in range(count):
                offset = vaddr_to_off(auxiliary, 16)
                _, _, _, name, next_aux = struct.unpack_from(en + "IHHII", data, offset)
                requirement = read_str(name)
                if requirement.startswith("GLIBC_"):
                    match = re.fullmatch(r"GLIBC_(\d+(?:\.\d+)+)", requirement)
                    if match is None:
                        raise ValueError(f"unsupported glibc symbol requirement {requirement}")
                    out["glibc_required"].append(tuple(map(int, match[1].split("."))))
                if item < count - 1 and next_aux < 16:
                    raise ValueError("truncated ELF version auxiliary chain")
                if item == count - 1 and next_aux != 0:
                    raise ValueError("ELF version auxiliary count disagrees with chain")
                auxiliary += next_aux
            if number < verneed_num - 1 and next_need < 16:
                raise ValueError("truncated ELF version requirement chain")
            if number == verneed_num - 1 and next_need != 0:
                raise ValueError("ELF version requirement count disagrees with chain")
            current += next_need
    return out


@dataclass(frozen=True)
class ElfFacts:
    magic: bool
    header: bool
    elf64_le: bool
    machine: int | None
    dynamic: ElfDynamicInfo | None
    error: str | None


def parse(data: bytes) -> ElfFacts:
    """Capture parsing errors as facts; callers decide which members require ELF."""
    magic = data.startswith(b"\x7fELF")
    header = _is_elf(data)
    elf64_le = data[4:6] == b"\x02\x01"
    machine = elf_machine(data)
    dynamic = None
    error = None
    if header:
        try:
            dynamic = elf_dynamic(data)
        except (ValueError, struct.error) as exc:
            error = str(exc)
    return ElfFacts(magic, header, elf64_le, machine, dynamic, error)
