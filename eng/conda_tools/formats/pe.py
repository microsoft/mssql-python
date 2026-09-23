"""Read PE header facts without loading native code."""

from __future__ import annotations

import struct


def pe_machine(data: bytes) -> int | None:
    """Return the PE COFF Machine value (int) for a Windows binary, or None.

    DOS header 'MZ' -> e_lfanew at offset 0x3C -> 'PE\\0\\0' signature -> COFF header,
    whose first 2 bytes are the Machine field (little-endian).
    """
    if len(data) < 0x40 or data[:2] != b"MZ":
        return None
    e_lfanew = struct.unpack_from("<I", data, 0x3C)[0]
    coff_offset = e_lfanew + 4
    if coff_offset + 20 > len(data) or data[e_lfanew:coff_offset] != b"PE\x00\x00":
        return None
    machine, section_count = struct.unpack_from("<HH", data, coff_offset)
    optional_size = struct.unpack_from("<H", data, coff_offset + 16)[0]
    optional_offset = coff_offset + 20
    section_table = optional_offset + optional_size
    if section_count == 0 or optional_size < 2 or section_table + section_count * 40 > len(data):
        return None
    optional_magic = struct.unpack_from("<H", data, optional_offset)[0]
    if optional_magic not in (0x10B, 0x20B):  # PE32 / PE32+
        return None
    for index in range(section_count):
        section_offset = section_table + index * 40
        raw_size, raw_offset = struct.unpack_from("<II", data, section_offset + 16)
        if raw_size and (raw_offset > len(data) or raw_size > len(data) - raw_offset):
            return None
    return machine
