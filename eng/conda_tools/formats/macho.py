"""Read thin and universal Mach-O architecture facts without native tools."""

from __future__ import annotations

import struct

# Mach-O cputype (mach/machine.h): the base type OR'd with the 64-bit ABI flag -> lipo name.
_CPU_ARCH_ABI64 = 0x01000000
_CPU_TYPE_X86 = 0x00000007
_CPU_TYPE_ARM = 0x0000000C
_CPU_ARCHES = {
    _CPU_TYPE_X86 | _CPU_ARCH_ABI64: "x86_64",  # 0x01000007
    _CPU_TYPE_ARM | _CPU_ARCH_ABI64: "arm64",  # 0x0100000C
    _CPU_TYPE_X86: "i386",
    _CPU_TYPE_ARM: "arm",
}

# Mach-O / fat magics (mach-o/loader.h, mach-o/fat.h). The fat header is ALWAYS big-endian on
# disk; a thin header's cputype word follows the header's own endianness.
_MH_MAGIC = 0xFEEDFACE  # 32-bit thin
_MH_MAGIC_64 = 0xFEEDFACF  # 64-bit thin (x86_64 / arm64)
_FAT_MAGIC = 0xCAFEBABE  # universal (fat_arch entries, 20 bytes each)
_FAT_MAGIC_64 = 0xCAFEBABF  # universal64 (fat_arch_64 entries, 32 bytes each)


def _thin_arch(data: bytes) -> str | None:
    if len(data) < 8:
        return None
    be = struct.unpack_from(">I", data, 0)[0]
    le = struct.unpack_from("<I", data, 0)[0]
    if le in (_MH_MAGIC, _MH_MAGIC_64):
        endian = "<"
        header_size = 32 if le == _MH_MAGIC_64 else 28
    elif be in (_MH_MAGIC, _MH_MAGIC_64):
        endian = ">"
        header_size = 32 if be == _MH_MAGIC_64 else 28
    else:
        return None
    if len(data) < header_size:
        return None
    ncmds, sizeofcmds = struct.unpack_from(f"{endian}II", data, 16)
    commands_end = header_size + sizeofcmds
    if ncmds == 0 or sizeofcmds < ncmds * 8 or commands_end > len(data):
        return None
    command_offset = header_size
    for _ in range(ncmds):
        if command_offset + 8 > commands_end:
            return None
        command_size = struct.unpack_from(f"{endian}I", data, command_offset + 4)[0]
        if command_size < 8 or command_offset + command_size > commands_end:
            return None
        command_offset += command_size
    if command_offset != commands_end:
        return None
    cputype = struct.unpack_from(f"{endian}I", data, 4)[0]
    return _CPU_ARCHES.get(cputype, hex(cputype))


def macho_arches(data: bytes) -> set[str] | None:
    """Return the SET of lipo-style arch names in a Mach-O binary (thin OR fat/universal), or
    None if the bytes are not Mach-O. Reads only headers -- no dependency on macOS tooling."""
    if len(data) < 8:
        return None
    be = struct.unpack_from(">I", data, 0)[0]  # fat magic is big-endian on disk
    if be in (_FAT_MAGIC, _FAT_MAGIC_64):
        nfat = struct.unpack_from(">I", data, 4)[0]
        entry = 20 if be == _FAT_MAGIC else 32  # fat_arch vs fat_arch_64
        table_end = 8 + nfat * entry
        if nfat == 0 or table_end > len(data):
            return None
        arches = set()
        for index in range(nfat):
            entry_offset = 8 + index * entry
            if be == _FAT_MAGIC:
                cputype, _, slice_offset, slice_size, _ = struct.unpack_from(
                    ">IIIII", data, entry_offset
                )
            else:
                cputype, _, slice_offset, slice_size, _, _ = struct.unpack_from(
                    ">IIQQII", data, entry_offset
                )
            if (
                slice_size == 0
                or slice_offset < table_end
                or slice_offset > len(data)
                or slice_size > len(data) - slice_offset
            ):
                return None
            declared_arch = _CPU_ARCHES.get(cputype, hex(cputype))
            embedded_arch = _thin_arch(data[slice_offset : slice_offset + slice_size])
            if embedded_arch != declared_arch:
                return None
            arches.add(declared_arch)
        return arches
    thin_arch = _thin_arch(data)
    return {thin_arch} if thin_arch is not None else None
