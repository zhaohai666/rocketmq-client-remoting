# -*- coding: utf-8 -*-
"""The bit set carried by the ``b`` field of a batch acknowledge body entry.

Byte layout (the one the broker reads back with ``BitSet.valueOf``): little-endian, so
bit ``i`` is bit ``i % 8`` of byte ``i // 8``, and trailing zero bytes are dropped.
Consequently ``{3}`` is ``b"\\x08"`` (one byte), ``{8}`` is ``b"\\x00\\x01"`` and an
empty set is ``b""``.
"""
from __future__ import annotations

from typing import Iterable, List


class BitSet:
    """An unbounded set of non-negative bit indexes with the protocol's byte layout."""

    __slots__ = ("_bits",)

    def __init__(self, bits: Iterable[int] = ()):
        self._bits = set()
        for index in bits:
            self.set_bit(index)

    def set_bit(self, index: int) -> None:
        """Mark bit ``index``; a negative index is a client-side bug, not data."""
        index = int(index)
        if index < 0:
            raise ValueError("bit index must not be negative: %d" % index)
        self._bits.add(index)

    def is_bit_set(self, index: int) -> bool:
        return int(index) in self._bits

    def clear_bit(self, index: int) -> None:
        self._bits.discard(int(index))

    def next_set_bit(self, from_index: int = 0):
        """The smallest set index ``>= from_index``, or None when there is none."""
        candidates = [index for index in self._bits if index >= from_index]
        return min(candidates) if candidates else None

    def set_indexes(self) -> List[int]:
        """Every set index, ascending -- the order the offsets are walked in."""
        return sorted(self._bits)

    def __len__(self) -> int:
        return len(self._bits)

    def __eq__(self, other) -> bool:
        if isinstance(other, BitSet):
            return self._bits == other._bits
        return NotImplemented

    def __hash__(self) -> int:
        return hash(frozenset(self._bits))

    def __repr__(self):
        return "BitSet%r" % (self.set_indexes(),)

    # ---------------- byte form ----------------
    def to_bytes(self) -> bytes:
        """The little-endian byte form, with trailing zero bytes dropped."""
        if not self._bits:
            return b""
        out = bytearray(max(self._bits) // 8 + 1)
        for index in self._bits:
            out[index // 8] |= 1 << (index % 8)
        return bytes(out)

    @staticmethod
    def from_bytes(data) -> "BitSet":
        """Inverse of :meth:`to_bytes`; accepts ``bytes`` or an iterable of ints."""
        bs = BitSet()
        for byte_index, byte in enumerate(bytes(data)):
            byte = byte & 0xFF
            if byte == 0:
                continue
            for bit_in_byte in range(8):
                if byte & (1 << bit_in_byte):
                    bs.set_bit(byte_index * 8 + bit_in_byte)
        return bs
