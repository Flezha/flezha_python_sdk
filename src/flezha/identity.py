"""The replay-stable identifier of a cell.

The algorithm is fixed by the SDK contract rather than by a library, so every
SDK derives the same value from the same cell: FNV-1a over ``identity | change
seq | per-invocation ordinal``, where the identity is the bytes the core hands
with the cell's first activation.
"""

from __future__ import annotations

import struct

__all__ = ["stable_id"]

_FNV128_OFFSET_BASIS = 0x6C62272E07BB014262B821756295C58D
_FNV128_PRIME = 0x0000000001000000000000000000013B
_MASK128 = (1 << 128) - 1


def _fnv1a128(seed: int, data: bytes) -> int:
    value = seed
    for byte in data:
        value = (value ^ byte) & _MASK128
        value = (value * _FNV128_PRIME) & _MASK128
    return value


def stable_id(identity: bytes, change_seq: int, ordinal: int) -> str:
    """Thirty-two lowercase hex characters.

    Repeated calls in one invocation differ; the n-th call of a replayed
    invocation equals the n-th call of the original, which is what makes it the
    replacement for a random UUID.
    """
    value = _fnv1a128(_FNV128_OFFSET_BASIS, identity)
    value = _fnv1a128(value, struct.pack(">Q", change_seq & 0xFFFFFFFFFFFFFFFF))
    value = _fnv1a128(value, struct.pack(">I", ordinal & 0xFFFFFFFF))
    return f"{value:032x}"
