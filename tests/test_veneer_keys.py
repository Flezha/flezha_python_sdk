"""The activation record keys this SDK reads, held to the veneer that writes them."""

from __future__ import annotations

import re

import contract

READERS = ("dispatch.py", "cell_state.py")


def test_every_record_key_read_is_one_the_veneer_writes():
    shape = contract.member("crates/flz_core_ffi/src/veneer/shape.rs").read_text()
    written = set(re.findall(r'Key::(?:flat|of)\("([a-z_]+)"', shape))
    read = {
        key
        for name in READERS
        for key in re.findall(
            r'\b(?:activation|item|request|body)(?:\.get\(|\[)"([a-z_]+)"',
            (contract.PACKAGE_ROOT / "src" / "flezha" / name).read_text(),
        )
    }
    assert read, "the record readers were found"
    assert read <= written, f"read but never written by the veneer: {sorted(read - written)}"
