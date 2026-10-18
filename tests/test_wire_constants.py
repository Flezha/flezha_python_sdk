"""This runtime's wire constants, held to the shared constants fixture."""

from __future__ import annotations

import contract
from flezha import ctx, graph, native

FIXTURE = contract.member("test/fixtures/wire_constants.txt")


def row(table: str, name: str) -> str:
    for line in FIXTURE.read_text().splitlines():
        fields = line.split("|")
        if fields[:2] == [table, name]:
            return fields[2]
    raise AssertionError(f"{FIXTURE} has no row {table}|{name}")


def test_the_language_byte_codec_and_schema_are_the_fixtures():
    assert graph.SDK_LANG_PYTHON == int(row("sdk_lang", "python"))
    assert graph.FORMAT_ID_MSGPACK == int(row("format_id", "msgpack"))
    assert native.ACTIVATION_SCHEMA_VERSION == int(row("schema_version", "activation"))


def test_the_output_stamps_are_reserved_keys():
    assert (ctx.CELL_STAMP, ctx.SEQ_STAMP, ctx.KEY_STAMP) == (
        row("flz_key", "cell"),
        row("flz_key", "seq"),
        row("flz_key", "key"),
    )
