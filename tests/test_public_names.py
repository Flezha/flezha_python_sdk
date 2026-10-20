"""Every public concept spelled as ``test/fixtures/sdk_public_names.txt`` spells it.

The fixture holds one row per concept and one column per SDK. A Python cell is
a dotted path under ``flezha`` (a submodule, a class, a member), optionally
followed by ``(parameter)`` for a keyword the callable takes. A cell starting
with ``!`` names something that must not be public, so a re-exported internal
fails here as surely as a renamed concept does.
"""

from __future__ import annotations

import importlib
import inspect
from types import ModuleType
from typing import Any

import pytest

import contract
import flezha

_FIXTURE = contract.member("test/fixtures/sdk_public_names.txt")
_COLUMN = 1  # concept|python|ruby|php|node|elixir|java|rust


def _cells() -> list[tuple[str, str]]:
    cells = []
    for line in _FIXTURE.read_text().splitlines():
        if not line.strip() or line.startswith("#"):
            continue
        row = line.split("|")
        cells += [(row[0], path) for path in row[_COLUMN].split() if path != "-"]
    return cells


def _member(owner: Any, name: str) -> Any:
    found = getattr(owner, name, None)
    if found is None and isinstance(owner, ModuleType):
        try:
            found = importlib.import_module(f"{owner.__name__}.{name}")
        except ModuleNotFoundError:
            return None
    return found


def _resolves(path: str) -> bool:
    dotted, _, parameter = path.partition("(")
    target: Any = flezha
    for name in dotted.split("."):
        target = _member(target, name)
        if target is None:
            return False
    if not parameter:
        return True
    return parameter.rstrip(")") in inspect.signature(target).parameters


@pytest.mark.parametrize(("concept", "path"), _cells(), ids=lambda cell: cell)
def test_a_concept_has_its_one_public_spelling(concept: str, path: str) -> None:
    hidden = path.startswith("!")
    assert _resolves(path.lstrip("!")) is not hidden, (
        f"{concept}: `{path.lstrip('!')}` is {'public' if hidden else 'missing'}"
    )


def test_the_package_exports_only_resolvable_names() -> None:
    assert [name for name in flezha.__all__ if not hasattr(flezha, name)] == []
