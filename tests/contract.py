"""The contract set this package is proven against.

``test/fixtures/contract.txt`` names every member (the declaration keys, the
knob parse rule, the reference topologies, the channel codecs, the C ABI
surface), and the release publishes them as one archive, so a package
developed outside the core's repository vendors them at ``contract/`` instead
of walking to a tree it has not got.

This resolves the directory those repository-relative paths hang off, once, in
the order every SDK uses: ``$FLEZHA_CONTRACT_DIR``, else the nearest enclosing
tree that carries the manifest, else ``contract/`` at the package root. The
enclosing tree comes first so a stale copy fetched into a package inside the
core's checkout never shadows the live files.
Resolving nowhere raises, naming the three; it never skips.
"""

from __future__ import annotations

import functools
import os
import pathlib

#: The member every candidate directory is recognised by.
MANIFEST = "test/fixtures/contract.txt"

#: The package root, whichever layout the package sits in.
PACKAGE_ROOT = pathlib.Path(__file__).resolve().parents[1]


def ascend_to(frm: pathlib.Path, marker: str) -> pathlib.Path | None:
    """The nearest directory at or above ``frm`` that carries ``marker``.

    Both of this suite's resolvers ask the same question: the contract set
    looks for its manifest, the test app for its ``conformance.json``. Asking
    it by walking rather than by counting ``..`` segments is what lets one
    answer serve both layouts this package lives in, under ``sdk/python`` in
    the core's tree and at the root of its own repository.
    """
    for candidate in (frm, *frm.parents):
        if (candidate / marker).is_file():
            return candidate
    return None


def _candidates(package_root: pathlib.Path, configured: str | None) -> list[pathlib.Path | None]:
    return [
        pathlib.Path(configured) if configured else None,
        ascend_to(package_root, MANIFEST),
        package_root / "contract",
    ]


def locate(package_root: pathlib.Path, configured: str | None) -> pathlib.Path:
    """Where the contract set is for a package at ``package_root``."""
    candidates = _candidates(package_root, configured)
    for candidate in candidates:
        if candidate is not None and (candidate / MANIFEST).is_file():
            return candidate
    unresolved = (
        "$FLEZHA_CONTRACT_DIR (unset)",
        f"no directory above {package_root} carries {MANIFEST}",
        "",
    )
    tried = "\n  ".join(
        str(candidate) if candidate else why for candidate, why in zip(candidates, unresolved)
    )
    raise FileNotFoundError(
        f"the contract set is nowhere this suite looked:\n  {tried}\n"
        f"Unpack flezha-fixtures-<version>.tar.gz at {package_root}/contract, "
        "or set FLEZHA_CONTRACT_DIR."
    )


@functools.cache
def root() -> pathlib.Path:
    """The directory the contract set's repository-relative paths hang off."""
    return locate(PACKAGE_ROOT, os.environ.get("FLEZHA_CONTRACT_DIR"))


def member(relative: str) -> pathlib.Path:
    """One member of the set, as an absolute path."""
    path = root() / relative
    if not path.is_file():
        raise FileNotFoundError(
            f"{relative} is a member of the contract set and is not in {root()}"
        )
    return path
