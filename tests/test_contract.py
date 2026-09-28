import pathlib

import contract


def _carry_manifest(tree: pathlib.Path) -> None:
    (tree / "test/fixtures").mkdir(parents=True)
    (tree / contract.MANIFEST).write_text("")


def test_an_in_tree_copy_does_not_shadow_the_checkout(tmp_path: pathlib.Path) -> None:
    package = tmp_path / "sdk/python"
    _carry_manifest(tmp_path)
    _carry_manifest(package / "contract")

    assert contract.locate(package, None) == tmp_path

    (tmp_path / contract.MANIFEST).unlink()
    assert contract.locate(package, None) == package / "contract"
