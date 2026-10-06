#!/usr/bin/env bash
# The three tiers this repository runs, and no controller anywhere.
#
#   unit        the package's own suite, including the fixture checks that read
#               the contract set
#   local mode  the declared graph run in process, over the five capabilities
#               the language layer owns
#   smoke       the fake controller on a socket directory, this repository's own
#               test app launched against it, and the binary's exit code
#
# Everything the core contributes is fetched from one of its releases and
# checked against that release's manifest: the extension module, the fake
# controller and the contract set. Nothing here builds the core, and nothing
# here needs a checkout of it.
#
#   scripts/ci.sh --core <dir|url> [--version 0.1.0]
#
# The interpreter is `FLEZHA_PYTHON` (default python3). A free-threaded one runs
# every tier against the release's free-threaded wheel for its version.
#
# What this cannot see is the fifteen-capability vocabulary the controller
# owns: persistence, effectively-once, correction, partitioning, steps relays,
# liveness, restarts. Those are end-to-end scenarios and they run in the core's
# own tree, against the commit it pins this repository at.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

CORE=""
VERSION="0.1.0"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --core)    CORE="$2"; shift 2 ;;
    --version) VERSION="$2"; shift 2 ;;
    -h|--help) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; exit 1 ;;
  esac
done
[[ -n "${CORE}" ]] || { echo "--core <dir|url> is required" >&2; exit 1; }

step() { printf '\n\033[1m== %s\033[0m\n' "$1"; }

step "installing"
# A checkout-local interpreter with the dev group's exact pins. `--group` needs
# pip 25.1, newer than what a system interpreter's venv starts with.
[[ -x .venv/bin/python ]] || "${FLEZHA_PYTHON:-python3}" -m venv .venv
.venv/bin/python -m pip install --quiet --disable-pip-version-check --upgrade "pip>=25.1"
.venv/bin/python -m pip install --quiet --disable-pip-version-check --group dev

step "fetching the core ${VERSION}"
scripts/fetch-core.sh --from "${CORE}" --version "${VERSION}" --python .venv/bin/python

step "lint"
.venv/bin/ruff check .
.venv/bin/ruff format --check .

# The local-mode tests drive the fixture classes `test_app/` declares, so both
# tiers are one run of the suite.
step "unit and local mode"
PYTHONPATH=src:tests PATH="${PWD}/.venv/bin:${PATH}" .venv/bin/python -m pytest tests examples/starter -q
PYTHONPATH=src .venv/bin/python -c "from flezha import native; print('tier:', native.build_tier())"

step "smoke"
# The fake controller launches the runner against a socket directory of its own,
# holds its standard input open for the run and prints its output on failure.
# The tier's verdict is the fake controller's, never the runner's: a terminal
# disconnect ends the runner non-zero on purpose, so the orchestrator that owns
# it restarts it.
exec .core/flz-fake-controller \
  --factory do_nothing --relay echo-1 --channel input \
  --input alpha --input beta --timeout-ms 60000 \
  -- .venv/bin/python test_app/runner.py smoke
