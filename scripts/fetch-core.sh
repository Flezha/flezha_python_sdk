#!/usr/bin/env bash
# Fetch the three things this package needs from one core release: the native
# extension module, the fake controller, and the contract set.
#
# This package is proven against a core it does not contain. A release of the
# core publishes every one of those as an asset with its SHA-256 in one
# manifest, so obtaining them is a download and a checksum rather than a
# checkout and a Rust toolchain, and nothing is used before its digest matches.
#
#   scripts/fetch-core.sh --from <dir|url> [--version 0.1.0] [--python <interpreter>]
#
# `--from` is a staged release directory (what `scripts/build-dist.sh
# --stage-release` writes in the core's tree) or the URL a GitHub release
# serves its assets from. Everything it installs is build output and is ignored
# by git: the extension module at `src/flezha/`, where `flezha.native` imports
# it, the fake controller at `.core/`, and the contract set at `contract/`.
#
# The module comes out of the wheel that interpreter installs: the `abi3` wheel
# for a regular build, and the free-threaded wheel of its own version for a
# free-threaded one, which cannot load an `abi3` module.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

FROM=""
VERSION="0.1.0"
PYTHON="python3"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --from)    FROM="$2"; shift 2 ;;
    --version) VERSION="$2"; shift 2 ;;
    --python)  PYTHON="$2"; shift 2 ;;
    -h|--help) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; exit 1 ;;
  esac
done

refuse() {
  echo "fetch-core: $*" >&2
  exit 1
}

[[ -n "${FROM}" ]] || refuse "--from <dir|url> is required"
command -v sha256sum >/dev/null 2>&1 || refuse "sha256sum is required"
command -v "${PYTHON}" >/dev/null 2>&1 || refuse "${PYTHON} is not an interpreter on PATH"

# The platform a core release names its per-platform assets by, the directory
# column of the core's `scripts/native_triples.sh`, and the tail of a wheel's
# platform tag for it. The rest of the tag is the manylinux or macOS
# floor the build met, which no script here can predict.
case "$(uname -s)-$(uname -m)" in
  Linux-x86_64)               platform=linux-x64;   wheel_tail='manylinux_[0-9_]+_x86_64' ;;
  Linux-aarch64|Linux-arm64)  platform=linux-arm64; wheel_tail='manylinux.*_aarch64' ;;
  Darwin-x86_64)              platform=macos-x64;   wheel_tail='macosx_[0-9_]+_x86_64' ;;
  Darwin-arm64)               platform=macos-arm64; wheel_tail='macosx_[0-9_]+_arm64' ;;
  *) refuse "no core release asset is published for $(uname -s) $(uname -m)" ;;
esac

scratch=.core/download
rm -rf "${scratch}"
mkdir -p "${scratch}"

# One asset's bytes, from a directory or a release URL.
asset() {
  local name="$1" into="$2"
  if [[ "${FROM}" =~ ^https?:// ]]; then
    curl -fsSL "${FROM%/}/${name}" -o "${into}" || refuse "${name}: not downloadable from ${FROM}"
  else
    [[ -f "${FROM}/${name}" ]] || refuse "${FROM}/${name} is not there"
    cp "${FROM}/${name}" "${into}"
  fi
}

# One asset, checked against the manifest and moved to `into` only if it matches.
verified() {
  local name="$1" into="$2" expected actual
  expected="$(awk -v name="${name}" '$2 == name || $2 == "*" name { print $1 }' "${scratch}/SHA256SUMS.txt")"
  [[ -n "${expected}" ]] || refuse "${name} is not in SHA256SUMS.txt; it was not part of this release"
  asset "${name}" "${scratch}/${name}"
  actual="$(sha256sum "${scratch}/${name}" | awk '{ print $1 }')"
  [[ "${actual}" == "${expected}" ]] || refuse "${name} does not match its manifest digest"
  mkdir -p "$(dirname "${into}")"
  mv "${scratch}/${name}" "${into}"
  echo "  verified ${name} -> ${into}"
}

echo "fetch-core: taking the ${VERSION} core from ${FROM}"
asset SHA256SUMS.txt "${scratch}/SHA256SUMS.txt"

# The extension module out of the wheel this interpreter would install. The
# wheel is what a package index serves, so what the suites load is what an
# installer gets.
abi="$("${PYTHON}" -c 'import sys, sysconfig
tag = f"cp{sys.version_info[0]}{sys.version_info[1]}"
print(f"{tag}-{tag}t" if sysconfig.get_config_var("Py_GIL_DISABLED") else "cp[0-9]+-abi3")')"
wheel="$(awk -v pattern="^flezha-${VERSION//./[.]}-${abi}-${wheel_tail}[.]whl\$" \
  '$2 ~ pattern { print $2 }' "${scratch}/SHA256SUMS.txt")"
[[ -n "${wheel}" && "${wheel}" != *$'\n'* ]] \
  || refuse "SHA256SUMS.txt lists no single ${abi} wheel for ${platform}; it was not part of this release"
verified "${wheel}" "${scratch}/extension.whl"
rm -f src/flezha/flz_core_ffi*.so src/flezha/flz_core_ffi*.pyd
"${PYTHON}" - "${scratch}/extension.whl" src/flezha <<'EXTRACT'
import pathlib
import sys
import zipfile

wheel, into = sys.argv[1], pathlib.Path(sys.argv[2])
with zipfile.ZipFile(wheel) as archive:
    members = [
        name
        for name in archive.namelist()
        if name.startswith("flezha/flz_core_ffi.") and name.endswith((".so", ".pyd"))
    ]
    if len(members) != 1:
        sys.exit(f"fetch-core: the wheel carries {len(members)} extension modules, not one")
    target = into / pathlib.PurePosixPath(members[0]).name
    target.write_bytes(archive.read(members[0]))
    print(f"  unpacked {members[0]} -> {target}")
EXTRACT

verified "flz-fake-controller-${platform}" .core/flz-fake-controller
chmod +x .core/flz-fake-controller

verified "flezha-fixtures-${VERSION}.tar.gz" "${scratch}/fixtures.tar.gz"
rm -rf contract
mkdir -p contract
tar -xzf "${scratch}/fixtures.tar.gz" -C contract
echo "  unpacked the contract set -> contract/"

rm -rf "${scratch}"
echo "fetch-core: done"
