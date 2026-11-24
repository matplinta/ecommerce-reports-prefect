#!/bin/bash
# This script prepares the Python environment by installing dependencies
set -euo pipefail

apt-get update && apt-get install -y --no-install-recommends build-essential

RUNTIME_PYTHON=$(python -c 'import sys; print(sys.executable)')
PREFECT_VERSION=$("$RUNTIME_PYTHON" -c 'from importlib.metadata import version; print(version("prefect"))')
PACKAGING_VERSION=$("$RUNTIME_PYTHON" -c 'from importlib.metadata import version; print(version("packaging"))')
echo "Image prefect version: ${PREFECT_VERSION}"
echo "Image packaging version: ${PACKAGING_VERSION}"

# Prefect may already have imported packaging before this pull step runs.
CONSTRAINTS=/tmp/runtime-constraints.txt
printf 'prefect==%s\npackaging==%s\n' "$PREFECT_VERSION" "$PACKAGING_VERSION" > "$CONSTRAINTS"

REQ=/tmp/requirements.txt
if ! uv pip compile pyproject.toml --python "$RUNTIME_PYTHON" --constraints "$CONSTRAINTS" -o "$REQ"; then
    echo "Resolution with the image Prefect version failed. Retrying with only the image packaging constraint."
    printf 'packaging==%s\n' "$PACKAGING_VERSION" > "$CONSTRAINTS"
    uv pip compile pyproject.toml --python "$RUNTIME_PYTHON" --constraints "$CONSTRAINTS" -o "$REQ"
fi

uv pip sync --python "$RUNTIME_PYTHON" "$REQ"
