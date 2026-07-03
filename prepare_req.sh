#!/bin/bash
# This script prepares the Python environment by installing dependencies
set -euo pipefail

apt-get update && apt-get install -y --no-install-recommends build-essential

PREFECT_VERSION=$(python -c "import prefect; print(prefect.__version__)")
echo "Image prefect version: ${PREFECT_VERSION}"

echo "prefect==${PREFECT_VERSION}" > /tmp/prefect-constraint.txt

REQ=/tmp/requirements.txt
if ! uv pip compile pyproject.toml --constraints /tmp/prefect-constraint.txt -o "$REQ"; then
    echo "prefect==${PREFECT_VERSION} not on PyPI yet; resolving without the exact pin"
    uv pip compile pyproject.toml -o "$REQ"
fi

uv pip sync "$REQ"