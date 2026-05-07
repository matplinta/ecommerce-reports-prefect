#!/bin/bash
# This script prepares the Python environment by installing dependencies
apt-get update && apt-get install -y --no-install-recommends build-essential
PREFECT_VERSION=$(python -c "import prefect; print(prefect.__version__)")
echo "prefect[email,slack]==${PREFECT_VERSION}" > /tmp/prefect-constraint.txt 
uv pip compile pyproject.toml --constraints /tmp/prefect-constraint.txt | uv pip sync -