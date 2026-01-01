#!/bin/bash
# This script prepares the Python environment by installing dependencies
apt-get update && apt-get install -y --no-install-recommends build-essential
uv pip compile pyproject.toml | uv pip sync -