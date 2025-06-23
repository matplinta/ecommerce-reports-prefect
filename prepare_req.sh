#!/bin/bash
# This script prepares the Python environment by installing dependencies
uv pip compile pyproject.toml | uv pip sync -