#!/usr/bin/env bash
set -e

mkdir cov || true

export CARGO_TARGET_DIR="cov/target"
cargo tarpaulin --run-types Tests Doctests --skip-clean -o html --output-dir cov
echo "Take a look at $PWD/cov/tarpaulin-report.html"
