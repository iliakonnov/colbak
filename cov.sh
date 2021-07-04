#!/usr/bin/env bash
set -e

rm -rf cov || true
mkdir cov

export RUSTFLAGS="-Z instrument-coverage"
export RUSTDOCFLAGS="$RUSTFLAGS -Z unstable-options --persist-doctests target/debug/doctestbins"
export LLVM_PROFILE_FILE="$PWD/cov/sparse-%m.profraw"
test_log="$(cargo test --message-format=json)"
objects=$( \
  for file in \
    $( \
        echo "$test_log" \
          | grep "^{" \
          | jq -r "select(.profile.test == true) | .filenames[]" \
          | grep -v dSYM - \
    ) \
    target/debug/doctestbins/*/rust_out; \
  do \
    [[ -x $file ]] && printf "%s %s " -object $file; \
  done \
)

cargo profdata -- merge -sparse cov/sparse-*.profraw -o cov/merged.profdata
cargo cov -- show \
  $objects \
  --ignore-filename-regex='/.cargo/registry' \
  --ignore-filename-regex='^/rust' \
  --instr-profile=cov/merged.profdata --summary-only \
  --output-dir=cov/ --format=html \
  --Xdemangler=rustfilt

echo "Take a look at $PWD/cov/index.html"
