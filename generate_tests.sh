#!/usr/bin/env bash
grep -RoPh '// SH: \K.*$' ./tests | while read -r line ; do
    echo "Executing \`$line\`"
    eval "$line"
done