#!/bin/bash
set -e

for arg in "$@"; do
    cd $arg
    cargo build --release
done
