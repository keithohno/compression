#!/bin/bash

set -e

mkdir -p $2

mv -f $1 count_zeros/

cd count_zeros
cargo run --release

echo SPLIT
du -sb $1

mv $1 ..
