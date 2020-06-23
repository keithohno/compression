#!/bin/bash

if [ -z "$COMPRESSION_HOME" ]; then
    echo "need to set COMPRESSION_HOME"
    exit 1
fi

cd $COMPRESSION_HOME/redis-loader
cargo build --release

cd $COMPRESSION_HOME/zero-counter
cargo build --release
