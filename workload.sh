#!/bin/bash

set -e

redis-cli flushdb
sudo systemctl restart redis

cd redis-loader
cargo run --release
cd ..

pid=$(pgrep -f redis-server)

sudo gcore $pid
rm -rf $1
rename core.$pid $1 *

redis-cli flushdb
sudo systemctl restart redis
