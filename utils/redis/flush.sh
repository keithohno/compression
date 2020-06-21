#!/bin/bash

set -e
redis-cli flushdb
sudo systemctl restart redis
sleep 1
