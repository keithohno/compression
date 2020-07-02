#!/bin/bash

set -e
sleep 1
redis-cli flushdb
sudo systemctl restart redis
sleep 1
