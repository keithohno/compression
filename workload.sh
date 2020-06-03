#!/bin/bash

redis-cli flushdb
sudo systemctl restart redis

#./YCSB/bin/ycsb.sh load redis -s -threads 2 -P YCSB/workloads/workloada -P params.dat
./YCSB/bin/ycsb.sh load redis -s -P YCSB/workloads/workloada -P params.dat
./YCSB/bin/ycsb.sh run redis -s -threads 2 -target 40000 -P YCSB/workloads/workloada -P params.dat

pid=$(pgrep -f redis-server)

sudo gcore $pid
rm -rf $1
rename core.$pid $1 *
