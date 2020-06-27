#!/bin/bash

set -e

mkdir -p $1
cd $1
rm -f core*
pid=$(pgrep -f redis-server)
sudo gcore $pid
rename core.$pid core *
