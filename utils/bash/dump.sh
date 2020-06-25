#!/bin/bash

set -e

cd $1
rm -f core*
pid=$(pgrep -f redis-server)
sudo gcore $pid
rename core.$pid core *
