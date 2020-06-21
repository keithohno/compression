#!/bin/bash

if [ -z "$COMPRESSION_HOME" ]; then
    echo "need to set COMPRESSION_HOME"
    exit 1
fi

cd $COMPRESSION_HOME/out
rm -f core*
set -e
pid=$(pgrep -f redis-server)
sudo gcore $pid
rename core.$pid core *
