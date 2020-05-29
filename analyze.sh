#!/bin/bash

core=$1
folder=$2
shift 2

cd $folder
du -sb $core

for f in *.lz4; do
  du -sb $f
done
