#!/bin/bash

set -e

core=$1
folder=$2
shift 2

mkdir -p $folder/lz4

echo SPLIT $core
du -sb $core

for cs in $@; do
  echo SPLIT lz4 $cs
  lz4 -q -f -B$cs $core $folder/$cs.lz4
  du -sb $folder/$cs.lz4
  rm $folder/$cs.lz4
done

rm -f $core
