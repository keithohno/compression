#!/bin/bash

core=$1
folder=$2
shift 2
mkdir -p $folder

for cs in $@; do
  echo compressing at chunk size $cs ...
  lz4 -q -B$cs $core $folder/$cs.lz4
done

mv $core $folder/
