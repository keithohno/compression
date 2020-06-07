#!/bin/bash

set -e

core=$1
folder=$2
shift 2

mkdir -p $folder/zstd
rm -rf $folder/junk
mkdir -p $folder/junk
split -n 6 $core $folder/junk/

echo SPLIT $core
du -sb $core

zstd_all() {
  mkdir _$2
  split -n 20 $2 _$2/
  for f in _$2/*; do
    zstd -b1 -B$1 -i0 $f
  done
}
export -f zstd_all

for cs in $@; do
  echo SPLIT zstd $cs
  cd $folder/junk*
  parallel zstd_all $cs ::: *
  cd -
done

rm -rf $folder/junk
