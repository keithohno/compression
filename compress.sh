#!/bin/bash

chunk_sizes=(256 512 1k 4k)

rm -rf c$1
mkdir c$1

for cs in ${chunk_sizes[@]}; do

  # split core file into initial n blocks
  mkdir c$1/core_${cs}
  split -n $2 $1 c$1/core_${cs}/_
  cd c$1/core_${cs}

  # split blocks into chunks
  for f in _[a-z]*; do
    mkdir _$f
    split -b $cs $f _$f/$f
    rm $f
  done

  mkdir lz4
  mkdir gzip
  cd ../..
done

lz4_all() {

  # lz4 compress each chunk
  cd $1
  for f in *; do
    lz4 -q $f ../lz4/$f
  done
  cd ..
}
export -f lz4_all

gzip_all() {

  # gzip compress each chunk
  cd $1
  for f in *; do
    gzip $f > ../gzip/$f.gz
  done
  cd ..
  rm -rf $1
}
export -f gzip_all

for cs in ${chunk_sizes[@]}; do

  cd c$1/core_${cs}

  startTime="$(date -u +%s)"
  parallel lz4_all ::: __*
  endTime="$(date -u +%s)"
  echo lz4 --- ${cs} --- $(($endTime-$startTime)) sec

  startTime="$(date -u +%s)"
  parallel gzip_all ::: __*
  endTime="$(date -u +%s)"
  echo gzip --- ${cs} --- $(($endTime-$startTime)) sec

  cd ../..

done

