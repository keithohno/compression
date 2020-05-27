#!/bin/bash

if [ -z $1 ] && [ -z $2 ]; then
  echo error: please enter core file name and proc limit
  exit
fi

chunk_sizes=(4k 1k 512 256)

rm -rf c$1
mkdir c$1

for cs in ${chunk_sizes[@]}; do

  # split core file into initial n blocks
  mkdir c$1/core_$cs
  split -n $2 $1 c$1/core_${cs}/_
  
  mkdir c$1/core_${cs}/lz4
  mkdir c$1/core_${cs}/gzip
  touch ctimes

done

split_all() {

  # split each block into chunks
  mkdir _$2
  split -b $1 $2 _$2/$2
  rm $2
}
export -f split_all

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
    gzip -c $f > ../gzip/$f.gz
  done
  cd ..
  rm -rf $1
}
export -f gzip_all

for cs in ${chunk_sizes[@]}; do

  cd c$1/core_$cs

  parallel split_all $cs ::: _[a-z]*

  startTime="$(date -u +%s)"
  parallel lz4_all ::: __*
  endTime="$(date -u +%s)"
  echo lz4 --- ${cs} --- $(($endTime-$startTime)) sec
  echo lz4 --- ${cs} --- $(($endTime-$startTime)) sec >> ../ctimes

  startTime="$(date -u +%s)"
  parallel gzip_all ::: __*
  endTime="$(date -u +%s)"
  echo gzip --- ${cs} --- $(($endTime-$startTime)) sec
  echo gzip --- ${cs} --- $(($endTime-$startTime)) sec >> ../ctimes

  cd ../..

done

mv $1 c$1/
