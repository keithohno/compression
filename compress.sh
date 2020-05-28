#!/bin/bash

if [ -z $1 ]; then
  echo error: please enter core file
  exit
fi

chunk_sizes=(4K 1K 512 256)

mkdir -p $2/$1
touch $2/$1/ctimes

for cs in ${chunk_sizes[@]}; do

  startTime="$(date -u +%s)"
  lz4 $1 -B$cs $2/$1/$cs.lz4
  endTime="$(date -u +%s)"
  echo ${cs} --- $(($endTime-$startTime)) sec
  echo ${cs} --- $(($endTime-$startTime)) sec >> $2/$1/ctimes

done

mv $1 $2/$1/
