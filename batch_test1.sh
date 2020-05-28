#!/bin/bash

rec_list=(100K 200K 400K 600K 800K 1M 2M 3M 4M)
op_list=(0M 1M 2M 4M 10M 20M)

for recs in ${rec_list[@]}; do
  for ops in ${op_list[@]}; do
    ./run_all.sh $recs $ops
  done
done
