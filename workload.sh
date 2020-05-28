
if [ -z $2 ]; then
  echo error: please enter record count and op count
  exit
fi

name=r${1}o${2}

recs=${1/M/000000}
recs=${recs/K/000}
ops=${2/M/000000}
ops=${ops/K/000}

redis-cli flushdb
sudo systemctl restart redis

./YCSB/bin/ycsb.sh load redis -s -threads 2 -P YCSB/workloads/workloada -P load_param.dat -p recordcount=$recs -p operationcount=$ops
./YCSB/bin/ycsb.sh run redis -s -threads 2 -target 40000 -P YCSB/workloads/workloada -P load_param.dat -p recordcount=$recs -p operationcount=$ops

pid=$(pgrep -f redis-server)

sudo gcore $pid
rm -rf $name
rename core.$pid $name *
