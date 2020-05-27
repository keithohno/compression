
if [ -z $1 ]; then
  echo error: please enter desired core file name
  exit
fi

redis-cli flushdb
sudo systemctl restart redis

./YCSB/bin/ycsb.sh load redis -s -P YCSB/workloads/workloada -P load_param.dat

pid=$(pgrep -f redis-server)

sudo gcore $pid
rm -rf $1
rename core.$pid $1 *
