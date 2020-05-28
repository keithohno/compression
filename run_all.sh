
name=r${1}o${2}

./workload.sh $1 $2
./compress.sh $name
./analyze.sh $name
./cleanup.sh $name
