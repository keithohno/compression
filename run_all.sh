
name=r${1}o${2}

./workload.sh $1 $2
./compress.sh $name $3
./analyze.sh $name $3
./cleanup.sh $name $3
