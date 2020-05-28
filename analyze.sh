du -sb $2/$1/r* $2/$1/*.lz4
du -sb $2/$1/r* $2/$1/*.lz4 > $2/$1/csizes
python analyze.py $2/$1/csizes > $2/$1/cratios
