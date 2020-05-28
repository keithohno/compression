du -sb cdata/$1/r* cdata/$1/*.lz4
du -sb cdata/$1/r* cdata/$1/*.lz4 > cdata/$1/csizes
python analyze.py cdata/$1/csizes > cdata/$1/cratios
