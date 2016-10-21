#!/bin/bash
cd /data/201609pageviews/output
for d in * 
do 
	echo $d
	cd $d
        cp $d-merged /data/201609pageviews/output/merged_day
	echo $d-merged
	cd ..
done
