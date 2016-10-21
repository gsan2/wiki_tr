#!/bin/bash
cd /data/201609pageviews/output
for d in * 
do 
	echo $d
	cd $d
	cat part-* > $d-merged
	echo $d-merged
	cd ..
done
