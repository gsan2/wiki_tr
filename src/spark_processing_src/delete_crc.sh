#!/bin/bash
cd output
for d in * 
do 
	echo $d
	cd $d
        rm .*.crc
	cd ..
done
