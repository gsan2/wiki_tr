#!/bin/bash
cd /data/201609pageviews/output/merged_day/
start=$1
end=$2
for i in {1..30}
do
	echo $i
	cp pday$i-merged ./staged_input_month/
done

python aggregate_days.py /data/201609pageviews/output/merged_day/staged_input /data/201609pageviews/output/merged_day/staged_output 1 30


