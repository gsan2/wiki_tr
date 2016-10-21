#!/bin/sh
cd /data/201609pageviews/output/$1/_temporary
rm -rf *
cd /data/201609pageviews/output/$1/
rmdir _temporary
cd /data/201609pageviews/output/
rmdir $1



