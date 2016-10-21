import sys
import os
import re
from urlparse import unquote
from unidecode import unidecode
import pyspark as ps
from pyspark import SparkContext

'''
def clean_line(line):

    if len(line.split()) == 2:
    	key,val = line.split()
	return key,val
    else:
	return None,'0'
'''	

def clean_line(tuple):
    if ',' in tuple:
        try:
            first,second = tuple.strip().split("', ")
            key = str(first[2:])
            val = second[:-1]
            return key,val
        except:
            return None,'0'
    else:
        return None,'0'

'''
def transform(tupl):
    key = tupl[1]
    val = tupl[2]			
    return key,val
'''

def sum_func(accum,n):
    try:	
    	return accum + int(n)	
    except:
    	return accum	


def second_pass(input_dir,output_dir,sday,eday):
    print "second pass"
    rdd = sc.textFile(input_dir+'/*')
    rdd = rdd.map(clean_line)
    rdd = rdd.reduceByKey(sum_func)
    rdd = rdd.sortBy(lambda x:-1 * int(x[1]))	
    rdd.saveAsTextFile(output_dir+'/AGG'+str(sday)+'-'+str(eday)) 
    rdd.unpersist()
	

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: process_agg_days <inputdir> <outputdir> <sday> <eday>")
        exit(-1)
    else:
	input_dir = sys.argv[1]
	output_dir = sys.argv[2]
	sday = int(sys.argv[3])
	eday = int(sys.argv[4])

    SparkContext.setSystemProperty('spark.executor.memory', '128g')
    sc = ps.SparkContext('local[40]')
    second_pass(input_dir,output_dir,sday,eday)	
    sc.stop()		
