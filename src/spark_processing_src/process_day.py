import sys
import os
import re
from urlparse import unquote
from unidecode import unidecode
import pyspark as ps
from pyspark import SparkContext


def clean_line(line):
    '''
    lst = line.split()
    if len(lst) == 4:
        key = lst[1]
        val = lst[2]
        return key,val
    elif len(lst) > 1:
        return lst[1], '0'
    else:
        return None,'0'
    '''
    if len(line.split()) == 4:
    	label, article, count, _ = line.split()
        key = unidecode(unquote(article))
        try:
     		val = int(count)
     		return label,key,val 
	except:
		return None, None, '0'	
    else:
	return None, None, '0'
	

def check_en(tupl):
    label = tupl[0]	
    if label != 'en':
	return False
    elif label == None:
	return False	
    else:
    	return True


def transform(tupl):
    key = tupl[1]
    val = tupl[2]			
    return key,val



def sum_func(accum,n):
    try:	
    	return accum + int(n)	
    except:
    	return accum	

'''
def sort_func(x):
    try:
    	return -1*int(x[1])
    except:
    	pass
'''		
def first_pass(input_dir,output_dir,day):
    print "first pass"
    #rdd = sc.textFile(input_dir+'/day'+{}+'/*').format(str(day))
    rdd = sc.textFile(input_dir+'/day'+str(day)+'/*')
    rdd = rdd.map(clean_line).filter(check_en)
    #rdd = rdd.map(clean_line)
    #rdd = rdd.filter(check_en)	
    rdd = rdd.map(transform).reduceByKey(sum_func)
    #rdd = rdd.map(transform)
    #rdd = rdd.reduceByKey(sum_func)
    #rdd = rdd.sortBy(sort_func)	
    rdd = rdd.sortBy(lambda x:-1 * int(x[1]))	
    rdd.saveAsTextFile(output_dir+'/pday'+str(day)) 
    rdd.unpersist()
	
def second_pass():
    print "second_pass"


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: process_day <inputdir> <outputdir> <day>")
        exit(-1)
    else:
	input_dir = sys.argv[1]
	output_dir = sys.argv[2]
	day = int(sys.argv[3])
  
    sc = ps.SparkContext('local[40]')
    first_pass(input_dir,output_dir,day)	
    sc.stop()		
