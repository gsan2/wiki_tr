from urllib2 import urlopen
#from bs4 import BeautifulSoup
import requests
import time
import os
from urlparse import unquote
from unidecode import unidecode

f = open("/home/ubuntu/pageedits/SPARK_AGG_VIEWS/Month/part-00000",'r')
lines = f.readlines()
article_list = []
not_found_list = []

for line in lines:
    line = line.strip().split(',')
    if line[0]:
        #article_list.append(line[0][3:-1])
        article = line[0][2:-1]
        article = article.replace('/',"")
        article_list.append(article)

cnt = 0

for article in article_list:
    r = requests.get("https://tools.wmflabs.org/xtools-articleinfo/?article=%s&project=en.wikipedia.org"%(article))
    if r.status_code == 200:
        try:
            with open('./Agg_scrapped_articles_dir/'+article+'.html','w') as outfile:
            outfile.write(unicode(r.content, errors='ignore'))
            cnt += 1
        except IOError:
            pass
    elif r.status_code == 404:
        not_found_list.append(article)
        time.sleep(1)
        if cnt == 100000:
        break


with open('./Agg_scrapped_articles_dir/'+'not_found.txt','w') as outfile2:
    outfile2.write(article)
    outfile2.write("\n".join(not_found_list))

