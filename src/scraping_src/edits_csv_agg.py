 # -*- coding: utf-8 -*-
import csv
import os
from bs4 import BeautifulSoup
import re
import glob
from unidecode import unidecode

path = "./Agg_scrapped_articles_dir/"
records = []
#dirs = os.listdir( path )
outf =open('pageedits_agg_subset.txt','w')
#for fname in dirs:
for fname in glob.glob('./Agg_scrapped_articles_dir/*'):
    #for fname in glob.glob(path):
    if "\\" in fname:
        continue
    with open(fname, 'r') as page:
        #page = open(fname,'r')
        soup = BeautifulSoup(page.read(), 'html.parser')
        txt =  soup.select('.table-condensed.xt-table .tdtop1')
        #txt =  soup.select('.table-condensed.xt-table')
        edit_list = []
        name = fname.split('/')[-1]
        real_name = name[:-5]
        edit_list.append(real_name)
        lst = [0,2,3,4,8,9,10,11,12,13,14,15]
        flag  = 0
        for item in lst:
            try:
                edit_list.append(txt[item].text)
            except:
                flag  = 1
        if flag == 0:
            outf.write('\t'.join(edit_list))
            outf.write('\n')
