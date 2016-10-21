import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import pylab as pl
import seaborn as sns

#load page_views and page_edits
df_edits = pd.read_csv('pageedits_agg_subset.txt', sep="\t", header=None,
                       names=["article","article_id","page_size","num_revisions",
                              "num_editors","avg_time_bet_edits","avg_edits_user"
                               ,"avg_edits_month","avg_edits_years",
                               "edits_day","edits_week","edits_month","edits_year"])

df_views = pd.read_csv('part-00000',sep=",", header=None, names=["article","page_views"])

#cleaning
df_views['article'] = df_views['article'].map(lambda x: x.strip("('"))
df_views['page_views'] = df_views['page_views'].map(lambda x: x.strip("u')"))
df_views['page_views'] = df_views['page_views'].map(lambda x: x[3:])

# join page_views and page_ids based on article name
df_edit_views = df_edits.merge(df_views)

#more cleaning
df_edit_views['edits_month'] = df_edit_views['edits_month'].map(lambda x: str(x).replace(',',''))
df_edit_views2 = df_edit_views[(df_edit_views['edits_month'].notnull()) & (df_edit_views['edits_month'] != 'nan')]
df_edit_views3 = df_edit_views2[df_edit_views['page_views'].notnull()]
df_edit_views4 = df_edit_views3[(df_edit_views3['page_views'] > 0) & (df_edit_views3['page_views'] != 'nan')]
df_edit_views4['edits_month'] = df_edit_views4['edits_month'].astype(int)

def remove_strings(x):
        try:
            return int(x)
        except:
            return 0


df_edit_views4['page_views'] = df_edit_views4['page_views'].apply(remove_strings)
df_edit_views4 = df_edit_views4[df_edit_views4['page_views'] > 0]
df_edit_views4['page_views'] = df_edit_views4['page_views'].astype(int)

# calculate wr_ratio and sort
df_edit_views4['wr_ratio'] = df_edit_views4['edits_month'].astype(int)/df_edit_views4['page_views'].astype(int)
df_edit_views5 = df_edit_views4.sort_values(["wr_ratio"],ascending=False)
df_edit_views5_unique = df_edit_views5.drop_duplicates(['article_id'])


#plot
num = len(df_edit_views5_unique)
x = np.linspace(1, num, num)

fig = plt.figure(figsize=(15, 10))
plt.xlim([0,10470])
#plt.yscale('log')
#plt.xscale('log')
plt.ylabel('Edit/Read Ratio')
y = df_edit_views5_unique['wr_ratio']
labels = []
for i in df_edit_views5_unique['wr_ratio']:
    if i > 0.2:
        labels.append(0)
    elif i > 0.0001:
        labels.append(1)
    else:
        labels.append(2)

s =[100*i for i in df_edit_views5_unique['wr_ratio']]
#pl.scatter(x,y)
plt.scatter(x,y,s=s)
#plt.scatter(x,y,color = (x,np.array(labels)),s=s)
plt.savefig('edit_reads_log_plot.pdf')



