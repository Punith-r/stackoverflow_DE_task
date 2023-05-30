#!/usr/bin/env python
# coding: utf-8

# In[351]:


import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
  
    
def get_tag(row):
    try:
        tag=row.a.text
    except:
        tag=np.nan
    return tag

def get_querys_cur_year(row):
    div = row.find('div' ,class_='flex--item s-anchors s-anchors__inherit')
    try:
        curr_query = div.a.text.split()[0]
    except:
        curr_query=0
    return curr_query

def get_total_query(row):
    div=i.find(class_='mt-auto d-flex jc-space-between fs-caption fc-black-400')
    try:
        total = div.find_all(class_='flex--item')[0].text.split()[0]
    except:
        total = 0
    return total

def get_href(row):
    try:
        URL = "https://stackapps.com/"
        href = row.find(class_='post-tag').get('href')
        link = URL+href
    except:
        link=np.nan
    return link


URL = "https://stackapps.com/"

r = requests.get(URL)
soup = BeautifulSoup(r.content, 'html.parser')

nav_tag = soup.find_all('a', attrs={'id':"nav-tags"})
tag_link = nav_tag[0].get('href')
tags_url = URL+tag_link
# print(tags_url)

tags_data= requests.get(tags_url)
tags=BeautifulSoup(tags_data.content, 'html.parser')

d={'tag':[],'total_ques':[],'ques_curr_year':[],'link':[]}

for i in tags.find_all(class_='grid--item s-card js-tag-cell d-flex fd-column'):
    d['tag'].append(get_tag(i))
    d['ques_curr_year'].append(get_querys_cur_year(i))
    d['total_ques'].append(get_total_query(i))
    d['link'].append(get_href(i))

df=pd.DataFrame.from_dict(d)

print(df)


# In[339]:


import pandas as pd
import psycopg2  as pg2


# In[340]:


conn=pg2.connect(database ='postgres',user='postgres',password='root')
cur=conn.cursor()
cur.execute("create database stackoverflow")
conn=pg2.connect(database ='stackoverflow',user='postgres',password='root')
cur=conn.cursor()
try: 
    
    cur.execute("CREATE TABLE IF NOT EXISTS trending_tags (tag varchar primary key, ques_curr_year bigint,total_ques bigint, link varchar(255));")
except pg2.Error as e: 
    print("Error: Issue creating table")
    print (e)


# In[341]:


trending_tags_insert=("INSERT INTO trending_tags ( tag, ques_curr_year, total_ques, link) VALUES ( %s, %s, %s, %s)")
for i,row in df.iterrows():
    cur.execute(trending_tags_insert,list(row))
conn.commit()


# In[345]:


cur.execute("select * from trending_tags order by ques_curr_year desc,total_ques desc  ")
cur.fetchone()


# In[ ]:




