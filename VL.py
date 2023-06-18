#!/usr/bin/env python
# coding: utf-8

# In[3]:


import re,pyodbc
import numpy as np
import pandas as pd
from sqlConn import reader


# In[2]:


query = "SELECT * FROM [Rating].[dbo].[VoiceLocalAll]"


# In[13]:


df = pd.read_sql_query(query,reader('uat'))


# In[25]:


df.head()


# In[26]:


df['DA_ID'].fillna(0,inplace=True)
df['RATE'] = df['SUM_AMOUNT']/df['SUM_DURATION']*60
df['DA_ID'] = df['DA_ID'].astype(int)


# In[24]:


for i,row in df.iterrows():
    if row[4]==1002:
        if row[3] in (0,9) and row[2]<2:
            df.at[i,'NORMAL_RATE'] = True
        else:
            df.at[i,'NORMAL_RATE'] = False
    elif row[4]==8000:
        if row[3] in (40,43) and row[2]<2:
            df.at[i,'NORMAL_RATE'] = True
        else:
            df.at[i,'NORMAL_RATE'] = False
    else:
        df.at[i,'NORMAL_RATE'] = False


# In[29]:


conn = reader('prod')
cursor = conn.cursor()
cursor.fast_executemany = True
cursor.executemany(
    "INSERT INTO [dbo].[VoiceLocalAll]\
           ([CALL_TIME],\
            [NET_FLAG]\
           ,[DA_ACCOUNT_USED]\
           ,[DA_ID]\
           ,[SERVICE_CLASS_ID]\
           ,[SUM_AMOUNT]\
           ,[SUM_DURATION]\
           ,[RATE]\
           ,[NORMAL_RATE])\
           VALUES (?,?,?,?,?,?,?,?,?)",
    list(df.itertuples(index=False, name=None)))
conn.commit()
conn.close()


# In[ ]:




