#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import re,odbc
from sqlConn import reader


# In[2]:


query = "SELECT * from [dbo].[sms_int]"


# In[3]:


df_test = pd.read_sql_query(query,reader('uat'))


# In[4]:


query2 = "SELECT * from [dbo].[SMS_Int_rate]"
df_rate = pd.read_sql_query(query2,reader('prod'))


# In[5]:


df_test['SERVICE_CLASS_ID']=df_test['SERVICE_CLASS_ID'].astype(int)


# In[6]:


rates = {}
countries={}
for i,row in df_rate.iterrows():
    rates.update({row[0]:row[1]})
    countries.update({row[0]:row[2]})


# In[7]:


for i,row in df_test.iterrows():
    temp = re.sub('^00','',row[0])
    while len(temp)>0:
        if temp in rates:
            df_test.at[i,'PREFIX'] = temp
            df_test.at[i,'RATE'] = rates[temp]
            df_test.at[i,'COUNTRY'] = countries[temp]
            break
        else:
            temp = temp[:len(temp)-1]
            df_test.at[i,'PREFIX'] = '0'
            df_test.at[i,'RATE'] = np.float(0)
            df_test.at[i,'COUNTRY'] = np.str('NULL')
df_test['PREFIX']=df_test['PREFIX'].astype(int)
df_test['PREFIX']=df_test['PREFIX'].astype(str)


# In[8]:


conn = reader('prod')
cursor = conn.cursor()
cursor.fast_executemany = True
cursor.executemany(
    "INSERT INTO [dbo].[sms_int]\
           ([DIALED_DIGIT]\
           ,[CALL_TIME]\
           ,[EVENT_AMOUNT]\
           ,[SERVICE_CLASS_ID]\
           ,[PAYTYPE]\
           ,[PREFIX],[RATE],[COUNTRY]) \
           VALUES (?, ?,?,?,?,?,?,?)",
         list(df_test.itertuples(index=False, name=None)))
conn.commit()
conn.close()

