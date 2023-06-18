#!/usr/bin/env python
# coding: utf-8

# In[1]:


import re,pyodbc
from sqlConn import reader
import pandas as pd
import numpy as np


# In[2]:


query = "SELECT * from [dbo].[gprs_MA]"


# In[3]:


df_test = pd.read_sql_query(query,reader('uat'))


# In[4]:


query_rate = "SELECT * from [dbo].[RoamingRates_clean]"


# In[5]:


df_rate = pd.read_sql_query(query_rate,reader('prod'))


# In[8]:


rates = {}
for i,row in df_rate.iterrows():
    rates.update({row[4]:row[12]})


# In[54]:

for i,row in df_test.iterrows():
    row[1] = row[1][:5]
    df_test.at[i,'LAI'] = row[1]
    if int(row[1]) in rates:
        df_test.at[i,'RATE_COUNTRY'] = rates[int(row[1])]  
    else:
        df_test.at[i,'RATE_COUNTRY'] = np.nan
    #if row[4] - row[6]<100:
        #df_test.at[i,'IS_RATED_CORRECT'] = True
    #else:
        #df_test.at[i,'IS_RATED_CORRECT'] = False
df_test['LAI'] = df_test['LAI'].astype(int)
df_test['IS_RATED_CORRECT'] = df_test['RATE_COUNTRY'] - df_test['RATE']<10
# In[57]:

df_test['DATE_KEY'] = df_test['DATE_KEY'].astype(float)
df_test['DATE_KEY'] = df_test['DATE_KEY'].astype(int)
df_test['DATE_KEY'] = df_test['DATE_KEY'].astype(str)
df_test = df_test.drop(columns=['CELL_ID'])
df_test['RATE_COUNTRY'] = df_test['RATE_COUNTRY'].fillna(0)

# In[59]:


conn = reader('prod')
cursor = conn.cursor()
cursor.fast_executemany = True
cursor.executemany(
    "INSERT INTO [dbo].[gprs_roam_ma] \
           ([DATE_KEY] \
           ,[SESSION_COST] \
           ,[RATED_UNITS_DEBIT] \
           ,[RATE] \
           ,[LAI] \
           ,[RATE_COUNTRY] \
           ,[IS_RATED_CORRECT]) \
           VALUES (?, ?,?,?,?,?,?)",
    list(df_test.itertuples(index=False, name=None)))
conn.commit()
conn.close()


# In[ ]:




