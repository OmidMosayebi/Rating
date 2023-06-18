#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pyodbc,re
import pandas as pd
import numpy as np
from sqlConn import reader


# In[2]:


query_select = "SELECT * FROM [dbo].[RVLA] where [LAI] is null"


# In[3]:


df_test = pd.read_sql(query_select,reader('uat'))


# In[4]:


df_test["CALL_TYPE"] = df_test["CALL_TYPE"].astype(int)
df_test["RECORD_TYPE"] = df_test["RECORD_TYPE"].astype(int)
df_test["CALL_DURATION"] = df_test["CALL_DURATION"].astype(int)
df_test["DA_ACCOUNT_USED"] = df_test["DA_ACCOUNT_USED"].astype(int)
df_test["RATED_UNITS_FREE_UNITS"] = df_test["RATED_UNITS_FREE_UNITS"].astype(int)
df_test["SERVICE_CLASS_ID"] = df_test["SERVICE_CLASS_ID"].astype(int)


# In[5]:


for i,row in df_test.iterrows():
    df_test.at[i,'ACTUAL_RATE'] = round(row[6]/((row[5]//60)+1),1) if row[5]%60 != 0 else round(row[6]/((row[5]//60)),1)
    
    temp=re.sub('^00','',str(row[3]))
    if temp[0:2] == '98':
        df_test.at[i,'EVENT_TYPE'] = 'CBH'
    else:
        df_test.at[i,'EVENT_TYPE'] = 'Intl'  


# In[6]:


df_test.drop(columns=['LAI'],inplace=True)
df_test['A_COUNTRY'] = "Other"


# In[7]:


conn = reader('prod')
cursor = conn.cursor()
for index,row in df_test.iterrows():
    try:
        cursor.execute("INSERT INTO [dbo].[RVLNA]            ([CALL_TYPE]            ,[RECORD_TYPE]            ,[MSISDN_NSK]            ,[DIALED_DIGIT]            ,[CALL_TIME]            ,[CALL_DURATION]            ,[EVENT_AMOUNT]            ,[DA_AMOUNT_USED]            ,[DA_ACCOUNT_USED]            ,[RATED_UNITS_FREE_UNITS]            ,[NET_FLAG]            ,[CHARGED_PARTY_START_CELL_ID]            ,[SERVICE_CLASS_ID]            ,[PAYTYPE]            ,[ACTUAL_RATE]            ,[EVENT_TYPE]            ,[A_COUNTRY])          VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", 
                   row[0],row[1],row[2],row[3],row[4],row[5],round(row[6],2),round(row[7],2),row[8],row[9],row[10],row[11],
                   row[12],row[13],row[14],row[15],row[16])
    except:
        continue
conn.commit()
conn.close()

# In[ ]:




