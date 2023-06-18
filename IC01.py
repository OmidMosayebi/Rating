#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pyodbc,re
import pandas as pd
from sqlConn import reader
import time


# In[2]:


query = "SELECT * FROM [Rating].[dbo].[Rates]"


# In[3]:

df = pd.read_sql(query,reader('prod'))


# In[4]:


#df.head()


# In[5]:


df["Sequence Priority"] = df["Sequence Priority"].astype(int)


# In[6]:


query_check = "SELECT *   FROM [Rating].[dbo].[InternationalAll]"


# In[7]:


df_check = pd.read_sql(query_check,reader('uat'))



df_result = dict()
df_type = dict()
start_time = time.time()
for i,row in df.iterrows():
    df_result.update({str(row['Destination Code']):row['Price_(IRR)']})
    df_type.update({str(row['Destination Code']):row['Charging Type']})
#print(df_result)
for i,row in df_check.iterrows():
    temp=re.sub('^00','',row[3])
    while len(temp)>0:
        #print(temp in df_result)
        if temp in df_result:
            df_check.at[i,'PREFIX'] = temp
            df_check.at[i,'ACTUAL_RATE'] = df_result[temp]
            #print(df_result[temp])
            if df_type[temp] == "Per second":
                df_check.at[i,'RATE'] = round(df_check.at[i,'EVENT_AMOUNT'] / df_check.at[i,'CALL_DURATION'] * 60,0)
            else:
                if (df_check.at[i,'CALL_DURATION']%60)==0:
                    df_check.at[i,'RATE'] = round(df_check.at[i,'EVENT_AMOUNT'] / (df_check.at[i,'CALL_DURATION']//60) ,0)
                else:
                    df_check.at[i,'RATE'] = round(df_check.at[i,'EVENT_AMOUNT'] / ((df_check.at[i,'CALL_DURATION']//60) +1),0)
            break
        else:
            temp = temp[0:len(temp)-1]


# In[10]:


#df_check['RATE'] = round(df_check['EVENT_AMOUNT'] / df_check['CALL_DURATION'] * 60,0) if df['Charging Type'] == "Per second" else round(df_check['EVENT_AMOUNT'] / df_check['CALL_DURATION'])
df_check['IS_RATED_CORRECT'] = df_check['RATE'] == df_check['ACTUAL_RATE']
df_check.dropna(subset=['EVENT_AMOUNT', 'ACTUAL_RATE','RATE'],inplace=True)

# In[11]:


conn = reader('prod')
cursor = conn.cursor()
cursor.fast_executemany = True
cursor.executemany("INSERT INTO [dbo].[InternationalAll]\
           ([CALL_TYPE]\
           ,[RECORD_TYPE]\
           ,[MSISDN_NSK]\
           ,[DIALED_DIGIT]\
           ,[CALL_TIME]\
           ,[CALL_DURATION]\
           ,[CHARGED_PARTY_START_CELL_ID]\
           ,[EVENT_AMOUNT]\
           ,[DA_AMOUNT_USED]\
           ,[DA_ACCOUNT_USED]\
           ,[RATED_UNITS_FREE_UNITS]\
           ,[SERVICE_CLASS_ID]\
           ,[PAYTYPE]\
           ,[PREFIX]\
           ,[ACTUAL_RATE]\
           ,[RATE]\
           ,[IS_RATED_CORRECT]) \
         VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", 
                   list(df_check.itertuples(index=False, name=None)))

conn.commit()
conn.close()


