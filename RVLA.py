#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pyodbc,re
import pandas as pd
import numpy as np
from sqlConn import reader


# In[2]:


query = "SELECT * FROM [dbo].[RoamingRates_clean]"


# In[3]:


df_rates = pd.read_sql(query,reader('prod'))


# In[4]:


query_select = "SELECT * FROM [dbo].[RVLA] where [LAI] is not null and CALL_TYPE in (800,001)"


# In[5]:


df_test = pd.read_sql(query_select,reader('uat'))

# In[6]:


df_test["CALL_TYPE"] = df_test["CALL_TYPE"].astype(int)
df_test["RECORD_TYPE"] = df_test["RECORD_TYPE"].astype(int)
df_test["CALL_DURATION"] = df_test["CALL_DURATION"].astype(int)
df_test["DA_ACCOUNT_USED"] = df_test["DA_ACCOUNT_USED"].astype(int)
df_test["RATED_UNITS_FREE_UNITS"] = df_test["RATED_UNITS_FREE_UNITS"].astype(int)
df_test["LAI"] = df_test["LAI"].astype(int)
df_test["SERVICE_CLASS_ID"] = df_test["SERVICE_CLASS_ID"].astype(int)


# In[7]:

codes = {}
LAI_code = {}
local = {}
intl = {}
CBH = {}
for i,row in df_rates.iterrows():
    if row['CC'] not in codes:
        codes.update({row['CC']:row['English Name']})
    if row['MCC-MNC'] not in LAI_code:
        LAI_code.update({row['MCC-MNC']:row['English Name']})
    local.update({row['MCC-MNC']:row['Local call (IRR)']})
    intl.update({row['MCC-MNC']:row['International']})
    CBH.update({row['MCC-MNC']:row['Call Back Home (IRR)']})

# In[8]:


for i,row in df_test.iterrows():
    df_test.at[i,'ACTUAL_RATE'] = round(row[6]/((row[5]//60)+1),1) if row[5]%60 != 0 else round(row[6]/((row[5]//60)),1)
    
    temp=re.sub('^00','',row[3])
    lai = row[11]
    if lai in LAI_code.keys():
            df_test.at[i,'A_COUNTRY'] = LAI_code[lai]
    if temp[0:2] == '98':
        df_test.at[i,'EVENT_TYPE'] = 'CBH'
        if lai in CBH.keys():
            df_test.at[i,'RATE'] = CBH[lai]


    else:
        while len(temp)>0:
            #print(row[11])
        #print(temp in df_result)
            try:
                if int(temp) in codes.keys():
                    if lai in LAI_code.keys():
                        if codes[int(temp)] == LAI_code[lai]:
                            df_test.at[i,'EVENT_TYPE'] = 'Local'
                            df_test.at[i,'RATE'] = local[lai]
                            break
                        else:
                            df_test.at[i,'EVENT_TYPE'] = 'Intl'
                            df_test.at[i,'RATE'] = intl[lai]
                            break
                    else:
                        df_test.at[i,'EVENT_TYPE'] = 'Other'
                        break
            #print(df_result[temp])
                else:
                    temp = temp[0:len(temp)-1]
            except ValueError:
                temp = temp[0:len(temp)-1]
            

# In[9]:


df_test['IS_RATED_CORRECT'] = df_test['RATE'] == df_test['ACTUAL_RATE']

df_test = df_test[df_test['RATE'].notnull()]


# In[10]:


conn = reader('prod')
cursor = conn.cursor()
for index,row in df_test.iterrows():
    try:
        cursor.execute("INSERT INTO [dbo].[RVLA] \
           ([CALL_TYPE] \
           ,[RECORD_TYPE]\
           ,[MSISDN_NSK]\
           ,[DIALED_DIGIT]\
           ,[CALL_TIME]\
           ,[CALL_DURATION]\
           ,[EVENT_AMOUNT]\
           ,[DA_AMOUNT_USED]\
           ,[DA_ACCOUNT_USED]\
           ,[RATED_UNITS_FREE_UNITS]\
           ,[NET_FLAG]\
           ,[LAI]\
           ,[CHARGED_PARTY_START_CELL_ID]\
           ,[SERVICE_CLASS_ID]\
           ,[PAYTYPE]\
           ,[EVENT_TYPE]\
           ,[RATE]\
           ,[ACTUAL_RATE]\
           ,[IS_RATED_CORRECT]\
           ,[A_COUNTRY]) \
         VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", 
                   row[0],row[1],row[2],row[3],row[4],row[5],round(row[6],2),round(row[7],2),row[8],row[9],row[10],row[11],
                   row[12],row[13],row[14],row[17],round(row[18],2),round(row[15],2),row[19],row[16])
    except:
        continue
conn.commit()
conn.close()
