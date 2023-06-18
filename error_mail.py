#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pyodbc,re,os
import pandas as pd
from sqlConn import reader
import cx_Oracle
import numpy as np
import sqlConn as sqlConn
from datetime import datetime,timedelta


# In[2]:


query = "select * from [dbo].[InternationalAll] where CALL_TIME = "+re.sub('-','',str(datetime.date(datetime.now()-timedelta(days=1))))+" and IS_RATED_CORRECT=0"


# In[3]:


conn_sql = sqlConn.reader('prod')


# In[4]:


df = pd.read_sql(query,conn_sql)


# In[5]:


def conn(db):
    user = 'omid_mo'
    with open('pass.txt','r') as tp:
        passw = tp.readline()
    connection = cx_Oracle.connect(user, passw, db)
    return connection
if df.empty:

    with open('miscdr.xlsx', 'w') as fp: 
        pass
    quit()
else:
    queries = []
    for i,row in df.iterrows():
        sub_part = row[2][-1]
        msisdn_nsk = row[2]
        dialed_digit = row[3]
        call_time = row[4]
        record_type = 10
        event_amount = round(row[7],0)
        queries.append(str.format("select /*+ parallel(c,5) */ *             from STG_CDR.CS5_CCN_VOICE_MA_IR subpartition for ({},{})             where RECORD_TYPE={} and MSISDN_NSK = {} and DIALED_DIGIT={} and round(EVENT_AMOUNT,0) = {}",
                      call_time,sub_part,record_type,msisdn_nsk,dialed_digit,event_amount))
    conc = conn("dru105:1521/ODS1P")
    cursor = conc.cursor()
    result_set = []
    for q in queries:
        cursor.execute(q)
        result_set.append(np.array(cursor.fetchone()))
    conc.close()
    df_res = pd.DataFrame(result_set,columns=['DW_FILE_ID','BATCH_ID','DW_FILE_ROW_NUMBER','CALL_TYPE','RECORD_TYPE','CHRONO_NUMBER','CHARGE_PARTY_NUMBER','IMSI_NUMBER','CALLED_CALLING_NUMBER','CALL_FORWARD_FLAG','CALL_FORWARD_NUMBER','DIALED_DIGIT','INCOMING_TRUNK','OUTGOING_TRUNK','CALL_TIME','CALL_TIMESTAMP','CALL_DURATION','FAX_DATA_VOICE_SMS_FLAG','HOT_BILLING_FLAG','IMEI_NUMBER','CHARGED_PARTY_START_CELL_ID','TELE_SERVICE_NUMBER','BEARER_NUMBER','TON_OF_CALLED_CALLING_NUMBER','NP_OF_CALLED_NUMBER','INTERMEDIATE_CALL_TYPE','CALL_SEQUENCE_NUMBER','MSC_ID','BSC_ID','IN_FLAG','CAMEL_RECORD_DATE_AND_TIME','CAMEL_DURATION','IN_SERVICE_KEY','SCF_ADDRESS','LEVEL_OF_CAMEL_SERVICE','MCR_DESTINATION_NUMBER','MSC_ADDRESS','CALL_REFERENCE_NUMBER','CAMEL_INIT_CF_INDICATOR','DEFAULT_CALL_HANDLING','CHANGE_FLAGS','DATA_VOLUME','CDR_TYPE','RING_DURATION','CHARGING_PARTY_END_CELL_ID','OTHER_PARTY_START_CELL_ID','OTHER_PARTY_END_CELL_ID','BALANCE_BEFORE_EVENT','BALANCE_AFTER_THE_EVENT','EVENT_AMOUNT','DA_VALUE_BEFORE_CALL','DA_VALUE_AFTER_CALL','DA_ID','DA_AMOUNT_USED','DA_ACCOUNT_USED','FRIENDS_AND_FAMILY_IND','NET_FLAG','PEAK_FLAG','LAI','ZONE_TYPE','ZONE_ID_1','ZONE_ID_2','ZONE_ID_3','COMMUNITY_INDICATOR_SELECTED','FRIENDS_AND_FAMILY_NUMBER','RATED_UNITS_FREE_UNITS','RATED_UNITS_DEBIT','RATED_UNITS_CREDIT','OFFER_ID','OFFER_TYPE','BONUS_AMOUNT','ACCOUNT_GROUP_ID','PAM_SERVICE_ID','PAM_CLASS_ID','SELECTION_TREE_TYPE','SERVED_ACCOUNT','SERVED_OFFERINGS','TERMINATION_CAUSE','CHARGING_CONTEXT_ID','SERVICE_CONTEXT_ID','SERVICE_SESSION_ID','DATE_KEY','MSISDN_NSK','CREATE_DT','SOURCE_FILE_NM','FLEX_1_TXT','FLEX_2_TXT','FLEX_3_TXT','FLEX_4_TXT','FLEX_5_TXT','DW_SUBPART','SERVICE_CLASS_ID','PAYTYPE','CALL_REFERENCE_ID','MAIN_AMOUNT_USE','SERVICE_IDENTIFIER','MNP_PORTIN','MNP_RN','MNP_RESULT','RESERVED1','RESERVED2','RESERVED3'])
    df_res.to_excel('miscdr.xlsx','w')


# In[6]:





# In[ ]:




