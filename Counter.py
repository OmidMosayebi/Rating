#!/usr/bin/env python
# coding: utf-8

# In[65]:


from sqlConn import reader
import pandas as pd
import numpy as np


# In[3]:


query1 = "select name from sys.tables"


# In[34]:


conn_sql = reader('prod')


# In[7]:


df = pd.read_sql(query1,conn_sql)


# In[108]:


list1 = []
list1.append(["DB","Count_Today"])
for i,row in df.iterrows():
    conn_sql = reader('prod')
    #print(row[0])
    try:
        query = "select count(*) from dbo."+row[0]+ " where DATE_KEY= format(getdate()-1,'yyyyMMdd')"
        cur = conn_sql.cursor()
        cur.execute(query)
        data = cur.fetchall()
        list1.append([row[0],data[0][0]])
    except Exception:
        try:
            query = "select count(*) from dbo."+row[0]+ " where CALL_TIME= format(getdate()-1,'yyyyMMdd')"
            cur = conn_sql.cursor()
            cur.execute(query)
            data = cur.fetchall()
            list1.append([row[0],data[0][0]])
        except Exception:
            pass
conn_sql.close()


# In[61]:


df2 = pd.DataFrame(list1[1:],columns=list1[0])


# In[106]:


from smtplib import SMTP
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Import the email modules we'll need
from email.message import EmailMessage

# Open the plain text file whose name is in textfile for reading.
email = 'omid.mo@mtnirancell.ir'
password = '_o6565s_'
send_to_emails = ['omid.mo@mtnirancell.ir']
subject = 'my test'
msg = MIMEMultipart()
html = """<html>
  <head></head>
  <body>
    {0}
  </body>
</html>
""".format(df2.to_html())
#html = html.replace("<table",'<table style="background-color:#FF0000" ')
part1 = MIMEText(html, 'html')
#encoders.encode_base64(part1)


server = SMTP('nbmail.mtnirancell.ir', 25)

# me == the sender's email address
# you == the recipient's email address
#msg['Subject'] = subject
#msg['From'] = email
#msg['To'] = send_to_emails
msg.attach(part1)
# Send the message via our own SMTP server.
txt = msg.as_string()
server.sendmail(email,send_to_emails,txt)
server.quit()
    


# In[ ]:




