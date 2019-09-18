import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import create_engine
from pandas.io.json import json_normalize


engine = create_engine('postgresql+psycopg2://username:password@host:5432/database')

#


# Create Connection to database 
try:
    conn = psycopg2.connect(dbname='dbname', user='user' , host='host' ,password='host')
except:
    print "READ MORE"
    
cur = conn.cursor()

#query
query = 'SELECT id, emails from public.copper_person'

#dataframe from query
df=pd.read_sql_query(query,engine)

#cleaning
df.id = df.id.astype(int)
df = pd.DataFrame([dict(y, id=i) for i, x in df.values.tolist() for y in x])
df = df.drop(labels = ['category'], axis = 1)

#tuple creation
tuples = [tuple(x) for x in df.values]

#into SQL 
ex = """INSERT into static.copper_connector (email, copper_person_id) VALUES (%s, %s);"""
#execute loop
for i in range(len(tuples)):
    cur.execute(ex,tuples[i])

#commit it to SEEEEEQUEELLLL
conn.commit() 