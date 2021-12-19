# -*- coding: utf-8 -*-
"""
Created on Sun May  9 14:59:07 2021

@author: ivaju
"""
import sqlite3
import config
from sqlite3 import Error

import psycopg2
import psycopg2.extras

#get tickers from local sqlite db to test, if it works move data to postgress

def create_connection(dbfile):
    conn= None
    
    try:
        conn = sqlite3.connect(dbfile)
    except Error as e:
        print(e)
    
    return conn

conn = create_connection('C:\\Users\\ivaju\\Documents\\9. work moved\\Trading\\Database\\product.sqlite')
cursor = conn.cursor()
cursor.execute('select * from listedShare')
rows = cursor.fetchall()


conn_p = psycopg2.connect(host=config.DB_HOST, database=config.DB_NAME, user=config.DB_USER, password=config.DB_PASS)
cur_p = conn_p.cursor(cursor_factory=psycopg2.extras.DictCursor)


for row in rows:
    if row[1] is None:
        print(f'{row[0]} is missing a name')
        continue
    cur_p.execute('''
                  INSERT INTO stock (name, symbol, exchange, is_etf)
                  VALUES (%s,%s,'NYSE',false)
                  ''',(row[1],row[0]))
                  
conn_p.commit()
    