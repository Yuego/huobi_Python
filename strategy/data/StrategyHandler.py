import sqlite3
import pandas as pd
conn = sqlite3.connect('market.db', timeout=10)
table = 'ethusdt_tick_mbp'
data = pd.read_sql('select * from ' + table, conn)
print(data)


# con = sqlite3.connect('market.db')
#
# def sql_fetch(con):
#
#     cursorObj = con.cursor()
#
#     cursorObj.execute('SELECT name from sqlite_master where type= "table"')
#
#     print(cursorObj.fetchall())
#
# sql_fetch(con)