import sqlite3
import pandas as pd

path_new_archive = r"C:\Users\Sam\FootballTrader v0.3.2\database\autotrader_data copy.db"

path = r"C:\Users\Sam\FootballTrader v0.3.2\database\autotrader_data.db"

cnx = sqlite3.connect(path_new_archive)
query = "SELECT * FROM archive"
df_new_archive = pd.read_sql_query(query, cnx)
cnx.close()
print(df_new_archive)

df_new_archive.rename(columns={'League': 'old_league'}, inplace=True)

df_new_archive['League'] = df_new_archive['Country'] + ', ' + df_new_archive['old_league']

print(df_new_archive['League'])

df_new_archive['Half FH Goals Avg'] = df_new_archive['Half FH Goals Avg'] * 100
df_new_archive['Half SH Goals Avg'] = df_new_archive['Half SH Goals Avg'] * 100

print(df_new_archive['Half FH Goals Avg'])

print(df_new_archive['Half FH Goals Avg'])

cnx = sqlite3.connect(path)
df_new_archive.to_sql(name='archive_v2', con=cnx, if_exists='replace')
cnx.close()