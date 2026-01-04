import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
import numpy as np

path = r"C:\Users\Sam\FootballTrader v0.3.3\database\autotrader_data.db"
cnx = sqlite3.connect(path, check_same_thread=False)
df = pd.read_sql_query("SELECT * from archive_v3", cnx)
df = df.loc[df['strategy'] == 'LTD60'].copy()
stake = 200

for rows in df.iterrows():
    if df.loc[rows[0], ['d_SP']].values > 4.0:
        df.loc[rows[0], ['d_SP']] = 4.0

    print(df.loc[rows[0], ['result', 'pnl']])
    if df.loc[rows[0], ['result']].values == 1:
        df.loc[rows[0], ['pnl']] = stake
    else:
        liability = (df.loc[rows[0], ['d_SP']].values - 1) * stake
        df.loc[rows[0], ['pnl']] = 0 - liability

# df = df[df['d_SP'] <= 4.5]
df['cum_pnl'] = df['pnl'].cumsum().copy()
print(df[['event_id', 'result', 'pnl', 'cum_pnl']])
df.reset_index(inplace=True)
df['cum_pnl'].plot()
plt.show()
