# imports
import sqlite3
import time
import logging
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
from inputimeout import inputimeout, TimeoutOccurred
from itertools import product
from progress.bar import Bar
import matplotlib.pyplot as plt

import betfairlightweight
import numpy as np
import pandas as pd
from betfairlightweight import filters
import pythoncom
import win32com.client

# Connect to Betfair API
api = betfairlightweight.APIClient('smudge2049', 'Dex17@£141117', '4oAYsDJiYA7P5Wej')
api.login_interactive()

# Paths to databases
autotrader_db_path = r'C:\Users\Sam\FootballTrader v0.3.2\database\autotrader_data.db'

class MatchFinder:
    def __init__(self, hours=12, continuos='off'):
        print('\nMatchFinder')
        # Market projection list to grab data required.
        self.market_projection_list = ['COMPETITION', 'EVENT', 'MARKET_START_TIME',
                                       'RUNNER_DESCRIPTION']
        # Variable placeholders.
        self.df = None
        self.df_sheets = None

        # Dictionary to select market depending on strategy
        self.strategy_market = {'LTD': 'MATCH_ODDS', 'O45': 'OVER_UNDER_45'}

        # Dictionary to select if the strategy should be live or paper
        self.live_select = None

        # Set data types for data frames.
        self.col_dtypes = {}

        if continuos == 'off':
            # Set hours market search time range.
            self.hours = int(input('Select hours to search upto: '))
        if continuos == 'on':
            self.hours = hours

    def get_betfair_details(self):
        """
        Query Betfair's market catalogue api for all football matches within the time range given. Saves all data to a
        df and sqlite database. This data will then later be used to grab additional required stats. The data includes 
        team information, market and selection id's and market start times.
        :return:
        """
        print('\nGetting matches from Betfair...')
        # Time range variables.
        MarketStartTime = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
        MarketEndTime = (datetime.now() + timedelta(hours=self.hours))
        MarketEndTime = MarketEndTime.strftime('%Y-%m-%dT%H:%M:%SZ')
        
        # Filter for markets and time range required.
        cat_filter = betfairlightweight.filters.market_filter(event_type_ids=[1],
                                                              market_type_codes=['MATCH_ODDS', 'OVER_UNDER_45'],
                                                              in_play_only=False,
                                                              market_start_time={"from": MarketStartTime,
                                                                                 "to": MarketEndTime}
                                                              )
        # API call to Betfair Market Catalogue.
        print(cat_filter)
        catalogue = api.betting.list_market_catalogue(filter=cat_filter,
                                                    market_projection=self.market_projection_list,
                                                    max_results=200,
                                                    lightweight=True)
        
        # Save data to data frame.
        self.df = pd.DataFrame(catalogue)

        # Extract required data
        
        self.df['home_team'] = [d[0].get('runnerName') for d in self.df['runners']]
        # self.df['home_odds_id'] = [d[0].get('selectionId') for d in self.df['runners']]
        self.df['away_team'] = [d[1].get('runnerName') for d in self.df['runners']]
        # self.df['away_odds_id'] = [d[1].get('selectionId') for d in self.df['runners']]
        self.df['event_name'] = [d.get('name') for d in self.df['event']]
        self.df['event_id'] = [d.get('id') for d in self.df['event']]
        self.df['marketStartTime'] = pd.to_datetime(self.df['marketStartTime'])
        self.df['start_date'] = self.df['marketStartTime'].dt.strftime('%d.%m.%Y')
        self.df['start_time'] = self.df['marketStartTime'].dt.strftime('%H:%M')
        self.df['comp'] = [d.get('name') for d in self.df['competition']]

        print(self.df)

        market_df = self.df[['event_id', 'marketName', 'marketId']]

        # Pivot the market IDs into separate columns based on market name
        pivot_df = market_df.pivot(index='event_id', columns='marketName', values='marketId').reset_index()
        pivot_df.columns.name = None  # remove hierarchy on columns

        # Rename the columns for clarity
        pivot_df = pivot_df.rename(columns={
            'Match Odds': 'marketID_match_odds',
            'Over/Under 4.5 Goals': 'marketID_overunder45'
        })
        print(pivot_df)

        # Drop duplicate event_ids in the original dataframe (keep first occurrence)
        merged_df = self.df.sort_values('marketName', ascending=True).drop_duplicates(subset='event_id', keep='first')

        # Merge with the pivoted market ID data
        self.df = pd.merge(merged_df, pivot_df, on='event_id', how='left')

        # Clean data
        self.df.dropna(inplace=True)
        self.df.drop(columns=['runners', 'event', 'competition', 'marketName', 'marketId'], inplace=True)

        # Save data to sqlite database.
        cnx = sqlite3.connect(autotrader_db_path, check_same_thread=False)
        self.df.to_sql(name='betfair_matches', con=cnx, if_exists='replace')
        cnx.close()

        # Placeholder for merged dataframes
        self.merge_data_match_odds = None
        
        print(self.df)

    def get_daily_sheets(self):
        print('\nQuerying Daily Sheets for stats...')
        ###############################################
        self.df_sheets.columns = self.df_sheets.iloc[0]

        self.df_sheets = self.df_sheets.drop(self.df_sheets.index[0])

        self.df_sheets.rename(columns={'Home Team': 'home_team'}, inplace=True)
        self.df_sheets.rename(columns={'Away Team': 'away_team'}, inplace=True)

        self.df_sheets['event'] = self.df_sheets['home_team'] + ' v ' + self.df_sheets['away_team']

        self.df_sheets.to_excel('daily_sheet_stats.xlsx', engine='openpyxl')
        cnx = sqlite3.connect(autotrader_db_path, check_same_thread=False)
        self.df_sheets.to_sql(name='daily_sheet_stats', con=cnx, if_exists='replace')
        cnx.close()

    def merge_data(self):
        """
        Merge data found from Betfair Market Catalogue with the stats found from Daily Sheets for selected strategies.
        This is done by matching the event_name from self.df to event from self.df_sheets then merging the data.
        Returns merged df for selected strategies.
        :return: merge_data_match_odds
        """
        market_name_select = {'MATCH_ODDS': 'Match Odds', 'OVER_UNDER_45': 'Over/Under 4.5 Goals'}

        # Dataframe for Match Odds market only.
        df_match_odds = self.df.copy()
        # Create list of all event names to add to temp df.
        event_name_list = df_match_odds['event_name'].to_list()
        # Temp df from daily sheets of event.
        df_temp = self.df_sheets[['Date', 'event']].copy()

        print(event_name_list)
        print(df_temp)

        # Empty temp df where the matching will take place.
        df_temp_2 = pd.DataFrame(columns=['Date', 'event', 'seq_score', 'event_name'])
        # Iterate through event name list and match to temp df event.
        while True:
            for event in event_name_list:
                df_temp['seq_score'] = df_temp['event'].apply(lambda e: SequenceMatcher(None, event, e).ratio())
                # Sort so best match is at index 0.
                df_temp.sort_values('seq_score', inplace=True, ascending=False, ignore_index=True)
                # If score below threshold then remove from list as match not found.
                if df_temp.loc[0, 'seq_score'] < 0.8:
                    event_name_list.remove(event)
                # Score above threshold event and sequence match score to temp df.
                else:
                    df_temp.loc[0, 'event_name'] = event
                    data_df = pd.DataFrame({'Date': [df_temp.loc[0, 'Date']],
                                            'event': [df_temp.loc[0, 'event']],
                                            'seq_score': [df_temp.loc[0, 'seq_score']],
                                            'event_name': [df_temp.loc[0, 'event_name']]})
                    # Continually add matched events to empty df.
                    df_temp_2 = pd.concat([df_temp_2, data_df], ignore_index=True, axis=0)
                    # Event can then be removed from list as match has been found.
                    event_name_list.remove(event)
                break
            if len(event_name_list) == 0:
                break
        # Merge matched events to self.df_sheets. Now self.df_sheets will have a column exactly matching event_name
        # from self.df.
        merge_data = pd.merge(self.df_sheets, df_temp_2, on='event', how='inner')
        # Merge self.df and self.df_sheets at event_name.
        self.merge_data_match_odds = pd.merge(df_match_odds, merge_data, on='event_name', how='inner')
        print(self.merge_data_match_odds)

    def get_match_prices(self):
        marketid_list = self.merge_data_match_odds['marketId'].to_list()
        print('TEST', marketid_list)
        # Get market prices
        while True:
            try:
                book = api.betting.list_market_book(market_ids=marketid_list,
                                                    lightweight=True)
                break
            except betfairlightweight.exceptions.APIError:     
                mid = len(marketid_list) // 2
                marketid_list, second_half = np.split(marketid_list, [mid])
                marketid_list = marketid_list.tolist()
                print('TEST2', marketid_list)
        if self.market_code == 'MATCH_ODDS':
            df = pd.DataFrame(
                {'marketId': [d.get('marketId') for d in book],
                'home_price': [d['runners'][0].get('lastPriceTraded') for d in book],
                'away_price': [d['runners'][1].get('lastPriceTraded') for d in book],
                'draw_price': [d['runners'][2].get('lastPriceTraded') for d in book]
                })
        if self.market_code == 'OVER_UNDER_45':
            df = pd.DataFrame(
                {'marketId': [d.get('marketId') for d in book],
                'home_price': [d['runners'][0].get('lastPriceTraded') for d in book],
                'away_price': [d['runners'][1].get('lastPriceTraded') for d in book],
                })
        merge_data_match_odds['marketId'] = merge_data_match_odds['marketId'].astype(str)
        df['marketId'] = df['marketId'].astype(str)
        merge = pd.merge(merge_data_match_odds, df, on='marketId', how='inner')
        
        # Add paper and favourite columns
        merge['paper'] = True
        merge['favourite'] = np.where(merge['home_price'] < merge['away_price'], 1, 2)
        print(merge.sort_values('seq_score', ascending=False, ignore_index=True))
        print(f'\n{len(merge)} matches found for {self.market_code} market.')
        return merge

    def add_matches_to_db(self):
        pass

    def remove_duplicates(self):
        pass


class AutoTrader:
    def __init__(self):
        pass

class BackTester:
    def __init__(self):
        pass

if __name__ == '__main__':
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    # pd.set_option('expand_frame_repr', False)
    mf = MatchFinder(continuos='off')
    mf.get_betfair_details()
    mf.get_daily_sheets()
    # mf.merge_data()
