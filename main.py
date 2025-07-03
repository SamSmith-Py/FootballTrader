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

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.edge.service import Service as EdgeService
from selenium.webdriver.edge.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os

# Connect to Betfair API
api = betfairlightweight.APIClient('smudge2049', 'Dex17@£141117', '4oAYsDJiYA7P5Wej')
api.login_interactive()

# Paths to databases
autotrader_db_path = r'C:\Users\Sam\FootballTrader v0.3.2\database\autotrader_data.db'

class MatchFinder:
    def __init__(self, hours=24, continuos='on'):
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
        self.df_merge_data_and_stats = None
        
        print(self.df)

    def get_sports_iq_stats(self):
        print('\nConnecting to Sports-IQ to download stats...')
        # Initialize Selenium WebDriver with Edge options
        edge_options = Options()
        edge_options.add_argument("--headless=new")  # or "--headless" if "new" doesn't work
        edge_options.add_argument("--window-size=1920,1080")  # needed if elements aren't visible

        # Delete file download if it already exists
        data_file_path = r"C:\Users\Sam\FootballTrader v0.3.2\sports-iq\Football Data Fixtures.xlsx"
        if os.path.exists(data_file_path):
            os.remove(data_file_path)


        prefs = {
                "download.default_directory": r"C:\Users\Sam\FootballTrader v0.3.2\sports-iq",  # Change to your desired directory
                "download.prompt_for_download": False,  # Disable download prompt
                "download.directory_upgrade": True,
                "safebrowsing.enabled": True  # Enable safe browsing
                }
        edge_options.add_experimental_option("prefs", prefs)

        service = EdgeService(executable_path=r'C:/Program Files (x86)/msedgedriver.exe')
        

        # Wait for the login form to be loaded and enter login details
        while True:
            try:
                driver = webdriver.Edge(service=service, options=edge_options)

                # Load the login page
                url = 'https://sports-iq.co.uk/login/'
                driver.get(url)
                # Make instances of input boxes for login
                username_input = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.ID, 'login_email')))  # Adjust selector as needed
                password_input = driver.find_element(By.ID, 'password')  # Adjust selector as needed

                # Enter your login credentials
                username_input.send_keys('samcsmith17@gmail.com')
                password_input.send_keys('Dexyboy17!')
                password_input.send_keys(Keys.RETURN)

                # Wait for the next page to load
                tool_dropdown = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, "//*[contains(@class, 'menu-text') and text()='Tools']"))
                )
                tool_dropdown.click()

                custom_tables = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, "//*[contains(@class, 'menu-text') and text()='Custom Tables']"))
                )
                custom_tables.click()

                football_table = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, "//*[contains(@class, 'table_name') and text()='Football Data']"))
                )
                football_table.click()

                excel_export = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, "//*[contains(@class, 'btn btn-secondary buttons-excel buttons-html5 ms-3 btn-sm btn-outline-default')]"))
                )
                excel_export.click()

                break
            except Exception as e:
                print(f"\nAn error occurred: {e}\n")
                driver.quit()
                time.sleep(10)
                print('Re-attempting connection with driver.')
        
        time.sleep(2)
        driver.quit()

    def merge_data(self):
        """
        Merge data found from Betfair Market Catalogue with the stats found from Sports-IQ for selected strategies.
        This is done by matching the event_name from self.df to Fixtures from Sports-IQ then merging the data.
        Returns merged df for selected strategies.
        :return: merge_data_match_odds
        """

        # Get Sports-IQ stats as dataframe
        df_sportsiq = pd.read_excel(r'C:\Users\Sam\FootballTrader v0.3.2\sports-iq\Football Data Fixtures.xlsx', engine='openpyxl', header=1)

        # Dataframe for Match Odds market only.
        df_match_odds = self.df.copy()
        # Create list of all event names to add to temp df.
        event_name_list = df_match_odds['event_name'].to_list()
        # Temp df from daily sheets of event.
        df_temp = df_sportsiq[['Kickoff', 'Fixture']].copy()

        # Empty temp df where the matching will take place.
        df_temp_2 = pd.DataFrame(columns=['Fixture', 'seq_score', 'event_name'])
        # Iterate through event name list and match to temp df event.
        while True:
            for event in event_name_list:
                df_temp['seq_score'] = df_temp['Fixture'].apply(lambda e: SequenceMatcher(None, event, e).ratio())
                # Sort so best match is at index 0.
                df_temp.sort_values('seq_score', inplace=True, ascending=False, ignore_index=True)
                # If score below threshold then remove from list as match not found.
                if df_temp.loc[0, 'seq_score'] < 0.8:
                    event_name_list.remove(event)
                # Score above threshold event and sequence match score to temp df.
                else:
                    df_temp.loc[0, 'event_name'] = event
                    data_df = pd.DataFrame({
                                            'Fixture': [df_temp.loc[0, 'Fixture']],
                                            'seq_score': [df_temp.loc[0, 'seq_score']],
                                            'event_name': [df_temp.loc[0, 'event_name']]})
                    # Continually add matched events to empty df.
                    df_temp_2 = pd.concat([df_temp_2, data_df], ignore_index=True, axis=0)
                    # Event can then be removed from list as match has been found.
                    event_name_list.remove(event)
                break
            if len(event_name_list) == 0:
                break
        # Merge matched events to df_sportsiq. Now df_sportsiq will have a column exactly matching event_name
        # from self.df.
        merge_data = pd.merge(df_sportsiq, df_temp_2, on='Fixture', how='inner')
        # Merge self.df and df_sportsiq at event_name.
        self.df_merge_data_and_stats = pd.merge(df_match_odds, merge_data, on='event_name', how='inner')

        # Remove any unnecessary columns
        self.df_merge_data_and_stats.drop(columns=['Kickoff', 'Fixture', 'seq_score'], inplace=True)

        # Find favourite
        self.df_merge_data_and_stats['favourite'] = np.where(self.df_merge_data_and_stats['Odds Betfair Home'] < self.df_merge_data_and_stats['Odds Betfair Away'], 1, 2)

        print(self.df_merge_data_and_stats.columns)

    def add_matches_to_db(self):
        """
        This method will add alll the information obtained from Betfair and SPorts-IQ to the AutoTrader SQLite database. Firstly it will add all the required columns needed for 
        live trading and betting. The user wil alos determine whether the events found will be traded using a live or paper method, pending criteria being matched to a strategy."""
        
        # Create df with the required columns for the AutoTrader
        cols = ['live/paper', 'strategy', 'market', 'inplay_state', 'market_state', 'time_elapsed',
                'home_score', 'away_score', 'score', 'lay_price', 'back_price', 'entry_condition', 'entry_ordered',
                'entry_status', 'entry_price_avg', 'entry_amount_matched', 'entry_amount_remaining', 'exit_condition',
                'exit_ordered', 'exit_status', 'exit_price_avg', 'exit_amount_matched', 'exit_amount_remaining',
                'current_order_status', 'current_order_side', 'current_order_betid', 'potential_pnl', 'cleared_pnl',
                'ht_score', 'ft_score', 'goals_15', 'goals_30', 'goals_45', 'goals_60', 'goals_75', 'goals_90', ]
        df_autotrader = pd.DataFrame(columns=cols)

        # Add new columns to the df created from Betfair details and Sports-IQ
        df_autotrader = pd.concat([df_autotrader, self.df_merge_data_and_stats], axis=1)

        df_autotrader['entry_condition'] = 0
        df_autotrader['entry_ordered'] = 0
        df_autotrader['entry_amount_matched'] = 0
        df_autotrader['exit_condition'] = 0
        df_autotrader['exit_ordered'] = 0
        df_autotrader.dropna(subset=['event_id'], inplace=True)
        values = {'goals_15': 0, 'goals_30': 0, 'goals_45': 0, 'goals_60': 0, 'goals_75': 0, 'goals_90': 0,
                  'home_score': 0, 'away_score': 0}
        df_autotrader.fillna(value=values, inplace=True)

        # Save the dataframe to AutoTrader database
        cnx = sqlite3.connect(autotrader_db_path, check_same_thread=False)
        df_autotrader.to_sql(name='autotrader_matches_v3', con=cnx, if_exists='append', index=False) # Change to append once testing is complete
        cnx.close()
        self.remove_duplicates()

    def remove_duplicates(self):
        cnx = sqlite3.connect(autotrader_db_path, check_same_thread=False)
        df = pd.read_sql_query(f"SELECT * from autotrader_matches_v3", cnx, dtype=self.col_dtypes)
        df.drop_duplicates(ignore_index=True, subset=['event_name', 'start_date'], inplace=True)
        df.to_sql(name='autotrader_matches_v3', con=cnx, if_exists='replace', index=False,
                    dtype=self.col_dtypes)
        cnx.close()

class AutoTrader:
    def __init__(self):
        pass

class BackTester:
    def __init__(self):
        pass

if __name__ == '__main__':
    #pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    #pd.set_option('expand_frame_repr', False)
    mf = MatchFinder(continuos='on')
    mf.get_betfair_details()
    mf.get_sports_iq_stats()
    mf.merge_data()
    mf.add_matches_to_db()
