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
    def __init__(self, hours=24, continuous='on'):
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

        if continuous == 'off':
            # Set hours market search time range.
            self.hours = int(input('Select hours to search upto: '))
        if continuous == 'on':
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
        self.df.dropna(inplace=True)
        
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

        directory = r"C:\Users\Sam\FootballTrader v0.3.2\sports-iq"


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
                
                # If correct file present then break loop
                if os.path.exists(data_file_path):
                    break
                # If wrong file name loaded then delete file and retry
                else:
                    for filename in os.listdir(directory):
                        file_path = os.path.join(directory, filename)
                        if file_path != data_file_path:
                            os.remove(file_path)
                    time.sleep(10)

            except Exception as e:
                print(f"\nAn error occurred: {e}\n")
                driver.quit()
                time.sleep(10)
                print('Re-attempting connection with driver.')
        
        time.sleep(3)
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
        for idx in self.df_merge_data_and_stats.index:
            if self.df_merge_data_and_stats.loc[idx, 'Odds Betfair Home'] != '-' and self.df_merge_data_and_stats.loc[idx, 'Odds Betfair Away'] != '-':
                if float(self.df_merge_data_and_stats.loc[idx, 'Odds Betfair Home']) < float(self.df_merge_data_and_stats.loc[idx, 'Odds Betfair Away']):
                    self.df_merge_data_and_stats.loc[idx, 'favourite'] = 1
                else:
                    self.df_merge_data_and_stats.loc[idx, 'favourite'] = 2

        return self.df_merge_data_and_stats

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
        # balance = api.account.get_account_funds()

        # Variable placeholders
        self.cnx = None
        self.df = None
        self.run = None      

        self.col_dtypes = {}   
        self.max_lay_the_draw_price = 5
        self.ltd_paper_stake_size = 100
        self.ltd_live_stake_size = 1

        # Time required to wait for next run_autotrader run through
        self.wait_time = 10  # Seconds

        self.initialise_data()
        self.assign_strategy()

    def initialise_data(self):
        # Connect to autotrader database if not conected.
        if not self.is_database_connected():
            self.connect_autotrader_db()
        self.df = pd.read_sql_query("SELECT * from autotrader_matches_v3", self.cnx, dtype=self.col_dtypes) 


    def connect_autotrader_db(self):
        """
        Creates an instance for the connection to the autotrader database.
        :return:
        """
        self.cnx = sqlite3.connect(autotrader_db_path, check_same_thread=False)
    
    def close_connection_db(self):
        """
        Close database connections if open.
        :return:
        """
        if self.cnx:
            self.cnx.close()

    def is_database_connected(self):
        try:
            # Attempt to execute a simple query
            self.cnx.execute("SELECT * from autotrader_matches_v3")
            return True
        except sqlite3.ProgrammingError:
            return False
        except AttributeError:
            return False

    def assign_strategy(self, betting='paper'):
        """
        Assign's strategies to events and gives the option to bet on them live or paper.
        """
        print('testing assign strategy')
        # Connect to autotrader database if not conected.
        if not self.is_database_connected():
            self.connect_autotrader_db()

        # Get list of leagues that are eligible to assign.
        leagues = pd.read_excel(r'C:\Users\Sam\FootballTrader v0.3.2\backtest\strategy\LTD\Q1 + 2 2025\league_strike_rate.xlsx', index_col=0)['League'].to_list()

        # Get LTD strategy criteria
        df_LTD_strat = pd.read_sql_query("SELECT * from LTD_strategy_criteria", self.cnx, dtype=self.col_dtypes)
        # Assign strategy to any events applicable
        for row in self.df.index:
            if int(self.df.loc[row, 'GP Avg']) >= 8 and \
                (int(self.df.loc[row, 'Form H v A']) >= df_LTD_strat.loc[0, 'hva_pos'] or int(self.df.loc[row, 'Form H v A']) <= df_LTD_strat.loc[0, 'hva_neg']) and \
                float(self.df.loc[row, 'Form Goal Edge']) <= df_LTD_strat.loc[0, 'goal_edge_pos'] and \
                float(self.df.loc[row, 'Form Goal Edge']) >= float(df_LTD_strat.loc[0, 'goal_edge_neg']) and \
                self.df.loc[row, 'League'] in leagues:
                    self.df.loc[row, 'strategy'] = 'LTD'
            else:
                self.df.loc[row, 'strategy'] = None
                self.df['live/paper'].loc[(self.df['strategy'] == 'LTD')] = None
        
        # Decide if live or paper betting for strategy
        self.df['live/paper'].loc[(self.df['strategy'] == 'LTD')] = betting

        # Save all updates to database
        self.df.to_sql(name='autotrader_matches_v3', con=self.cnx, if_exists='replace', index=False,
                        dtype=self.col_dtypes)
        self.close_connection_db()

    def calculate_LTD_stake(self):
        balance = api.account.get_account_funds()

    def run_autotrader(self, continuous='off'):
        """
        Starts the loop to monitor for trade-able matches. Loop will stop if no matches are found.
        :return:
        """
        # Ensure while loop starts if run_autotrader is called after a stop_autotrader.
        self.run = True

        while self.run:
            # Connect to autotrader database and set to data frame.
            self.initialise_data()
            if len(self.df) > 0:  # Check there is data.
                for row in self.df.index:
                    # Get event id
                    event_id = self.df.loc[row, 'event_id']
                    # Run queries for selected match.
                    try:
                        # Check if matches have been removed.
                        if not self.check_inplay_state(event_id=event_id, idx=row):
                            self.check_market_state(idx=row)
                            self.check_time_elapsed(event_id=event_id, idx=row)
                            self.check_score(event_id=event_id, idx=row)
                            self.check_lay_price(idx=row)
                            self.check_back_price(idx=row)
                            self.check_current_orders(idx=row, check=1)
                            self.check_potential_uncleared_pnl(idx=row)
                        else:
                            break
                    except betfairlightweight.exceptions.APIError:
                        print('Connection lost. Attempting reconnect...')
                        logging.exception('API ERROR')
                        time.sleep(10)
                        continue
                    # Check for match strategy conditions.
                    try:
                        self.strategy_ltd(idx=row)
                    except betfairlightweight.exceptions.APIError:
                        print('Connection lost. Attempting reconnect...')
                        logging.exception('API ERROR')
                        time.sleep(10)
                        continue
            elif len(self.df) == 0:  
                # If no data, wait until next hour for continous MatchFinder
                if continuous == 'on': 
                    # Get the current time
                    now = datetime.now()
                    # Calculate the next hour
                    next_hour = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
                    # Calculate the time difference in seconds
                    time_to_sleep = (next_hour - now).total_seconds()
                    # Sleep until the next hour
                    time.sleep(time_to_sleep)
                # If continuous MatchFinder is off then stop autotrader
                else:
                    self.stop_autotrader()
            
            # Connect to autotrader database and set to data frame.
            if not self.is_database_connected():
                self.connect_autotrader_db()
            # Save all updates to database
            self.df.to_sql(name='autotrader_matches_v3', con=self.cnx, if_exists='replace', index=False,
                           dtype=self.col_dtypes)
            self.close_connection_db()

            # TESTING
            print(self.df[['event_name', 'League', 'live/paper', 'strategy', 'marketStartTime', 'start_date', 'start_time', 'inplay_state', 'time_elapsed', 'market_state',
                           'score', 'entry_ordered', 'GP Avg', 'Form H v A', 'Form Goal Edge']].sort_values(by=['start_date', 'start_time']))

            self.continuos_match_finder(activate=continuous)

            try:
                quit_running = inputimeout(prompt='Stop runnning? y/n ', timeout=5)
                if quit_running == 'y':
                    self.stop_autotrader()
            except TimeoutOccurred:
                pass

            # Wait
            time.sleep(self.wait_time)

    def check_inplay_state(self, event_id, idx):
        """
        Checks the in play state of the selected match. Updates database. If match has finished and traded, archive and
        remove from databases as necessary.
        :return:
        """
        get_scores = api.in_play_service.get_scores(event_ids=[event_id])
        # Get inplay state if available.
        if len(get_scores) > 0:
            for x in get_scores:
                self.df.loc[idx, 'inplay_state'] = x.match_status

        # If time is over 3 hours past kick off then mark as finished
        self.df['marketStartTime'] = pd.to_datetime(self.df['marketStartTime'])
        if datetime.now(timezone.utc) - self.df.loc[idx, 'marketStartTime'] > timedelta(hours=4):
            self.df.loc[idx, 'ft_score'] = self.df.loc[idx, 'score']
            self.df.loc[idx, 'inplay_state'] = 'Finished'
            self.check_paper_bet_result(idx)

        # If match finished check if traded. If traded then update trading log. Archive all and remove from trading log
        # database if un-traded.
        if self.df.loc[idx, 'inplay_state'] == 'Finished':
            self.df.loc[idx, 'ft_score'] = self.df.loc[idx, 'score']
            self.check_paper_bet_result(idx)
            self.check_cleared_orders_pnl(idx=idx)
            self.archive_autotrader_match(idx)
            self.remove_finished_matches_autotrader(idx)
            return True
        return False
    
    def check_market_state(self, idx):
        """
        Checks market state of selected match. Updates database.
        :return:
        """
        if self.df.loc[idx, 'strategy'] == 'LTD':
            market_book = api.betting.list_market_book(market_ids=[str(self.df.loc[idx, 'marketID_match_odds'])])
            if len(market_book) > 0:
                self.df.loc[idx, 'market_state'] = market_book[0].status
        if self.df.loc[idx, 'strategy'] == 'LO45':
            market_book = api.betting.list_market_book(market_ids=[str(self.df.loc[idx, 'marketID_overunder45'])])
            if len(market_book) > 0:
                self.df.loc[idx, 'market_state'] = market_book[0].status

    def check_time_elapsed(self, event_id, idx):
        """
        Checks the time elapsed for selected match. Shows negative number to indicate minutes before match starts.
        Updates database.
        :return:
        """
        try:
            time_elap = api.in_play_service.get_scores(event_ids=[event_id])
            if len(time_elap) > 0:
                self.df.loc[idx, 'time_elapsed'] = time_elap[0].time_elapsed
        except betfairlightweight.exceptions.StatusCodeError:
            print('GetScores Status code error')
            pass

    def check_score(self, event_id, idx):
        """
        Checks score for selected match. Updates database.
        :return:
        """
        try:
            scores = api.in_play_service.get_scores(event_ids=[event_id])
        
            if len(scores) > 0 and self.df.loc[idx, 'time_elapsed'] != None:
                for x in scores:
                    self.df.loc[idx, 'home_score'] = int(x.score.home.score)
                    self.df.loc[idx, 'away_score'] = int(x.score.away.score)
                    self.df.loc[idx, 'score'] = f'{int(x.score.home.score)} - {int(x.score.away.score)}'
                try:
                    
                    # Get score for first 15 minutes
                    if self.df.loc[idx, 'time_elapsed'] <= 15:
                        self.df.loc[idx, 'goals_15'] = f"{self.df.loc[idx, 'home_score']} - {self.df.loc[idx, 'away_score']}"            
                    # Get score within 15 - 30 minutes
                    if 15 < self.df.loc[idx, 'time_elapsed'] <= 30 and self.df.loc[idx, 'goals_15'] is not None:
                        self.df.loc[idx, 'goals_30'] = f"{int(self.df.loc[idx, 'home_score'])} - {int(self.df.loc[idx, 'away_score'])}"
                    # Get score within 30 - 45 minutes    
                    if 30 < self.df.loc[idx, 'time_elapsed'] <= 60 and \
                            self.df.loc[idx, 'inplay_state'] == 'KickOff' and \
                            self.df.loc[idx, 'goals_15'] is not None and \
                            self.df.loc[idx, 'goals_30'] is not None:
                        self.df.loc[idx, 'goals_45'] = f"{int(self.df.loc[idx, 'home_score'])} - {int(self.df.loc[idx, 'away_score'])}"
                    # Get score within 45 - 60 minutes     
                    if 45 <= self.df.loc[idx, 'time_elapsed'] <= 60 and \
                            self.df.loc[idx, 'inplay_state'] == 'SecondHalfKickOff' and \
                            self.df.loc[idx, 'goals_15'] is not None and \
                            self.df.loc[idx, 'goals_30'] is not None and \
                            self.df.loc[idx, 'goals_45'] is not None:
                        self.df.loc[idx, 'goals_60'] = f"{int(self.df.loc[idx, 'home_score'])} - {int(self.df.loc[idx, 'away_score'])}"
                    # Get score within 60 - 75 minutes   
                    if 60 < self.df.loc[idx, 'time_elapsed'] <= 75 and \
                            self.df.loc[idx, 'goals_15'] is not None and \
                            self.df.loc[idx, 'goals_30'] is not None and \
                            self.df.loc[idx, 'goals_45'] is not None and \
                            self.df.loc[idx, 'goals_60'] is not None:
                        self.df.loc[idx, 'goals_75'] = f"{int(self.df.loc[idx, 'home_score'])} - {int(self.df.loc[idx, 'away_score'])}"
                    # Get score within 75 - 90 minutes
                    if 75 < self.df.loc[idx, 'time_elapsed'] <= 120 and \
                            self.df.loc[idx, 'goals_15'] is not None and \
                            self.df.loc[idx, 'goals_30'] is not None and \
                            self.df.loc[idx, 'goals_45'] is not None and \
                            self.df.loc[idx, 'goals_60'] is not None and \
                            self.df.loc[idx, 'goals_75'] is not None:
                        self.df.loc[idx, 'goals_90'] = f"{int(self.df.loc[idx, 'home_score'])} - {int(self.df.loc[idx, 'away_score'])}"

                    if self.df.loc[idx, 'inplay_state'] == 'FirstHalfEnd':
                        self.df.loc[idx, 'ht_score'] = self.df.loc[idx, 'score']    

                except TypeError:
                    logging.exception()
        except betfairlightweight.exceptions.StatusCodeError:
            print('GetScores Status code error')
            logging.exception()

    def check_lay_price(self, idx):
        """
        Checks the lay price of the market for selected match. Updates database.
        :return:
        """
        if self.df.loc[idx, 'strategy'] == 'LTD':
            try:
                # Get current lay price.
                lay_price = api.betting.list_runner_book(market_id=str(self.df.loc[idx, 'marketID_match_odds']),
                                                        selection_id=58805,
                                                        price_projection={'priceData': ['EX_ALL_OFFERS']})
                lay_the_draw_price = lay_price[0].runners[0].ex.available_to_lay[0].price

                self.df.loc[idx, 'lay_price'] = lay_the_draw_price
            except IndexError:
                self.df.loc[idx, 'lay_price'] = -1
            except betfairlightweight.exceptions.StatusCodeError:
                self.df.loc[idx, 'lay_price'] = -1

        if self.df.loc[idx, 'strategy'] == 'LO45':
            try:
                # Get current lay price.
                lay_price = api.betting.list_runner_book(market_id=str(self.df.loc[idx, 'marketID_overunder45']),
                                                        selection_id=1222346,
                                                        price_projection={'priceData': ['EX_ALL_OFFERS']})
                lay_the_draw_price = lay_price[0].runners[0].ex.available_to_lay[0].price

                self.df.loc[idx, 'lay_price'] = lay_the_draw_price
            except IndexError:
                self.df.loc[idx, 'lay_price'] = -1
            except betfairlightweight.exceptions.StatusCodeError:
                self.df.loc[idx, 'lay_price'] = -1

    def check_back_price(self, idx):
        """
        Checks the back price of the market for selected match. Updates database.
        :return:
        """
        if self.df.loc[idx, 'strategy'] == 'LTD':
            try:
                # Get current back price.
                back_price = api.betting.list_runner_book(market_id=str(self.df.loc[idx, 'marketID_match_odds']),
                                                        selection_id=58805,
                                                        price_projection={'priceData': ['EX_ALL_OFFERS']})
                back_the_draw_price = back_price[0].runners[0].ex.available_to_back[0].price
                self.df.loc[idx, 'back_price'] = back_the_draw_price
            except IndexError:
                self.df.loc[idx, 'back_price'] = -1
            except betfairlightweight.exceptions.StatusCodeError:
                    self.df.loc[idx, 'back_price'] = -1
        if self.df.loc[idx, 'strategy'] == 'LO45':
            try:
                # Get current back price.
                back_price = api.betting.list_runner_book(market_id=str(self.df.loc[idx, 'marketID_overunder45']),
                                                        selection_id=1222346,
                                                        price_projection={'priceData': ['EX_ALL_OFFERS']})
                back_the_draw_price = back_price[0].runners[0].ex.available_to_back[0].price
                self.df.loc[idx, 'back_price'] = back_the_draw_price
            except IndexError:
                self.df.loc[idx, 'back_price'] = -1
            except betfairlightweight.exceptions.StatusCodeError:
                    self.df.loc[idx, 'back_price'] = -1

    def check_current_orders(self, idx, check=0):
        """
        Check all current orders.
        If there are Lay/Back orders then an entry/exit has been ordered. Update order status, if there are multiple
        orders of the same side then get average price. Get size matched and remaining for entry and exits.
        If size remains unmatched then order should be updated with current price and the remaining size that requires
        matching.
        :return:
        """
        if self.df.loc[idx, 'live/paper'] == 'live' and self.df.loc[idx, 'strategy'] == 'LTD':
            orders = api.betting.list_current_orders(market_ids=[self.df.loc[idx, 'marketID_match_odds']],
                                                     sort_dir='LATEST_TO_EARLIEST', lightweight=True)
            self.df.loc[idx, 'current_order_side'] = orders.orders[0].side
            self.df.loc[idx, 'current_order_status'] = orders.orders[0].status
            self.df.loc[idx, 'current_order_betid'] = orders.orders[0].bet_id
            for order in orders['currentOrders']:
                # Check if any Lay orders.
                if order['side'] == 'LAY':
                    # Update if entry ordered.
                    self.df.loc[idx, 'entry_ordered'] = 1
                    # Update entry order status.
                    self.df.loc[idx, 'entry_status'] = order['status']
                    # get average price of entry's.
                    self.df.loc[idx, 'entry_price_avg'] = round(order['averagePriceMatched'], 2)
                    # Update entry size matched and remaining.
                    self.df.loc[idx, 'entry_amount_matched'] = order['sizeMatched']
                    self.df.loc[idx, 'entry_amount_remaining'] = order['sizeRemaining']
                # Check if any Back orders. - No current need for BACK ORDERS this will need to be revised in the future.
                """if order['side'] == 'BACK':
                    # Update if exit ordered.
                    self.df.loc[idx, 'exit_ordered'] = 1
                    # Update exit order status.
                    self.df.loc[idx, 'exit_status'] = order['status']
                    # get average price of exit's.
                    self.df.loc[idx, 'exit_price_avg'] = round(order['averagePriceMatched'], 2)
                    # Update exit size matched and remaining.
                    self.df.loc[idx, 'exit_amount_matched'] = order['sizeMatched']
                    self.df.loc[idx, 'exit_amount_remaining'] = order['sizeRemaining']"""
            # Check if no orders.
            if len(orders['currentOrders']) == 0:
                self.df.loc[idx, 'entry_ordered'] = 0
                self.df.loc[idx, 'exit_ordered'] = 0

            # Check if orders partially matched and update.
            if check == 1:
                if self.df.loc[idx, 'entry_amount_remaining'] >= 1.0:
                    self.update_current_orders(idx, self.df.loc[idx, 'lay_price'],
                                               self.df.loc[idx, 'current_order_betid'])
                # No current need for BACK ORDERS this will need to be revised in the future.
                """if self.df.loc[idx, 'exit_amount_remaining'] >= 1.0:
                    self.update_current_orders(idx, self.df.loc[idx, 'lay_price'],
                                               self.df.loc[idx, 'current_order_betid'])"""
                    
    def update_current_orders(self, idx, price, betid):
        if self.df.loc[idx, 'strategy'] == 'LTD':
            instruction = filters.replace_instruction(bet_id=betid, new_price=price)
            api.betting.replace_orders(market_id=str(self.df.loc[idx, 'marketID_match_odds']), instructions=[instruction])

    def check_potential_uncleared_pnl(self, idx):
        if self.df.loc[idx, 'entry_ordered'] == 1:
            back_stake = round(self.df.loc[idx, 'entry_price_avg'] *
                               self.df.loc[idx, 'entry_amount_matched'] /
                               self.df.loc[idx, 'back_price'], 2)
            self.df.loc[idx, 'potential_pnl'] = round(self.df.loc[idx, 'entry_amount_matched'] - back_stake, 2)
        # No current need for BACK ORDERS this will need to be revised in the future.
        """if self.df.loc[idx, 'exit_ordered'] == 1:
            back_stake = round(self.df.loc[idx, 'entry_price_avg'] *
                               self.df.loc[idx, 'entry_amount_matched'] /
                               self.df.loc[idx, 'exit_price_avg'], 2)
            self.df.loc[idx, 'potential_pnl'] = round(self.df.loc[idx, 'entry_amount_matched'] - back_stake, 2)
        """

    def check_cleared_orders_pnl(self, idx):
        if self.df.loc[idx, 'strategy'] == 'LTD' and int(self.df.loc[idx, 'entry_amount_matched']) > 0 and self.df.loc[idx, 'live/paper'] == 'live':
            cleared = api.betting.list_cleared_orders(market_ids=[self.df.loc[idx, 'marketID_match_odds']], group_by='MARKET',
                                                      lightweight=True)
            print(cleared)
            if len(cleared['clearedOrders']) > 0:
                self.df.loc[idx, 'cleared_pnl'] = cleared.orders[0].profit

    def place_lay_order(self, size, price, ptype, idx, side):
        selection_id = {'LTD': 58805}
        if self.df.loc[idx, 'live/paper'] == 'live':
            limit_order = filters.limit_order(size=size, price=price, persistence_type=ptype)
            instruction = filters.place_instruction(
                order_type="LIMIT",
                selection_id=selection_id[self.df.loc[idx, 'strategy']],
                side=side,
                limit_order=limit_order)
            place_orders = api.betting.place_orders(
                market_id=str(self.df.loc[idx, 'marketID_match_odds']), instructions=[instruction], lightweight=True)
            if place_orders.place_instruction_reports[0].status == 'SUCCESS':
                self.check_current_orders(idx=idx)
                self.df.loc[idx, 'entry_status'] = place_orders.place_instruction_reports[0].order_status
            if place_orders.place_instruction_reports[0].status == 'TIMEOUT':
                time.sleep(15)
                self.check_current_orders(idx=idx)
            if place_orders.place_instruction_reports[0].status == 'FAILURE':
                self.df.loc[idx, 'entry_ordered'] = 0
        if self.df.loc[idx, 'strategy'] == 'LTD' and self.df.loc[idx, 'live/paper'] == 'paper':
            self.df.loc[idx, 'entry_ordered'] = 1
            self.df.loc[idx, 'entry_status'] = 'EXECUTION_COMPLETE'
            self.df.loc[idx, 'entry_price_avg'] = self.df.loc[idx, 'lay_price']
            self.df.loc[idx, 'entry_amount_matched'] = self.ltd_paper_stake_size
            self.df.loc[idx, 'entry_amount_remaining'] = 0
            self.df.loc[idx, 'current_order_side'] = side
            self.df.loc[idx, 'current_order_status'] = 'EXECUTION_COMPLETE'
            self.df.loc[idx, 'current_order_betid'] = 0

            self.adjust_paper_account(amount=self.ltd_paper_stake_size * (self.df.loc[idx, 'lay_price'] - 1), adjustment='decrease')

    def stop_autotrader(self):
        """
        Stops the loop that monitors for trade-able matches.
        :return:
        """
        # Check if there is an open connection, if not connect to database.
        if not self.is_database_connected():
            self.connect_autotrader_db()
        self.run = False  # Stop while loop.
        # Save all match updates to database.
        self.df.to_sql(name='autotrader_matches_v3', con=self.cnx, if_exists='replace', index=False)
        self.close_connection_db()  # Close any connection to db.

    def remove_finished_matches_autotrader(self, idx):
        while True:
            if self.is_database_connected():
                print('test remove')

                # Remove finished matches
                cur = self.cnx.cursor()
                cur.execute("DELETE FROM autotrader_matches_v3 WHERE inplay_state = ?", ('Finished',))
                self.cnx.commit()
                self.close_connection_db()
                self.df.drop(index=idx, inplace=True)  # Remove finished match from df
                break
            else:
                self.connect_autotrader_db()

    def archive_autotrader_match(self, idx):
        while True:
            if self.is_database_connected():

                # Archive match.
                df_temp = pd.DataFrame(self.df.loc[idx, self.df.columns.to_list()]).T.apply(lambda x: x.astype(str))
                df_temp.to_sql(name='archive_v2', con=self.cnx,
                               if_exists='append', index=False,
                               dtype=self.col_dtypes)  # Change if_exists to append after initial submit

                # Remove any duplicates after submitting to archive
                # self.drop_duplicates('autotrader_archive')
                break
            else:
                self.connect_autotrader_db()

    def drop_duplicates(self, table):
        while True:
            if self.is_database_connected():
                df = pd.read_sql_query(f"SELECT * from {table}", self.cnx, dtype=self.col_dtypes)
                df.drop_duplicates(ignore_index=True, subset=['event_name', 'start_date'])
                df.to_sql(name=table, con=self.cnx, if_exists='replace', index=False,
                          dtype=self.col_dtypes)
                break
            else:
                self.connect_autotrader_db()

    def continuos_match_finder(self, activate='off'):
        
            # This is the Continuos MAtchFinder feature. When this is activated it will stop the AutoTrader tool, run the MatchFinder tool then restart the AutoTrader.
            self.df['marketStartTime'] = pd.to_datetime(self.df['marketStartTime'])
            latest_kickoff  = self.df['marketStartTime'].max()
            if activate == 'on':
                # Check if time is half past or on the hour
                if pd.Timestamp.now().minute == 0 or pd.Timestamp.now().minute == 30:
                    print('*'*200)
                    print(pd.Timestamp.now())
                    print('*'*200)
                    # if datetime.now(timezone.utc) > latest_kickoff:
                    self.stop_autotrader()
                    
                    mf = MatchFinder(continuous=activate)
                    mf.get_betfair_details()
                    mf.get_sports_iq_stats()
                    df = mf.merge_data()
                    if len(df) > 0:
                        mf.add_matches_to_db()
                        self.initialise_data()
                        self.assign_strategy()
                    self.run_autotrader(continuous=activate)

    def adjust_paper_account(self, amount, adjustment):
        if not self.is_database_connected():
            self.connect_autotrader_db()
        paper_account_df = pd.read_sql_query("SELECT * from paper_account", self.cnx)
        print(paper_account_df, amount, adjustment)
        if adjustment == 'decrease':
            print('test', 'decrease')
            paper_account_df['balance'] = paper_account_df['balance'].astype(int) - amount
        if adjustment == 'increase':
            print('test', 'increase')
            paper_account_df['balance'] = paper_account_df['balance'].astype(int) + amount
        print(paper_account_df)
        paper_account_df.to_sql(name='paper_account', con=self.cnx, if_exists='replace', index=False)

    def check_paper_bet_result(self, idx):
        if self.df.loc[idx, 'strategy'] == 'LTD' and self.df.loc[idx, 'live/paper'] == 'paper':
            if int(self.df.loc[idx, 'entry_ordered']) == 1:
                if self.df.loc[idx, 'home_score'] != self.df.loc[idx, 'away_score']:
                    paper_profit = (self.ltd_paper_stake_size * (self.df.loc[idx, 'entry_price_avg'] - 1)) + (self.ltd_paper_stake_size - (self.ltd_paper_stake_size * 0.02))
                    self.adjust_paper_account(amount=paper_profit , adjustment='increase')

    def strategy_ltd(self, idx):
        if self.df.loc[idx, 'strategy'] == 'LTD' and int(self.df.loc[idx, 'entry_ordered']) == 0:
            self.df['marketStartTime'] = pd.to_datetime(self.df['marketStartTime'])
            if datetime.now(timezone.utc) > self.df.loc[idx, 'marketStartTime'] - timedelta(minutes=5):
                print(f"ENTRY ORDER PLACED for LTD: {self.df.loc[idx, 'event_name']}")
                self.place_lay_order(size=self.ltd_live_stake_size, 
                                     price=self.df.loc[idx, 'lay_price'], 
                                     ptype='PERSIST',
                                     idx=idx,
                                     side='LAY')
            if datetime.now(timezone.utc) < self.df.loc[idx, 'marketStartTime'] and float(self.df.loc[idx, 'lay_price']) < 3.4:
                print(f"ENTRY ORDER PLACED for LTD: {self.df.loc[idx, 'event_name']}")
                self.place_lay_order(size=self.ltd_live_stake_size, 
                                     price=self.df.loc[idx, 'lay_price'], 
                                     ptype='PERSIST',
                                     idx=idx,
                                     side='LAY')

class BackTester:
    def __init__(self):
        self.con = sqlite3.connect(autotrader_db_path, check_same_thread=False)
        df = pd.read_sql_query("SELECT * from archive_v2", self.con)

        # Set up columns required for backtesting
        df['home_ht_score'] = [int(x[0]) for x in df['ht_score'].values]
        df['away_ht_score'] = [int(x[-1]) for x in df['ht_score'].values]

        df['home_ft_score'] = [int(x[0]) for x in df['ft_score'].values]
        df['away_ft_score'] = [int(x[-1]) for x in df['ft_score'].values]

        df['total_goals'] = df['home_ft_score'] + df['away_ft_score']

        df['ht_result'] = np.where(df['home_ht_score'] == df['away_ht_score'], 'draw', 0)
        df['ht_result'] = np.where(df['home_ht_score'] > df['away_ht_score'], 'home', df['ht_result'])
        df['ht_result'] = np.where(df['home_ht_score'] < df['away_ht_score'], 'away', df['ht_result'])

        df['ft_result'] = np.where(df['home_ft_score'] == df['away_ft_score'], 'draw', 0)
        df['ft_result'] = np.where(df['home_ft_score'] > df['away_ft_score'], 'home', df['ft_result'])
        df['ft_result'] = np.where(df['home_ft_score'] < df['away_ft_score'], 'away', df['ft_result'])

        df.copy()['Odds Betfair Draw'] = np.where(df['Odds Betfair Draw'] == 'nan', np.nan, df['Odds Betfair Draw'])
        df['Odds Betfair Draw'] = np.where(df['Odds Betfair Draw'] == 'None', np.nan, df['Odds Betfair Draw'])
        df.replace('None', np.nan, inplace=True)
        df.replace('nan', np.nan, inplace=True)
        df.replace('-', np.nan, inplace=True)
        df['Odds Betfair Draw'].bfill(inplace=True)
        df['Odds Betfair Draw'].ffill(inplace=True)

        train_data, validate_data, test_data = self.split_data(df)

        # Optimise LTD strategy with Training data
        filt, opt = self.optimise_ltd_strategy(train_data)
       
        # Validate LTD strategy
        self.validate_ltd_strategy(validate_data, filt, opt)

        # self.test_ltd_strategy(test_data, filt, opt)

    def split_data(self, df):
        split_60_percent = round(len(df)*0.6)
        split_80_percent = round(len(df)*0.8)
        
        train_data = df.iloc[:split_60_percent]
        validate_data = df.iloc[split_60_percent:split_80_percent]
        test_data = df.iloc[split_80_percent:]
        return train_data, validate_data, test_data
    
    def no_strategy_LTD(self, train_data):
        train_data['pnl'] = 98
        train_data['pnl'] = np.where(train_data['ft_result'] == 'draw', -100 * (train_data['Odds Betfair Draw'].astype(float) - 1), train_data['pnl'])
        train_data['cumsum'] = train_data.copy()['pnl'].cumsum()
        train_data.reset_index(inplace=True)
        train_data.to_html(r"C:\Users\Sam\FootballTrader v0.3.2\backtest\strategy\LTD\no_strategy_LTD.html")
        train_data['cumsum'].plot(title='LTD NO STRATEGY Training Data - P&L')
        plt.savefig(r'C:\Users\Sam\FootballTrader v0.3.2\backtest\strategy\LTD\Plots\no_strategy_LTD_pnl.png')

        # create league performance results
        leagues_pnl = round(train_data.groupby(['League'])['pnl'].sum().reset_index(), 2)
        leagues_lose = train_data.groupby('League')['ft_result'].apply(lambda x: (x=='draw').sum()).reset_index()
        leagues_lose.rename(columns={'ft_result': 'loss'}, inplace=True)
        leagues_win = train_data.groupby('League')['ft_result'].apply(lambda x: (x!='draw').sum()).reset_index()
        leagues_win.rename(columns={'ft_result': 'win'}, inplace=True)
        league_data = leagues_pnl.merge(leagues_win, on='League', how='inner').merge(leagues_lose, on='League', how='inner')
        league_data['win_rate'] = round(league_data['win']/(league_data['win'] + league_data['loss'])*100, 2)
        league_data.sort_values('pnl', ascending=False).to_html(r'C:\Users\Sam\FootballTrader v0.3.2\backtest\strategy\LTD\no_strategy_LTD_league_performance.html')

        # filter low games played leagues
        league_data['gp'] = league_data['win'] + league_data['loss']
        league_data = league_data.loc[league_data['gp'] >= 8]
        league_data.sort_values('pnl', ascending=False).to_html(r'C:\Users\Sam\FootballTrader v0.3.2\backtest\strategy\LTD\no_strategy_LTD_league_performance_1.html')
        print(league_data['pnl'].sum())
        league_data = league_data.loc[league_data['win_rate'] >= 75]
        league_data.sort_values('pnl', ascending=False).to_html(r'C:\Users\Sam\FootballTrader v0.3.2\backtest\strategy\LTD\no_strategy_LTD_league_performance_2.html')
        print(league_data['pnl'].sum())

        league_list = league_data['League'].to_list()

        train_data = train_data[train_data['League'].isin(league_list)]
        train_data['pnl'] = 98
        train_data['pnl'] = np.where(train_data['ft_result'] == 'draw', -100 * (train_data['Odds Betfair Draw'].astype(float) - 1), train_data['pnl'])
        train_data['cumsum'] = train_data.copy()['pnl'].cumsum()
        train_data.reset_index(inplace=True)
        train_data.to_html(r"C:\Users\Sam\FootballTrader v0.3.2\backtest\strategy\LTD\no_strategy_LTD.html")
        train_data['cumsum'].plot(title='LTD NO STRATEGY 1 Training Data - P&L')
        plt.savefig(r'C:\Users\Sam\FootballTrader v0.3.2\backtest\strategy\LTD\Plots\no_strategy_LTD_pnl_1.png')

        return league_list
    
    def validate_no_strategy_LTD(self, league_list, validate_data):
        print(league_list)
        df = validate_data[validate_data['League'].isin(league_list)]
         # Add pnl and cumsum 
        df['pnl'] = 98
        df['pnl'] = np.where(df['ft_result'] == 'draw', -100 * (df['Odds Betfair Draw'].astype(float) - 1), df['pnl'])
        df['cumsum'] = df['pnl'].cumsum()
        df.reset_index(inplace=True)
        df.to_html(r"C:\Users\Sam\FootballTrader v0.3.2\backtest\strategy\LTD\Validated_Strategy_Results\all_trades_no_strategy.html")
        df['cumsum'].plot(title='Validated General Strategy - Validate Data - P&L').legend(['Optimised LTD', 'Removed Leagues LTD', 'Validated LTD', 'Validated Removed Leagues LTD'])
        plt.savefig(r'C:\Users\Sam\FootballTrader v0.3.2\backtest\strategy\LTD\Plots\no_strategys_validated_LTD_strategy_validate_data_pnl.png')

        # create league perormance results
        leagues_pnl = round(df.groupby(['League'])['pnl'].sum().reset_index(), 2)
        leagues_lose = df.groupby('League')['ft_result'].apply(lambda x: (x=='draw').sum()).reset_index()
        leagues_lose.rename(columns={'ft_result': 'loss'}, inplace=True)
        leagues_win = df.groupby('League')['ft_result'].apply(lambda x: (x!='draw').sum()).reset_index()
        leagues_win.rename(columns={'ft_result': 'win'}, inplace=True)
        league_data = leagues_pnl.merge(leagues_win, on='League', how='inner').merge(leagues_lose, on='League', how='inner')
        league_data['win_rate'] = round(league_data['win']/(league_data['win'] + league_data['loss'])*100, 2)
        league_data.sort_values('pnl', ascending=False).to_html(r'C:\Users\Sam\FootballTrader v0.3.2\backtest\strategy\LTD\no_strategy_validated_LTD_league_performance.html')


    def optimise_ltd_strategy(self, train_data, df=None):
        
        # Set strategy criteria to be optimised 
        hva_pos = [10, 11, 12, 13, 14, 15]
        hva_neg = [-10, -11, -12, -13, -14, -15]
        goal_edge_pos = [3]
        goal_edge_neg = [-3]
        last8_25 = [0.3, 0.35, 0.4, 0.45, 0.5]
        draw_odds = [5]

        # Create list of combination lists
        combinations = list(product(hva_pos, hva_neg, goal_edge_pos, goal_edge_neg, last8_25, draw_odds))

        # Empty list to append results to display in a df 
        hva_pos_col = []
        hva_neg_col = []
        goal_edge_pos_col = []
        goal_edge_neg_col = []
        last8_25_col = []
        draw_perc_col = []
        total_matches_col = []
        total_no_draw_col = []
        total_draw_col = []
        avg_loss_draw_price = []

        # Iterate through combinations for strategy criteria
        with Bar('Processing...', max=len(combinations), fill="\u26BD") as bar:
            # bar.max(len(train_data))
            for comb in combinations:
                df_opt = train_data[(train_data['GP Avg'].astype(float) >= 8) &
                                    ((train_data['Form H v A'].astype(float) >= comb[0]) | (train_data['Form H v A'].astype(float) <= comb[1])) &  # Rel2 pos and neg min limits
                                    ((train_data['Form Goal Edge'] <= comb[2]) & (train_data['Form Goal Edge'] >= comb[3])) &  # Magic number range
                                    (train_data['Goals 2.5+ L8 Avg'] >= comb[4]) & # Percentage of 2.5 goals
                                    (train_data['Odds Betfair Draw'].astype(float) <= comb[5])]  
                # Add data to relevant column lists
                hva_pos_col.append(comb[0])
                hva_neg_col.append(comb[1])
                goal_edge_pos_col.append(comb[2])
                goal_edge_neg_col.append(comb[3])
                last8_25_col.append(comb[4])

                draw_perc_col.append(df_opt['ft_result'].value_counts(normalize=True).get('draw'))
                total_matches_col.append(len(df_opt))
                total_no_draw_col.append(len(df_opt[(df_opt['ft_result'] != 'draw')]))
                total_draw_col.append(len(df_opt[(df_opt['ft_result'] == 'draw')]))
                draw_result_df = df_opt[(df_opt['ft_result'] == 'draw')]
                draw_result_df.copy().loc[draw_result_df['Odds Betfair Draw'] == 'nan'] = np.nan
                draw_result_df.copy()['Odds Betfair Draw'] = draw_result_df['Odds Betfair Draw'].bfill()
                avg_loss_draw_price.append(draw_result_df['Odds Betfair Draw'].astype(float).mean(skipna=True))
                bar.next()

        # Create df of all optimised results
        optimised_df = pd.DataFrame({'hva_pos': hva_pos_col, 'hva_neg': hva_neg_col, 
                                     'goal_edge_pos': goal_edge_pos_col,'goal_edge_neg': goal_edge_neg_col, 'last8_25': last8_25_col,
                                    'draw_perc': draw_perc_col,
                                    'total_matches': total_matches_col,
                                    'no_draw': total_no_draw_col,
                                    'draw': total_draw_col,
                                    'avg_draw_loss_price': avg_loss_draw_price}).sort_values(by=['draw_perc'])
        
        # Calculate the pnl for all results
        optimised_df['profit'] = optimised_df['no_draw'] * 98
        optimised_df['loss'] = ((optimised_df['avg_draw_loss_price'] - 1) * 100) * optimised_df['draw']
        optimised_df['loss'].loc[optimised_df['draw'] == 0] = 0
        optimised_df['pnl'] = optimised_df['profit'] - optimised_df['loss'] 
        optimised_df = optimised_df.loc[(optimised_df['draw_perc'] <= 0.17) & (optimised_df['total_matches'] > 200)]
        optimised_df.sort_values(by=['pnl', 'total_matches', 'draw_perc'], ascending=[False, False, True], inplace=True)  # sort df by highest pnl value

        # Save the top 1000 LTD strategy optimised results
        optimised_df.head(1000).to_html(r"C:\Users\Sam\FootballTrader v0.3.2\backtest\strategy\LTD\Optimised_Strategy_Results\optimised_LTD_strategy.html")
        optimised_df.reset_index(inplace=True)

        # Apply general strategy to train data    
        idx = 0   
        df_train = train_data[(train_data['GP Avg'].astype(float) >= 8) &
                              ((train_data['Form H v A'].astype(float) >= optimised_df.loc[idx, 'hva_pos']) | (train_data['Form H v A'].astype(float) <= optimised_df.loc[idx, 'hva_neg'])) &  # Rel2 pos and neg min limits
                                ((train_data['Form Goal Edge'] <= optimised_df.loc[idx, 'goal_edge_pos']) & (train_data['Form Goal Edge'] >= optimised_df.loc[idx, 'goal_edge_neg'])) &  # Magic number range
                                (train_data['Goals 2.5+ L8 Avg'] >= optimised_df.loc[idx, 'last8_25']) &
                                    (train_data['Odds Betfair Draw'].astype(float) <= 5)]
        df_train['pnl'] = 98
        df_train['pnl'] = np.where(df_train['ft_result'] == 'draw', -100 * (df_train['Odds Betfair Draw'].astype(float) - 1), df_train['pnl'])
        df_train['cumsum'] = df_train.copy()['pnl'].cumsum()
        df_train.reset_index(inplace=True)
        df_train.to_html(r"C:\Users\Sam\FootballTrader v0.3.2\backtest\strategy\LTD\Optimised_Strategy_Results\all_trades.html")
        df_train['cumsum'].plot(title='LTD Strategy Training Data - P&L').legend(['Optimised GS'])
        plt.savefig(r'C:\Users\Sam\FootballTrader v0.3.2\backtest\strategy\LTD\Plots\optimised_LTD_strategy_train_data_pnl.png')

        # create league performance results
        leagues_pnl = round(df_train.groupby(['League'])['pnl'].sum().reset_index(), 2)
        leagues_lose = df_train.groupby('League')['ft_result'].apply(lambda x: (x=='draw').sum()).reset_index()
        leagues_lose.rename(columns={'ft_result': 'loss'}, inplace=True)
        leagues_win = df_train.groupby('League')['ft_result'].apply(lambda x: (x!='draw').sum()).reset_index()
        leagues_win.rename(columns={'ft_result': 'win'}, inplace=True)
        league_data = leagues_pnl.merge(leagues_win, on='League', how='inner').merge(leagues_lose, on='League', how='inner')
        league_data['win_rate'] = round(league_data['win']/(league_data['win'] + league_data['loss'])*100, 2)
        league_data.sort_values('pnl', ascending=False).to_html(r'C:\Users\Sam\FootballTrader v0.3.2\backtest\strategy\LTD\Optimised_Strategy_Results\optimised_LTD_league_performance.html')
        
        poor_result_league = league_data.loc[(league_data['win_rate'] < 75) | (league_data['win'] + league_data['loss'] < 3)]
        print(poor_result_league['League'].to_list())
        

        # Create a list of underpeforming leagues to remove from validation and testing. 
        ## These leagues are identified to be unprofitable using LTD strategy.
        # under_performing = ['England, National League', 'Denmark, Superliga', 'England, League Two', 'Italy, Serie B', 'Austria, 2. Liga', 'Germany, Bundesliga']
        under_performing = set(poor_result_league['League'].to_list())
        print('All leagues to be removed')
        for x in under_performing:
            print(x)
        # under_performing = ['Denmark, Superliga', 'Italy, Serie B', 'Germany, Bundesliga']
        all_leagues = set(league_data['League'].values)
        filtered_leagues = [league for league in all_leagues if league not in under_performing]
        remove_league_train_data = train_data[train_data['League'].isin(filtered_leagues)]
        print('\nLeagues to be traded')
        for x in set(remove_league_train_data['League'].to_list()):
            print(x)

        remove_league_train_data = remove_league_train_data[(remove_league_train_data['GP Avg'].astype(float) >= 8) &
                              ((remove_league_train_data['Form H v A'].astype(float) >= optimised_df.loc[idx, 'hva_pos']) | (remove_league_train_data['Form H v A'].astype(float) <= optimised_df.loc[idx, 'hva_neg'])) &  # Rel2 pos and neg min limits
                                ((remove_league_train_data['Form Goal Edge'] <= optimised_df.loc[idx, 'goal_edge_pos']) & (remove_league_train_data['Form Goal Edge'] >= optimised_df.loc[idx, 'goal_edge_neg'])) &  # Magic number range
                                (remove_league_train_data['Goals 2.5+ L8 Avg'] >= optimised_df.loc[idx, 'last8_25']) &
                                    (remove_league_train_data['Odds Betfair Draw'].astype(float) <= 5)]
        remove_league_train_data['pnl'] = 98
        remove_league_train_data['pnl'] = np.where(remove_league_train_data['ft_result'] == 'draw', -100 * (remove_league_train_data['Odds Betfair Draw'].astype(float) - 1), remove_league_train_data['pnl'])
        remove_league_train_data['cumsum'] = remove_league_train_data.copy()['pnl'].cumsum()
        remove_league_train_data.reset_index(inplace=True)
        remove_league_train_data.to_html(r"C:\Users\Sam\FootballTrader v0.3.2\backtest\strategy\LTD\Optimised_Strategy_Results\all_trades_remove_league.html")
        remove_league_train_data['cumsum'].plot(title='LTD Strategy Training Data - P&L').legend(['Optimised LTD', 'Removed Leagues LTD'])
        plt.savefig(r'C:\Users\Sam\FootballTrader v0.3.2\backtest\strategy\LTD\Plots\remove_league_train_data_pnl.png')

         # create league performance results
        leagues_pnl = round(remove_league_train_data.groupby(['League'])['pnl'].sum().reset_index(), 2)
        leagues_lose = remove_league_train_data.groupby('League')['ft_result'].apply(lambda x: (x=='draw').sum()).reset_index()
        leagues_lose.rename(columns={'ft_result': 'loss'}, inplace=True)
        leagues_win = remove_league_train_data.groupby('League')['ft_result'].apply(lambda x: (x!='draw').sum()).reset_index()
        leagues_win.rename(columns={'ft_result': 'win'}, inplace=True)
        league_data = leagues_pnl.merge(leagues_win, on='League', how='inner').merge(leagues_lose, on='League', how='inner')
        league_data['win_rate'] = round(league_data['win']/(league_data['win'] + league_data['loss'])*100, 2)
        league_data.sort_values('pnl', ascending=False).to_html(r'C:\Users\Sam\FootballTrader v0.3.2\backtest\strategy\LTD\Optimised_Strategy_Results\remove_leagues_optimised_LTD_league_performance.html')

        return filtered_leagues, optimised_df

    def validate_ltd_strategy(self, validate_data, filtered_leagues, optimised_df):
        idx=0

        # Apply validated general strategy criteria to validate data
        df = validate_data[(validate_data['GP Avg'].astype(float) >= 8) &
                              ((validate_data['Form H v A'].astype(float) >= optimised_df.loc[idx, 'hva_pos']) | (validate_data['Form H v A'].astype(float) <= optimised_df.loc[idx, 'hva_neg'])) &  # Rel2 pos and neg min limits
                                ((validate_data['Form Goal Edge'] <= optimised_df.loc[idx, 'goal_edge_pos']) & (validate_data['Form Goal Edge'] >= optimised_df.loc[idx, 'goal_edge_neg'])) &  # Magic number range
                                (validate_data['Goals 2.5+ L8 Avg'] >= optimised_df.loc[idx, 'last8_25']) &
                                    (validate_data['Odds Betfair Draw'].astype(float) <= 5)]
        
        # Add pnl and cumsum 
        df['pnl'] = 98
        df['pnl'] = np.where(df['ft_result'] == 'draw', -100 * (df['Odds Betfair Draw'].astype(float) - 1), df['pnl'])
        df['cumsum'] = df['pnl'].cumsum()
        df.reset_index(inplace=True)
        df.to_html(r"C:\Users\Sam\FootballTrader v0.3.2\backtest\strategy\LTD\Validated_Strategy_Results\all_trades.html")
        df['cumsum'].plot(title='Validated General Strategy - Validate Data - P&L').legend(['Optimised LTD', 'Removed Leagues LTD', 'Validated LTD'])
        plt.savefig(r'C:\Users\Sam\FootballTrader v0.3.2\backtest\strategy\LTD\Plots\validated_LTD_strategy_validate_data_pnl.png')

        # create league perormance results
        leagues_pnl = round(df.groupby(['League'])['pnl'].sum().reset_index(), 2)
        leagues_lose = df.groupby('League')['ft_result'].apply(lambda x: (x=='draw').sum()).reset_index()
        leagues_lose.rename(columns={'ft_result': 'loss'}, inplace=True)
        leagues_win = df.groupby('League')['ft_result'].apply(lambda x: (x!='draw').sum()).reset_index()
        leagues_win.rename(columns={'ft_result': 'win'}, inplace=True)
        league_data = leagues_pnl.merge(leagues_win, on='League', how='inner').merge(leagues_lose, on='League', how='inner')
        league_data['win_rate'] = round(league_data['win']/(league_data['win'] + league_data['loss'])*100, 2)
        league_data.sort_values('pnl', ascending=False).to_html(r'C:\Users\Sam\FootballTrader v0.3.2\backtest\strategy\LTD\Validated_Strategy_Results\validated_LTD_league_performance.html')

       

        validate_data = validate_data[validate_data['League'].isin(filtered_leagues)]
        # Apply validated general strategy criteria to validate data
        df = validate_data[(validate_data['GP Avg'].astype(float) >= 8) &
                              ((validate_data['Form H v A'].astype(float) >= optimised_df.loc[idx, 'hva_pos']) | (validate_data['Form H v A'].astype(float) <= optimised_df.loc[idx, 'hva_neg'])) &  # Rel2 pos and neg min limits
                                ((validate_data['Form Goal Edge'] <= optimised_df.loc[idx, 'goal_edge_pos']) & (validate_data['Form Goal Edge'] >= optimised_df.loc[idx, 'goal_edge_neg'])) &  # Magic number range
                                (validate_data['Goals 2.5+ L8 Avg'] >= optimised_df.loc[idx, 'last8_25']) &
                                    (validate_data['Odds Betfair Draw'].astype(float) <= 5)]
        
        # Add pnl and cumsum 
        df['pnl'] = 98
        df['pnl'] = np.where(df['ft_result'] == 'draw', -100 * (df['Odds Betfair Draw'].astype(float) - 1), df['pnl'])
        df['cumsum'] = df['pnl'].cumsum()
        df.reset_index(inplace=True)
        df.to_html(r"C:\Users\Sam\FootballTrader v0.3.2\backtest\strategy\LTD\Validated_Strategy_Results\all_trades_remove_leagues.html")
        df['cumsum'].plot(title='Validated General Strategy - Validate Data - P&L').legend(['Optimised LTD', 'Removed Leagues LTD', 'Validated LTD', 'Validated Removed Leagues LTD'])
        plt.savefig(r'C:\Users\Sam\FootballTrader v0.3.2\backtest\strategy\LTD\Plots\removed_leagues_validated_LTD_strategy_validate_data_pnl.png')

        

        # create league perormance results
        leagues_pnl = round(df.groupby(['League'])['pnl'].sum().reset_index(), 2)
        leagues_lose = df.groupby('League')['ft_result'].apply(lambda x: (x=='draw').sum()).reset_index()
        leagues_lose.rename(columns={'ft_result': 'loss'}, inplace=True)
        leagues_win = df.groupby('League')['ft_result'].apply(lambda x: (x!='draw').sum()).reset_index()
        leagues_win.rename(columns={'ft_result': 'win'}, inplace=True)
        league_data = leagues_pnl.merge(leagues_win, on='League', how='inner').merge(leagues_lose, on='League', how='inner')
        league_data['win_rate'] = round(league_data['win']/(league_data['win'] + league_data['loss'])*100, 2)
        league_data.sort_values('pnl', ascending=False).to_html(r'C:\Users\Sam\FootballTrader v0.3.2\backtest\strategy\LTD\Validated_Strategy_Results\removed_leagues_validated_LTD_league_performance.html')

    def test_ltd_strategy(self, test_data, filtered_leagues, optimised_df):
        
        test_data = test_data[test_data['League'].isin(filtered_leagues)]
        # Apply validated general strategy criteria to validate data
        idx = 0
        df = test_data[(test_data['GP Avg'].astype(float) >= 8) &
                              ((test_data['Form H v A'].astype(float) >= optimised_df.loc[idx, 'hva_pos']) | (test_data['Form H v A'].astype(float) <= optimised_df.loc[idx, 'hva_neg'])) &  # Rel2 pos and neg min limits
                                ((test_data['Form Goal Edge'] <= optimised_df.loc[idx, 'goal_edge_pos']) & (test_data['Form Goal Edge'] >= optimised_df.loc[idx, 'goal_edge_neg'])) &  # Magic number range
                                (test_data['Goals 2.5+ L8 Avg'] >= optimised_df.loc[idx, 'last8_25']) &
                                    (test_data['Odds Betfair Draw'].astype(float) <= 5)]
        
        # Add pnl and cumsum 
        df['pnl'] = 98
        df['pnl'] = np.where(df['ft_result'] == 'draw', -100 * (df['Odds Betfair Draw'].astype(float) - 1), df['pnl'])
        df['cumsum'] = df['pnl'].cumsum()
        df.reset_index(inplace=True)
        df.to_html(r"C:\Users\Sam\FootballTrader v0.3.2\backtest\strategy\LTD\Test_Strategy_Results\all_trades_remove_leagues.html")
        df['cumsum'].plot(title='Validated General Strategy - Validate Data - P&L').legend(['Optimised LTD', 'Removed Leagues LTD', 'Validated LTD', 'Validated Removed Leagues LTD'])
        plt.savefig(r'C:\Users\Sam\FootballTrader v0.3.2\backtest\strategy\LTD\Plots\removed_leagues_test_LTD_strategy_validate_data_pnl.png')

        print(df)
        print(df['pnl'].sum())

        # create league perormance results
        leagues_pnl = round(df.groupby(['League'])['pnl'].sum().reset_index(), 2)
        leagues_lose = df.groupby('League')['ft_result'].apply(lambda x: (x=='draw').sum()).reset_index()
        leagues_lose.rename(columns={'ft_result': 'loss'}, inplace=True)
        leagues_win = df.groupby('League')['ft_result'].apply(lambda x: (x!='draw').sum()).reset_index()
        leagues_win.rename(columns={'ft_result': 'win'}, inplace=True)
        league_data = leagues_pnl.merge(leagues_win, on='League', how='inner').merge(leagues_lose, on='League', how='inner')
        league_data['win_rate'] = round(league_data['win']/(league_data['win'] + league_data['loss'])*100, 2)
        league_data.sort_values('pnl', ascending=False).to_html(r'C:\Users\Sam\FootballTrader v0.3.2\backtest\strategy\LTD\Test_Strategy_Results\removed_leagues_test_LTD_league_performance.html')

class BackTester_v2:
    def __init__(self):
        self.con = sqlite3.connect(autotrader_db_path, check_same_thread=False)
        df = pd.read_sql_query("SELECT * from archive_v2", self.con)

        # Set up columns required for backtesting
        df['home_ht_score'] = [int(x[0]) for x in df['ht_score'].values]
        df['away_ht_score'] = [int(x[-1]) for x in df['ht_score'].values]

        df['home_ft_score'] = [int(x[0]) for x in df['ft_score'].values]
        df['away_ft_score'] = [int(x[-1]) for x in df['ft_score'].values]

        df['total_goals'] = df['home_ft_score'] + df['away_ft_score']

        df['ht_result'] = np.where(df['home_ht_score'] == df['away_ht_score'], 'draw', 0)
        df['ht_result'] = np.where(df['home_ht_score'] > df['away_ht_score'], 'home', df['ht_result'])
        df['ht_result'] = np.where(df['home_ht_score'] < df['away_ht_score'], 'away', df['ht_result'])

        df['ft_result'] = np.where(df['home_ft_score'] == df['away_ft_score'], 'draw', 0)
        df['ft_result'] = np.where(df['home_ft_score'] > df['away_ft_score'], 'home', df['ft_result'])
        df['ft_result'] = np.where(df['home_ft_score'] < df['away_ft_score'], 'away', df['ft_result'])

        df.copy()['Odds Betfair Draw'] = np.where(df['Odds Betfair Draw'] == 'nan', np.nan, df['Odds Betfair Draw'])
        df['Odds Betfair Draw'] = np.where(df['Odds Betfair Draw'] == 'None', np.nan, df['Odds Betfair Draw'])
        df.replace('None', np.nan, inplace=True)
        df.replace('nan', np.nan, inplace=True)
        df.replace('-', np.nan, inplace=True)
        df['Odds Betfair Draw'].bfill(inplace=True)
        df['Odds Betfair Draw'].ffill(inplace=True)

        # Ensure +2.5 goal stats are same format
        df['Goals 2.5+ L8 Avg'] = df['Goals 2.5+ L8 Avg'].apply(
        lambda x: x * 100 if x < 1 else x)

        print(len(df))

        # Run backtester
        self.backtest_LTD_strategy(df)

    def backtest_LTD_strategy(self, df):

        def select_leagues(df):
            league_groups = df.groupby('League')['ft_result'].value_counts()

            # Step 1: Turn the grouped series into a DataFrame
            league_df = league_groups.reset_index(name='count')

            # Step 2: Pivot the data to get ft_result categories as columns
            pivot_df = league_df.pivot(index='League', columns='ft_result', values='count').fillna(0)

            # Step 3: Rename columns to lowercase for easier access (optional)
            pivot_df.columns = [col.lower() for col in pivot_df.columns]

            # Step 4: Create new columns: GP, Won, Draw
            pivot_df['GP'] = pivot_df.sum(axis=1)
            pivot_df['Won'] = pivot_df.get('home', 0) + pivot_df.get('away', 0)
            pivot_df['Draw'] = pivot_df.get('draw', 0)

            # Step 5: Keep only the desired columns and reset index
            final_df = pivot_df[['GP', 'Won', 'Draw']].reset_index()
            final_df['strike'] = round(final_df['Won'] / final_df['GP'] * 100, 2)
            final_df['pnl 4'] = round(final_df['Won'] * 98 - (final_df['Draw'] * 300), 2)
            final_df['pnl 3.5'] = round(final_df['Won'] * 98 - (final_df['Draw'] * 250), 2)
            final_df = final_df.loc[(final_df['GP'] > 10) & (final_df['strike'] > 76)]

            final_df.to_excel('league_strike_rate.xlsx')

            selected_leagues = final_df['League'].to_list()
            print(selected_leagues)
            print(len(selected_leagues))

            return selected_leagues

        def split_data(selected_leagues, df):
            # Filter to only slected leagues
            filtered_df = df[df['League'].isin(selected_leagues)]  # Filter data to only include selected leagues
            
            split_60_percent = round(len(filtered_df)*0.6)
            # split_80_percent = round(len(df)*0.8)
            
            train_data = filtered_df.iloc[:split_60_percent]
            validate_data = filtered_df.iloc[split_60_percent:]
            
            return train_data, validate_data
        
        def randomise_strategy_test(df, selected_leagues, optimised_df):
            
            for _ in range(100):
                # Randomise the entire data set then split data 
                rand_df = df.sample(frac=1).reset_index(drop=True)

                train_data, validate_data = split_data(selected_leagues, rand_df)
                
                # Apply general strategy to train data    
                idx = 0   
                df_train = train_data[(train_data['GP Avg'].astype(float) >= 8) &
                                    ((train_data['Form H v A'].astype(float) >= abs(optimised_df.loc[idx, 'hva'])) | (train_data['Form H v A'].astype(float) <= optimised_df.loc[idx, 'hva'])) &  # Rel2 pos and neg min limits
                                        ((train_data['Form Goal Edge'] <= abs(optimised_df.loc[idx, 'goal_edge'])) & (train_data['Form Goal Edge'] >= optimised_df.loc[idx, 'goal_edge']))  # Magic number range
                                        ]
                df_train['pnl'] = 98
                df_train['pnl'] = np.where(df_train['ft_result'] == 'draw', -100 * (df_train['Odds Betfair Draw'].astype(float) - 1), df_train['pnl'])
                df_train['cumsum'] = df_train.copy()['pnl'].cumsum()
                df_train.reset_index(inplace=True)
                df_train['cumsum'].plot(title='LTD Strategy Training Data - P&L', linewidth=0.1, color='black').legend(['Optimised LTD Strategy', 'Randomised Data'])
                plt.savefig(r'C:\Users\Sam\FootballTrader v0.3.2\backtest\strategy\LTD\Plots\optimised_LTD_strategy_random_data_pnl.png')
           
 

        def reverse_strategy(train_data, optimised_df):
            # Apply general strategy to train data    
            idx = 0   
            df_train = train_data[(train_data['GP Avg'].astype(float) >= 8) &
                                ((train_data['Form H v A'].astype(float) <= abs(optimised_df.loc[idx, 'hva'])) & (train_data['Form H v A'].astype(float) >= optimised_df.loc[idx, 'hva'])) &  # Rel2 pos and neg min limits
                                    ((train_data['Form Goal Edge'] >= abs(optimised_df.loc[idx, 'goal_edge'])) | (train_data['Form Goal Edge'] <= optimised_df.loc[idx, 'goal_edge']))  # Magic number range
                                    ]
            df_train['pnl'] = 98
            df_train['pnl'] = np.where(df_train['ft_result'] == 'draw', -100 * (df_train['Odds Betfair Draw'].astype(float) - 1), df_train['pnl'])
            df_train['cumsum'] = df_train.copy()['pnl'].cumsum()
            df_train.reset_index(inplace=True)
            
            df_train['cumsum'].plot(title='LTD Strategy Training Data - P&L', color='red', linewidth=0.5).legend(['Optimised LTD Strategy', 'Randomised Data'])
            plt.savefig(r'C:\Users\Sam\FootballTrader v0.3.2\backtest\strategy\LTD\Plots\optimised_LTD_reverse_strategy_train_data_pnl.png')

            # create league performance results
            leagues_pnl = round(df_train.groupby(['League'])['pnl'].sum().reset_index(), 2)
            leagues_lose = df_train.groupby('League')['ft_result'].apply(lambda x: (x=='draw').sum()).reset_index()
            leagues_lose.rename(columns={'ft_result': 'loss'}, inplace=True)
            leagues_win = df_train.groupby('League')['ft_result'].apply(lambda x: (x!='draw').sum()).reset_index()
            leagues_win.rename(columns={'ft_result': 'win'}, inplace=True)
            league_data = leagues_pnl.merge(leagues_win, on='League', how='inner').merge(leagues_lose, on='League', how='inner')
            league_data['win_rate'] = round(league_data['win']/(league_data['win'] + league_data['loss'])*100, 2)
            league_data.sort_values('pnl', ascending=False).to_html(r'C:\Users\Sam\FootballTrader v0.3.2\backtest\strategy\LTD\Optimised_Strategy_Results\reverse_LTD_league_performance.html')

        def optimise_strategy(train_data):
            # Set strategy criteria to be optimised 
            hva = [-5, -6, -7, -8, -9, -10, -11, -12]
            goal_edge = [-3, -4, -5]
            
            # Create list of combination lists
            combinations = list(product(hva, goal_edge))

            # Empty list to append results to display in a df 
            hva_col = []
            goal_edge_col = []
            draw_perc_col = []
            total_matches_col = []
            total_no_draw_col = []
            total_draw_col = []
            avg_loss_draw_price = []

            # Iterate through combinations for strategy criteria
            with Bar('Processing...', max=len(combinations), fill="\u26BD") as bar:
                for comb in combinations:
                    df_opt = train_data[(train_data['GP Avg'].astype(float) >= 8) &
                                        ((train_data['Form H v A'].astype(float) >= abs(comb[0])) | (train_data['Form H v A'].astype(float) <= comb[0])) &  # Rel2 pos and neg min limits
                                        ((train_data['Form Goal Edge'] <= abs(comb[1])) & (train_data['Form Goal Edge'] >= comb[1])) # Magic number range
                                        ]  # Percentage of 2.5 goals 
                    
                    # Add data to relevant column lists
                    hva_col.append(comb[0])
                    
                    goal_edge_col.append(comb[1])

                    draw_perc_col.append(df_opt['ft_result'].value_counts(normalize=True).get('draw'))
                    total_matches_col.append(len(df_opt))
                    total_no_draw_col.append(len(df_opt[(df_opt['ft_result'] != 'draw')]))
                    total_draw_col.append(len(df_opt[(df_opt['ft_result'] == 'draw')]))
                    draw_result_df = df_opt[(df_opt['ft_result'] == 'draw')]
                    draw_result_df.copy().loc[draw_result_df['Odds Betfair Draw'] == 'nan'] = np.nan
                    draw_result_df.copy()['Odds Betfair Draw'] = draw_result_df['Odds Betfair Draw'].bfill()
                    avg_loss_draw_price.append(draw_result_df['Odds Betfair Draw'].astype(float).mean(skipna=True))
                    bar.next()

            # Create df of all optimised results
            optimised_df = pd.DataFrame({'hva': hva_col, 
                                        'goal_edge': goal_edge_col,
                                        'draw_perc': draw_perc_col,
                                        'total_matches': total_matches_col,
                                        'no_draw': total_no_draw_col,
                                        'draw': total_draw_col,
                                        'avg_draw_loss_price': avg_loss_draw_price}).sort_values(by=['draw_perc'])
            
            # Calculate the pnl for all results
            optimised_df['profit'] = optimised_df['no_draw'] * 98
            optimised_df['loss'] = ((optimised_df['avg_draw_loss_price'] - 1) * 100) * optimised_df['draw']
            optimised_df['loss'].loc[optimised_df['draw'] == 0] = 0
            optimised_df['pnl'] = optimised_df['profit'] - optimised_df['loss'] 
            # optimised_df = optimised_df.loc[(optimised_df['draw_perc'] <= 0.17) & (optimised_df['total_matches'] > 200)]
            optimised_df.sort_values(by=['pnl', 'total_matches', 'draw_perc'], ascending=[False, False, True], inplace=True)  # sort df by highest pnl value

            # Save the top 1000 LTD strategy optimised results
            optimised_df.head(1000).to_html(r"C:\Users\Sam\FootballTrader v0.3.2\backtest\strategy\LTD\Optimised_Strategy_Results\optimised_LTD_strategy.html")
            optimised_df.reset_index(inplace=True)

            return optimised_df

        

        def train_strategy(train_data, optimised_df):
            # Apply general strategy to train data    
            idx = 0   
            df_train = train_data[(train_data['GP Avg'].astype(float) >= 8) &
                                ((train_data['Form H v A'].astype(float) >= abs(optimised_df.loc[idx, 'hva'])) | (train_data['Form H v A'].astype(float) <= optimised_df.loc[idx, 'hva'])) &  # Rel2 pos and neg min limits
                                    ((train_data['Form Goal Edge'] <= abs(optimised_df.loc[idx, 'goal_edge'])) & (train_data['Form Goal Edge'] >= optimised_df.loc[idx, 'goal_edge']))  # Magic number range
                                    ]
            df_train['pnl'] = 98
            df_train['pnl'] = np.where(df_train['ft_result'] == 'draw', -100 * (df_train['Odds Betfair Draw'].astype(float) - 1), df_train['pnl'])
            df_train['cumsum'] = df_train.copy()['pnl'].cumsum()
            df_train.reset_index(inplace=True)
            df_train.to_html(r"C:\Users\Sam\FootballTrader v0.3.2\backtest\strategy\LTD\Optimised_Strategy_Results\all_trades.html")
            df_train['cumsum'].plot(title='LTD Strategy Training Data - P&L').legend(['Optimised LTD Strategy'])
            plt.savefig(r'C:\Users\Sam\FootballTrader v0.3.2\backtest\strategy\LTD\Plots\optimised_LTD_strategy_train_data_pnl.png')

            # create league performance results
            leagues_pnl = round(df_train.groupby(['League'])['pnl'].sum().reset_index(), 2)
            leagues_lose = df_train.groupby('League')['ft_result'].apply(lambda x: (x=='draw').sum()).reset_index()
            leagues_lose.rename(columns={'ft_result': 'loss'}, inplace=True)
            leagues_win = df_train.groupby('League')['ft_result'].apply(lambda x: (x!='draw').sum()).reset_index()
            leagues_win.rename(columns={'ft_result': 'win'}, inplace=True)
            league_data = leagues_pnl.merge(leagues_win, on='League', how='inner').merge(leagues_lose, on='League', how='inner')
            league_data['win_rate'] = round(league_data['win']/(league_data['win'] + league_data['loss'])*100, 2)
            league_data.sort_values('pnl', ascending=False).to_html(r'C:\Users\Sam\FootballTrader v0.3.2\backtest\strategy\LTD\Optimised_Strategy_Results\optimised_LTD_league_performance.html')

        def validate_strategy(validate_data, optimised_df):
            idx=0

            # Apply validated general strategy criteria to validate data
            df = validate_data[(validate_data['GP Avg'].astype(float) >= 8) &
                                ((validate_data['Form H v A'].astype(float) >= abs(optimised_df.loc[idx, 'hva'])) | (validate_data['Form H v A'].astype(float) <= optimised_df.loc[idx, 'hva'])) &  # Rel2 pos and neg min limits
                                    ((validate_data['Form Goal Edge'] <= abs(optimised_df.loc[idx, 'goal_edge'])) & (validate_data['Form Goal Edge'] >= optimised_df.loc[idx, 'goal_edge']))  # Magic number range
                                    ]
            
            # Add pnl and cumsum 
            df['pnl'] = 98
            df['pnl'] = np.where(df['ft_result'] == 'draw', -100 * (df['Odds Betfair Draw'].astype(float) - 1), df['pnl'])
            df['cumsum'] = df['pnl'].cumsum()
            df.reset_index(inplace=True)
            df.to_html(r"C:\Users\Sam\FootballTrader v0.3.2\backtest\strategy\LTD\Validated_Strategy_Results\all_trades.html")
            df['cumsum'].plot(title='Validated General Strategy - Validate Data - P&L', color='green')
            plt.savefig(r'C:\Users\Sam\FootballTrader v0.3.2\backtest\strategy\LTD\Plots\validated_LTD_strategy_validate_data_pnl.png')

            # create league perormance results
            leagues_pnl = round(df.groupby(['League'])['pnl'].sum().reset_index(), 2)
            leagues_lose = df.groupby('League')['ft_result'].apply(lambda x: (x=='draw').sum()).reset_index()
            leagues_lose.rename(columns={'ft_result': 'loss'}, inplace=True)
            leagues_win = df.groupby('League')['ft_result'].apply(lambda x: (x!='draw').sum()).reset_index()
            leagues_win.rename(columns={'ft_result': 'win'}, inplace=True)
            league_data = leagues_pnl.merge(leagues_win, on='League', how='inner').merge(leagues_lose, on='League', how='inner')
            league_data['win_rate'] = round(league_data['win']/(league_data['win'] + league_data['loss'])*100, 2)
            league_data.sort_values('pnl', ascending=False).to_html(r'C:\Users\Sam\FootballTrader v0.3.2\backtest\strategy\LTD\Validated_Strategy_Results\validated_LTD_league_performance.html')


        selected_leagues = select_leagues(df)

        train_data, validate_data = split_data(selected_leagues, df)

        optimised_df = optimise_strategy(train_data)

        train_strategy(train_data, optimised_df)

        randomise_strategy_test(df, selected_leagues, optimised_df)

        reverse_strategy(train_data, optimised_df)

        validate_strategy(validate_data, optimised_df)


if __name__ == '__main__':
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    pd.set_option('expand_frame_repr', False)
    print('\nWelcome to Football Trader v0.3.2')
    continuous = 'off'
    run = True
    while run:
        print('\nCurrent tools available:\n- MatchFinder = mf\n- AutoTrader(WiP) = at\n- BackTester(WiP) = bt')
        print('Continuos MatchFinder setting is currently:', continuous)
        tool = input('Select Tool: ')
        if tool == 'MatchFinder' or tool == 'mf':
            
            mf = MatchFinder(continuous=continuous)
            mf.get_betfair_details()
            mf.get_sports_iq_stats()
            mf.merge_data()
            
            add_matches = input('Add matches to AutoTrader? y/n ')
            if add_matches == 'y':
                mf.add_matches_to_db()
                if input('Run AutoTrader? y/n ') == 'y':
                    if input('Turn on continuos MatchFinder? y/n ') == 'y':
                        continuos = 'on'
                    else:
                        continuos = 'off'
                    print('<AutoTrader Running>')
                    while True:
                        try:
                            at = AutoTrader()
                            at.run_autotrader(continuous=continuous)
                            break
                        except betfairlightweight.exceptions.APIError:
                            logging.exception('API ERROR')
                            print('Betfair API Error...')
                            time.sleep(10)

        if tool == 'AutoTrader'or tool == 'at':
            print('<AutoTrader Running>')
            if input('Turn on continuos MatchFinder? y/n ') == 'y':
                continuous = 'on'
            else:
                continuous = 'off'
            while True:
                try:
                    at = AutoTrader()
                    at.run_autotrader(continuous=continuous)
                    break
                except betfairlightweight.exceptions.APIError:
                    logging.exception('API ERROR')
                    print('Betfair API Error...')
                    time.sleep(10)

        if tool == 'BackTester' or tool == 'bt':
            print('<BackTester>')
            bt = BackTester_v2()

        if tool == 'exit':
            run = False
