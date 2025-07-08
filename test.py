from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.edge.service import Service as EdgeService
from selenium.webdriver.edge.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import os
import betfairlightweight


def get_sports_iq_stats():
    # Initialize Selenium WebDriver with Edge options
    edge_options = Options()
    # edge_options.add_argument("--headless=new")  # or "--headless" if "new" doesn't work
    # edge_options.add_argument("--disable-gpu")   # (optional) improves compatibility
    # edge_options.add_argument("--window-size=1920,1080")  # needed if elements aren't visible

    
    # Delete file download if it already exists
    data_file_path = r"C:\Users\Sam\FootballTrader v0.3.2\Football Data Fixtures.xlsx"
    if os.path.exists(data_file_path):
        os.remove(data_file_path)


    prefs = {
            "download.default_directory": r"C:\Users\Sam\FootballTrader v0.3.2",  # Change to your desired directory
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

"""# Connect to Betfair API
api = betfairlightweight.APIClient('smudge2049', 'Dex17@£141117', '4oAYsDJiYA7P5Wej')
api.login_interactive()
cleared = api.betting.list_cleared_orders(market_ids=[1.245350965], group_by='MARKET',
                                                      lightweight=True)
print(cleared)
c = cleared.orders[0].profit
print(c)"""
