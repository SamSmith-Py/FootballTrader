from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.edge.service import Service as EdgeService
from selenium.webdriver.edge.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

 # Initialize Selenium WebDriver with Edge options
edge_options = Options()
# edge_options.add_argument("--headless")  # Run headless Edge to avoid opening a browser window

service = EdgeService(executable_path=r'C:/Program Files (x86)/msedgedriver.exe')
driver = webdriver.Edge(service=service, options=edge_options)

# Load the login page
url = 'https://sports-iq.co.uk/login/'
driver.get(url)


# Wait for the login form to be loaded and enter login details
while True:
    try:
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

        break
    except Exception as e:
        print(f"\nAn error occurred: {e}\n")
        driver.quit()
        time.sleep(10)
        print('Re-attempting connection with driver.')  

time.sleep(10)

# <button class="btn btn-secondary buttons-excel buttons-html5 ms-3 btn-sm btn-outline-default" tabindex="0" aria-controls="fixtures_table" type="button"><span>Excel</span></button>