import gspread
import json
import pandas as pd
import requests
import streamlit as st
import time
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium_stealth import stealth
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.core.os_manager import ChromeType

def safe_extract(data, *keys):
    try:
        for key in keys:
            data = data[key]
        return data
    except (KeyError, IndexError, TypeError):
        return None
def get_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--disable-gpu")
    options.add_argument("--headless")
    options.add_argument("--window-size=1920x1080")
    options.add_argument("--lang=en-US")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.set_capability('goog:loggingPrefs', {'performance': 'ALL'})
    return webdriver.Chrome(
        service=Service(
            ChromeDriverManager(chrome_type=ChromeType.CHROMIUM).install()
        ),
        options=options,
    )
def retrieve_tokens_selenium(li_at):
    driver = get_driver()
    stealth(driver,
        languages=["en-US", "en"],
        vendor="Google Inc.",
        platform="Win32",
        webgl_vendor="Intel Inc.",
        renderer="Intel Iris OpenGL Engine",
        fix_hairline=True,
        )
    try:
        driver.get("https://www.linkedin.com")
        time.sleep(2)
        driver.add_cookie({
            'name': 'li_at',
            'value': li_at,
            'domain': '.linkedin.com'
        })
        driver.get("https://www.linkedin.com/sales/home")
        time.sleep(5)
        driver.get("https://www.linkedin.com/sales/search/people?query=(filters%3AList((type%3ACURRENT_COMPANY%2Cvalues%3AList((id%3Aurn%253Ali%253Aorganization%253A18875652%2CselectionType%3AINCLUDED)))))")
        time.sleep(10)
        logs = driver.get_log('performance')
        csrf_token = None
        for entry in logs:
            log = json.loads(entry['message'])['message']
            if log['method'] == 'Network.requestWillBeSent':
                request_url = log['params']['request']['url']
                if request_url.startswith("https://www.linkedin.com/sales-api/salesApiAccess"):
                    csrf_token = log['params']['request']['headers'].get('Csrf-Token')
                    print(f"CSRF token found: {csrf_token}")
                    break
        all_cookies = driver.get_cookies()
        cookies = {cookie['name']: cookie['value'] for cookie in all_cookies}
        JSESSIONID = cookies.get('JSESSIONID')
        li_a = cookies.get('li_a')
        driver.quit()
        return JSESSIONID, li_a, csrf_token, cookies
    except Exception as e:
        print(f"An error occurred: {e}")
        driver.quit()
        return None, None, None, None
def write_into_spreadsheet(spreadsheet_url, sheet_name, dataframe, key_dict):
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    credentials = Credentials.from_service_account_info(key_dict, scopes=scope)
    client = gspread.authorize(credentials)
    spreadsheet = client.open_by_url(spreadsheet_url)
    worksheet = spreadsheet.worksheet(sheet_name)
    set_with_dataframe(worksheet, dataframe, include_index=False, resize=True, allow_formulas=True)
def write_into_csv(dataframe, file_name):
    dataframe.to_csv(f'{file_name}.csv',index=False,escapechar='\\')
def retrieve_spreadsheet(spreadsheet_url, sheet_name, key_dict):
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    credentials = Credentials.from_service_account_info(key_dict, scopes=scope)
    client = gspread.authorize(credentials)
    try:
        spreadsheet = client.open_by_url(spreadsheet_url)
        worksheet = spreadsheet.worksheet(sheet_name)
        data = worksheet.get_all_values()
        dataframe = pd.DataFrame(data[1:], columns=data[0])
        return dataframe
    except PermissionError:
        print("Incorrect spreadsheet URL or missing share with kalungi-google-colab@invertible-now-393117.iam.gserviceaccount.com. Please correct it and try again.")
        return None
    except gspread.exceptions.WorksheetNotFound:
        print(f"Incorrect or non-existent sheet name. Please correct it and try again.")
        return None
    except gspread.exceptions.SpreadsheetNotFound:
        print("Incorrect or non-existent spreadsheet URL. Please correct it and try again.")
        return None
def check_zenrows_usage(api_key, streamlit_execution):
    headers = {
        'X-API-Key': api_key,
    }
    try:
        response = requests.get('https://api.zenrows.com/v1/subscriptions/self/details', headers=headers)
        response.raise_for_status()
        response_json = response.json()
        #--STREAMLIT--#
        if streamlit_execution:
            st.write("Credits already used: " + str(response_json['usage']))
            st.write("Credits already used percentage: " + str(response_json['usage_percent']))
        #--STREAMLIT--#
        print("Credits already used: " + str(response_json['usage']))
        print("Credits already used percentage: " + str(response_json['usage_percent']))
    except requests.RequestException as e:        
        print(f"Error fetching ZenRows usage details: {e}")