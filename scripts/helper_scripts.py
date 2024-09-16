import gspread
import json
import pandas as pd
import requests
import streamlit as st
import time
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe
from playwright.async_api import async_playwright
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
    print("Starting token retrieval...")
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
        print("Navigating to LinkedIn login page...")
        driver.get("https://www.linkedin.com")
        time.sleep(2)
        print("Setting 'li_at' cookie...")
        driver.add_cookie({
            'name': 'li_at',
            'value': li_at,
            'domain': '.linkedin.com'
        })
        print("'li_at' cookie set successfully.")
        
        print("Navigating to LinkedIn Sales home...")
        driver.get("https://www.linkedin.com/sales/home")
        time.sleep(5)

        print("Navigating to LinkedIn sales search page...")
        driver.get("https://www.linkedin.com/sales/search/people?query=(filters%3AList((type%3ACURRENT_COMPANY%2Cvalues%3AList((id%3Aurn%253Ali%253Aorganization%253A18875652%2CselectionType%3AINCLUDED)))))")
        time.sleep(10)

        print("Retrieving performance logs for CSRF token...")
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
        if not csrf_token:
            print("CSRF token not found.")

        print("Extracting cookies...")
        all_cookies = driver.get_cookies()
        cookies = {cookie['name']: cookie['value'] for cookie in all_cookies}
        JSESSIONID = cookies.get('JSESSIONID')
        li_a = cookies.get('li_a')

        print(f"JSESSIONID: {JSESSIONID}")
        print(f"li_a: {li_a}")

        print("Token retrieval completed successfully.")
        driver.quit()
        return JSESSIONID, li_a, csrf_token, cookies

    except Exception as e:
        print(f"An error occurred: {e}")
        driver.quit()
        return None, None, None, None
async def retrieve_tokens(li_at):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        await context.add_cookies([{
            'name': 'li_at',
            'value': li_at,
            'domain': '.linkedin.com',
            'path': '/',
            'secure': True,
            'httpOnly': True,
            'sameSite': 'None'
        }])
        page = await context.new_page()
        csrf_token = None
        async def log_request(request):
            nonlocal csrf_token
            if request.url.startswith('https://www.linkedin.com/sales-api/salesApiAccess'):
                csrf_token = request.headers.get('csrf-token')
        page.on('request', log_request)
        try:
            await page.goto('https://www.linkedin.com/sales/home')
        except Exception:
            await browser.close()
            print('The li_at cookie was misspelled or has expired. Please correct it and try again.')
            raise 'The li_at cookie was misspelled or has expired. Please correct it and try again.'
            return None, None, None, None
        await page.wait_for_timeout(5000)
        try:
            await page.goto('https://www.linkedin.com/sales/search/people?query=(filters%3AList((type%3ACURRENT_COMPANY%2Cvalues%3AList((id%3Aurn%253Ali%253Aorganization%253A18875652%2CselectionType%3AINCLUDED)))))')
        except Exception:
            await browser.close()
            print('The li_at cookie was misspelled or has expired. Please correct it and try again.')
            raise 'The li_at cookie was misspelled or has expired. Please correct it and try again.'
            return None, None, None, None
        await page.wait_for_timeout(10000)
        cookies = await context.cookies()
        cookies_dict = {}
        JSESSIONID = None
        li_a = None
        for cookie in cookies:
            cookies_dict[cookie['name']] = cookie['value']
            if cookie['name'] == 'JSESSIONID':
                JSESSIONID = cookie['value']
            elif cookie['name'] == 'li_a':
                li_a = cookie['value']
        await browser.close()
        return JSESSIONID, li_a, csrf_token, cookies_dict
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