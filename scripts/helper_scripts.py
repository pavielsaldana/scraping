import gspread
import pandas as pd
import requests
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe
from playwright.async_api import async_playwright

def safe_extract(data, *keys):
    try:
        for key in keys:
            data = data[key]
        return data
    except (KeyError, IndexError, TypeError):
        return None
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
def check_zenrows_usage(api_key):
    headers = {
        'X-API-Key': api_key,
    }
    try:
        response = requests.get('https://api.zenrows.com/v1/subscriptions/self/details', headers=headers)
        response.raise_for_status()
        response_json = response.json()
        print("Credits already used: " + str(response_json['usage']))
        print("Credits already used percentage: " + str(response_json['usage_percent']))
    except requests.RequestException as e:
        print(f"Error fetching ZenRows usage details: {e}")