import calendar
import os
import pandas as pd
import re
import requests
import sys
sys.path.append(os.path.abspath('../scripts/helper_scripts'))
from scripts.helper_scripts import *
import time
import tldextract
import urllib.parse
from datetime import datetime, timezone
from operator import itemgetter
from tqdm import tqdm
from urllib.parse import unquote, urlparse

def sales_navigator_lead_export(li_at, JSESSIONID, li_a, csrf_token, dataframe, column_name, max_pages=26, streamlit_execution=False):
    cookies = {
        'li_at': li_at,
        'JSESSIONID': JSESSIONID,
        'li_a': li_a,
    }
    headers = {
        'csrf-token': csrf_token,
        'referer': 'https://www.linkedin.com/sales/search/people',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
        'x-restli-protocol-version': '2.0.0',
    }
    def format_duration(years, months):
        year_suffix = " year" if years == 1 else " years"
        month_suffix = " month" if months == 1 else " months"
        formatted_values = [f"{years}{year_suffix}", f"{months}{month_suffix}"]
        return ' '.join(formatted_values) + ' in role' if formatted_values else None
    def build_logo_url(contact, index):
        root_url = safe_extract(contact, 'currentPositions', 0, 'companyUrnResolutionResult', 'companyPictureDisplayImage', 'rootUrl')
        segment = safe_extract(contact, 'currentPositions', 0, 'companyUrnResolutionResult', 'companyPictureDisplayImage', 'artifacts', index, 'fileIdentifyingUrlPathSegment')
        return root_url + segment if root_url and segment else None
    def extract_company_urn(contact):
        urn = safe_extract(contact, 'currentPositions', 0, 'companyUrn')
        return re.search(r'\d+', urn).group() if urn and re.search(r'\d+', urn) else None
    def format_start_date(contact):
        month = safe_extract(contact, 'currentPositions', 0, 'startedOn', 'month')
        year = safe_extract(contact, 'currentPositions', 0, 'startedOn', 'year')
        if month and year:
            month_name = calendar.month_abbr[month]
            return f"{month_name} {year}"
        return str(year) if year else None
    def extract_entity_urn(contact):
        urn = safe_extract(contact, 'entityUrn')
        return re.search(r'\(([^,]+)', urn).group(1) if urn and re.search(r'\(([^,]+)', urn) else None
    def map_degree(degree):
        mapping = {3: "3rd", 2: "2nd", 1: "1st", -1: "Out of network"}
        return mapping.get(degree, degree)
    def build_linkedin_profile_url(contact):
        urn = extract_entity_urn(contact)
        return f'https://www.linkedin.com/in/{urn}/' if urn else None
    def build_profile_url(contact):
        urn = extract_entity_urn(contact)
        return f'https://www.linkedin.com/sales/lead/{urn},' if urn else None
    def build_company_url(contact):
        company_urn = extract_company_urn(contact)
        return f'https://www.linkedin.com/sales/company/{company_urn}/' if company_urn else None
    def build_regular_company_url(contact):
        company_urn = extract_company_urn(contact)
        return f'https://www.linkedin.com/company/{company_urn}/' if company_urn else None    
    dataframe.drop_duplicates(subset=[column_name], inplace=True)
    columnName_values = dataframe[column_name].tolist()
    print("Sales Navigator lead export")
    progress_bar = tqdm(total=len(columnName_values))
    df_final = pd.DataFrame()
    too_many_requests = False
    #--STREAMLIT--#
    if streamlit_execution:
        st.write("---Sales Navigator lead search export---")
        progress_bar_sales_navigator_lead_export = st.progress(0)
        number_iterations = len(columnName_values)
        index_steamlit = 0
    #--STREAMLIT--#
    for index, value in enumerate(columnName_values):
        if index != 0 and index % 300 == 0:
            print("Waiting for 90 seconds...")
            time.sleep(90)
        if too_many_requests:
            break
        query_pattern = re.compile(r'\#(?=query=)')
        corrected_value = query_pattern.sub('?', value)
        query_params = unquote(urlparse(corrected_value).query)
        request_url = f'https://www.linkedin.com/sales-api/salesApiLeadSearch?q=searchQuery&{query_params}&start=0&count=100&decorationId=com.linkedin.sales.deco.desktop.searchv2.LeadSearchResult-14'
        loop_counter = 0
        page_counter = 0
        while True:
            response = requests.get(url=request_url, cookies=cookies, headers=headers)
            status_code = response.status_code
            if status_code == 400:
                df_final = pd.concat([df_final, pd.DataFrame({'query': [value], 'error': ['Wrong query']})])
                break
            if status_code == 429:
                print("Too many requests!")
                too_many_requests = True
                break
            if status_code == 200:
                data = response.json()
                totalDisplayCount = data.get('metadata', {}).get('totalDisplayCount', None)
                try:
                    totalDisplayCount = int(totalDisplayCount)
                except ValueError:
                    totalDisplayCount = 2500
                if not totalDisplayCount:
                    df_final = pd.concat([df_final, pd.DataFrame({'query': [value], 'error': ['No result found']})])
                    break
                contact_details = safe_extract(data['elements'])
                for contact in contact_details:
                    contact_info = {
                        'query': value,
                        'error': None,
                        'contact_lastName': safe_extract(contact, 'lastName'),
                        'contact_geoRegion': safe_extract(contact, 'geoRegion'),
                        'contact_openLink': safe_extract(contact, 'openLink'),
                        'contact_premium': safe_extract(contact, 'premium'),
                        'durationInRole': format_duration(
                            safe_extract(contact, 'currentPositions', 0, 'tenureAtPosition', 'numYears'),
                            safe_extract(contact, 'currentPositions', 0, 'tenureAtPosition', 'numMonths')
                        ),
                        'currentPositions_companyName': safe_extract(contact, 'currentPositions', 0, 'companyName'),
                        'currentPositions_title': safe_extract(contact, 'currentPositions', 0, 'title'),
                        'currentPositions_companyUrnResolutionResult_name': safe_extract(contact, 'currentPositions', 0, 'companyUrnResolutionResult', 'name'),
                        'logo200x200': build_logo_url(contact, 0),
                        'logo100x100': build_logo_url(contact, 1),
                        'logo400x400': build_logo_url(contact, 2),
                        'currentPositions_companyUrnResolutionResult_location': safe_extract(contact, 'currentPositions', 0, 'companyUrnResolutionResult', 'location'),
                        'currentPositions_companyUrn': extract_company_urn(contact),
                        'currentPositions_current': safe_extract(contact, 'currentPositions', 0, 'current'),
                        'durationInCompany': format_duration(
                            safe_extract(contact, 'currentPositions', 0, 'tenureAtCompany', 'numYears'),
                            safe_extract(contact, 'currentPositions', 0, 'tenureAtCompany', 'numMonths')
                        ),
                        'startDate': format_start_date(contact),
                        'contact_entityUrn': extract_entity_urn(contact),
                        'contact_degree': map_degree(safe_extract(contact, 'degree')),
                        'contact_fullName': safe_extract(contact, 'fullName'),
                        'contact_firstName': safe_extract(contact, 'firstName'),
                        'linkedInProfileUrl': build_linkedin_profile_url(contact),
                        'profileUrl': build_profile_url(contact),
                        'companyUrl': build_company_url(contact),
                        'regularCompanyUrl': build_regular_company_url(contact),
                        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
                    df_final = pd.concat([df_final, pd.DataFrame(contact_info, index=[0])])
                page_counter += 1
                if page_counter >= max_pages:
                    break
                if totalDisplayCount == 2500 and loop_counter >= 25:
                    break
                if int(urlparse(request_url).query.split('&start=')[1].split('&')[0]) + 100 < data['paging']['total']:
                    start_val = int(urlparse(request_url).query.split('&start=')[1].split('&')[0]) + 100
                    request_url = request_url.replace(f'start={start_val - 100}', f'start={start_val}')
                    loop_counter += 1
                else:
                    break
        index += 1
        progress_bar.update(1)
        #--STREAMLIT--#
        if streamlit_execution:
            index_steamlit += 1
            progress_bar_sales_navigator_lead_export.progress(index_steamlit / number_iterations)
        #--STREAMLIT--#
    progress_bar.close()
    columns_rename = {
        'contact_lastName': 'lastName',
        'contact_geoRegion': 'location',
        'contact_openLink': 'isOpenLink',
        'contact_premium': 'isPremium',
        'currentPositions_companyName': 'companyName',
        'currentPositions_title': 'title',
        'currentPositions_companyUrnResolutionResult_name': 'linkedinCompanyName',
        'currentPositions_companyUrnResolutionResult_location': 'companyLocation',
        'currentPositions_companyUrn': 'companyId',
        'currentPositions_current': 'isCurrent',
        'startDate': 'startDateInRole',
        'contact_entityUrn': 'vmid',
        'contact_degree': 'connectionDegree',
        'contact_fullName': 'fullName',
        'contact_firstName': 'firstName'
    }
    df_final.rename(columns=columns_rename, inplace=True)
    columns_desired = ['query', 'error', 'profileUrl', 'linkedInProfileUrl', 'vmid', 'firstName', 'lastName', 'fullName', 'location', 'title', 'companyId', 'companyUrl', 'regularCompanyUrl', 'companyName', 'linkedinCompanyName', 'startDateInRole', 'durationInRole', 'durationInCompany', 'companyLocation', 'logo100x100', 'logo200x200', 'logo400x400', 'connectionDegree', 'isCurrent', 'isOpenLink', 'isPremium', 'timestamp']
    df_final = df_final.reindex(columns=columns_desired)
    df_final = df_final[columns_desired]
    return df_final
def sales_navigator_account_export(li_at, JSESSIONID, li_a, csrf_token, dataframe, column_name, max_pages=17, streamlit_execution=False):
    cookies = {
        'li_at': li_at,
        'JSESSIONID': JSESSIONID,
        'li_a': li_a,
    }
    headers = {
        'csrf-token': csrf_token,
        'referer': 'https://www.linkedin.com/sales/search/people',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
        'x-restli-protocol-version': '2.0.0',
    }
    def build_logo_url(contact, index):
        root_url = safe_extract(contact, 'companyPictureDisplayImage', 'rootUrl')
        segment = safe_extract(contact, 'companyPictureDisplayImage', 'artifacts', index, 'fileIdentifyingUrlPathSegment')
        return root_url + segment if root_url and segment else None
    def extract_company_urn(contact):
        urn = safe_extract(contact, 'entityUrn')
        return re.search(r'\d+', urn).group() if urn and re.search(r'\d+', urn) else None
    def build_company_url(contact):
        company_urn = extract_company_urn(contact)
        return f'https://www.linkedin.com/sales/company/{company_urn}/' if company_urn else None
    def build_regular_company_url(contact):
        company_urn = extract_company_urn(contact)
        return f'https://www.linkedin.com/company/{company_urn}/' if company_urn else None
    dataframe.drop_duplicates(subset=[column_name], inplace=True)
    columnName_values = dataframe[column_name].tolist()
    print("Sales Navigator account export")
    progress_bar = tqdm(total=len(columnName_values))
    df_final = pd.DataFrame()
    too_many_requests = False
    #--STREAMLIT--#
    if streamlit_execution:
        st.write("---Sales Navigator account export---")
        progress_bar_sales_navigator_account_export = st.progress(0)
        number_iterations = len(columnName_values)
        index_steamlit = 0
    #--STREAMLIT--#
    for index, value in enumerate(columnName_values):
        if index != 0 and (index % 300 == 0 or index % 299 == 0):
            print("Waiting for 90 seconds...")
            time.sleep(90)
        if too_many_requests:
            break
        query_pattern = re.compile(r'\#(?=query=)')
        corrected_value = query_pattern.sub('?', value)
        query_params = unquote(urlparse(corrected_value).query)
        request_url = f'https://www.linkedin.com/sales-api/salesApiAccountSearch?q=searchQuery&{query_params}&start=0&count=100&decorationId=com.linkedin.sales.deco.desktop.searchv2.AccountSearchResult-4'
        loop_counter = 0
        page_counter = 0
        while True:
            response = requests.get(url=request_url, cookies=cookies, headers=headers)
            status_code = response.status_code
            if status_code == 400:
                df_final = pd.concat([df_final, pd.DataFrame({'query': [value], 'error': ['Wrong query']})])
                break
            if status_code == 429:
                print("Too many requests!")
                too_many_requests = True
                break
            if status_code == 200:
                data = response.json()
                total_display_count = data.get('metadata', {}).get('totalDisplayCount', None)
                try:
                    total_display_count = int(total_display_count)
                except ValueError:
                    total_display_count = 1600
                if not total_display_count:
                    df_final = pd.concat([df_final, pd.DataFrame({'query': [value], 'error': ['No result found']})])
                    break
                account_details = safe_extract(data, 'elements')
                if not account_details:
                    account_details = []
                for account in account_details:
                    account_info = {
                        'query': value,
                        'error': None,
                        'companyName': safe_extract(account, 'companyName'),
                        'description': safe_extract(account, 'description'),
                        'industry': safe_extract(account, 'industry'),
                        'employeeCountRange': safe_extract(account, 'employeeCountRange'),
                        'employeesCount': safe_extract(account, 'employeeDisplayCount'),
                        'companyId': extract_company_urn(account),
                        'companyUrl': build_company_url(account),
                        'regularCompanyUrl': build_regular_company_url(account),
                        'logo200x200': build_logo_url(account, 0),
                        'logo100x100': build_logo_url(account, 1),
                        'logo400x400': build_logo_url(account, 2),
                        'isHiring': False,
                        'had2SeniorLeadershipChanges': False,
                        'has1Connection': False,
                        'hadFundingEvent': False,
                        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }
                    spotlight_badges = safe_extract(account, 'spotlightBadges')
                    if not spotlight_badges:
                        spotlight_badges = []
                    for badge in spotlight_badges:
                        badge_id = safe_extract(badge, 'id')
                        if badge_id == 'FIRST_DEGREE_CONNECTION':
                            account_info['has1Connection'] = True
                        if badge_id == 'HIRING_ON_LINKEDIN':
                            account_info['isHiring'] = True
                        if badge_id == 'SENIOR_LEADERSHIP_CHANGE':
                            account_info['had2SeniorLeadershipChanges'] = True
                        if badge_id == 'RECENT_FUNDING_EVENT':
                            account_info['hadFundingEvent'] = True
                    df_final = pd.concat([df_final, pd.DataFrame(account_info, index=[0])])
                page_counter += 1
                if page_counter >= max_pages:
                    break
                if total_display_count == 1600 and loop_counter >= 16:
                    break
                start_val = int(urlparse(request_url).query.split('&start=')[1].split('&')[0]) + 100
                if start_val < data['paging']['total']:
                    request_url = request_url.replace(f'start={start_val - 100}', f'start={start_val}')
                    loop_counter += 1
                else:
                    break
        progress_bar.update(1)
        progress_bar.refresh()
        #--STREAMLIT--#
        if streamlit_execution:
            index_steamlit += 1
            progress_bar_sales_navigator_account_export.progress(index_steamlit / number_iterations)
        #--STREAMLIT--#
    progress_bar.close()
    columns_desired = ['query', 'error', 'companyUrl', 'companyName', 'description', 'companyId', 'regularCompanyUrl', 'industry', 'employeesCount', 'employeeCountRange', 'logo100x100', 'logo200x200', 'logo400x400', 'isHiring', 'had2SeniorLeadershipChanges', 'has1Connection', 'hadFundingEvent', 'timestamp']
    df_final = df_final.reindex(columns=columns_desired)
    df_final = df_final[columns_desired]
    return df_final
def linkedin_account(li_at, JSESSIONID, li_a, csrf_token, dataframe, column_name, cookies_dict, location_count=100, streamlit_execution=False):
    headers = {
        'csrf-token': csrf_token,
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
        'x-restli-protocol-version': '2.0.0',
        "accept-language": "en-AU,en-GB;q=0.9,en-US;q=0.8,en;q=0.7",
        "x-li-lang": "en_US",
    }
    def get_company(data):
        if not data or "elements" not in data or not data["elements"]:
            return {}
        company = data["elements"][0]
        return company
    def extract_from_pattern(value, pattern):
        match = pattern.search(value)
        return match.group(1) if match else None
    def create_salesNavigatorLink(mainCompanyID):
        return f"https://www.linkedin.com/sales/company/{mainCompanyID}/" if mainCompanyID else None
    def extract_domain_from_website(website):
        try:
            return tldextract.extract(website).registered_domain
        except (ValueError, AttributeError):
            return None
    def safe_str(value):
        return str(value) if value is not None else None
    def get_full_name(mapping, key):
        return mapping.get(key, key)
    def extract_hashtag(value):
        parts = value.split(":")
        return parts[-1] if len(parts) > 0 else None
    dict_geographicArea = {'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas', 'CA': 'California', 'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware', 'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii', 'ID': 'Idaho', 'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas', 'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland', 'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi', 'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada', 'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico', 'NY': 'New York', 'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma', 'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina', 'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah', 'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia', 'WI': 'Wisconsin', 'WY': 'Wyoming'}
    '''
    Last time updated (05/08/2023)
    https://learn.microsoft.com/en-us/linkedin/shared/references/reference-tables/country-codes
    '''
    dict_country = {'AD': 'Andorra', 'AE': 'United Arab Emirates', 'AF': 'Afghanistan', 'AG': 'Antigua and Barbuda', 'AI': 'Anguilla', 'AL': 'Albania', 'AM': 'Armenia', 'AN': 'Netherlands Antilles', 'AO': 'Angola', 'AQ': 'Antarctica', 'AR': 'Argentina', 'AS': 'American Samoa', 'AT': 'Austria', 'AU': 'Australia', 'AW': 'Aruba', 'AX': 'Aland Islands', 'AZ': 'Azerbaijan', 'BA': 'Bosnia and Herzegovina', 'BB': 'Barbados', 'BD': 'Bangladesh', 'BE': 'Belgium', 'BF': 'Burkina Faso', 'BG': 'Bulgaria', 'BH': 'Bahrain', 'BI': 'Burundi', 'BJ': 'Benin', 'BM': 'Bermuda', 'BN': 'Brunei Darussalam', 'BO': 'Bolivia', 'BR': 'Brazil', 'BS': 'Bahamas', 'BT': 'Bhutan', 'BV': 'Bouvet Island', 'BW': 'Botswana', 'BY': 'Belarus', 'BZ': 'Belize', 'CA': 'Canada', 'CB': 'Caribbean Nations', 'CC': 'Cocos (Keeling) Islands', 'CD': 'Democratic Republic of the Congo', 'CF': 'Central African Republic', 'CG': 'Congo', 'CH': 'Switzerland', 'CI': "Cote D'Ivoire (Ivory Coast)", 'CK': 'Cook Islands', 'CL': 'Chile', 'CM': 'Cameroon', 'CN': 'China', 'CO': 'Colombia', 'CR': 'Costa Rica', 'CS': 'Serbia and Montenegro', 'CU': 'Cuba', 'CV': 'Cape Verde', 'CX': 'Christmas Island', 'CY': 'Cyprus', 'CZ': 'Czech Republic', 'DE': 'Germany', 'DJ': 'Djibouti', 'DK': 'Denmark', 'DM': 'Dominica', 'DO': 'Dominican Republic', 'DZ': 'Algeria', 'EC': 'Ecuador', 'EE': 'Estonia', 'EG': 'Egypt', 'EH': 'Western Sahara', 'ER': 'Eritrea', 'ES': 'Spain', 'ET': 'Ethiopia', 'FI': 'Finland', 'FJ': 'Fiji', 'FK': 'Falkland Islands (Malvinas)', 'FM': 'Federated States of Micronesia', 'FO': 'Faroe Islands', 'FR': 'France', 'FX': 'France, Metropolitan', 'GA': 'Gabon', 'GB': 'United Kingdom', 'GD': 'Grenada', 'GE': 'Georgia', 'GF': 'French Guiana', 'GH': 'Ghana', 'GI': 'Gibraltar', 'GL': 'Greenland', 'GM': 'Gambia', 'GN': 'Guinea', 'GP': 'Guadeloupe', 'GQ': 'Equatorial Guinea', 'GR': 'Greece', 'GS': 'S. Georgia and S. Sandwich Islands', 'GT': 'Guatemala', 'GU': 'Guam', 'GW': 'Guinea-Bissau', 'GY': 'Guyana', 'HK': 'Hong Kong', 'HM': 'Heard Island and McDonald Islands', 'HN': 'Honduras', 'HR': 'Croatia', 'HT': 'Haiti', 'HU': 'Hungary', 'ID': 'Indonesia', 'IE': 'Ireland', 'IL': 'Israel', 'IN': 'India', 'IO': 'British Indian Ocean Territory', 'IQ': 'Iraq', 'IR': 'Iran', 'IS': 'Iceland', 'IT': 'Italy', 'JM': 'Jamaica', 'JO': 'Jordan', 'JP': 'Japan', 'KE': 'Kenya', 'KG': 'Kyrgyzstan', 'KH': 'Cambodia', 'KI': 'Kiribati', 'KM': 'Comoros', 'KN': 'Saint Kitts and Nevis', 'KP': 'Korea (North)', 'KR': 'Korea', 'KW': 'Kuwait', 'KY': 'Cayman Islands', 'KZ': 'Kazakhstan', 'LA': 'Laos', 'LB': 'Lebanon', 'LC': 'Saint Lucia', 'LI': 'Liechtenstein', 'LK': 'Sri Lanka', 'LR': 'Liberia', 'LS': 'Lesotho', 'LT': 'Lithuania', 'LU': 'Luxembourg', 'LV': 'Latvia', 'LY': 'Libya', 'MA': 'Morocco', 'MC': 'Monaco', 'MD': 'Moldova', 'MG': 'Madagascar', 'MH': 'Marshall Islands', 'MK': 'Macedonia', 'ML': 'Mali', 'MM': 'Myanmar', 'MN': 'Mongolia', 'MO': 'Macao', 'MP': 'Northern Mariana Islands', 'MQ': 'Martinique', 'MR': 'Mauritania', 'MS': 'Montserrat', 'MT': 'Malta', 'MU': 'Mauritius', 'MV': 'Maldives', 'MW': 'Malawi', 'MX': 'Mexico', 'MY': 'Malaysia', 'MZ': 'Mozambique', 'NA': 'Namibia', 'NC': 'New Caledonia', 'NE': 'Niger', 'NF': 'Norfolk Island', 'NG': 'Nigeria', 'NI': 'Nicaragua', 'NL': 'Netherlands', 'NO': 'Norway', 'NP': 'Nepal', 'NR': 'Nauru', 'NU': 'Niue', 'NZ': 'New Zealand', 'OM': 'Sultanate of Oman', 'OO': 'Other', 'PA': 'Panama', 'PE': 'Peru', 'PF': 'French Polynesia', 'PG': 'Papua New Guinea', 'PH': 'Philippines', 'PK': 'Pakistan', 'PL': 'Poland', 'PM': 'Saint Pierre and Miquelon', 'PN': 'Pitcairn', 'PR': 'Puerto Rico', 'PS': 'Palestinian Territory', 'PT': 'Portugal', 'PW': 'Palau', 'PY': 'Paraguay', 'QA': 'Qatar', 'RE': 'Reunion', 'RO': 'Romania', 'RU': 'Russian Federation', 'RW': 'Rwanda', 'SA': 'Saudi Arabia', 'SB': 'Solomon Islands', 'SC': 'Seychelles', 'SD': 'Sudan', 'SE': 'Sweden', 'SG': 'Singapore', 'SH': 'Saint Helena', 'SI': 'Slovenia', 'SJ': 'Svalbard and Jan Mayen', 'SK': 'Slovak Republic', 'SL': 'Sierra Leone', 'SM': 'San Marino', 'SN': 'Senegal', 'SO': 'Somalia', 'SR': 'Suriname', 'ST': 'Sao Tome and Principe', 'SV': 'El Salvador', 'SY': 'Syria', 'SZ': 'Swaziland', 'TC': 'Turks and Caicos Islands', 'TD': 'Chad', 'TF': 'French Southern Territories', 'TG': 'Togo', 'TH': 'Thailand', 'TJ': 'Tajikistan', 'TK': 'Tokelau', 'TL': 'Timor-Leste', 'TM': 'Turkmenistan', 'TN': 'Tunisia', 'TO': 'Tonga', 'TP': 'East Timor', 'TR': 'Turkey', 'TT': 'Trinidad and Tobago', 'TV': 'Tuvalu', 'TW': 'Taiwan', 'TZ': 'Tanzania', 'UA': 'Ukraine', 'UG': 'Uganda', 'US': 'United States', 'UY': 'Uruguay', 'UZ': 'Uzbekistan', 'VA': 'Vatican City State (Holy See)', 'VC': 'Saint Vincent and the Grenadines', 'VE': 'Venezuela', 'VG': 'Virgin Islands (British)', 'VI': 'Virgin Islands (U.S.)', 'VN': 'Vietnam', 'VU': 'Vanuatu', 'WF': 'Wallis and Futuna', 'WS': 'Samoa', 'YE': 'Yemen', 'YT': 'Mayotte', 'YU': 'Yugoslavia', 'ZA': 'South Africa', 'ZM': 'Zambia', 'ZW': 'Zimbabwe'}
    def fetch_linkedin_insights(mainCompanyID, max_retries=3, retry_delay=5):
        if mainCompanyID is None:
            return {}
        for attempt in range(max_retries):
            with requests.session() as session:
                session.cookies['li_at'] = li_at
                session.cookies["JSESSIONID"] = JSESSIONID
                session.headers.update(headers)
                session.headers["csrf-token"] = JSESSIONID.strip('"')
                params = {'q': 'company', 'company': 'urn:li:fsd_company:' + mainCompanyID, 'decorationId': 'com.linkedin.voyager.dash.premium.companyinsights.CompanyInsightsCardCollection-12'}
                try:
                    response = session.get(insights_url, params=params)
                    response.raise_for_status()
                    return response.json()
                except (requests.ConnectionError, requests.Timeout) as e:
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
                        continue
                    else:
                        return {}
                except Exception as e:
                    return {}
        return {}
    def format_growth(percentage):
        if percentage is None:
            return None
        if percentage > 0:
            return f"{percentage}% increase"
        elif percentage < 0:
            return f"{percentage}% decrease"
        else:
            return "No change"
    def extract_linkedin_universal_name(linkedin_url, pattern):
        try:
            return pattern.match(str(linkedin_url)).group(1)
        except AttributeError:
            return None
    def generate_columns(count, patterns):
        return [pattern.format(i+1) for i in range(count) for pattern in patterns]
    pattern_universalName = re.compile(r"^(?:https?:\/\/)?(?:[\w]+\.)?linkedin\.com\/(?:company|company-beta|school)\/([A-Za-z0-9\._\%&'-]+?)(?:\/|\?|#|$)", re.IGNORECASE)
    df_final = pd.DataFrame()
    max_confirmedLocations = 0
    insights_url = 'https://www.linkedin.com/voyager/api/voyagerPremiumDashCompanyInsightsCard'
    dataframe.drop_duplicates(subset=[column_name], inplace=True)
    dataframe['wordToSearch'] = dataframe[column_name].apply(lambda x: extract_linkedin_universal_name(x, pattern_universalName))
    original_to_wordToSearch = dict(zip(dataframe[column_name], dataframe['wordToSearch']))
    columnName_values = dataframe[column_name].tolist()
    #-->Loop
    print("LinkedIn account scrape")
    progress_bar = tqdm(total = len(columnName_values))
    #--STREAMLIT--#
    if streamlit_execution:
        st.write("---LinkedIn account scrape---")
        progress_bar_linkedin_account = st.progress(0)
        number_iterations = len(columnName_values)
        index_steamlit = 0
    #--STREAMLIT--#
    for index, company in enumerate(columnName_values):
        df_loop_final = pd.DataFrame()
        df_loop_base = pd.DataFrame()
        df_loop_confirmedLocations = pd.DataFrame()
        df_loop_premium_not_final = pd.DataFrame()
        df_loop_headcountInsights = pd.DataFrame()
        df_loop_latestHeadcountByFunction = pd.DataFrame()
        df_loop_headcountGrowthByFunction = pd.DataFrame()
        df_loop_jobOpeningsByFunction = pd.DataFrame()
        df_loop_jobOpeningsGrowthByFunction = pd.DataFrame()
        df_loop_hireCounts = pd.DataFrame()
        df_loop_seniorHires = pd.DataFrame()
        df_loop_hiresInsights = pd.DataFrame()
        df_loop_alumniInsights = pd.DataFrame()
        wordToSearch = original_to_wordToSearch.get(columnName_values[index])
        error = None
        if wordToSearch is None:
            error = "Invalid LinkedIn URL"
            df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
            df_loop_final = pd.concat([df_loop_final, df_loop_base, df_loop_confirmedLocations, df_loop_premium_not_final, df_loop_headcountInsights, df_loop_latestHeadcountByFunction, df_loop_headcountGrowthByFunction, df_loop_jobOpeningsByFunction, df_loop_jobOpeningsGrowthByFunction, df_loop_hiresInsights, df_loop_alumniInsights], axis=1)
            df_final = pd.concat([df_final, df_loop_final])
            index += 1
            progress_bar.update(1)
            #--STREAMLIT--#
            if streamlit_execution:
                index_steamlit += 1
                progress_bar_linkedin_account.progress(index_steamlit / number_iterations)
            #--STREAMLIT--#
            continue
        #-->WAIT FOR EMPLOYEESONLINKEDIN TO BE MORE THAN 0 START
        max_employeesOnLinkedIn_tries = 3
        employeesOnLinkedIn_tries = 0
        params = {
            "decorationId": "com.linkedin.voyager.deco.organization.web.WebFullCompanyMain-12",
            "q": "universalName",
            "universalName": urllib.parse.unquote(wordToSearch),
        }
        request_url = 'https://www.linkedin.com/voyager/api/organization/companies'
        while employeesOnLinkedIn_tries < max_employeesOnLinkedIn_tries:
            try:
                response = requests.get(url=request_url, cookies=cookies_dict, headers=headers, params=params)
                response.raise_for_status()
                response_json = response.json()
                company = get_company(response_json)
                employeesOnLinkedIn = int(company.get('staffCount', 0))
                if employeesOnLinkedIn > 0:
                    break
            except (requests.RequestException, KeyError, ValueError):
                employeesOnLinkedIn = 0
            employeesOnLinkedIn_tries += 1
            time.sleep(5)
        #-->WAIT FOR EMPLOYEESONLINKEDIN TO BE MORE THAN 0 END        
        try:
            params = {
                "decorationId": "com.linkedin.voyager.deco.organization.web.WebFullCompanyMain-12",
                "q": "universalName",
                "universalName": urllib.parse.unquote(wordToSearch),
            }
            request_url = f'https://www.linkedin.com/voyager/api/organization/companies'
            response = requests.get(url=request_url, cookies=cookies_dict, headers=headers, params=params)
            response_json = response.json()
            company = get_company(response_json)
        except KeyError as e:
            company = {}
            error = "LinkedIn down"
            df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
            df_loop_final = pd.concat([df_loop_final, df_loop_base, df_loop_confirmedLocations, df_loop_premium_not_final, df_loop_headcountInsights, df_loop_latestHeadcountByFunction, df_loop_headcountGrowthByFunction, df_loop_jobOpeningsByFunction, df_loop_jobOpeningsGrowthByFunction, df_loop_hiresInsights, df_loop_alumniInsights], axis=1)
            df_final = pd.concat([df_final, df_loop_final])
            index += 1
            progress_bar.update(1)
            #--STREAMLIT--#
            if streamlit_execution:
                index_steamlit += 1
                progress_bar_linkedin_account.progress(index_steamlit / number_iterations)
            #--STREAMLIT--#
            continue
        except Exception as e:
            company = {}
            error = f'{e}'
            df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
            df_loop_final = pd.concat([df_loop_final, df_loop_base, df_loop_confirmedLocations, df_loop_premium_not_final, df_loop_headcountInsights, df_loop_latestHeadcountByFunction, df_loop_headcountGrowthByFunction, df_loop_jobOpeningsByFunction, df_loop_jobOpeningsGrowthByFunction, df_loop_hiresInsights, df_loop_alumniInsights], axis=1)
            df_final = pd.concat([df_final, df_loop_final])
            index += 1
            progress_bar.update(1)
            #--STREAMLIT--#
            if streamlit_execution:
                index_steamlit += 1
                progress_bar_linkedin_account.progress(index_steamlit / number_iterations)
            #--STREAMLIT--#
            time.sleep(2.5)
            continue
        #-->DATA MANIPULATION START<--
        #-->BASE FIELDS<--
        #-->companyUrl
        companyUrl = company.get('url', None)
        if companyUrl and not companyUrl.endswith('/'):
            companyUrl += '/'
        #-->mainCompanyID
        pattern_mainCompanyID = re.compile(r"urn:li:fs_normalized_company:(.+)")
        mainCompanyID = extract_from_pattern(company.get('entityUrn', ''), pattern_mainCompanyID)
        #-->salesNavigatorLink
        salesNavigatorLink = create_salesNavigatorLink(mainCompanyID)
        #-->universalName
        universalName = company.get('universalName', None)
        #-->companyName
        companyName = company.get('name', None)
        #-->followerCount
        followerCount = company.get('followingInfo', {}).get('followerCount', '')
        #-->employeesOnLinkedIn
        employeesOnLinkedIn = company.get('staffCount', None)
        #-->tagLine
        tagLine = company.get('tagline', None)
        #-->description
        description = company.get('description', None)
        #-->website
        website = company.get('companyPageUrl', None)
        #-->domain
        domain = extract_domain_from_website(website)
        domain = domain.lower() if domain is not None else None
        #-->industryCode
        pattern_industryCode = re.compile(r"urn:li:fs_industry:(.+)")
        industryUrn = company.get('companyIndustries', [{}])[0].get('entityUrn', '')
        industryCode = extract_from_pattern(industryUrn, pattern_industryCode)
        #-->industry
        industry = company.get('companyIndustries', [{}])[0].get('localizedName', '')
        #-->companySize
        start_companySize = safe_str(company.get('staffCountRange', {}).get('start'))
        end_companySize = safe_str(company.get('staffCountRange', {}).get('end'))
        companySize = (start_companySize or "") + ("-" if start_companySize and end_companySize else "") + (end_companySize or "")
        companySize = companySize if companySize else None
        #-->headquarter
        city_headquarter = safe_str(company.get('headquarter', {}).get('city'))
        geographicArea_headquarter = safe_str(company.get('headquarter', {}).get('geographicArea'))
        geographicArea_headquarter = get_full_name(dict_geographicArea, geographicArea_headquarter)
        headquarter = (city_headquarter or "") + (", " if city_headquarter and geographicArea_headquarter else "") + (geographicArea_headquarter or "")
        headquarter = headquarter if headquarter else None
        #-->founded
        founded = company.get('foundedOn', {}).get('year', '')
        #-->specialities
        list_specialities = company.get('specialities', [])
        specialities = ', '.join(list_specialities) if list_specialities else None
        #-->companyType
        companyType = company.get('companyType', {}).get('localizedName', '')
        #-->phone
        phone = company.get('phone', {}).get('number', '')
        #-->headquarter_line1, headquarter_line2, headquarter_city, headquarter_geographicArea, headquarter_postalCode, headquarter_country, headquarter_companyAddress
        headquarter_line1 = safe_str(company.get('headquarter', {}).get('line1'))
        headquarter_line2 = safe_str(company.get('headquarter', {}).get('line2'))
        headquarter_city = safe_str(company.get('headquarter', {}).get('city'))
        headquarter_geographicArea = safe_str(company.get('headquarter', {}).get('geographicArea'))
        headquarter_geographicArea = get_full_name(dict_geographicArea, headquarter_geographicArea)
        headquarter_postalCode = safe_str(company.get('headquarter', {}).get('postalCode'))
        headquarter_country = safe_str(company.get('headquarter', {}).get('country'))
        headquarter_country = get_full_name(dict_country, headquarter_country)
        components_headquarter_companyAddress = [headquarter_line1,headquarter_line2,headquarter_city,headquarter_geographicArea,headquarter_postalCode,headquarter_country]
        headquarter_companyAddress = ', '.join(filter(None, components_headquarter_companyAddress))
        #-->locationN_description, locationN_line1, locationN_line2, locationN_city, locationN_geographicArea, locationN_postalCode, locationN_country, locationN_companyAddress
        confirmedLocations = company.get('confirmedLocations', [])
        flattened_confirmedLocations = {}
        for idx, location in enumerate(confirmedLocations, start=1):
            prefix = f"location{idx}_"
            geographicArea = get_full_name(dict_geographicArea, location.get("geographicArea", ""))
            country = get_full_name(dict_country, location.get("country", ""))
            keys = ['description', 'line1', 'line2', 'city', 'postalCode']
            for key in keys:
                flattened_confirmedLocations[prefix + key] = location.get(key, "")
            flattened_confirmedLocations[prefix + "geographicArea"] = geographicArea
            flattened_confirmedLocations[prefix + "country"] = country
            address_components = [location.get('line1'), location.get('line2'), location.get('city'), geographicArea, location.get('postalCode'), country]
            address = ', '.join(filter(None, address_components))
            flattened_confirmedLocations[prefix + "companyAddress"] = address
        if 'idx' in locals() and idx > max_confirmedLocations:
                max_confirmedLocations = idx
        df_loop_confirmedLocations = pd.DataFrame([flattened_confirmedLocations])
        #-->banner10000x10000, banner400x400, banner200x200
        background_image = company.get('backgroundCoverImage', {})
        image = background_image.get('image', {})
        vector_image = image.get('com.linkedin.common.VectorImage', {})
        root_url = vector_image.get('rootUrl')
        banner_names = ['banner10000x10000', 'banner400x400', 'banner200x200']
        banners = dict.fromkeys(banner_names, None)
        if root_url:
            artifacts = vector_image.get('artifacts', [])
            sorted_artifacts = sorted(artifacts, key=lambda x: x.get('width', 0), reverse=True)
            for idx, artifact in enumerate(sorted_artifacts):
                if idx >= len(banner_names):
                    break
                banner_name = banner_names[idx]
                banners[banner_name] = root_url + artifact.get('fileIdentifyingUrlPathSegment')
        banner10000x10000 = banners['banner10000x10000']
        banner400x400 = banners['banner400x400']
        banner200x200 = banners['banner200x200']
        #-->logo400x400, logo200x200, logo100x100
        logo_image = company.get('logo', {})
        image = logo_image.get('image', {})
        vector_image = image.get('com.linkedin.common.VectorImage', {})
        root_url = vector_image.get('rootUrl')
        logo_names = ['logo400x400', 'logo200x200', 'logo100x100']
        logos = dict.fromkeys(logo_names, None)
        if root_url:
            artifacts = vector_image.get('artifacts', [])
            sorted_artifacts = sorted(artifacts, key=lambda x: x.get('width', 0), reverse=True)
            for idx, artifact in enumerate(sorted_artifacts):
                if idx >= len(logo_names):
                    break
                logo_name = logo_names[idx]
                logos[logo_name] = root_url + artifact.get('fileIdentifyingUrlPathSegment')
        logo400x400 = logos['logo400x400']
        logo200x200 = logos['logo200x200']
        logo100x100 = logos['logo100x100']
        #-->showcase
        showcase = company.get('showcase', None)
        #-->autoGenerated
        autoGenerated = company.get('autoGenerated', None)
        #-->isClaimable
        isClaimable = company.get('claimable', None)
        #-->jobSearchPageUrl
        jobSearchPageUrl = company.get('jobSearchPageUrl', None)
        #-->associatedHashtags
        associatedHashtags = ', '.join(filter(None, (extract_hashtag(item) for item in company.get('associatedHashtags', []))))
        #-->callToActionUrl
        callToActionUrl = safe_str(company.get('callToAction', {}).get('url'))
        #-->timestamp
        current_timestamp = datetime.now()
        timestamp = current_timestamp.strftime('%Y-%m-%d %H:%M:%S')
        #-->Create dataframe with base fields
        all_variables = locals()
        selected_vars = {var: [all_variables[var]] for var in ["companyUrl", "mainCompanyID", "salesNavigatorLink", "universalName", "companyName", "followerCount","employeesOnLinkedIn", "tagLine", "description", "website", "domain", "industryCode", "industry","companySize", "headquarter", "founded", "specialities", "companyType", "phone", "headquarter_line1","headquarter_line2", "headquarter_city", "headquarter_geographicArea", "headquarter_postalCode","headquarter_country", "headquarter_companyAddress", "banner10000x10000", "banner400x400","banner200x200", "logo400x400", "logo200x200", "logo100x100", "showcase", "autoGenerated","isClaimable", "jobSearchPageUrl", "associatedHashtags", "callToActionUrl", "timestamp"]}
        df_loop_base = pd.DataFrame(selected_vars)
        #-->PREMIUM FIELDS<--
        try:
            company_insights = fetch_linkedin_insights(mainCompanyID)
        except KeyError as e:
            company_insights = None
            df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
            df_loop_final = pd.concat([df_loop_final, df_loop_base, df_loop_confirmedLocations, df_loop_premium_not_final, df_loop_headcountInsights, df_loop_latestHeadcountByFunction, df_loop_headcountGrowthByFunction, df_loop_jobOpeningsByFunction, df_loop_jobOpeningsGrowthByFunction, df_loop_hiresInsights, df_loop_alumniInsights], axis=1)
            df_final = pd.concat([df_final, df_loop_final])
            index += 1
            progress_bar.update(1)
            #--STREAMLIT--#
            if streamlit_execution:
                index_steamlit += 1
                progress_bar_linkedin_account.progress(index_steamlit / number_iterations)
            #--STREAMLIT--#
            continue
        if len(company_insights.get('elements', [])) > 0:
            #-->headcountInsights, functionHeadcountInsights, hiresInsights, alumniInsights, jobOpeningsInsights
            headcountInsights = [company_union.get('companyInsightsUnion', {}).get('headcountInsights') for company_union in company_insights.get('elements', [])]            
            functionHeadcountInsights = [{key: insights.get(key)  for key in ['latestHeadcountByFunction', 'headcountGrowthByFunction'] if insights.get(key) is not None} for company_union in company_insights.get('elements', [])  for insights in [company_union.get('companyInsightsUnion', {}).get('functionHeadcountInsights', {})] if insights.get('latestHeadcountByFunction') or insights.get('headcountGrowthByFunction')]
            hiresInsights = [insight.get('hiresInsights') for company_union in company_insights.get('elements', []) for insight in [company_union.get('companyInsightsUnion', {})] if insight.get('hiresInsights') is not None]
            alumniInsights = [insight.get('alumniInsights') for company_union in company_insights.get('elements', []) for insight in [company_union.get('companyInsightsUnion', {})] if insight.get('alumniInsights') is not None]
            jobOpeningsInsights = [{key: insights.get(key)  for key in ['jobOpeningsByFunction', 'jobOpeningsGrowthByFunction'] if insights.get(key) is not None} for company_union in company_insights.get('elements', [])  for insights in [company_union.get('companyInsightsUnion', {}).get('jobOpeningsInsights', {})] if insights.get('jobOpeningsByFunction') or insights.get('jobOpeningsGrowthByFunction')]
            #-->totalEmployeeCount
            totalEmployeeCount = headcountInsights[0].get('totalEmployees') if headcountInsights and headcountInsights[0] else None
            #-->growth6Mth, growth1Yr, growth2Yr
            #growth_data = headcountInsights[0]['growthPeriods'] if headcountInsights and headcountInsights[0] else []
            growth_data = safe_extract(headcountInsights, 0, 'growthPeriods')
            if growth_data is not None:
                growth_dict = {item['monthDifference']: item['changePercentage'] for item in growth_data}
                growth6Mth = format_growth(growth_dict.get(6, None))
                growth1Yr = format_growth(growth_dict.get(12, None))
                growth2Yr = format_growth(growth_dict.get(24, None))
            else:
                growth_dict = {}
                growth6Mth = None
                growth1Yr = None
                growth2Yr = None            
            #-->averageTenure
            tenure_text = headcountInsights[0]['headcounts']['medianTenureYears']['text'] if headcountInsights and headcountInsights[0] and 'headcounts' in headcountInsights[0] and 'medianTenureYears' in headcountInsights[0]['headcounts'] else ''
            averageTenure = tenure_text.split("Median employee tenure ‧ ", 1)[-1] if "Median employee tenure ‧ " in tenure_text else None
            #Create dataframe with some premium fields
            all_variables = locals()
            selected_vars_premium = {var: [all_variables.get(var, None)] for var in ["totalEmployeeCount", "growth6Mth", "growth1Yr", "growth2Yr", "averageTenure"]}
            df_loop_premium_not_final = pd.DataFrame(selected_vars_premium)
            #-->headcountGrowthMonthNDate, headcountGrowthMonthNCount
            headcount_data = headcountInsights[0].get('headcounts', {}) if headcountInsights and headcountInsights[0] else {}
            headcount_growth_data = headcount_data.get('headcountGrowth', [])
            flattened_headcount = {f"headcountGrowthMonth{i}Date": None for i in range(25)}
            flattened_headcount.update({f"headcountGrowthMonth{i}Count": None for i in range(25)})
            if isinstance(headcount_growth_data, list):
                for i, entry in enumerate(headcount_growth_data[:25]):
                    date = entry.get('startedOn')
                    if date:
                        flattened_headcount[f"headcountGrowthMonth{i}Date"] = f"{date.get('month')}/{date.get('day')}/{date.get('year')}"
                    flattened_headcount[f"headcountGrowthMonth{i}Count"] = entry.get('employeeCount')
            df_loop_headcountInsights = pd.DataFrame([flattened_headcount])
            df_loop_headcountInsights = df_loop_headcountInsights[['headcountGrowthMonth0Date', 'headcountGrowthMonth0Count', 'headcountGrowthMonth1Date', 'headcountGrowthMonth1Count', 'headcountGrowthMonth2Date', 'headcountGrowthMonth2Count', 'headcountGrowthMonth3Date', 'headcountGrowthMonth3Count', 'headcountGrowthMonth4Date', 'headcountGrowthMonth4Count', 'headcountGrowthMonth5Date', 'headcountGrowthMonth5Count', 'headcountGrowthMonth6Date', 'headcountGrowthMonth6Count', 'headcountGrowthMonth7Date', 'headcountGrowthMonth7Count', 'headcountGrowthMonth8Date', 'headcountGrowthMonth8Count', 'headcountGrowthMonth9Date', 'headcountGrowthMonth9Count', 'headcountGrowthMonth10Date', 'headcountGrowthMonth10Count', 'headcountGrowthMonth11Date', 'headcountGrowthMonth11Count', 'headcountGrowthMonth12Date', 'headcountGrowthMonth12Count', 'headcountGrowthMonth13Date', 'headcountGrowthMonth13Count', 'headcountGrowthMonth14Date', 'headcountGrowthMonth14Count', 'headcountGrowthMonth15Date', 'headcountGrowthMonth15Count', 'headcountGrowthMonth16Date', 'headcountGrowthMonth16Count', 'headcountGrowthMonth17Date', 'headcountGrowthMonth17Count', 'headcountGrowthMonth18Date', 'headcountGrowthMonth18Count', 'headcountGrowthMonth19Date', 'headcountGrowthMonth19Count', 'headcountGrowthMonth20Date', 'headcountGrowthMonth20Count', 'headcountGrowthMonth21Date', 'headcountGrowthMonth21Count', 'headcountGrowthMonth22Date', 'headcountGrowthMonth22Count', 'headcountGrowthMonth23Date', 'headcountGrowthMonth23Count', 'headcountGrowthMonth24Date', 'headcountGrowthMonth24Count']]
            #-->distributionFunction and distributionFunctionPercentage
            mapping_latestHeadcountByFunction = {
                "urn:li:fsd_function:1": ('distributionAccounting', 'distributionAccountingPercentage'),
                "urn:li:fsd_function:2": ('distributionAdministrative', 'distributionAdministrativePercentage'),
                "urn:li:fsd_function:3": ('distributionArtsAndDesign', 'distributionArtsAndDesignPercentage'),
                "urn:li:fsd_function:4": ('distributionBusinessDevelopment', 'distributionBusinessDevelopmentPercentage'),
                "urn:li:fsd_function:5": ('distributionCommunityAndSocialServices', 'distributionCommunityAndSocialServicesPercentage'),
                "urn:li:fsd_function:6": ('distributionConsulting', 'distributionConsultingPercentage'),
                "urn:li:fsd_function:7": ('distributionEducation', 'distributionEducationPercentage'),
                "urn:li:fsd_function:8": ('distributionEngineering', 'distributionEngineeringPercentage'),
                "urn:li:fsd_function:9": ('distributionEntrepreneurship', 'distributionEntrepreneurshipPercentage'),
                "urn:li:fsd_function:10": ('distributionFinance', 'distributionFinancePercentage'),
                "urn:li:fsd_function:11": ('distributionHealthcareServices', 'distributionHealthcareServicesPercentage'),
                "urn:li:fsd_function:12": ('distributionHumanResources', 'distributionHumanResourcesPercentage'),
                "urn:li:fsd_function:13": ('distributionInformationTechnology', 'distributionInformationTechnologyPercentage'),
                "urn:li:fsd_function:14": ('distributionLegal', 'distributionLegalPercentage'),
                "urn:li:fsd_function:15": ('distributionMarketing', 'distributionMarketingPercentage'),
                "urn:li:fsd_function:16": ('distributionMediaAndCommunication', 'distributionMediaAndCommunicationPercentage'),
                "urn:li:fsd_function:17": ('distributionMilitaryAndProtectiveServices', 'distributionMilitaryAndProtectiveServicesPercentage'),
                "urn:li:fsd_function:18": ('distributionOperations', 'distributionOperationsPercentage'),
                "urn:li:fsd_function:19": ('distributionProductManagement', 'distributionProductManagementPercentage'),
                "urn:li:fsd_function:20": ('distributionProgramAndProjectManagement', 'distributionProgramAndProjectManagementPercentage'),
                "urn:li:fsd_function:21": ('distributionPurchasing', 'distributionPurchasingPercentage'),
                "urn:li:fsd_function:22": ('distributionQualityAssurance', 'distributionQualityAssurancePercentage'),
                "urn:li:fsd_function:23": ('distributionRealEstate', 'distributionRealEstatePercentage'),
                "urn:li:fsd_function:24": ('distributionResearch', 'distributionResearchPercentage'),
                "urn:li:fsd_function:25": ('distributionSales', 'distributionSalesPercentage'),
                "urn:li:fsd_function:26": ('distributionSupport', 'distributionSupportPercentage')
            }
            try:
                data_latestHeadcountByFunction = functionHeadcountInsights[0]['latestHeadcountByFunction']['countByFunction']
            except (IndexError, KeyError):
                data_latestHeadcountByFunction = []
            flattened_latestHeadcountByFunction = {k: None for key_tuple in mapping_latestHeadcountByFunction.values() for k in key_tuple}
            for item in data_latestHeadcountByFunction:
                function_urn = item.get('functionUrn')
                if function_urn in mapping_latestHeadcountByFunction:
                    var, var_per = mapping_latestHeadcountByFunction[function_urn]
                    flattened_latestHeadcountByFunction[var] = item.get('functionCount')
                    flattened_latestHeadcountByFunction[var_per] = item.get('functionPercentage')
            df_loop_latestHeadcountByFunction = pd.DataFrame([flattened_latestHeadcountByFunction])
            #-->growth6MthFunction and growth1YrFunction
            try:
                data_headcountGrowthByFunction = functionHeadcountInsights[0]['headcountGrowthByFunction']
            except (IndexError, KeyError):
                data_headcountGrowthByFunction = []
            mapping_headcountGrowthByFunction = {
                "urn:li:fsd_function:1": ('growth6MthAccounting', 'growth1YrAccounting'),
                "urn:li:fsd_function:2": ('growth6MthAdministrative', 'growth1YrAdministrative'),
                "urn:li:fsd_function:3": ('growth6MthArtsAndDesign', 'growth1YrArtsAndDesign'),
                "urn:li:fsd_function:4": ('growth6MthBusinessDevelopment', 'growth1YrBusinessDevelopment'),
                "urn:li:fsd_function:5": ('growth6MthCommunityAndSocialServices', 'growth1YrCommunityAndSocialServices'),
                "urn:li:fsd_function:6": ('growth6MthConsulting', 'growth1YrConsulting'),
                "urn:li:fsd_function:7": ('growth6MthEducation', 'growth1YrEducation'),
                "urn:li:fsd_function:8": ('growth6MthEngineering', 'growth1YrEngineering'),
                "urn:li:fsd_function:9": ('growth6MthEntrepreneurship', 'growth1YrEntrepreneurship'),
                "urn:li:fsd_function:10": ('growth6MthFinance', 'growth1YrFinance'),
                "urn:li:fsd_function:11": ('growth6MthHealthcareServices', 'growth1YrHealthcareServices'),
                "urn:li:fsd_function:12": ('growth6MthHumanResources', 'growth1YrHumanResources'),
                "urn:li:fsd_function:13": ('growth6MthInformationTechnology', 'growth1YrInformationTechnology'),
                "urn:li:fsd_function:14": ('growth6MthLegal', 'growth1YrLegal'),
                "urn:li:fsd_function:15": ('growth6MthMarketing', 'growth1YrMarketing'),
                "urn:li:fsd_function:16": ('growth6MthMediaAndCommunication', 'growth1YrMediaAndCommunication'),
                "urn:li:fsd_function:17": ('growth6MthMilitaryAndProtectiveServices', 'growth1YrMilitaryAndProtectiveServices'),
                "urn:li:fsd_function:18": ('growth6MthOperations', 'growth1YrOperations'),
                "urn:li:fsd_function:19": ('growth6MthProductManagement', 'growth1YrProductManagement'),
                "urn:li:fsd_function:20": ('growth6MthProgramAndProjectManagement', 'growth1YrProgramAndProjectManagement'),
                "urn:li:fsd_function:21": ('growth6MthPurchasing', 'growth1YrPurchasing'),
                "urn:li:fsd_function:22": ('growth6MthQualityAssurance', 'growth1YrQualityAssurance'),
                "urn:li:fsd_function:23": ('growth6MthRealEstate', 'growth1YrRealEstate'),
                "urn:li:fsd_function:24": ('growth6MthResearch', 'growth1YrResearch'),
                "urn:li:fsd_function:25": ('growth6MthSales', 'growth1YrSales'),
                "urn:li:fsd_function:26": ('growth6MthSupport', 'growth1YrSupport')
            }
            flattened_headcountGrowthByFunction = {column_name: None for urn_values in mapping_headcountGrowthByFunction.values() for column_name in urn_values}
            for entry in data_headcountGrowthByFunction:
                if 'function' in entry and 'entityUrn' in entry['function']:
                    function_urn = entry['function']['entityUrn']
                elif 'functionUrn' in entry:
                    function_urn = entry['functionUrn']
                else:
                    function_urn = None
                column_info = mapping_headcountGrowthByFunction.get(function_urn)
                if column_info:
                    for period in entry['growthPeriods']:
                        month_diff = period['monthDifference']
                        change_percentage = period['changePercentage']
                        if month_diff == 6:
                            column_name = column_info[0]
                        elif month_diff == 12:
                            column_name = column_info[1]
                        else:
                            continue
                        flattened_headcountGrowthByFunction[column_name] = change_percentage
            df_loop_headcountGrowthByFunction = pd.DataFrame([flattened_headcountGrowthByFunction])
            #-->distributionFunctionJobs and distributionFunctionPercentageJobs
            mapping_jobOpeningsByFunction = {
                "urn:li:fsd_function:1": ('distributionAccountingJobs', 'distributionAccountingPercentageJobs'),
                "urn:li:fsd_function:2": ('distributionAdministrativeJobs', 'distributionAdministrativePercentageJobs'),
                "urn:li:fsd_function:3": ('distributionArtsAndDesignJobs', 'distributionArtsAndDesignPercentageJobs'),
                "urn:li:fsd_function:4": ('distributionBusinessDevelopmentJobs', 'distributionBusinessDevelopmentPercentageJobs'),
                "urn:li:fsd_function:5": ('distributionCommunityAndSocialServicesJobs', 'distributionCommunityAndSocialServicesPercentageJobs'),
                "urn:li:fsd_function:6": ('distributionConsultingJobs', 'distributionConsultingPercentageJobs'),
                "urn:li:fsd_function:7": ('distributionEducationJobs', 'distributionEducationPercentageJobs'),
                "urn:li:fsd_function:8": ('distributionEngineeringJobs', 'distributionEngineeringPercentageJobs'),
                "urn:li:fsd_function:9": ('distributionEntrepreneurshipJobs', 'distributionEntrepreneurshipPercentageJobs'),
                "urn:li:fsd_function:10": ('distributionFinanceJobs', 'distributionFinancePercentageJobs'),
                "urn:li:fsd_function:11": ('distributionHealthcareServicesJobs', 'distributionHealthcareServicesPercentageJobs'),
                "urn:li:fsd_function:12": ('distributionHumanResourcesJobs', 'distributionHumanResourcesPercentageJobs'),
                "urn:li:fsd_function:13": ('distributionInformationTechnologyJobs', 'distributionInformationTechnologyPercentageJobs'),
                "urn:li:fsd_function:14": ('distributionLegalJobs', 'distributionLegalPercentageJobs'),
                "urn:li:fsd_function:15": ('distributionMarketingJobs', 'distributionMarketingPercentageJobs'),
                "urn:li:fsd_function:16": ('distributionMediaAndCommunicationJobs', 'distributionMediaAndCommunicationPercentageJobs'),
                "urn:li:fsd_function:17": ('distributionMilitaryAndProtectiveServicesJobs', 'distributionMilitaryAndProtectiveServicesPercentageJobs'),
                "urn:li:fsd_function:18": ('distributionOperationsJobs', 'distributionOperationsPercentageJobs'),
                "urn:li:fsd_function:19": ('distributionProductManagementJobs', 'distributionProductManagementPercentageJobs'),
                "urn:li:fsd_function:20": ('distributionProgramAndProjectManagementJobs', 'distributionProgramAndProjectManagementPercentageJobs'),
                "urn:li:fsd_function:21": ('distributionPurchasingJobs', 'distributionPurchasingPercentageJobs'),
                "urn:li:fsd_function:22": ('distributionQualityAssuranceJobs', 'distributionQualityAssurancePercentageJobs'),
                "urn:li:fsd_function:23": ('distributionRealEstateJobs', 'distributionRealEstatePercentageJobs'),
                "urn:li:fsd_function:24": ('distributionResearchJobs', 'distributionResearchPercentageJobs'),
                "urn:li:fsd_function:25": ('distributionSalesJobs', 'distributionSalesPercentageJobs'),
                "urn:li:fsd_function:26": ('distributionSupportJobs', 'distributionSupportPercentageJobs')
            }
            try:
                data_jobOpeningsByFunction = jobOpeningsInsights[0]['jobOpeningsByFunction'][0]['countByFunction']
            except (IndexError, KeyError):
                data_jobOpeningsByFunction = []
            flattened_jobOpeningsByFunction = {k: None for key_tuple in mapping_jobOpeningsByFunction.values() for k in key_tuple}
            for item in data_jobOpeningsByFunction:
                function_urn = item.get('functionUrn')
                if function_urn in mapping_jobOpeningsByFunction:
                    var, var_per = mapping_jobOpeningsByFunction[function_urn]
                    flattened_jobOpeningsByFunction[var] = item.get('functionCount')
                    flattened_jobOpeningsByFunction[var_per] = item.get('functionPercentage')
            df_loop_jobOpeningsByFunction = pd.DataFrame([flattened_jobOpeningsByFunction])
            #-->growth3MthFunctionJobs, growth6MthFunctionJobs, growth1YrFunctionJobs
            mapping_jobOpeningsGrowthByFunction = {
                "urn:li:fsd_function:1": ('growth3MthAccountingJobs', 'growth6MthAccountingJobs', 'growth1YrAccountingJobs'),
                "urn:li:fsd_function:2": ('growth3MthAdministrativeJobs', 'growth6MthAdministrativeJobs', 'growth1YrAdministrativeJobs'),
                "urn:li:fsd_function:3": ('growth3MthArtsAndDesignJobs', 'growth6MthArtsAndDesignJobs', 'growth1YrArtsAndDesignJobs'),
                "urn:li:fsd_function:4": ('growth3MthBusinessDevelopmentJobs', 'growth6MthBusinessDevelopmentJobs', 'growth1YrBusinessDevelopmentJobs'),
                "urn:li:fsd_function:5": ('growth3MthCommunityAndSocialServicesJobs', 'growth6MthCommunityAndSocialServicesJobs', 'growth1YrCommunityAndSocialServicesJobs'),
                "urn:li:fsd_function:6": ('growth3MthConsultingJobs', 'growth6MthConsultingJobs', 'growth1YrConsultingJobs'),
                "urn:li:fsd_function:7": ('growth3MthEducationJobs', 'growth6MthEducationJobs', 'growth1YrEducationJobs'),
                "urn:li:fsd_function:8": ('growth3MthEngineeringJobs', 'growth6MthEngineeringJobs', 'growth1YrEngineeringJobs'),
                "urn:li:fsd_function:9": ('growth3MthEntrepreneurshipJobs', 'growth6MthEntrepreneurshipJobs', 'growth1YrEntrepreneurshipJobs'),
                "urn:li:fsd_function:10": ('growth3MthFinanceJobs', 'growth6MthFinanceJobs', 'growth1YrFinanceJobs'),
                "urn:li:fsd_function:11": ('growth3MthHealthcareServicesJobs', 'growth6MthHealthcareServicesJobs', 'growth1YrHealthcareServicesJobs'),
                "urn:li:fsd_function:12": ('growth3MthHumanResourcesJobs', 'growth6MthHumanResourcesJobs', 'growth1YrHumanResourcesJobs'),
                "urn:li:fsd_function:13": ('growth3MthInformationTechnologyJobs', 'growth6MthInformationTechnologyJobs', 'growth1YrInformationTechnologyJobs'),
                "urn:li:fsd_function:14": ('growth3MthLegalJobs', 'growth6MthLegalJobs', 'growth1YrLegalJobs'),
                "urn:li:fsd_function:15": ('growth3MthMarketingJobs', 'growth6MthMarketingJobs', 'growth1YrMarketingJobs'),
                "urn:li:fsd_function:16": ('growth3MthMediaAndCommunicationJobs', 'growth6MthMediaAndCommunicationJobs', 'growth1YrMediaAndCommunicationJobs'),
                "urn:li:fsd_function:17": ('growth3MthMilitaryAndProtectiveServicesJobs', 'growth6MthMilitaryAndProtectiveServicesJobs', 'growth1YrMilitaryAndProtectiveServicesJobs'),
                "urn:li:fsd_function:18": ('growth3MthOperationsJobs', 'growth6MthOperationsJobs', 'growth1YrOperationsJobs'),
                "urn:li:fsd_function:19": ('growth3MthProductManagementJobs', 'growth6MthProductManagementJobs', 'growth1YrProductManagementJobs'),
                "urn:li:fsd_function:20": ('growth3MthProgramAndProjectManagementJobs', 'growth6MthProgramAndProjectManagementJobs', 'growth1YrProgramAndProjectManagementJobs'),
                "urn:li:fsd_function:21": ('growth3MthPurchasingJobs', 'growth6MthPurchasingJobs', 'growth1YrPurchasingJobs'),
                "urn:li:fsd_function:22": ('growth3MthQualityAssuranceJobs', 'growth6MthQualityAssuranceJobs', 'growth1YrQualityAssuranceJobs'),
                "urn:li:fsd_function:23": ('growth3MthRealEstateJobs', 'growth6MthRealEstateJobs', 'growth1YrRealEstateJobs'),
                "urn:li:fsd_function:24": ('growth3MthResearchJobs', 'growth6MthResearchJobs', 'growth1YrResearchJobs'),
                "urn:li:fsd_function:25": ('growth3MthSalesJobs', 'growth6MthSalesJobs', 'growth1YrSalesJobs'),
                "urn:li:fsd_function:26": ('growth3MthSupportJobs', 'growth6MthSupportJobs', 'growth1YrSupportJobs')
            }
            try:
                data_jobOpeningsGrowthByFunction = jobOpeningsInsights[0]['jobOpeningsGrowthByFunction']
            except (IndexError, KeyError):
                data_jobOpeningsGrowthByFunction = []
            flattened_jobOpeningsGrowthByFunction = {column_name: None for urn_values in mapping_jobOpeningsGrowthByFunction.values() for column_name in urn_values}
            for entry in data_jobOpeningsGrowthByFunction:
                function_data = entry.get('function')
                if function_data:
                    function_urn = function_data.get('entityUrn')
                column_info = mapping_jobOpeningsGrowthByFunction.get(function_urn)
                if column_info:
                    for period in entry['growthPeriods']:
                        month_diff = period['monthDifference']
                        change_percentage = period['changePercentage']
                        if month_diff == 3:
                            column_name = column_info[0]
                        elif month_diff == 6:
                            column_name = column_info[1]
                        elif month_diff == 12:
                            column_name = column_info[2]
                        else:
                            continue
                        flattened_jobOpeningsGrowthByFunction[column_name] = change_percentage
            df_loop_jobOpeningsGrowthByFunction = pd.DataFrame([flattened_jobOpeningsGrowthByFunction])
            #-->totalNumberOfSeniorHires
            totalNumberOfSeniorHires = hiresInsights[0].get('totalNumberOfSeniorHires', {}) if hiresInsights and hiresInsights[0] else None
            #-->hireAllCountMonthNDate, hireAllCountMonthN, hireSeniorCountMonthNDate, hireSeniorCountMonthN
            hireCounts = hiresInsights[0].get('hireCounts', {}) if hiresInsights and hiresInsights[0] else []
            flattened_hire = {f"hireAllCountMonth{i}Date": None for i in range(25)}
            flattened_hire.update({f"hireAllCountMonth{i}": None for i in range(25)})
            flattened_hire.update({f"hireSeniorCountMonth{i}Date": None for i in range(25)})
            flattened_hire.update({f"hireSeniorCountMonth{i}": None for i in range(25)})
            if isinstance(hireCounts, list):
                for i, entry in enumerate(hireCounts[:25]):
                    date = entry.get('yearMonthOn')
                    if date:
                        formatted_date = f"{date.get('month')}/{date.get('day')}/{date.get('year')}"
                        flattened_hire[f"hireAllCountMonth{i}Date"] = formatted_date
                        flattened_hire[f"hireSeniorCountMonth{i}Date"] = formatted_date
                    flattened_hire[f"hireAllCountMonth{i}"] = entry.get('allEmployeeHireCount')
                    flattened_hire[f"hireSeniorCountMonth{i}"] = entry.get('seniorHireCount')
            columns_order = [item for sublist in [[f"hireAllCountMonth{i}Date", f"hireAllCountMonth{i}", f"hireSeniorCountMonth{i}Date", f"hireSeniorCountMonth{i}"] for i in range(25)] for item in sublist]
            df_loop_hireCounts = pd.DataFrame([flattened_hire])
            df_loop_hireCounts = df_loop_hireCounts[columns_order]
            #-->seniorHiresN_title, seniorHiresN_linkedInUrl, seniorHiresN_fullName, seniorHiresN_date
            seniorHires = hiresInsights[0].get('seniorHires', {}) if hiresInsights and hiresInsights[0] else {}
            flattened_seniorHire = {}
            for i, entry in enumerate(seniorHires, 1):
                title = entry.get('hiredPosition', {}).get('text')
                linkedInUrl = entry.get('entityLockup', {}).get('navigationUrl')
                fullName = entry.get('entityLockup', {}).get('title', {}).get('text')
                hire_date = entry.get('hireYearMonthOn', {})
                month = hire_date.get('month')
                day = hire_date.get('day')
                year = hire_date.get('year')
                date = f"{month}/{day}/{year}" if month and day and year else None
                flattened_seniorHire[f"seniorHires{i}_title"] = title
                flattened_seniorHire[f"seniorHires{i}_linkedInUrl"] = linkedInUrl
                flattened_seniorHire[f"seniorHires{i}_fullName"] = fullName
                flattened_seniorHire[f"seniorHires{i}_date"] = date
            df_loop_seniorHires = pd.DataFrame([flattened_seniorHire])
            #-->Concatenate df_loop_hiresInsights
            df_loop_hiresInsights = pd.concat([df_loop_hireCounts, df_loop_seniorHires], axis=1)
            df_loop_hiresInsights.insert(0, 'totalNumberOfSeniorHires', totalNumberOfSeniorHires)
            #-->alumniN_currentTitle, alumniN_linkedInUrl, alumniN_fullName, alumniN_exitDate, alumniN_exitTitle
            alumni_elements = alumniInsights[0].get('alumni', []) if alumniInsights else []
            flattened_alumni = {}
            for j, alumni in enumerate(alumni_elements, 1):
                entity_lockup = alumni.get('entityLockup', {})
                subtitle = entity_lockup.get('subtitle', {})
                title = entity_lockup.get('title', {})
                exit_year_month_on = alumni.get('exitYearMonthOn', {})
                exit_date = "{}/{}/{}".format(exit_year_month_on.get('month', ''), exit_year_month_on.get('day', ''), exit_year_month_on.get('year', '')) if exit_year_month_on else None
                alumni_prefix = f"alumni{j}_"
                flattened_alumni[alumni_prefix + 'currentTitle'] = subtitle.get('text')
                flattened_alumni[alumni_prefix + 'linkedInUrl'] = entity_lockup.get('navigationUrl')
                flattened_alumni[alumni_prefix + 'fullName'] = title.get('text')
                flattened_alumni[alumni_prefix + 'exitDate'] = exit_date
                flattened_alumni[alumni_prefix + 'exitTitle'] = alumni.get('exitedPosition', {}).get('text')
            df_loop_alumniInsights = pd.DataFrame([flattened_alumni])
            #-->DATA MANIPULATION END<--
            error = None
            df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
            df_loop_final = pd.concat([df_loop_final, df_loop_base, df_loop_confirmedLocations, df_loop_premium_not_final, df_loop_headcountInsights, df_loop_latestHeadcountByFunction, df_loop_headcountGrowthByFunction, df_loop_jobOpeningsByFunction, df_loop_jobOpeningsGrowthByFunction, df_loop_hiresInsights, df_loop_alumniInsights], axis=1)
            df_loop_final = df_loop_final.dropna(axis=1, how='all')
            df_final = pd.concat([df_final, df_loop_final])
            index += 1
            progress_bar.update(1)
            #--STREAMLIT--#
            if streamlit_execution:
                index_steamlit += 1
                progress_bar_linkedin_account.progress(index_steamlit / number_iterations)
            #--STREAMLIT--#
        else:
            df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
            df_loop_final = pd.concat([df_loop_final, df_loop_base, df_loop_confirmedLocations, df_loop_premium_not_final, df_loop_headcountInsights, df_loop_latestHeadcountByFunction, df_loop_headcountGrowthByFunction, df_loop_jobOpeningsByFunction, df_loop_jobOpeningsGrowthByFunction, df_loop_hiresInsights, df_loop_alumniInsights], axis=1)
            df_final = pd.concat([df_final, df_loop_final])
            index += 1
            progress_bar.update(1)
            #--STREAMLIT--#
            if streamlit_execution:
                index_steamlit += 1
                progress_bar_linkedin_account.progress(index_steamlit / number_iterations)
            #--STREAMLIT--#
            continue
    progress_bar.close()
    COLUMN_PATTERNS = {
        'location': [
            'location{}_description', 'location{}_line1', 'location{}_line2',
            'location{}_city', 'location{}_postalCode', 'location{}_geographicArea',
            'location{}_country', 'location{}_companyAddress'
        ]
    }
    MAX_COUNTS = {'location': min(max_confirmedLocations, 100)}
    try:
        location_int_count = int(location_count)
    except ValueError:
        location_int_count = max_confirmedLocations
    counts = {'location': location_int_count}
    columns_lists = {}    
    for key, max_count in MAX_COUNTS.items():
        count = min(counts[key], max_count)
        columns_lists[key] = generate_columns(count, COLUMN_PATTERNS[key])
    final_list = ["query", "error", "companyUrl", "mainCompanyID", "salesNavigatorLink", "universalName", "companyName", "followerCount","employeesOnLinkedIn", "tagLine", "description", "website", "domain", "industryCode", "industry","companySize", "headquarter", "founded", "specialities", "companyType", "phone", "headquarter_line1","headquarter_line2", "headquarter_city", "headquarter_geographicArea", "headquarter_postalCode","headquarter_country", "headquarter_companyAddress", "banner10000x10000", "banner400x400","banner200x200", "logo400x400", "logo200x200", "logo100x100", "showcase", "autoGenerated","isClaimable", "jobSearchPageUrl", "associatedHashtags", "callToActionUrl", "timestamp"]
    locations_list = columns_lists['location']
    final_list.extend(locations_list)
    fields_check = include_headcountInsights = include_headcountByFunction = include_headcountGrowthByFunction = include_jobOpeningsByFunction = include_jobOpeningsGrowthByFunction = include_hiresInsights = include_alumniInsights = bool(li_a)
    if fields_check == True:
        temporal_list_1 = ["totalEmployeeCount", "growth6Mth", "growth1Yr", "growth2Yr", "averageTenure"]
        final_list.extend(temporal_list_1)
    if include_headcountInsights == True:
        headcountInsights_column_list = ['headcountGrowthMonth0Date', 'headcountGrowthMonth0Count', 'headcountGrowthMonth1Date', 'headcountGrowthMonth1Count', 'headcountGrowthMonth2Date', 'headcountGrowthMonth2Count', 'headcountGrowthMonth3Date', 'headcountGrowthMonth3Count', 'headcountGrowthMonth4Date', 'headcountGrowthMonth4Count', 'headcountGrowthMonth5Date', 'headcountGrowthMonth5Count', 'headcountGrowthMonth6Date', 'headcountGrowthMonth6Count', 'headcountGrowthMonth7Date', 'headcountGrowthMonth7Count', 'headcountGrowthMonth8Date', 'headcountGrowthMonth8Count', 'headcountGrowthMonth9Date', 'headcountGrowthMonth9Count', 'headcountGrowthMonth10Date', 'headcountGrowthMonth10Count', 'headcountGrowthMonth11Date', 'headcountGrowthMonth11Count', 'headcountGrowthMonth12Date', 'headcountGrowthMonth12Count', 'headcountGrowthMonth13Date', 'headcountGrowthMonth13Count', 'headcountGrowthMonth14Date', 'headcountGrowthMonth14Count', 'headcountGrowthMonth15Date', 'headcountGrowthMonth15Count', 'headcountGrowthMonth16Date', 'headcountGrowthMonth16Count', 'headcountGrowthMonth17Date', 'headcountGrowthMonth17Count', 'headcountGrowthMonth18Date', 'headcountGrowthMonth18Count', 'headcountGrowthMonth19Date', 'headcountGrowthMonth19Count', 'headcountGrowthMonth20Date', 'headcountGrowthMonth20Count', 'headcountGrowthMonth21Date', 'headcountGrowthMonth21Count', 'headcountGrowthMonth22Date', 'headcountGrowthMonth22Count', 'headcountGrowthMonth23Date', 'headcountGrowthMonth23Count', 'headcountGrowthMonth24Date', 'headcountGrowthMonth24Count']
        final_list.extend(headcountInsights_column_list)
    if include_headcountByFunction == True:
        headcountByFunction_column_list = ['distributionAccounting', 'distributionAccountingPercentage', 'distributionAdministrative', 'distributionAdministrativePercentage', 'distributionArtsAndDesign', 'distributionArtsAndDesignPercentage', 'distributionBusinessDevelopment', 'distributionBusinessDevelopmentPercentage', 'distributionCommunityAndSocialServices', 'distributionCommunityAndSocialServicesPercentage', 'distributionConsulting', 'distributionConsultingPercentage', 'distributionEducation', 'distributionEducationPercentage', 'distributionEngineering', 'distributionEngineeringPercentage', 'distributionEntrepreneurship', 'distributionEntrepreneurshipPercentage', 'distributionFinance', 'distributionFinancePercentage', 'distributionHealthcareServices', 'distributionHealthcareServicesPercentage', 'distributionHumanResources', 'distributionHumanResourcesPercentage', 'distributionInformationTechnology', 'distributionInformationTechnologyPercentage', 'distributionLegal', 'distributionLegalPercentage', 'distributionMarketing', 'distributionMarketingPercentage', 'distributionMediaAndCommunication', 'distributionMediaAndCommunicationPercentage', 'distributionMilitaryAndProtectiveServices', 'distributionMilitaryAndProtectiveServicesPercentage', 'distributionOperations', 'distributionOperationsPercentage', 'distributionProductManagement', 'distributionProductManagementPercentage', 'distributionProgramAndProjectManagement', 'distributionProgramAndProjectManagementPercentage', 'distributionPurchasing', 'distributionPurchasingPercentage', 'distributionQualityAssurance', 'distributionQualityAssurancePercentage', 'distributionRealEstate', 'distributionRealEstatePercentage', 'distributionResearch', 'distributionResearchPercentage', 'distributionSales', 'distributionSalesPercentage', 'distributionSupport', 'distributionSupportPercentage']
        final_list.extend(headcountByFunction_column_list)
    if include_headcountGrowthByFunction == True:
        headcountGrowthByFunction_column_list = ['growth6MthAccounting', 'growth1YrAccounting', 'growth6MthAdministrative', 'growth1YrAdministrative', 'growth6MthArtsAndDesign', 'growth1YrArtsAndDesign', 'growth6MthBusinessDevelopment', 'growth1YrBusinessDevelopment', 'growth6MthCommunityAndSocialServices', 'growth1YrCommunityAndSocialServices', 'growth6MthConsulting', 'growth1YrConsulting', 'growth6MthEducation', 'growth1YrEducation', 'growth6MthEngineering', 'growth1YrEngineering', 'growth6MthEntrepreneurship', 'growth1YrEntrepreneurship', 'growth6MthFinance', 'growth1YrFinance', 'growth6MthHealthcareServices', 'growth1YrHealthcareServices', 'growth6MthHumanResources', 'growth1YrHumanResources', 'growth6MthInformationTechnology', 'growth1YrInformationTechnology', 'growth6MthLegal', 'growth1YrLegal', 'growth6MthMarketing', 'growth1YrMarketing', 'growth6MthMediaAndCommunication', 'growth1YrMediaAndCommunication', 'growth6MthMilitaryAndProtectiveServices', 'growth1YrMilitaryAndProtectiveServices', 'growth6MthOperations', 'growth1YrOperations', 'growth6MthProductManagement', 'growth1YrProductManagement', 'growth6MthProgramAndProjectManagement', 'growth1YrProgramAndProjectManagement', 'growth6MthPurchasing', 'growth1YrPurchasing', 'growth6MthQualityAssurance', 'growth1YrQualityAssurance', 'growth6MthRealEstate', 'growth1YrRealEstate', 'growth6MthResearch', 'growth1YrResearch', 'growth6MthSales', 'growth1YrSales', 'growth6MthSupport', 'growth1YrSupport']
        final_list.extend(headcountGrowthByFunction_column_list)
    if include_jobOpeningsByFunction == True:
        jobOpeningsByFunction_column_list = ['distributionAccountingJobs', 'distributionAccountingPercentageJobs', 'distributionAdministrativeJobs', 'distributionAdministrativePercentageJobs', 'distributionArtsAndDesignJobs', 'distributionArtsAndDesignPercentageJobs', 'distributionBusinessDevelopmentJobs', 'distributionBusinessDevelopmentPercentageJobs', 'distributionCommunityAndSocialServicesJobs', 'distributionCommunityAndSocialServicesPercentageJobs', 'distributionConsultingJobs', 'distributionConsultingPercentageJobs', 'distributionEducationJobs', 'distributionEducationPercentageJobs', 'distributionEngineeringJobs', 'distributionEngineeringPercentageJobs', 'distributionEntrepreneurshipJobs', 'distributionEntrepreneurshipPercentageJobs', 'distributionFinanceJobs', 'distributionFinancePercentageJobs', 'distributionHealthcareServicesJobs', 'distributionHealthcareServicesPercentageJobs', 'distributionHumanResourcesJobs', 'distributionHumanResourcesPercentageJobs', 'distributionInformationTechnologyJobs', 'distributionInformationTechnologyPercentageJobs', 'distributionLegalJobs', 'distributionLegalPercentageJobs', 'distributionMarketingJobs', 'distributionMarketingPercentageJobs', 'distributionMediaAndCommunicationJobs', 'distributionMediaAndCommunicationPercentageJobs', 'distributionMilitaryAndProtectiveServicesJobs', 'distributionMilitaryAndProtectiveServicesPercentageJobs', 'distributionOperationsJobs', 'distributionOperationsPercentageJobs', 'distributionProductManagementJobs', 'distributionProductManagementPercentageJobs', 'distributionProgramAndProjectManagementJobs', 'distributionProgramAndProjectManagementPercentageJobs', 'distributionPurchasingJobs', 'distributionPurchasingPercentageJobs', 'distributionQualityAssuranceJobs', 'distributionQualityAssurancePercentageJobs', 'distributionRealEstateJobs', 'distributionRealEstatePercentageJobs', 'distributionResearchJobs', 'distributionResearchPercentageJobs', 'distributionSalesJobs', 'distributionSalesPercentageJobs', 'distributionSupportJobs', 'distributionSupportPercentageJobs']
        final_list.extend(jobOpeningsByFunction_column_list)
    if include_jobOpeningsGrowthByFunction == True:
        jobOpeningsGrowthByFunction_column_list = ['growth3MthAccountingJobs', 'growth6MthAccountingJobs', 'growth1YrAccountingJobs', 'growth3MthAdministrativeJobs', 'growth6MthAdministrativeJobs', 'growth1YrAdministrativeJobs', 'growth3MthArtsAndDesignJobs', 'growth6MthArtsAndDesignJobs', 'growth1YrArtsAndDesignJobs', 'growth3MthBusinessDevelopmentJobs', 'growth6MthBusinessDevelopmentJobs', 'growth1YrBusinessDevelopmentJobs', 'growth3MthCommunityAndSocialServicesJobs', 'growth6MthCommunityAndSocialServicesJobs', 'growth1YrCommunityAndSocialServicesJobs', 'growth3MthConsultingJobs', 'growth6MthConsultingJobs', 'growth1YrConsultingJobs', 'growth3MthEducationJobs', 'growth6MthEducationJobs', 'growth1YrEducationJobs', 'growth3MthEngineeringJobs', 'growth6MthEngineeringJobs', 'growth1YrEngineeringJobs', 'growth3MthEntrepreneurshipJobs', 'growth6MthEntrepreneurshipJobs', 'growth1YrEntrepreneurshipJobs', 'growth3MthFinanceJobs', 'growth6MthFinanceJobs', 'growth1YrFinanceJobs', 'growth3MthHealthcareServicesJobs', 'growth6MthHealthcareServicesJobs', 'growth1YrHealthcareServicesJobs', 'growth3MthHumanResourcesJobs', 'growth6MthHumanResourcesJobs', 'growth1YrHumanResourcesJobs', 'growth3MthInformationTechnologyJobs', 'growth6MthInformationTechnologyJobs', 'growth1YrInformationTechnologyJobs', 'growth3MthLegalJobs', 'growth6MthLegalJobs', 'growth1YrLegalJobs', 'growth3MthMarketingJobs', 'growth6MthMarketingJobs', 'growth1YrMarketingJobs', 'growth3MthMediaAndCommunicationJobs', 'growth6MthMediaAndCommunicationJobs', 'growth1YrMediaAndCommunicationJobs', 'growth3MthMilitaryAndProtectiveServicesJobs', 'growth6MthMilitaryAndProtectiveServicesJobs', 'growth1YrMilitaryAndProtectiveServicesJobs', 'growth3MthOperationsJobs', 'growth6MthOperationsJobs', 'growth1YrOperationsJobs', 'growth3MthProductManagementJobs', 'growth6MthProductManagementJobs', 'growth1YrProductManagementJobs', 'growth3MthProgramAndProjectManagementJobs', 'growth6MthProgramAndProjectManagementJobs', 'growth1YrProgramAndProjectManagementJobs', 'growth3MthPurchasingJobs', 'growth6MthPurchasingJobs', 'growth1YrPurchasingJobs', 'growth3MthQualityAssuranceJobs', 'growth6MthQualityAssuranceJobs', 'growth1YrQualityAssuranceJobs', 'growth3MthRealEstateJobs', 'growth6MthRealEstateJobs', 'growth1YrRealEstateJobs', 'growth3MthResearchJobs', 'growth6MthResearchJobs', 'growth1YrResearchJobs', 'growth3MthSalesJobs', 'growth6MthSalesJobs', 'growth1YrSalesJobs', 'growth3MthSupportJobs', 'growth6MthSupportJobs', 'growth1YrSupportJobs']
        final_list.extend(jobOpeningsGrowthByFunction_column_list)
    if include_hiresInsights == True:
        hiresInsights_column_list = ['totalNumberOfSeniorHires', 'hireAllCountMonth0Date', 'hireAllCountMonth0', 'hireSeniorCountMonth0Date', 'hireSeniorCountMonth0', 'hireAllCountMonth1Date', 'hireAllCountMonth1', 'hireSeniorCountMonth1Date', 'hireSeniorCountMonth1', 'hireAllCountMonth2Date', 'hireAllCountMonth2', 'hireSeniorCountMonth2Date', 'hireSeniorCountMonth2', 'hireAllCountMonth3Date', 'hireAllCountMonth3', 'hireSeniorCountMonth3Date', 'hireSeniorCountMonth3', 'hireAllCountMonth4Date', 'hireAllCountMonth4', 'hireSeniorCountMonth4Date', 'hireSeniorCountMonth4', 'hireAllCountMonth5Date', 'hireAllCountMonth5', 'hireSeniorCountMonth5Date', 'hireSeniorCountMonth5', 'hireAllCountMonth6Date', 'hireAllCountMonth6', 'hireSeniorCountMonth6Date', 'hireSeniorCountMonth6', 'hireAllCountMonth7Date', 'hireAllCountMonth7', 'hireSeniorCountMonth7Date', 'hireSeniorCountMonth7', 'hireAllCountMonth8Date', 'hireAllCountMonth8', 'hireSeniorCountMonth8Date', 'hireSeniorCountMonth8', 'hireAllCountMonth9Date', 'hireAllCountMonth9', 'hireSeniorCountMonth9Date', 'hireSeniorCountMonth9', 'hireAllCountMonth10Date', 'hireAllCountMonth10', 'hireSeniorCountMonth10Date', 'hireSeniorCountMonth10', 'hireAllCountMonth11Date', 'hireAllCountMonth11', 'hireSeniorCountMonth11Date', 'hireSeniorCountMonth11', 'hireAllCountMonth12Date', 'hireAllCountMonth12', 'hireSeniorCountMonth12Date', 'hireSeniorCountMonth12', 'hireAllCountMonth13Date', 'hireAllCountMonth13', 'hireSeniorCountMonth13Date', 'hireSeniorCountMonth13', 'hireAllCountMonth14Date', 'hireAllCountMonth14', 'hireSeniorCountMonth14Date', 'hireSeniorCountMonth14', 'hireAllCountMonth15Date', 'hireAllCountMonth15', 'hireSeniorCountMonth15Date', 'hireSeniorCountMonth15', 'hireAllCountMonth16Date', 'hireAllCountMonth16', 'hireSeniorCountMonth16Date', 'hireSeniorCountMonth16', 'hireAllCountMonth17Date', 'hireAllCountMonth17', 'hireSeniorCountMonth17Date', 'hireSeniorCountMonth17', 'hireAllCountMonth18Date', 'hireAllCountMonth18', 'hireSeniorCountMonth18Date', 'hireSeniorCountMonth18', 'hireAllCountMonth19Date', 'hireAllCountMonth19', 'hireSeniorCountMonth19Date', 'hireSeniorCountMonth19', 'hireAllCountMonth20Date', 'hireAllCountMonth20', 'hireSeniorCountMonth20Date', 'hireSeniorCountMonth20', 'hireAllCountMonth21Date', 'hireAllCountMonth21', 'hireSeniorCountMonth21Date', 'hireSeniorCountMonth21', 'hireAllCountMonth22Date', 'hireAllCountMonth22', 'hireSeniorCountMonth22Date', 'hireSeniorCountMonth22', 'hireAllCountMonth23Date', 'hireAllCountMonth23', 'hireSeniorCountMonth23Date', 'hireSeniorCountMonth23', 'hireAllCountMonth24Date', 'hireAllCountMonth24', 'hireSeniorCountMonth24Date', 'hireSeniorCountMonth24', 'seniorHires1_title', 'seniorHires1_linkedInUrl', 'seniorHires1_fullName', 'seniorHires1_date', 'seniorHires2_title', 'seniorHires2_linkedInUrl', 'seniorHires2_fullName', 'seniorHires2_date', 'seniorHires3_title', 'seniorHires3_linkedInUrl', 'seniorHires3_fullName', 'seniorHires3_date']
        final_list.extend(hiresInsights_column_list)
    if include_alumniInsights == True:
        alumniInsights_column_list = ['alumni1_currentTitle', 'alumni1_linkedInUrl', 'alumni1_fullName', 'alumni1_exitDate', 'alumni1_exitTitle', 'alumni2_currentTitle', 'alumni2_linkedInUrl', 'alumni2_fullName', 'alumni2_exitDate', 'alumni2_exitTitle', 'alumni3_currentTitle', 'alumni3_linkedInUrl', 'alumni3_fullName', 'alumni3_exitDate', 'alumni3_exitTitle']
        final_list.extend(alumniInsights_column_list)
    missing_columns = set(final_list) - set(df_final.columns)
    if missing_columns:
        df_final = pd.concat([df_final, pd.DataFrame(columns=list(missing_columns))], axis=1)
    df_final = df_final[final_list]
    return df_final
def linkedin_lead(csrf_token, dataframe, column_name, cookies_dict, streamlit_execution=False):
    headers = {
        'csrf-token': csrf_token,
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
        'x-restli-protocol-version': '2.0.0',
        "accept-language": "en-AU,en-GB;q=0.9,en-US;q=0.8,en;q=0.7",
        "x-li-lang": "en_US",
    }
    def get_profile(data):
        if not data or "status" in data and data["status"] != 200:
            return {}
        profile = data["profile"]
        if "miniProfile" in profile:
            if "picture" in profile["miniProfile"]:
                profile["displayPictureUrl"] = profile["miniProfile"]["picture"][
                    "com.linkedin.common.VectorImage"
                ]["rootUrl"]
                images_data = profile["miniProfile"]["picture"][
                    "com.linkedin.common.VectorImage"
                ]["artifacts"]
                for img in images_data:
                    w, h, url_segment = itemgetter(
                        "width", "height", "fileIdentifyingUrlPathSegment"
                    )(img)
                    profile[f"img_{w}_{h}"] = url_segment
            profile["profile_id"] = profile["miniProfile"]["entityUrn"].split(":")[3]
            profile["profile_urn"] = profile["miniProfile"]["entityUrn"]
            profile["member_urn"] = profile["miniProfile"]["objectUrn"]
            profile["public_id"] = profile["miniProfile"]["publicIdentifier"]
            del profile["miniProfile"]
        del profile["defaultLocale"]
        del profile["supportedLocales"]
        del profile["versionTag"]
        del profile["showEducationOnProfileTopCard"]
        experience = data["positionView"]["elements"]
        for item in experience:
            if "company" in item and "miniCompany" in item["company"]:
                if "logo" in item["company"]["miniCompany"]:
                    logo = item["company"]["miniCompany"]["logo"].get(
                        "com.linkedin.common.VectorImage"
                    )
                    if logo:
                        item["companyLogoUrl"] = logo["rootUrl"]
                del item["company"]["miniCompany"]
        profile["experience"] = experience
        education = data["educationView"]["elements"]
        for item in education:
            if "school" in item:
                if "logo" in item["school"]:
                    item["school"]["logoUrl"] = item["school"]["logo"][
                        "com.linkedin.common.VectorImage"
                    ]["rootUrl"]
                    del item["school"]["logo"]
        profile["education"] = education
        languages = data["languageView"]["elements"]
        for item in languages:
            del item["entityUrn"]
        profile["languages"] = languages
        publications = data["publicationView"]["elements"]
        for item in publications:
            del item["entityUrn"]
            for author in item.get("authors", []):
                del author["entityUrn"]
        profile["publications"] = publications
        certifications = data["certificationView"]["elements"]
        for item in certifications:
            del item["entityUrn"]
        profile["certifications"] = certifications
        volunteer = data["volunteerExperienceView"]["elements"]
        for item in volunteer:
            del item["entityUrn"]
        profile["volunteer"] = volunteer
        honors = data["honorView"]["elements"]
        for item in honors:
            del item["entityUrn"]
        profile["honors"] = honors
        projects = data["projectView"]["elements"]
        for item in projects:
            del item["entityUrn"]
        profile["projects"] = projects
        return profile
    def get_profile_contact_info(data):
        contact_info = {
            "email_address": data.get("emailAddress"),
            "websites": [],
            "twitter": data.get("twitterHandles"),
            "birthdate": data.get("birthDateOn"),
            "ims": data.get("ims"),
            "phone_numbers": data.get("phoneNumbers", []),
        }
        websites = data.get("websites", [])
        for item in websites:
            if "com.linkedin.voyager.identity.profile.StandardWebsite" in item["type"]:
                item["label"] = item["type"]["com.linkedin.voyager.identity.profile.StandardWebsite"]["category"]
            elif "" in item["type"]:
                item["label"] = item["type"]["com.linkedin.voyager.identity.profile.CustomWebsite"]["label"]
            del item["type"]
        contact_info["websites"] = websites
        return contact_info
    def get_profile_skills(data):
        skills = data.get("elements", [])
        for item in skills:
            del item["entityUrn"]
        return skills
    def get_profile_network_info(data):
        return data.get("data", data)
    def generate_columns(count, patterns):
        return [pattern.format(i+1) for i in range(count) for pattern in patterns]
    MAX_COUNTS = {'experience': 5, 'education': 3, 'language': 10, 'certification': 10, 'volunteer': 3, 'honor': 10}
    COLUMN_PATTERNS = {
        'experience': [
            'experience{}_company', 'experience{}_companyId', 'experience{}_companyUrl',
            'experience{}_jobTitle', 'experience{}_jobLocation', 'experience{}_jobDescription',
            'experience{}_jobDateRange', 'experience{}_jobDuration', 'experience{}_companyIndustry',
            'experience{}_companySize'
        ],
        'education': [
            'education{}_school', 'education{}_schoolId', 'education{}_schoolUrl',
            'education{}_schoolDegree', 'education{}_schoolFieldOfStudy', 'education{}_schoolDescription',
            'education{}_schoolDateRange', 'education{}_schoolDuration', 'education{}_schoolActive'
        ],
        'language': [
            'language{}_name', 'language{}_proficiency'
        ],
        'certification': [
            'certification{}_name', 'certification{}_dateRange', 'certification{}_duration',
            'certification{}_url', 'certification{}_companyName', 'certification{}_companyId',
            'certification{}_companyUrl', 'certification{}_universalName', 'certification{}_logo100x100',
            'certification{}_logo200x200', 'certification{}_logo400x400', 'certification{}_showcase',
            'certification{}_companyActive'
        ],
        'volunteer': [
            'volunteer{}_role', 'volunteer{}_companyName', 'volunteer{}_dateRange',
            'volunteer{}_duration', 'volunteer{}_description', 'volunteer{}_cause'
        ],
        'honor': [
            'honor{}_title', 'honor{}_issuer', 'honor{}_date'
        ]
    }
    counts = {'experience': 5, 'education': 3, 'language': 10, 'certification': 10, 'volunteer': 3, 'honor': 10}
    columns_lists = {}
    for key, max_count in MAX_COUNTS.items():
        count = counts[key]
        if count > max_count:
            raise ValueError(f"{key.capitalize()} count exceeds {max_count}. Change the number and execute the script again.")
        columns_lists[key] = generate_columns(count, COLUMN_PATTERNS[key])
    def create_linkedinProfileUrl(universalName):
        return f"https://www.linkedin.com/in/{universalName}/" if universalName else None
    def create_linkedinProfile(vmid):
        return f"https://www.linkedin.com/in/{vmid}/" if vmid else None
    def extract_userId(member_urn, pattern):
        if member_urn:
            match = pattern.search(member_urn)
            return match.group(1) if match else None
        return None
    def create_linkedinSalesNavigatorUrl(vmid):
        return f"https://www.linkedin.com/sales/people/{vmid},name" if vmid else None
    def create_picture_urls(base_url, img_suffix):
        return f"{base_url}{img_suffix}" if base_url and img_suffix else None
    def create_birthdate(birthdateDay, birthdateMonth):
        return f"{birthdateMonth} {birthdateDay}" if birthdateDay and birthdateMonth else None
    def create_twitter(twitterUsername):
        return f"https://twitter.com/{twitterUsername}/" if twitterUsername else None
    def extract_industryCode(industryUrn, pattern):
        if industryUrn:
            match = pattern.search(industryUrn)
            return match.group(1) if match else None
        return None
    def extract_company_id(company_urn):
        match = re.search(r"urn:li:(fs_miniCompany|company):(.+)", company_urn or '')
        return match.group(2) if match else None
    def create_company_url(company_id):
        return f"https://www.linkedin.com/company/{company_id}/" if company_id else None
    def date_range_str(start_date, end_date):
        if not start_date.get('month') or not start_date.get('year'):
            return None
        start_str = f"{months[start_date.get('month')]} {start_date.get('year')}"
        end_str = f"{months[end_date.get('month')]} {end_date.get('year')}" if end_date and end_date.get('month') and end_date.get('year') else "Present"
        return f"{start_str} - {end_str}"
    def date_diff_in_years_format(start_date, end_date=None):
        if not start_date.get('month') or not start_date.get('year'):
            return None
        start = datetime(start_date['year'], start_date['month'], 1)
        end = datetime.now() if not end_date or not end_date.get('year') or not end_date.get('month') else datetime(end_date['year'], end_date['month'], 1)
        delta = end - start
        years, remainder = divmod(delta.days, 365.25)
        months = round(remainder / 30.44)
        if months == 12:
            years += 1
            months = 0
        year_str = f"{int(years)} yr" if years == 1 else f"{int(years)} yrs" if years > 1 else ""
        month_str = f"{months} mo" if months == 1 else f"{months} mos" if months > 1 else ""
        return f"{year_str} {month_str}".strip()
    def flattened_experience(profile):
        experiences = profile.get('experience', [])
        flattened_data = {
            f"experience{idx+1}_{key}": value
            for idx, experience in enumerate(experiences)
            for key, value in {
                "company": experience.get('companyName'),
                "companyId": extract_company_id(experience.get('companyUrn')),
                "companyUrl": create_company_url(extract_company_id(experience.get('companyUrn'))),
                "jobTitle": experience.get('title'),
                "jobLocation": experience.get('locationName'),
                "jobDescription": experience.get('description'),
                "jobDateRange": date_range_str(experience.get('timePeriod', {}).get('startDate', {}), experience.get('timePeriod', {}).get('endDate')),
                "jobDuration": date_diff_in_years_format(experience.get('timePeriod', {}).get('startDate', {}), experience.get('timePeriod', {}).get('endDate')),
                "companyIndustry": ', '.join(experience.get('company', {}).get('industries', [])),
                "companySize": "-".join([str(val) for val in experience.get('company', {}).get('employeeCountRange', {}).values() if val])
            }.items()
        }
        return flattened_data
    def extract_school_id(school_urn):
        match = re.search(r"urn:li:school:(.+)", school_urn or '')
        return match.group(1) if match else None
    def create_school_url(school_id):
        return f"https://www.linkedin.com/school/{school_id}/" if school_id else None
    def flattened_education(profile):
        educations = profile.get('education', [])
        flattened_data = {
            f"education{idx+1}_{key}": value
            for idx, education in enumerate(educations)
            for key, value in {
                "school": education.get('schoolName'),
                "schoolId": extract_school_id(education.get('schoolUrn')),
                "schoolUrl": create_school_url(extract_school_id(education.get('schoolUrn'))),
                "schoolDegree": education.get('degreeName'),
                "schoolFieldOfStudy": education.get('fieldOfStudy'),
                "schoolDescription": education.get('description'),
                "schoolDateRange": date_range_str(education.get('timePeriod', {}).get('startDate', {}), education.get('timePeriod', {}).get('endDate')),
                "schoolDuration": date_diff_in_years_format(education.get('timePeriod', {}).get('startDate', {}), education.get('timePeriod', {}).get('endDate')),
                "schoolActive": education.get('active', False)
            }.items()
        }
        return flattened_data
    def flattened_project(profile):
        projects = profile.get('projects', [])
        flattened_data = {
            f"project{idx+1}_{key}": value
            for idx, project in enumerate(projects)
            for key, value in {
                "title": project.get('title'),
                "description": project.get('description'),
                "dateRange": date_range_str(project.get('timePeriod', {}).get('startDate', {}), project.get('timePeriod', {}).get('endDate', {})),
                "duration": date_diff_in_years_format(project.get('timePeriod', {}).get('startDate', {}), project.get('timePeriod', {}).get('endDate', {})),
            }.items()
        }
        return flattened_data
    def flattened_languages(profile):
        languages = profile.get('languages', [])
        flattened_data = {
            f"language{idx+1}_{key}": value
            for idx, language in enumerate(languages)
            for key, value in {
                "name": language.get('name'),
                "proficiency": language.get('proficiency'),
            }.items()
        }
        return flattened_data
    def get_logo_url(artifacts, root_url, desired_position=None):
        if not artifacts or not root_url:
            return None
        valid_artifacts = [artifact for artifact in artifacts if artifact.get('width') is not None]
        if not valid_artifacts:
            return None
        sorted_artifacts = sorted(valid_artifacts, key=lambda x: x.get('width'))
        position_map = {
            'lowest': 0,
            'middle': len(sorted_artifacts) // 2,
            'highest': -1
        }
        desired_position_index = position_map.get(desired_position)
        if desired_position_index is None:
            return None
        desired_artifact = sorted_artifacts[desired_position_index]
        return root_url + desired_artifact.get('fileIdentifyingUrlPathSegment')
    def flattened_certification(profile):
        certifications = profile.get('certifications', [])
        flattened_data = {
            f"certification{idx+1}_{key}": value
            for idx, certification in enumerate(certifications)
            for key, value in {
                "name": certification.get('name'),
                "dateRange": date_range_str(certification.get('timePeriod', {}).get('startDate', {}), certification.get('timePeriod', {}).get('endDate')),
                "duration": date_diff_in_years_format(certification.get('timePeriod', {}).get('startDate', {}), certification.get('timePeriod', {}).get('endDate')),
                "url": certification.get('url'),
                "companyName": certification.get('company', {}).get('name'),
                "companyId": extract_company_id(certification.get('company', {}).get('objectUrn')),
                "companyUrl": create_company_url(extract_company_id(certification.get('company', {}).get('objectUrn'))),
                "universalName": certification.get('company', {}).get('universalName'),
                "logo100x100": get_logo_url(certification.get('company', {}).get('logo', {}).get('com.linkedin.common.VectorImage', {}).get('artifacts'),
                                            certification.get('company', {}).get('logo', {}).get('com.linkedin.common.VectorImage', {}).get('rootUrl'), 'lowest'),
                "logo200x200": get_logo_url(certification.get('company', {}).get('logo', {}).get('com.linkedin.common.VectorImage', {}).get('artifacts'),
                                            certification.get('company', {}).get('logo', {}).get('com.linkedin.common.VectorImage', {}).get('rootUrl'), 'middle'),
                "logo400x400": get_logo_url(certification.get('company', {}).get('logo', {}).get('com.linkedin.common.VectorImage', {}).get('artifacts'),
                                            certification.get('company', {}).get('logo', {}).get('com.linkedin.common.VectorImage', {}).get('rootUrl'), 'highest'),
                "showcase": certification.get('company', {}).get('showcase'),
                "companyActive": certification.get('company', {}).get('active')
            }.items()
        }
        return flattened_data
    def flattened_volunteer(profile):
        volunteers = profile.get('volunteer', [])
        flattened_data = {
            f"volunteer{idx+1}_{key}": value
            for idx, volunteer in enumerate(volunteers)
            for key, value in {
                "role": volunteer.get('role'),
                "companyName": volunteer.get('companyName'),
                "dateRange": date_range_str(volunteer.get('timePeriod', {}).get('startDate', {}), volunteer.get('timePeriod', {}).get('endDate')),
                "duration": date_diff_in_years_format(volunteer.get('timePeriod', {}).get('startDate', {}), volunteer.get('timePeriod', {}).get('endDate')),
                "description": volunteer.get('description'),
                "cause": volunteer.get('cause')
            }.items()
        }
        return flattened_data
    def flattened_honor(profile):
        honors = profile.get('honors', [])
        flattened_data = {
            f"honor{idx+1}_{key}": value
            for idx, honor in enumerate(honors)
            for key, value in {
                "title": honor.get('title'),
                "issuer": honor.get('issuer'),
                "date": f"{months[honor.get('issueDate', {}).get('month', 0)]} {honor.get('issueDate', {}).get('year', '')}" if honor.get('issueDate') else None
            }.items() if value is not None
        }
        return flattened_data
    def extract_linkedin_universal_name(linkedin_url, pattern):
        try:
            return pattern.match(str(linkedin_url)).group(3)
        except AttributeError:
            return None
    pattern_universalName = re.compile(r"https?://([\w]+\.)?linkedin\.com/(in|sales/lead|sales/people)/([A-z0-9\._\%&'-]+)/?")
    df_final = pd.DataFrame()
    dataframe.drop_duplicates(subset=[column_name], inplace=True)
    dataframe['wordToSearch'] = dataframe[column_name].apply(lambda x: extract_linkedin_universal_name(x, pattern_universalName))
    original_to_wordToSearch = dict(zip(dataframe[column_name], dataframe['wordToSearch']))
    columnName_values = dataframe[column_name].tolist()
    print("LinkedIn lead scrape")
    progress_bar = tqdm(total = len(columnName_values))
    #--STREAMLIT--#
    if streamlit_execution:
        st.write("---LinkedIn lead scrape---")
        progress_bar_linkedin_lead = st.progress(0)
        number_iterations = len(columnName_values)
        index_steamlit = 0
    #--STREAMLIT--#
    for index, profile in enumerate(columnName_values):
        df_loop_final = pd.DataFrame()
        df_loop_base = pd.DataFrame()
        df_loop_websites = pd.DataFrame()
        df_loop_experience = pd.DataFrame()
        df_loop_education = pd.DataFrame()
        df_loop_project = pd.DataFrame()
        df_loop_languages = pd.DataFrame()
        df_loop_certifications = pd.DataFrame()
        df_loop_volunteers = pd.DataFrame()
        df_loop_honors = pd.DataFrame()
        wordToSearch = original_to_wordToSearch.get(columnName_values[index])
        error = None
        if wordToSearch is None:
            error = "Invalid LinkedIn URL"
            df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
            df_loop_final = pd.concat([df_loop_final, df_loop_base, df_loop_websites, df_loop_experience, df_loop_education, df_loop_project, df_loop_languages, df_loop_certifications, df_loop_volunteers, df_loop_honors], axis=1)
            df_final = pd.concat([df_final, df_loop_final])
            index += 1
            progress_bar.update(1)
            #--STREAMLIT--#
            if streamlit_execution:
                index_steamlit += 1
                progress_bar_linkedin_lead.progress(index_steamlit / number_iterations)
            #--STREAMLIT--#
            continue
        try:
            request_url_profileView = f'https://www.linkedin.com/voyager/api/identity/profiles/{urllib.parse.unquote(wordToSearch)}/profileView'
            response_profileView = requests.get(url=request_url_profileView, cookies=cookies_dict, headers=headers)
            response_profileView_json = response_profileView.json()
            profile = get_profile(response_profileView_json)
        except KeyError as e:
            profile = {}
            error = "LinkedIn down"
            df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
            df_loop_final = pd.concat([df_loop_final, df_loop_base, df_loop_websites, df_loop_experience, df_loop_education, df_loop_project, df_loop_languages, df_loop_certifications, df_loop_volunteers, df_loop_honors], axis=1)
            df_final = pd.concat([df_final, df_loop_final])
            index += 1
            progress_bar.update(1)
            #--STREAMLIT--#
            if streamlit_execution:
                index_steamlit += 1
                progress_bar_linkedin_lead.progress(index_steamlit / number_iterations)
            #--STREAMLIT--#
            continue
        except Exception as e:
            profile = {}
            error = str(e)
            df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
            df_loop_final = pd.concat([df_loop_final, df_loop_base, df_loop_websites, df_loop_experience, df_loop_education, df_loop_project, df_loop_languages, df_loop_certifications, df_loop_volunteers, df_loop_honors], axis=1)
            df_final = pd.concat([df_final, df_loop_final])
            index += 1
            progress_bar.update(1)
            #--STREAMLIT--#
            if streamlit_execution:
                index_steamlit += 1
                progress_bar_linkedin_lead.progress(index_steamlit / number_iterations)
            #--STREAMLIT--#
            continue
        #-->network_info
        try:
            request_url_networkinfo = f'https://www.linkedin.com/voyager/api/identity/profiles/{urllib.parse.unquote(wordToSearch)}/networkinfo'
            response_networkinfo = requests.get(url=request_url_networkinfo, cookies=cookies_dict, headers=headers)
            response_networkinfo_json = response_networkinfo.json()
            profile["network_info"] = get_profile_network_info(response_networkinfo_json) or {}
        except Exception as e:
            profile["network_info"] = {}
        #-->contact_info
        try:
            request_url_profileContactInfo = f'https://www.linkedin.com/voyager/api/identity/profiles/{urllib.parse.unquote(wordToSearch)}/profileContactInfo'
            response_profileContactInfo = requests.get(url=request_url_profileContactInfo, cookies=cookies_dict, headers=headers)
            response_profileContactInfo_json = response_profileContactInfo.json()
            profile["contact_info"] = get_profile_contact_info(response_profileContactInfo_json) or {}
        except Exception as e:
            profile["contact_info"] = {}
        #-->skills_info
        try:
            request_url_skills = 'https://www.linkedin.com/voyager/api/identity/profiles/zainkahn/skills'
            response_skills = requests.get(url=request_url_skills, cookies=cookies_dict, headers=headers)
            response_skills_json = response_skills.json()
            profile["skills_info"] = get_profile_skills(response_skills_json) or {}
        except Exception as e:
            profile["skills_info"] = {} 
        #-->universalName
        universalName = profile.get('public_id', None)
        #-->linkedinProfileUrl
        linkedinProfileUrl = create_linkedinProfileUrl(universalName)
        #-->description
        description = profile.get('summary', None)
        #-->headline
        headline = profile.get('headline', None)
        #-->location
        location = profile.get('geoLocationName', None)
        #-->country
        country = profile.get('geoCountryName', None)
        #-->firstName
        firstName = profile.get('firstName', None)
        #-->lastName
        lastName = profile.get('lastName', None)
        #-->fullName
        fullName = ' '.join([name for name in (firstName, lastName) if name is not None]).strip()
        #-->subscribers
        subscribers = profile.get("network_info", {}).get('followersCount', None)
        #-->connectionDegree
        connectionDegree = profile.get("network_info", {}).get("distance", {}).get("value", None)
        connectionDegree_mapping = {"DISTANCE_3": "3rd", "DISTANCE_2": "2nd", "DISTANCE_1": "1st", "OUT_OF_NETWORK": "Out of network"}
        connectionDegree = connectionDegree_mapping.get(connectionDegree, connectionDegree)
        #-->vmid
        vmid = profile.get('profile_id', None)
        #-->linkedinProfile
        linkedinProfile = create_linkedinProfile(vmid)
        #-->userId
        pattern_userId = re.compile(r"urn:li:member:(.+)")
        userId = extract_userId(profile.get('member_urn', None), pattern_userId)
        #-->linkedinSalesNavigatorUrl
        linkedinSalesNavigatorUrl = create_linkedinSalesNavigatorUrl(vmid)
        #-->connectionsCount
        connectionsCount = profile.get("network_info", {}).get('connectionsCount', None)
        #-->picture100x100, picture200x200, picture400x400, picture800x800
        img_sizes = ['100_100', '200_200', '400_400', '800_800']
        displayPictureUrl = profile.get('displayPictureUrl', None)
        picture_urls = {}
        for size in img_sizes:
            img_suffix = profile.get(f'img_{size}', None)
            picture_urls[size] = create_picture_urls(displayPictureUrl, img_suffix)
        picture100x100 = picture_urls['100_100']
        picture200x200 = picture_urls['200_200']
        picture400x400 = picture_urls['400_400']
        picture800x800 = picture_urls['800_800']
        #-->birthdate
        months = [None, 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        birthdateDay = profile.get("contact_info", {}).get("birthdate", {}).get('day', None) if isinstance(profile.get("contact_info", {}).get("birthdate"), dict) else None
        birthdateMonthNumber = profile.get("contact_info", {}).get("birthdate", {}).get('month', None) if isinstance(profile.get("contact_info", {}).get("birthdate"), dict) else None
        birthdateMonth = months[birthdateMonthNumber] if birthdateMonthNumber else None
        birthdate = create_birthdate(birthdateDay, birthdateMonth)
        #-->emailAddress
        emailAddress = profile.get("contact_info", {}).get("email_address", None)
        '''
        #-->website1, website2, website3, websiteN
        websites_data = profile.get("contact_info",{}).get("websites", [])
        websites_dict = {f'website{i+1}': site.get('url') for i, site in enumerate(websites_data) if 'url' in site}
        df_loop_websites = pd.DataFrame([websites_dict])
        '''
        #-->twitter
        twitterUsername = next((t.get('name') for t in (profile.get("contact_info", {}).get("twitter") or []) if 'name' in t), None)
        twitter = create_twitter(twitterUsername)
        #-->phoneNumberType and phoneNumberValue
        phone_data = next((pn for pn in profile.get("contact_info", {}).get("phone_numbers", []) if 'number' in pn), {})
        phoneNumberType = phone_data.get("type", None)
        phoneNumberValue = phone_data.get("number", None)
        #-->industryCode
        industryCode_pattern = re.compile(r"urn:li:fs_industry:(.+)")
        industryCode = extract_industryCode(profile.get('industryUrn', None), industryCode_pattern)
        #-->industry
        industry = profile.get('industryName', None)
        #-->isStudent
        isStudent = profile.get('student', None)
        #-->df_loop_experience
        df_loop_experience = pd.DataFrame([flattened_experience(profile)])
        #-->df_loop_education
        df_loop_education = pd.DataFrame([flattened_education(profile)])
        #-->df_loop_project
        df_loop_project = pd.DataFrame([flattened_project(profile)])
        #-->df_loop_languages
        df_loop_languages = pd.DataFrame([flattened_languages(profile)])
        #-->df_loop_certifications
        df_loop_certifications = pd.DataFrame([flattened_certification(profile)])
        #-->df_loop_volunteers
        df_loop_volunteers = pd.DataFrame([flattened_volunteer(profile)])
        #-->df_loop_honors
        df_loop_honors = pd.DataFrame([flattened_honor(profile)])
        #-->allSkills
        skills = profile["skills_info"]
        allSkills = ", ".join([skill["name"] for skill in skills])
        #-->timestamp
        current_timestamp = datetime.now()
        timestamp = current_timestamp.strftime('%Y-%m-%d %H:%M:%S')
        all_variables = locals()
        selected_vars = {var: [all_variables[var]] for var in ["universalName", "linkedinProfileUrl", "firstName", "lastName", "fullName", "description", "headline", "location", "country", "subscribers", "connectionDegree", "vmid", "linkedinProfile", "userId", "linkedinSalesNavigatorUrl", "connectionsCount", "picture100x100", "picture200x200", "picture400x400", "picture800x800", "birthdate", "emailAddress", "twitter", "phoneNumberType", "phoneNumberValue", "industryCode", "industry", "isStudent", "allSkills", "timestamp"]}
        df_loop_base = pd.DataFrame(selected_vars)        
        error = None
        df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
        df_loop_final = pd.concat([df_loop_final, df_loop_base, df_loop_websites, df_loop_experience, df_loop_education, df_loop_project, df_loop_languages, df_loop_certifications, df_loop_volunteers, df_loop_honors], axis=1)
        df_final = pd.concat([df_final, df_loop_final])
        index += 1
        progress_bar.update(1)
        #--STREAMLIT--#
        if streamlit_execution:
            index_steamlit += 1
            progress_bar_linkedin_lead.progress(index_steamlit / number_iterations)
        #--STREAMLIT--#
    progress_bar.close()    
    selected_columns = []
    for columns in columns_lists.values():
        selected_columns.extend(columns)
    final_columns = ["query","error","universalName", "linkedinProfileUrl", "firstName", "lastName", "fullName", "description", "headline", "location", "country", "subscribers", "connectionDegree", "vmid", "linkedinProfile", "userId", "linkedinSalesNavigatorUrl", "connectionsCount", "picture100x100", "picture200x200", "picture400x400", "picture800x800", "birthdate", "emailAddress", "twitter", "phoneNumberType", "phoneNumberValue", "industryCode", "industry", "isStudent", "allSkills", "timestamp"]
    final_columns.extend(selected_columns)
    df_final = df_final.reindex(columns=final_columns)
    df_final = df_final[final_columns]
    return df_final
def company_activity_extractor(csrf_token, dataframe, column_name, cookies_dict, streamlit_execution=False):
    headers = {
        'csrf-token': csrf_token,
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
        'x-restli-protocol-version': '2.0.0',
        "accept-language": "en-AU,en-GB;q=0.9,en-US;q=0.8,en;q=0.7",
        "x-li-lang": "en_US",
    }
    def create_sharedPostUrl(sharedPostUrl):
        if sharedPostUrl:
            return f"https://www.linkedin.com/feed/update/{sharedPostUrl}/"
        return None
    def extract_sharedJobUrl(sharedJobUrl):
        if sharedJobUrl:
            match = re.search(r'(https://www\.linkedin\.com/jobs/view/\d+)/', sharedJobUrl)
            if match:
                return match.group(1) + "/"
        return None
    def extract_postDate(postDate):
        if postDate:
            match = re.search(r'^(.*?)\s*•', postDate)
        if match:
            return match.group(1).strip()
        return None
    def create_profileUrl(profileUrl):
        if profileUrl:
            return f"https://www.linkedin.com/in/{profileUrl}/"
        return None
    class ForbiddenAccessException(Exception):
        pass
    def extract_linkedin_universal_name(linkedin_url, pattern):
        try:
            return pattern.match(str(linkedin_url)).group(1)
        except AttributeError:
            return None
    def fetch_company_updates(company, start=0, pagination_token=None, accumulated_elements=None):
        if accumulated_elements is None:
            accumulated_elements = []
        params = {
            "companyUniversalName": company,
            "q": "companyFeedByUniversalName",
            "moduleKey": "member-share",
            "count": 100,
            "start": start,
        }
        if pagination_token:
            params["paginationToken"] = pagination_token
        request_url = 'https://www.linkedin.com/voyager/api/feed/updates'
        try:
            response = requests.get(url=request_url, params=params, cookies=cookies_dict, headers=headers)
            response.raise_for_status()
            response_json = response.json()
        except requests.HTTPError as http_err:
            if http_err.response.status_code == 403:
                raise ForbiddenAccessException('Access denied. Please check your permissions or authentication tokens.')
            else:
                print(f'HTTP error occurred: {http_err}')
                return accumulated_elements
        except Exception as err:
            print(f'An error occurred: {err}')
            return accumulated_elements
        if 'elements' in response_json and 'paging' in response_json:
            new_elements = response_json.get('elements', [])
            accumulated_elements.extend(new_elements)
            total = response_json['paging'].get('total', 0)
            next_start = start + params["count"]
            if next_start < total:
                pagination_token = response_json.get('metadata', {}).get('paginationToken')
                return fetch_company_updates(company, start=next_start, pagination_token=pagination_token, accumulated_elements=accumulated_elements)
        else:
            print('Unexpected response structure:', response_json)
        return accumulated_elements
    pattern_universalName = re.compile(r"^(?:https?:\/\/)?(?:[\w]+\.)?linkedin\.com\/(?:company|company-beta|school)\/([A-Za-z0-9\._\%&'-]+?)(?:\/|\?|#|$)", re.IGNORECASE)
    df_final = pd.DataFrame()
    dataframe.drop_duplicates(subset=[column_name], inplace=True)
    dataframe['wordToSearch'] = dataframe[column_name].apply(lambda x: extract_linkedin_universal_name(x, pattern_universalName))
    original_to_wordToSearch = dict(zip(dataframe[column_name], dataframe['wordToSearch']))
    columnName_values = dataframe[column_name].tolist()
    print("LinkedIn company activity extractor")
    progress_bar = tqdm(total = len(columnName_values))
    #--STREAMLIT--#
    if streamlit_execution:
        st.write("---LinkedIn account activity scrape---")
        progress_bar_company_activity_extractor = st.progress(0)
        number_iterations = len(columnName_values)
        index_steamlit = 0
    #--STREAMLIT--#
    for index, company in enumerate(columnName_values):
        df_loop_final = pd.DataFrame()
        df_loop_base = pd.DataFrame()
        wordToSearch = original_to_wordToSearch.get(columnName_values[index])
        error = None
        if wordToSearch is None:
            error = "Invalid LinkedIn URL"
            df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
            df_final = pd.concat([df_final, df_loop_final])
            index += 1
            progress_bar.update(1)
            #--STREAMLIT--#
            if streamlit_execution:
                index_steamlit += 1
                progress_bar_company_activity_extractor.progress(index_steamlit / number_iterations)
            #--STREAMLIT--#
            continue
        try:
            company_posts = fetch_company_updates(wordToSearch)
            if not company_posts:
                error = "No activities"
                df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
                df_final = pd.concat([df_final, df_loop_final])
                index += 1
                progress_bar.update(1)
                #--STREAMLIT--#
                if streamlit_execution:
                    index_steamlit += 1
                    progress_bar_company_activity_extractor.progress(index_steamlit / number_iterations)
                #--STREAMLIT--#
                continue
        except ForbiddenAccessException as e:
            company_posts = {}
            error = "LinkedIn down"
            df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
            df_final = pd.concat([df_final, df_loop_final])
            index += 1
            progress_bar.update(1)
            #--STREAMLIT--#
            if streamlit_execution:
                index_steamlit += 1
                progress_bar_company_activity_extractor.progress(index_steamlit / number_iterations)
            #--STREAMLIT--#
            continue
        except Exception as e:
            company_posts = {}
            error = str(e)
            df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
            df_final = pd.concat([df_final, df_loop_final])
            index += 1
            progress_bar.update(1)
            #--STREAMLIT--#
            if streamlit_execution:
                index_steamlit += 1
                progress_bar_company_activity_extractor.progress(index_steamlit / number_iterations)
            #--STREAMLIT--#
            continue
        #-->DATA MANIPULATION START<--
        for post in company_posts:
            #-->postUrl
            postUrl = safe_extract(post, 'permalink')
            #-->imgUrl
            rootUrl = safe_extract(post, 'value', 'com.linkedin.voyager.feed.render.UpdateV2', 'content', 'com.linkedin.voyager.feed.render.ImageComponent', 'images', 0, 'attributes', 0, 'vectorImage', 'rootUrl')
            fileIdentifyingUrlPathSegment = safe_extract(post, 'value', 'com.linkedin.voyager.feed.render.UpdateV2', 'content', 'com.linkedin.voyager.feed.render.ImageComponent', 'images', 0, 'attributes', 0, 'vectorImage', 'artifacts', 5, 'fileIdentifyingUrlPathSegment')
            imgUrl = None
            if rootUrl and fileIdentifyingUrlPathSegment:
                imgUrl = rootUrl + fileIdentifyingUrlPathSegment
            #-->postContent
            postContent = safe_extract(post, 'value', 'com.linkedin.voyager.feed.render.UpdateV2', 'commentary', 'text', 'text')
            #-->postType
            postType = None
            if postContent:
                postType = "Text"
            if imgUrl:
                postType = "Image"
            #-->likeCount
            likeCount = safe_extract(post, 'value', 'com.linkedin.voyager.feed.render.UpdateV2', 'socialDetail', 'totalSocialActivityCounts', 'numLikes')
            #-->commentCount
            commentCount = safe_extract(post, 'value', 'com.linkedin.voyager.feed.render.UpdateV2', 'socialDetail', 'totalSocialActivityCounts', 'numComments')
            #-->repostCount
            repostCount = safe_extract(post, 'value', 'com.linkedin.voyager.feed.render.UpdateV2', 'socialDetail', 'totalSocialActivityCounts', 'numShares')
            #-->postDate
            postDate = extract_postDate(safe_extract(post, 'value', 'com.linkedin.voyager.feed.render.UpdateV2', 'actor', 'subDescription', 'text'))
            #-->action
            action = safe_extract(post, 'value', 'com.linkedin.voyager.feed.render.UpdateV2', 'header', 'text', 'text')
            if not action:
                action = "Post"
            #-->profileUrl
            profileUrl = create_profileUrl(wordToSearch)
            #-->sharedPostUrl
            sharedPostUrl = create_sharedPostUrl(safe_extract(post, 'value', 'com.linkedin.voyager.feed.render.UpdateV2', 'resharedUpdate', 'updateMetadata', 'urn'))
            #-->sharedJobUrl
            sharedJobUrl = extract_sharedJobUrl(safe_extract(post, 'value', 'com.linkedin.voyager.feed.render.UpdateV2', 'content', 'com.linkedin.voyager.feed.render.EntityComponent', 'ctaButton', 'navigationContext', 'actionTarget'))
            #-->isSponsored
            isSponsored = safe_extract(post, 'isSponsored')
            #-->DATA MANIPULATION END<--
            current_timestamp = datetime.now()
            timestamp = current_timestamp.strftime('%Y-%m-%d %H:%M:%S')
            all_variables = locals()
            selected_vars = {var: [all_variables[var]] for var in ["postUrl","imgUrl","postContent","postType","likeCount","commentCount","repostCount","postDate","action","profileUrl","timestamp","sharedPostUrl","sharedJobUrl","isSponsored"]}
            df_loop_base = pd.DataFrame(selected_vars)
            error = None
            df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
            df_loop_final = pd.concat([df_loop_final, df_loop_base], axis=1)
            df_final = pd.concat([df_final, df_loop_final])
        index += 1
        progress_bar.update(1)
        #--STREAMLIT--#
        if streamlit_execution:
            index_steamlit += 1
            progress_bar_company_activity_extractor.progress(index_steamlit / number_iterations)
        #--STREAMLIT--#
    progress_bar.close()
    #-->Columns manipulation
    final_columns = ["query","error","postUrl","imgUrl","postContent","postType","likeCount","commentCount","repostCount","postDate","action","profileUrl","timestamp","sharedPostUrl","sharedJobUrl","isSponsored"]
    df_final = df_final.reindex(columns=final_columns, fill_value=None)
    return df_final
def job_offers_extractor(csrf_token, dataframe, column_name, cookies_dict, streamlit_execution=False):
    headers = {
        'csrf-token': csrf_token,
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
        'x-restli-protocol-version': '2.0.0',
        "accept-language": "en-AU,en-GB;q=0.9,en-US;q=0.8,en;q=0.7",
        "x-li-lang": "en_US",
    }
    def extract_linkedin_universal_name(linkedin_url, pattern):
        try:
            return pattern.match(str(linkedin_url)).group(1)
        except AttributeError:
            return None
    def extract_main_company_id(company):
        pattern = re.compile(r"urn:li:fs_normalized_company:(.+)")
        entity_urn = company.get('entityUrn', '')
        match = pattern.search(entity_urn)
        return match.group(1) if match else None
    def get_all_job_postings(companyId):
        #-->Get the last 1000 active job postings
        job_postings = []
        start = 0
        count = 100
        has_more = True
        headers['accept'] = 'application/vnd.linkedin.normalized+json+2.1'
        while has_more:
            request_url = f'https://www.linkedin.com/voyager/api/voyagerJobsDashJobCards?decorationId=com.linkedin.voyager.dash.deco.jobs.search.JobSearchCardsCollectionLite-61&count={count}&q=jobSearch&query=(origin:COMPANY_PAGE_JOBS_CLUSTER_EXPANSION,locationUnion:(geoId:92000000),selectedFilters:(company:List({companyId}),originToLandingJobPostings:List({companyId})),spellCorrectionEnabled:true)&servedEventEnabled=false&start={start}'
            response = requests.get(url = request_url, cookies = cookies_dict, headers = headers)
            if response.status_code != 200:
                print(f"Failed to fetch job postings: {response.status_code}")
                break
            response_payload = response.json()
            elements = response_payload.get("included", [])
            current_job_postings = [i for i in elements if i["$type"] == 'com.linkedin.voyager.dash.jobs.JobPosting']
            if not current_job_postings:
                has_more = False
            else:
                job_postings.extend(current_job_postings)
                start += count
        if 'accept' in headers:
            del headers['accept']
        return job_postings
    def get_company(data):
        #-->Get company information
        if not data or "elements" not in data or not data["elements"]:
            return {}
        company = data["elements"][0]
        return company
    class ForbiddenAccessException(Exception):
        pass
    pattern_universalName = re.compile(r"^(?:https?:\/\/)?(?:[\w]+\.)?linkedin\.com\/(?:company|company-beta|school)\/([A-Za-z0-9\._\%&'-]+?)(?:\/|\?|#|$)", re.IGNORECASE)
    df_final = pd.DataFrame()
    dataframe.drop_duplicates(subset=[column_name], inplace=True)
    dataframe['wordToSearch'] = dataframe[column_name].apply(lambda x: extract_linkedin_universal_name(x, pattern_universalName))
    original_to_wordToSearch = dict(zip(dataframe[column_name], dataframe['wordToSearch']))
    columnName_values = dataframe[column_name].tolist()
    print('LinkedIn job offers extractor')
    progress_bar = tqdm(total = len(columnName_values))
    #--STREAMLIT--#
    if streamlit_execution:
        st.write("---LinkedIn job offers scrape---")
        progress_bar_job_offers_extractor = st.progress(0)
        number_iterations = len(columnName_values)
        index_steamlit = 0
    #--STREAMLIT--#
    for index, company in enumerate(columnName_values):
        df_loop_final = pd.DataFrame()
        wordToSearch = original_to_wordToSearch.get(columnName_values[index])
        if wordToSearch:
            wordToSearch = str(wordToSearch)
        error = None
        if wordToSearch is None:
            error = 'Invalid LinkedIn company URL'
            df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
            df_final = pd.concat([df_final, df_loop_final])
            index += 1
            progress_bar.update(1)
            #--STREAMLIT--#
            if streamlit_execution:
                index_steamlit += 1
                progress_bar_job_offers_extractor.progress(index_steamlit / number_iterations)
            #--STREAMLIT--#
            continue
        try:
            int(wordToSearch)
        except ValueError:
            params = {
                "decorationId": "com.linkedin.voyager.deco.organization.web.WebFullCompanyMain-12",
                "q": "universalName",
                "universalName": urllib.parse.unquote(wordToSearch),
            }
            request_url = f'https://www.linkedin.com/voyager/api/organization/companies'
            response = requests.get(url=request_url, cookies=cookies_dict, headers=headers, params=params)
            response_json = response.json()
            company_result = get_company(response_json)
            wordToSearch = extract_main_company_id(company_result)
            if wordToSearch is None:
                error = 'Invalid LinkedIn company URL'
                df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
                df_final = pd.concat([df_final, df_loop_final])
                index += 1
                progress_bar.update(1)
                #--STREAMLIT--#
                if streamlit_execution:
                    index_steamlit += 1
                    progress_bar_job_offers_extractor.progress(index_steamlit / number_iterations)
                #--STREAMLIT--#
                continue
        try:
            job_postings = get_all_job_postings(wordToSearch)
            if not job_postings:
                error = 'No job offers'
                df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
                df_final = pd.concat([df_final, df_loop_final])
                index += 1
                progress_bar.update(1)
                #--STREAMLIT--#
                if streamlit_execution:
                    index_steamlit += 1
                    progress_bar_job_offers_extractor.progress(index_steamlit / number_iterations)
                #--STREAMLIT--#
                continue
        except ForbiddenAccessException as e:
            job_postings = []
            error = 'Invalid LinkedIn company URL'
            df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
            df_final = pd.concat([df_final, df_loop_final])
            index += 1
            progress_bar.update(1)
            #--STREAMLIT--#
            if streamlit_execution:
                index_steamlit += 1
                progress_bar_job_offers_extractor.progress(index_steamlit / number_iterations)
            #--STREAMLIT--#
            continue
        except Exception as e:
            job_postings = []
            error = str(e)
            df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
            df_final = pd.concat([df_final, df_loop_final])
            index += 1
            progress_bar.update(1)
            #--STREAMLIT--#
            if streamlit_execution:
                index_steamlit += 1
                progress_bar_job_offers_extractor.progress(index_steamlit / number_iterations)
            #--STREAMLIT--#
            continue       
        for job_posting in job_postings:
             #-->DATA MANIPULATION START<--
            repostedJob = safe_extract(job_posting, 'repostedJob')
            title = safe_extract(job_posting, 'title')
            posterId = safe_extract(job_posting, 'posterId')
            contentSource = safe_extract(job_posting, 'contentSource')
            entityUrn = safe_extract(job_posting, 'entityUrn')
            try:
                entityUrn = entityUrn.split(':')[-1]
            except:
                pass
            jobUrl = None
            if entityUrn:
                jobUrl = f'https://www.linkedin.com/jobs/search/?currentJobId={entityUrn}'
            #-->DATA MANIPULATION END<--
            current_timestamp = datetime.now()
            timestamp = current_timestamp.strftime('%Y-%m-%d %H:%M:%S')
            all_variables = locals()
            selected_vars = {var: [all_variables[var]] for var in ['entityUrn', 'jobUrl', 'title', 'posterId', 'contentSource', 'repostedJob', 'timestamp']}
            df_loop_base = pd.DataFrame(selected_vars)
            error = None
            df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
            df_loop_final = pd.concat([df_loop_final, df_loop_base], axis=1)
            df_final = pd.concat([df_final, df_loop_final])
        index += 1
        progress_bar.update(1)
        #--STREAMLIT--#
        if streamlit_execution:
            index_steamlit += 1
            progress_bar_job_offers_extractor.progress(index_steamlit / number_iterations)
        #--STREAMLIT--#
    progress_bar.close()
    job_postings_rename_dict = {
        "entityUrn": "jobOfferId",
        "jobUrl": "jobOfferUrl",
        "title": "jobOfferTitle",
    }
    df_final.rename(columns = job_postings_rename_dict, inplace = True)
    #-->Columns manipulation
    final_columns = ['query', 'error', 'jobOfferId', 'jobOfferUrl', 'jobOfferTitle', 'posterId', 'contentSource', 'repostedJob', 'timestamp']
    df_final = df_final.reindex(columns=final_columns, fill_value=None)
    return df_final
def job_offers_details_extractor(csrf_token, dataframe, column_name, cookies_dict, streamlit_execution=False):
    headers = {
        'csrf-token': csrf_token,
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
        'x-restli-protocol-version': '2.0.0',
        "accept-language": "en-AU,en-GB;q=0.9,en-US;q=0.8,en;q=0.7",
        "x-li-lang": "en_US",
    }
    def convertToTimestamp(milliseconds):
        if milliseconds:
            return datetime.utcfromtimestamp(milliseconds/1000).strftime('%Y-%m-%d %H:%M:%S')
        else:
            None
    def get_job(job_id):
        # -->Get job offer details
        params = {
            "decorationId": "com.linkedin.voyager.deco.jobs.web.shared.WebLightJobPosting-23", }
        request_url = f'https://www.linkedin.com/voyager/api/jobs/jobPostings/{job_id}'
        response = requests.get(url=request_url, params=params,
                                headers=headers, cookies=cookies_dict)
        if response.status_code != 200:
            print(f"Failed to fetch job offer: {response.status_code}")
            return {}
        data = response.json()
        return data
    class ForbiddenAccessException(Exception):
        pass
    dataframe.drop_duplicates(subset=[column_name], inplace=True)
    columnName_values = dataframe[column_name].tolist()
    df_final = pd.DataFrame()
    print('LinkedIn job offers scraping')
    progress_bar = tqdm(total=len(columnName_values))
    #--STREAMLIT--#
    if streamlit_execution:
        st.write("---LinkedIn job offer details scrape---")
        progress_bar_job_offers_details_extractor = st.progress(0)
        number_iterations = len(columnName_values)
        index_steamlit = 0
    #--STREAMLIT--#
    for item in columnName_values:
        job_json = get_job(item)
        # Company
        name = safe_extract(job_json, "companyDetails", "com.linkedin.voyager.deco.jobs.web.shared.WebCompactJobPostingCompany", "companyResolutionResult", "name")
        picture_artifacts = safe_extract(job_json, "companyDetails", "com.linkedin.voyager.deco.jobs.web.shared.WebCompactJobPostingCompany", "companyResolutionResult", "logo", "image", "com.linkedin.common.VectorImage", "artifacts")
        picture_rootUrl = safe_extract(job_json, "companyDetails", "com.linkedin.voyager.deco.jobs.web.shared.WebCompactJobPostingCompany", "companyResolutionResult", "logo", "image", "com.linkedin.common.VectorImage", "rootUrl")
        picture100x100 = picture200x200 = picture400x400 = None
        if picture_artifacts and picture_rootUrl:
            for artifact in picture_artifacts:
                file_segment = artifact['fileIdentifyingUrlPathSegment']
                if '100_100' in file_segment:
                    picture100x100 = f"{picture_rootUrl}{file_segment}"
                elif '200_200' in file_segment:
                    picture200x200 = f"{picture_rootUrl}{file_segment}"
                elif '400_400' in file_segment:
                    picture400x400 = f"{picture_rootUrl}{file_segment}"
                if picture100x100 and picture200x200 and picture400x400:
                    break
        universalName = safe_extract(job_json, "companyDetails", "com.linkedin.voyager.deco.jobs.web.shared.WebCompactJobPostingCompany", "companyResolutionResult", "universalName")
        url = safe_extract(job_json, "companyDetails", "com.linkedin.voyager.deco.jobs.web.shared.WebCompactJobPostingCompany", "companyResolutionResult", "url")
        company = safe_extract(job_json, "companyDetails", "com.linkedin.voyager.deco.jobs.web.shared.WebCompactJobPostingCompany", "company")
        try:
            company = company.split(':')[-1]
        except:
            pass
        # Job
        jobState = safe_extract(job_json, "jobState")
        text = safe_extract(job_json, "description", "text")
        title = safe_extract(job_json, "title")
        workRemoteAllowed = safe_extract(job_json, "workRemoteAllowed")
        companyApplyUrl = safe_extract(job_json, "applyMethod", "com.linkedin.voyager.jobs.OffsiteApply", "companyApplyUrl")
        talentHubJob = safe_extract(job_json, "talentHubJob")
        formattedLocation = safe_extract(job_json, "formattedLocation")
        try:
            listedAt = convertToTimestamp(safe_extract(job_json, "listedAt"))
        except:
            listedAt = safe_extract(job_json, "listedAt")
        jobPostingId = safe_extract(job_json, "jobPostingId")
        onsite = False
        onsite_localizedName = safe_extract(job_json, "workplaceTypesResolutionResults", "urn:li:fs_workplaceType:1", "localizedName")
        if onsite_localizedName:
            onsite = True
        remote = False
        remote_localizedName = safe_extract(job_json, "workplaceTypesResolutionResults", "urn:li:fs_workplaceType:2", "localizedName")
        if remote_localizedName:
            remote = True
        hybrid = False
        hybrid_localizedName = safe_extract(job_json, "workplaceTypesResolutionResults", "urn:li:fs_workplaceType:3", "localizedName")
        if hybrid_localizedName:
            hybrid = True
        all_variables = locals()
        selected_vars = {var: [all_variables[var]] for var in ["jobPostingId", "text", "title", "formattedLocation", "companyApplyUrl", "listedAt", "workRemoteAllowed", "talentHubJob", "onsite", "remote", "hybrid", "jobState", "name", "company", "universalName", "url", "picture100x100", "picture200x200", "picture400x400"]}
        df_loop = pd.DataFrame(selected_vars)
        df_final = pd.concat([df_final, df_loop])
        time.sleep(1.5)
        progress_bar.update(1)
        #--STREAMLIT--#
        if streamlit_execution:
            index_steamlit += 1
            progress_bar_job_offers_details_extractor.progress(index_steamlit / number_iterations)
        #--STREAMLIT--#
    progress_bar.close()
    job_rename_dict = {
        "name": "companyName",
        "picture100x100": "companyPicture100x100",
        "picture200x200": "companyPicture200x200",
        "picture400x400": "companyPicture400x400",
        "universalName": "companyUniversalName",
        "url": "companyUrl",
        "company": "companyId",
        "text": "jobOfferDescription",
        "title": "jobOfferTitle",
        "workRemoteAllowed": "jobOfferWorkRemoteAllowed",
        "companyApplyUrl": "jobOfferCompanyApplyUrl",
        "talentHubJob": "jobOfferTalentHubJob",
        "formattedLocation": "jobOfferFormattedLocation",
        "listedAt": "jobOfferListedAt",
        "jobPostingId": "jobOfferId",
    }
    df_final.rename(columns=job_rename_dict, inplace=True)
    return df_final
def post_commenters_extractor(csrf_token, dataframe, column_name, cookies_dict, streamlit_execution=False):
    headers = {
        'csrf-token': csrf_token,
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
        'x-restli-protocol-version': '2.0.0',
        "accept-language": "en-AU,en-GB;q=0.9,en-US;q=0.8,en;q=0.7",
        "x-li-lang": "en_US",
    }
    def create_profileLink(profileLink):
        if profileLink:
            return f"https://www.linkedin.com/in/{profileLink}/"
        return None
    def extract_linkedin_universal_name(linkedin_url, pattern):
        try:
            return pattern.match(str(linkedin_url)).group(1)
        except AttributeError:
            return None
    def extract_author(author):
        if author:
            match = re.search(r"^(.*?)-activity", author)
            if match:
                return match.group(1)
        else:
            return None
    def create_postUrl(postUrl):
        if postUrl:
            return f"https://www.linkedin.com/feed/update/urn:li:activity:{postUrl}/"
        return None
    class ForbiddenAccessException(Exception):
        pass
    def fetch_activity_comments(activity, start=0, pagination_token=None, accumulated_elements=None):
        if accumulated_elements is None:
            accumulated_elements = []
        params = {
                "count": 100,
                "start": start,
                "q": "comments",
                "sortOrder": "RELEVANCE",
                "updateId": f"activity:{activity}"
        }
        if pagination_token:
            params["paginationToken"] = pagination_token
        request_url = f"https://www.linkedin.com/voyager/api/feed/comments"
        try:
            response = requests.get(url=request_url, params=params, cookies=cookies_dict, headers=headers)
            response.raise_for_status()
            response_json = response.json()
        except requests.HTTPError as http_err:
            if http_err.response.status_code == 403:
                raise ForbiddenAccessException('Access denied. Please check your permissions or authentication tokens.')
            else:
                print(f'HTTP error occurred: {http_err}')
                return accumulated_elements
        except Exception as err:
            print(f'An error occurred: {err}')
            return accumulated_elements
        if 'elements' in response_json and 'paging' in response_json:
            new_elements = response_json.get('elements', [])
            accumulated_elements.extend(new_elements)
            total = response_json['paging'].get('total', 0)
            next_start = start + params["count"]
            if next_start < total:
                pagination_token = response_json.get('metadata', {}).get('paginationToken')
                return fetch_activity_comments(activity, start=next_start, pagination_token=pagination_token, accumulated_elements=accumulated_elements)
        else:
            print('Unexpected response structure:', response_json)
        return accumulated_elements
    pattern_universalName = re.compile(r"https?://(?:[\w]+\.)?linkedin\.com/feed/update/urn:li:activity:(\d+)/?")
    df_final = pd.DataFrame()
    dataframe.drop_duplicates(subset=[column_name], inplace=True)
    dataframe['wordToSearch'] = dataframe[column_name].apply(lambda x: extract_linkedin_universal_name(x, pattern_universalName))
    original_to_wordToSearch = dict(zip(dataframe[column_name], dataframe['wordToSearch']))
    columnName_values = dataframe[column_name].tolist()
    print("LinkedIn post commenters extractor")
    progress_bar = tqdm(total = len(columnName_values))
    #--STREAMLIT--#
    if streamlit_execution:
        st.write("---LinkedIn post commenters scrape---")
        progress_bar_post_commenters_extractor = st.progress(0)
        number_iterations = len(columnName_values)
        index_steamlit = 0
    #--STREAMLIT--#
    for index, profile in enumerate(columnName_values):
        df_loop_final = pd.DataFrame()
        df_loop_base = pd.DataFrame()
        wordToSearch = original_to_wordToSearch.get(columnName_values[index])
        if wordToSearch:
            wordToSearch = str(wordToSearch)
        error = None
        if wordToSearch is None:
            error = "Invalid LinkedIn activity URL"
            df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
            df_final = pd.concat([df_final, df_loop_final])
            index += 1
            progress_bar.update(1)
            #--STREAMLIT--#
            if streamlit_execution:
                index_steamlit += 1
                progress_bar_post_commenters_extractor.progress(index_steamlit / number_iterations)
            #--STREAMLIT--#
            continue
        try:
            activity_comments = fetch_activity_comments(wordToSearch)
            if not activity_comments:
                error = "No comments"
                df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
                df_final = pd.concat([df_final, df_loop_final])
                index += 1
                progress_bar.update(1)
                #--STREAMLIT--#
                if streamlit_execution:
                    index_steamlit += 1
                    progress_bar_post_commenters_extractor.progress(index_steamlit / number_iterations)
                #--STREAMLIT--#
                continue
        except ForbiddenAccessException as e:
            activity_comments = {}
            error = "Invalid activity"
            df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
            df_final = pd.concat([df_final, df_loop_final])
            index += 1
            progress_bar.update(1)
            #--STREAMLIT--#
            if streamlit_execution:
                index_steamlit += 1
                progress_bar_post_commenters_extractor.progress(index_steamlit / number_iterations)
            #--STREAMLIT--#
            continue
        except Exception as e:
            activity_comments = {}
            error = str(e)
            df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
            df_final = pd.concat([df_final, df_loop_final])
            index += 1
            progress_bar.update(1)
            #--STREAMLIT--#
            if streamlit_execution:
                index_steamlit += 1
                progress_bar_post_commenters_extractor.progress(index_steamlit / number_iterations)
            #--STREAMLIT--#
            continue
        #-->DATA MANIPULATION START<--
        for comment in activity_comments:
            check_company = safe_extract(comment, 'commenter', 'com.linkedin.voyager.feed.CompanyActor')
            #-->check_company
            if check_company:
                vmid = safe_extract(comment, 'commenterProfileId')
                profileLink = create_profileLink(vmid)
                publicIdentifier = safe_extract(comment, 'commenter', 'com.linkedin.voyager.feed.CompanyActor', 'miniCompany', 'universalName')
                publicProfileLink = create_profileLink(publicIdentifier)
                firstName = lastName = occupation = degree = None
                fullName = safe_extract(comment, 'commenter', 'com.linkedin.voyager.feed.CompanyActor', 'miniCompany', 'name')
                commentText = safe_extract(comment, 'commentV2', 'text')
                commentUrl = safe_extract(comment, 'permalink')
                isFromPostAuthor = safe_extract(comment, 'commenterForDashConversion', 'author')
                commentDate = None
                commentDate_ms = safe_extract(comment, 'createdTime')
                likesCount = safe_extract(comment, 'socialDetail', 'totalSocialActivityCounts', 'numLikes')
                commentsCount = safe_extract(comment, 'socialDetail', 'totalSocialActivityCounts', 'numComments')
                if commentDate_ms:
                    commentDate_datetime = datetime.fromtimestamp(commentDate_ms / 1000.0, tz=timezone.utc)
                    commentDate = commentDate_datetime.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
                postUrl = create_postUrl(wordToSearch)
                current_timestamp = datetime.now()
                timestamp = current_timestamp.strftime('%Y-%m-%d %H:%M:%S')
                banner200x800 = banner350x1400 = None
                picture_artifacts = safe_extract(comment, 'commenter', 'com.linkedin.voyager.feed.CompanyActor', 'miniCompany', 'logo', 'com.linkedin.common.VectorImage', 'artifacts')
                picture_rootUrl = safe_extract(comment, 'commenter', 'com.linkedin.voyager.feed.CompanyActor', 'miniCompany', 'logo', 'com.linkedin.common.VectorImage', 'rootUrl')
                picture100x100 = picture200x200 = picture400x400 = picture800x800 = None
                if picture_artifacts and picture_rootUrl:
                    for artifact in picture_artifacts:
                        file_segment = artifact['fileIdentifyingUrlPathSegment']
                        if '100_100' in file_segment:
                            picture100x100 = f"{picture_rootUrl}{file_segment}"
                        elif '200_200' in file_segment:
                            picture200x200 = f"{picture_rootUrl}{file_segment}"
                        elif '400_400' in file_segment:
                            picture400x400 = f"{picture_rootUrl}{file_segment}"
                        elif '800_800' in file_segment:
                            picture800x800 = f"{picture_rootUrl}{file_segment}"
                        if picture100x100 and picture200x200 and picture400x400 and picture800x800:
                            break
                all_variables = locals()
                selected_vars = {var: [all_variables[var]] for var in ["profileLink","vmid","publicProfileLink","publicIdentifier","firstName","lastName","fullName","occupation","degree","commentText","commentUrl","isFromPostAuthor","commentDate","likesCount","commentsCount","postUrl","timestamp","banner200x800","banner350x1400","picture100x100","picture200x200","picture400x400","picture800x800"]}
                df_loop_base = pd.DataFrame(selected_vars)
                error = "Company profile"
                df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
                df_loop_final = pd.concat([df_loop_final, df_loop_base], axis=1)
                df_final = pd.concat([df_final, df_loop_final])
                continue
            check_InfluencerActor = safe_extract(comment, 'commenter', 'com.linkedin.voyager.feed.InfluencerActor')
            #-->vmid
            vmid = safe_extract(comment, 'commenterProfileId')
            #-->profileLink
            profileLink = create_profileLink(vmid)
            #-->publicIdentifier
            publicIdentifier = safe_extract(comment, 'commenter', 'com.linkedin.voyager.feed.MemberActor', 'miniProfile', 'publicIdentifier')
            #-->publicProfileLink
            publicProfileLink = create_profileLink(publicIdentifier)
            #-->firstName
            firstName = safe_extract(comment, 'commenter', 'com.linkedin.voyager.feed.MemberActor', 'miniProfile', 'firstName')
            #-->lastName
            lastName = safe_extract(comment, 'commenter', 'com.linkedin.voyager.feed.MemberActor', 'miniProfile', 'lastName')
            #-->fullName
            fullName = None
            if firstName and lastName:
                fullName = firstName + " " + lastName
                fullName = fullName.strip() if fullName is not None else None
            elif firstName:
                fullName = firstName
                fullName = fullName.strip() if fullName is not None else None
            else:
                fullName = lastName
                fullName = fullName.strip() if fullName is not None else None
            #-->occupation
            occupation = safe_extract(comment, 'commenter', 'com.linkedin.voyager.feed.MemberActor', 'miniProfile', 'occupation')
            #-->degree
            degree = safe_extract(comment, 'commenter', 'com.linkedin.voyager.feed.MemberActor', 'distance', 'value')
            degree_mapping = {"DISTANCE_2": "2nd", "DISTANCE_1": "1st", "OUT_OF_NETWORK": "3rd"}
            degree = degree_mapping.get(degree, degree)
            #-->comment
            commentText = safe_extract(comment, 'commentV2', 'text')
            #-->commentUrl
            commentUrl = safe_extract(comment, 'permalink')
            #-->isFromPostAuthor
            isFromPostAuthor = safe_extract(comment, 'commenterForDashConversion', 'author')
            #-->commentDate
            commentDate = None
            commentDate_ms = safe_extract(comment, 'createdTime')
            if commentDate_ms:
                commentDate_datetime = datetime.utcfromtimestamp(commentDate_ms / 1000.0)
                commentDate = commentDate_datetime.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
            #-->likesCount
            likesCount = safe_extract(comment, 'socialDetail', 'totalSocialActivityCounts', 'numLikes')
            #-->commentsCount
            commentsCount = safe_extract(comment, 'socialDetail', 'totalSocialActivityCounts', 'numComments')
            #-->banner200x800 and banner350x1400
            background_artifacts = safe_extract(comment, 'commenter', 'com.linkedin.voyager.feed.MemberActor', 'miniProfile', 'backgroundImage', 'com.linkedin.common.VectorImage', 'artifacts')
            background_rootUrl = safe_extract(comment, 'commenter', 'com.linkedin.voyager.feed.MemberActor', 'miniProfile', 'backgroundImage', 'com.linkedin.common.VectorImage', 'rootUrl')
            banner200x800 = banner350x1400 = None
            if background_artifacts and background_rootUrl:
                for artifact in background_artifacts:
                    file_segment = artifact['fileIdentifyingUrlPathSegment']
                    if '200_800' in file_segment:
                        banner200x800 = f"{background_rootUrl}{file_segment}"
                    elif '350_1400' in file_segment:
                        banner350x1400 = f"{background_rootUrl}{file_segment}"
                    if banner200x800 and banner350x1400:
                        break
            #-->picture100x100, picture200x200, picture400x400 and picture800x800
            picture_artifacts = safe_extract(comment, 'commenter', 'com.linkedin.voyager.feed.MemberActor', 'miniProfile', 'picture', 'com.linkedin.common.VectorImage', 'artifacts')
            picture_rootUrl = safe_extract(comment, 'commenter', 'com.linkedin.voyager.feed.MemberActor', 'miniProfile', 'picture', 'com.linkedin.common.VectorImage', 'rootUrl')
            picture100x100 = picture200x200 = picture400x400 = picture800x800 = None
            if picture_artifacts and picture_rootUrl:
                for artifact in picture_artifacts:
                    file_segment = artifact['fileIdentifyingUrlPathSegment']
                    if '100_100' in file_segment:
                        picture100x100 = f"{picture_rootUrl}{file_segment}"
                    elif '200_200' in file_segment:
                        picture200x200 = f"{picture_rootUrl}{file_segment}"
                    elif '400_400' in file_segment:
                        picture400x400 = f"{picture_rootUrl}{file_segment}"
                    elif '800_800' in file_segment:
                        picture800x800 = f"{picture_rootUrl}{file_segment}"
                    if picture100x100 and picture200x200 and picture400x400 and picture800x800:
                        break
            postUrl = create_postUrl(wordToSearch)
            #-->check_InfluencerActor
            if check_InfluencerActor:
                publicIdentifier = safe_extract(comment, 'commenter', 'com.linkedin.voyager.feed.InfluencerActor', 'miniProfile', 'publicIdentifier')
                publicProfileLink = create_profileLink(publicIdentifier)
                firstName = safe_extract(comment, 'commenter', 'com.linkedin.voyager.feed.InfluencerActor', 'miniProfile', 'firstName')
                lastName = safe_extract(comment, 'commenter', 'com.linkedin.voyager.feed.InfluencerActor', 'miniProfile', 'lastName')
                fullName = None
                if firstName and lastName:
                    fullName = firstName + " " + lastName
                    fullName = fullName.strip() if fullName is not None else None
                elif firstName:
                    fullName = firstName
                    fullName = fullName.strip() if fullName is not None else None
                else:
                    fullName = lastName
                    fullName = fullName.strip() if fullName is not None else None
                occupation = safe_extract(comment, 'commenter', 'com.linkedin.voyager.feed.InfluencerActor', 'miniProfile', 'occupation')
                degree = safe_extract(comment, 'commenter', 'com.linkedin.voyager.feed.InfluencerActor', 'distance', 'value')
                degree_mapping = {"DISTANCE_2": "2nd", "DISTANCE_1": "1st", "OUT_OF_NETWORK": "3rd"}
                degree = degree_mapping.get(degree, degree)
                background_artifacts = safe_extract(comment, 'commenter', 'com.linkedin.voyager.feed.InfluencerActor', 'miniProfile', 'backgroundImage', 'com.linkedin.common.VectorImage', 'artifacts')
                background_rootUrl = safe_extract(comment, 'commenter', 'com.linkedin.voyager.feed.InfluencerActor', 'miniProfile', 'backgroundImage', 'com.linkedin.common.VectorImage', 'rootUrl')
                banner200x800 = banner350x1400 = None
                if background_artifacts and background_rootUrl:
                    for artifact in background_artifacts:
                        file_segment = artifact['fileIdentifyingUrlPathSegment']
                        if '200_800' in file_segment:
                            banner200x800 = f"{background_rootUrl}{file_segment}"
                        elif '350_1400' in file_segment:
                            banner350x1400 = f"{background_rootUrl}{file_segment}"
                        if banner200x800 and banner350x1400:
                            break
                picture_artifacts = safe_extract(comment, 'commenter', 'com.linkedin.voyager.feed.InfluencerActor', 'miniProfile', 'picture', 'com.linkedin.common.VectorImage', 'artifacts')
                picture_rootUrl = safe_extract(comment, 'commenter', 'com.linkedin.voyager.feed.InfluencerActor', 'miniProfile', 'picture', 'com.linkedin.common.VectorImage', 'rootUrl')
                picture100x100 = picture200x200 = picture400x400 = picture800x800 = None
                if picture_artifacts and picture_rootUrl:
                    for artifact in picture_artifacts:
                        file_segment = artifact['fileIdentifyingUrlPathSegment']
                        if '100_100' in file_segment:
                            picture100x100 = f"{picture_rootUrl}{file_segment}"
                        elif '200_200' in file_segment:
                            picture200x200 = f"{picture_rootUrl}{file_segment}"
                        elif '400_400' in file_segment:
                            picture400x400 = f"{picture_rootUrl}{file_segment}"
                        elif '800_800' in file_segment:
                            picture800x800 = f"{picture_rootUrl}{file_segment}"
                        if picture100x100 and picture200x200 and picture400x400 and picture800x800:
                            break
            #-->DATA MANIPULATION END<--
            current_timestamp = datetime.now()
            timestamp = current_timestamp.strftime('%Y-%m-%d %H:%M:%S')
            all_variables = locals()
            selected_vars = {var: [all_variables[var]] for var in ["profileLink","vmid","publicProfileLink","publicIdentifier","firstName","lastName","fullName","occupation","degree","commentText","commentUrl","isFromPostAuthor","commentDate","likesCount","commentsCount","postUrl","timestamp","banner200x800","banner350x1400","picture100x100","picture200x200","picture400x400","picture800x800"]}
            df_loop_base = pd.DataFrame(selected_vars)
            error = None
            df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
            df_loop_final = pd.concat([df_loop_final, df_loop_base], axis=1)
            df_final = pd.concat([df_final, df_loop_final])
        index += 1
        progress_bar.update(1)
        #--STREAMLIT--#
        if streamlit_execution:
            index_steamlit += 1
            progress_bar_post_commenters_extractor.progress(index_steamlit / number_iterations)
        #--STREAMLIT--#
    progress_bar.close()
    #-->Columns manipulation
    final_columns = ["query","error","profileLink","vmid","publicProfileLink","publicIdentifier","firstName","lastName","fullName","occupation","degree","commentText","commentUrl","isFromPostAuthor","commentDate","likesCount","commentsCount","postUrl","timestamp","banner200x800","banner350x1400","picture100x100","picture200x200","picture400x400","picture800x800"]
    df_final = df_final.reindex(columns=final_columns, fill_value=None)
    return df_final
def profile_activity_extractor(csrf_token, dataframe, column_name, cookies_dict, streamlit_execution=False):
    headers = {
        'csrf-token': csrf_token,
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36',
        'x-restli-protocol-version': '2.0.0',
        "accept-language": "en-AU,en-GB;q=0.9,en-US;q=0.8,en;q=0.7",
        "x-li-lang": "en_US",
    }
    def create_sharedPostUrl(sharedPostUrl):
        if sharedPostUrl:
            return f"https://www.linkedin.com/feed/update/{sharedPostUrl}/"
        return None
    def extract_sharedJobUrl(sharedJobUrl):
        if sharedJobUrl:
            match = re.search(r'(https://www\.linkedin\.com/jobs/view/\d+)/', sharedJobUrl)
            if match:
                return match.group(1) + "/"
        return None
    def extract_postDate(postDate):
        if postDate:
            match = re.search(r'^(.*?)\s*•', postDate)
            if match:
                return match.group(1).strip()
        return None
    def create_profileUrl(profileUrl):
        if profileUrl:
            return f"https://www.linkedin.com/in/{profileUrl}/"
        return None
    class ForbiddenAccessException(Exception):
        pass
    def extract_linkedin_universal_name(linkedin_url, pattern):
        try:
            return pattern.match(str(linkedin_url)).group(3)
        except AttributeError:
            return None
    def fetch_person_updates(profile, start=0, pagination_token=None, accumulated_elements=None):
        if accumulated_elements is None:
            accumulated_elements = []
        params = {
            "profileId": profile,
            "q": "memberShareFeed",
            "moduleKey": "member-share",
            "count": 100,
            "start": start,
        }
        if pagination_token:
            params["paginationToken"] = pagination_token
        request_url = 'https://www.linkedin.com/voyager/api/feed/updates'
        try:
            response = requests.get(url=request_url, params=params, cookies=cookies_dict, headers=headers)
            response.raise_for_status()
            response_json = response.json()
        except requests.HTTPError as http_err:
            if http_err.response.status_code == 403:
                raise ForbiddenAccessException('Access denied. Please check your permissions or authentication tokens.')
            else:
                print(f'HTTP error occurred: {http_err}')
                return accumulated_elements
        except Exception as err:
            print(f'An error occurred: {err}')
            return accumulated_elements
        if 'elements' in response_json:
            new_elements = response_json.get('elements', [])
            if not new_elements:
                return accumulated_elements
            accumulated_elements.extend(new_elements)
            next_start = start + len(new_elements)
            pagination_token = response_json.get('metadata', {}).get('paginationToken')
            return fetch_person_updates(profile, start=next_start, pagination_token=pagination_token, accumulated_elements=accumulated_elements)
        else:
            print('Unexpected response structure:', response_json)
        return accumulated_elements
    pattern_universalName = re.compile(r"https?://([\w]+\.)?linkedin\.com/(in|sales/lead|sales/people)/([A-z0-9\._\%&'-]+)/?")
    df_final = pd.DataFrame()
    dataframe.drop_duplicates(subset=[column_name], inplace=True)
    dataframe['wordToSearch'] = dataframe[column_name].apply(lambda x: extract_linkedin_universal_name(x, pattern_universalName))
    original_to_wordToSearch = dict(zip(dataframe[column_name], dataframe['wordToSearch']))
    columnName_values = dataframe[column_name].tolist()
    print("LinkedIn profile activity extractor")
    progress_bar = tqdm(total = len(columnName_values))
    #--STREAMLIT--#
    if streamlit_execution:
        st.write("---LinkedIn lead activity scrape---")
        progress_bar_profile_activity_extractor = st.progress(0)
        number_iterations = len(columnName_values)
        index_steamlit = 0
    #--STREAMLIT--#
    for index, profile in enumerate(columnName_values):
        df_loop_final = pd.DataFrame()
        df_loop_base = pd.DataFrame()
        wordToSearch = original_to_wordToSearch.get(columnName_values[index])
        error = None
        if wordToSearch is None:
            error = "Invalid LinkedIn URL"
            df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
            df_final = pd.concat([df_final, df_loop_final])
            index += 1
            progress_bar.update(1)
            #--STREAMLIT--#
            if streamlit_execution:
                index_steamlit += 1
                progress_bar_profile_activity_extractor.progress(index_steamlit / number_iterations)
            #--STREAMLIT--#
            continue
        try:
            profile_posts = fetch_person_updates(wordToSearch)
            if not profile_posts:
                error = "No activities"
                df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
                df_final = pd.concat([df_final, df_loop_final])
                index += 1
                progress_bar.update(1)
                #--STREAMLIT--#
                if streamlit_execution:
                    index_steamlit += 1
                    progress_bar_profile_activity_extractor.progress(index_steamlit / number_iterations)
                #--STREAMLIT--#
                continue
        except ForbiddenAccessException as e:
            profile_posts = {}
            error = "LinkedIn down"
            df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
            df_final = pd.concat([df_final, df_loop_final])
            index += 1
            progress_bar.update(1)
            #--STREAMLIT--#
            if streamlit_execution:
                index_steamlit += 1
                progress_bar_profile_activity_extractor.progress(index_steamlit / number_iterations)
            #--STREAMLIT--#
            continue
        except Exception as e:
            profile_posts = {}
            error = str(e)
            df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
            df_final = pd.concat([df_final, df_loop_final])
            index += 1
            progress_bar.update(1)
            #--STREAMLIT--#
            if streamlit_execution:
                index_steamlit += 1
                progress_bar_profile_activity_extractor.progress(index_steamlit / number_iterations)
            #--STREAMLIT--#
            continue
        #-->DATA MANIPULATION START<--
        for post in profile_posts:
            #-->postUrl
            postUrl = safe_extract(post, 'permalink')
            #-->imgUrl
            rootUrl = safe_extract(post, 'value', 'com.linkedin.voyager.feed.render.UpdateV2', 'content', 'com.linkedin.voyager.feed.render.ImageComponent', 'images', 0, 'attributes', 0, 'vectorImage', 'rootUrl')
            fileIdentifyingUrlPathSegment = safe_extract(post, 'value', 'com.linkedin.voyager.feed.render.UpdateV2', 'content', 'com.linkedin.voyager.feed.render.ImageComponent', 'images', 0, 'attributes', 0, 'vectorImage', 'artifacts', 5, 'fileIdentifyingUrlPathSegment')
            imgUrl = None
            if rootUrl and fileIdentifyingUrlPathSegment:
                imgUrl = rootUrl + fileIdentifyingUrlPathSegment
            #-->postContent
            postContent = safe_extract(post, 'value', 'com.linkedin.voyager.feed.render.UpdateV2', 'commentary', 'text', 'text')
            #-->postType
            postType = None
            if postContent:
                postType = "Text"
            if imgUrl:
                postType = "Image"
            #-->likeCount
            likeCount = safe_extract(post, 'value', 'com.linkedin.voyager.feed.render.UpdateV2', 'socialDetail', 'totalSocialActivityCounts', 'numLikes')
            #-->commentCount
            commentCount = safe_extract(post, 'value', 'com.linkedin.voyager.feed.render.UpdateV2', 'socialDetail', 'totalSocialActivityCounts', 'numComments')
            #-->repostCount
            repostCount = safe_extract(post, 'value', 'com.linkedin.voyager.feed.render.UpdateV2', 'socialDetail', 'totalSocialActivityCounts', 'numShares')
            #-->postDate
            postDate = extract_postDate(safe_extract(post, 'value', 'com.linkedin.voyager.feed.render.UpdateV2', 'actor', 'subDescription', 'text'))
            #-->action
            action = safe_extract(post, 'value', 'com.linkedin.voyager.feed.render.UpdateV2', 'header', 'text', 'text')
            if not action:
                action = "Post"
            #-->profileUrl
            profileUrl = create_profileUrl(wordToSearch)
            #-->sharedPostUrl
            sharedPostUrl = create_sharedPostUrl(safe_extract(post, 'value', 'com.linkedin.voyager.feed.render.UpdateV2', 'resharedUpdate', 'updateMetadata', 'urn'))
            #-->sharedJobUrl
            sharedJobUrl = extract_sharedJobUrl(safe_extract(post, 'value', 'com.linkedin.voyager.feed.render.UpdateV2', 'content', 'com.linkedin.voyager.feed.render.EntityComponent', 'ctaButton', 'navigationContext', 'actionTarget'))
            #-->isSponsored
            isSponsored = safe_extract(post, 'isSponsored')
            #-->DATA MANIPULATION END<--
            current_timestamp = datetime.now()
            timestamp = current_timestamp.strftime('%Y-%m-%d %H:%M:%S')
            all_variables = locals()
            selected_vars = {var: [all_variables[var]] for var in ["postUrl","imgUrl","postContent","postType","likeCount","commentCount","repostCount","postDate","action","profileUrl","timestamp","sharedPostUrl","sharedJobUrl","isSponsored"]}
            df_loop_base = pd.DataFrame(selected_vars)
            error = None
            df_loop_final = pd.DataFrame({'query': [columnName_values[index]], 'wordToSearch': [wordToSearch], 'error': [error]})
            df_loop_final = pd.concat([df_loop_final, df_loop_base], axis=1)
            df_final = pd.concat([df_final, df_loop_final])
        index += 1
        progress_bar.update(1)
        #--STREAMLIT--#
        if streamlit_execution:
            index_steamlit += 1
            progress_bar_profile_activity_extractor.progress(index_steamlit / number_iterations)
        #--STREAMLIT--#
    progress_bar.close()
    #-->Columns manipulation
    final_columns = ["query","error","postUrl","imgUrl","postContent","postType","likeCount","commentCount","repostCount","postDate","action","profileUrl","timestamp","sharedPostUrl","sharedJobUrl","isSponsored"]
    df_final = df_final.reindex(columns=final_columns, fill_value=None)
    return df_final