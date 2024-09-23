import os
import streamlit as st
import sys
import pandas as pd
import requests
import time
sys.path.append(os.path.abspath('../scripts/helper_scripts'))
from scripts.helper_scripts import *
from stqdm import stqdm

def apollo_contact_enrichment(api_key, df, first_name_column_name, last_name_column_name, name_column_name, email_column_name, organization_name_column_name, domain_column_name):
        hashed_email_column_name = ''
        reveal_personal_emails = 'False'
        reveal_personal_emails_bool = (reveal_personal_emails == 'True')
        reveal_phone_number = 'False'
        webhook_url_column_name = ''
        reveal_phone_number_bool = (reveal_phone_number == 'True')
        id_column_name = ''

        all_columns = {
            "loop_record_found": "Record found?",
            "loop_revealed_for_current_team": "Contact - Revealed for current team?",
            "loop_email": "Contact - Email",
            "loop_email_status": "Contact - Email status",
            "loop_id": "Contact - ID",
            "loop_first_name": "Contact - First name",
            "loop_last_name": "Contact - Last name",
            "loop_name": "Contact - Full name",
            "loop_linkedin_url": "Contact - LinkedIn URL",
            "loop_title": "Contact - Title",
            "loop_photo_url": "Contact - Photo URL",
            "loop_twitter_url": "Contact - Twitter URL",
            "loop_github_url": "Contact - Github URL",
            "loop_facebook_url": "Contact - Facebook URL",
            "loop_extrapolated_email_confidence": "Contact - Extrapolated email confidence",
            "loop_headline": "Contact - Headline",
            "loop_organization_id": "Contact - Organization ID",
            "loop_state": "Contact - State",
            "loop_city": "Contact - City",
            "loop_country": "Contact - Country",
            "loop_organization_name": "Organization - Name",
            "loop_organization_website_url": "Organization - Website URL",
            "loop_organization_blog_url": "Organization - Blog URL",
            "loop_organization_angellist_url": "Organization - Angellist URL",
            "loop_organization_linkedin_url": "Organization - LinkedIn URL",
            "loop_organization_twitter_url": "Organization - Twitter URL",
            "loop_organization_facebook_url": "Organization - Facebook URL",
            "loop_organization_primary_phone_number": "Organization - Primary phone number",
            "loop_organization_primary_phone_source": "Organization - Primary phone source",
            "loop_organization_languages_values": "Organization - Languages",
            "loop_organization_alexa_ranking": "Organization - Alexa ranking",
            "loop_organization_phone": "Organization - Phone",
            "loop_organization_linkedin_uid": "Organization - Linkedin UID",
            "loop_organization_founded_year": "Organization - Founded year",
            "loop_organization_publicly_traded_symbol": "Organization - Publicly traded symbol",
            "loop_organization_publicly_traded_exchange": "Organization - Publicly traded exchange",
            "loop_organization_logo_url": "Organization - Logo URL",
            "loop_organization_crunchbase_url": "Organization - Crunchbase URL",
            "loop_organization_primary_domain": "Organization - Primary domain",
            "loop_organization_industry": "Organization - Industry",
            "loop_organization_keywords_values": "Organization - Keywords",
            "loop_organization_estimated_num_employees": "Organization - Estimated number of employees",
            "loop_organization_snippets_loaded": "Organization - Snippets loaded?",
            "loop_organization_industry_tag_id": "Organization - Industry tag ID",
            "loop_organization_retail_location_count": "Organization - Retail location count",
            "loop_organization_raw_address": "Organization - Raw address",
            "loop_organization_street_address": "Organization - Street address",
            "loop_organization_city": "Organization - City",
            "loop_organization_state": "Organization - State",
            "loop_organization_postal_code": "Organization - Postal code",
            "loop_organization_country": "Organization - Country",
            "loop_account_id": "Contact - Account ID",
            "loop_account_name": "Account - Name",
            "loop_account_website_url": "Account - Website URL",
            "loop_account_blog_url": "Account - Blog URL",
            "loop_account_angellist_url": "Account - Angellist URL",
            "loop_account_linkedin_url": "Account - LinkedIn URL",
            "loop_account_twitter_url": "Account - Twitter URL",
            "loop_account_facebook_url": "Account - Facebook URL",
            "loop_account_primary_phone_number": "Account - Primary phone number",
            "loop_account_primary_phone_source": "Account - Primary phone source",
            "loop_account_languages_values": "Account - Languages",
            "loop_account_alexa_ranking": "Account - Alexa ranking",
            "loop_account_phone": "Account - Phone",
            "loop_account_linkedin_uid": "Account - LinkedIn UID",
            "loop_account_founded_year": "Account - Founded year",
            "loop_account_publicly_traded_symbol": "Account - Publicly traded symbol",
            "loop_account_publicly_traded_exchange": "Account - Publicly traded exchange",
            "loop_account_logo_url": "Account - Logo URL",
            "loop_account_crunchbase_url": "Account - Crunchbase URL",
            "loop_account_primary_domain": "Account - Primary domain",
            "loop_account_domain": "Account - Domain",
            "loop_account_team_id": "Account - Team ID",
            "loop_account_organization_id": "Account - Organization ID",
            "loop_account_sanitized_phone": "Account - Sanitized phone",
            "loop_phone_number_raw_number": "Phone number - Raw number",
            "loop_phone_number_sanitized_number": "Phone number - Sanitized number",
            "loop_phone_number_type": "Phone number - Type",
            "loop_phone_number_position": "Phone number - Position",
            "loop_phone_number_status": "Phone number - Status"
        }
        def row_to_detail(row):
            column_mapping = {
                first_name_column_name: "first_name",
                last_name_column_name: "last_name",
                name_column_name: "name",
                email_column_name: "email",
                hashed_email_column_name: "hashed_email",
                organization_name_column_name: "organization_name",
                domain_column_name: "domain",
                id_column_name: "id",
                webhook_url_column_name: "webhook_url"
            }
            valid_columns = [col for col in column_mapping if col and col in row]
            return {column_mapping[col]: row[col] for col in valid_columns if pd.notna(row[col]) and row[col] != ''}
        def send_batch_request(batch):
            json_data = {
                'api_key': api_key,
                'reveal_personal_emails': reveal_personal_emails_bool,
                'reveal_phone_number': reveal_phone_number_bool,
                'details': batch,
            }
            response = requests.post(API_ENDPOINT, headers=headers, json=json_data)
            return response.headers, response.json()
        def add_quote_to_plus(x):
            return "'" + str(x) if pd.notna(x) and str(x).startswith('+') else x
        batches = [df.iloc[i:i+10] for i in range(0, len(df), 10)]

        df_original = df.copy()
        total_rows = sum(len(batch) for batch in batches)
        df_final = pd.DataFrame(index=range(total_rows), columns=all_columns.keys())
        current_row = 0
        API_ENDPOINT = 'https://api.apollo.io/api/v1/people/bulk_match'
        headers = {'Content-Type': 'application/json', 'Cache-Control': 'no-cache',}
        for index, batch in enumerate(stqdm(batches, desc="Processing contact batches")):
            details_batch = [row_to_detail(row[1]) for row in batch.iterrows()]
            response_headers, response_data = send_batch_request(details_batch)    
            matches = response_data.get('matches', [])    
            for idx, match in enumerate(matches):
                loop_record_found = 'Found' if match else 'Not found'
                loop_data = {
                    'loop_record_found': loop_record_found,
                    'loop_revealed_for_current_team': safe_extract(match, 'revealed_for_current_team'),
                    'loop_email': safe_extract(match, 'email') or safe_extract(details_batch[idx], 'email'),
                    'loop_email_status': safe_extract(match, 'email_status'),
                    'loop_id': safe_extract(match, 'id') or safe_extract(details_batch[idx], 'id'),
                    'loop_first_name': safe_extract(match, 'first_name') or safe_extract(details_batch[idx], 'first_name'),
                    'loop_last_name': safe_extract(match, 'last_name') or safe_extract(details_batch[idx], 'last_name'),
                    'loop_name': safe_extract(match, 'name') or safe_extract(details_batch[idx], 'name'),
                    'loop_linkedin_url': safe_extract(match, 'linkedin_url'),
                    'loop_title': safe_extract(match, 'title'),
                    'loop_photo_url': safe_extract(match, 'photo_url'),
                    'loop_twitter_url': safe_extract(match, 'twitter_url'),
                    'loop_github_url': safe_extract(match, 'github_url'),
                    'loop_facebook_url': safe_extract(match, 'facebook_url'),
                    'loop_extrapolated_email_confidence': safe_extract(match, 'extrapolated_email_confidence'),
                    'loop_headline': safe_extract(match, 'headline'),
                    'loop_organization_id': safe_extract(match, 'organization_id'),
                    'loop_state': safe_extract(match, 'state'),
                    'loop_city': safe_extract(match, 'city'),
                    'loop_country': safe_extract(match, 'country'),
                }
                # Organization data
                org_data = safe_extract(match, 'organization') or {}
                loop_data.update({
                    'loop_organization_name': safe_extract(org_data, 'organization_name') or safe_extract(details_batch[idx], 'organization_name'),
                    'loop_organization_website_url': safe_extract(org_data, 'website_url'),
                    'loop_organization_blog_url': safe_extract(org_data, 'blog_url'),
                    'loop_organization_angellist_url': safe_extract(org_data, 'angellist_url'),
                    'loop_organization_linkedin_url': safe_extract(org_data, 'linkedin_url'),
                    'loop_organization_twitter_url': safe_extract(org_data, 'twitter_url'),
                    'loop_organization_facebook_url': safe_extract(org_data, 'facebook_url'),
                    'loop_organization_primary_phone_number': safe_extract(org_data, 'primary_phone', 'number'),
                    'loop_organization_primary_phone_source': safe_extract(org_data, 'primary_phone', 'source'),
                    'loop_organization_languages_values': ', '.join(safe_extract(org_data, 'languages') or []),
                    'loop_organization_alexa_ranking': safe_extract(org_data, 'alexa_ranking'),
                    'loop_organization_phone': safe_extract(org_data, 'phone'),
                    'loop_organization_linkedin_uid': safe_extract(org_data, 'linkedin_uid'),
                    'loop_organization_founded_year': safe_extract(org_data, 'founded_year'),
                    'loop_organization_publicly_traded_symbol': safe_extract(org_data, 'publicly_traded_symbol'),
                    'loop_organization_publicly_traded_exchange': safe_extract(org_data, 'publicly_traded_exchange'),
                    'loop_organization_logo_url': safe_extract(org_data, 'logo_url'),
                    'loop_organization_crunchbase_url': safe_extract(org_data, 'crunchbase_url'),
                    'loop_organization_primary_domain': safe_extract(org_data, 'primary_domain') or safe_extract(details_batch[idx], 'domain'),
                    'loop_organization_industry': safe_extract(org_data, 'industry'),
                    'loop_organization_keywords_values': ', '.join(safe_extract(org_data, 'keywords') or []),
                    'loop_organization_estimated_num_employees': safe_extract(org_data, 'estimated_num_employees'),
                    'loop_organization_snippets_loaded': safe_extract(org_data, 'snippets_loaded'),
                    'loop_organization_industry_tag_id': safe_extract(org_data, 'industry_tag_id'),
                    'loop_organization_retail_location_count': safe_extract(org_data, 'retail_location_count'),
                    'loop_organization_raw_address': safe_extract(org_data, 'raw_address'),
                    'loop_organization_street_address': safe_extract(org_data, 'street_address'),
                    'loop_organization_city': safe_extract(org_data, 'city'),
                    'loop_organization_state': safe_extract(org_data, 'state'),
                    'loop_organization_postal_code': safe_extract(org_data, 'postal_code'),
                    'loop_organization_country': safe_extract(org_data, 'country'),
                })
                # Account data
                account_data = safe_extract(match, 'account') or {}
                loop_data.update({
                    'loop_account_id': safe_extract(match, 'account_id'),
                    'loop_account_name': safe_extract(account_data, 'name'),
                    'loop_account_website_url': safe_extract(account_data, 'website_url'),
                    'loop_account_blog_url': safe_extract(account_data, 'blog_url'),
                    'loop_account_angellist_url': safe_extract(account_data, 'angellist_url'),
                    'loop_account_linkedin_url': safe_extract(account_data, 'linkedin_url'),
                    'loop_account_twitter_url': safe_extract(account_data, 'twitter_url'),
                    'loop_account_facebook_url': safe_extract(account_data, 'facebook_url'),
                    'loop_account_primary_phone_number': safe_extract(account_data, 'primary_phone', 'number'),
                    'loop_account_primary_phone_source': safe_extract(account_data, 'primary_phone', 'source'),
                    'loop_account_languages_values': ', '.join(safe_extract(account_data, 'languages') or []),
                    'loop_account_alexa_ranking': safe_extract(account_data, 'alexa_ranking'),
                    'loop_account_phone': safe_extract(account_data, 'phone'),
                    'loop_account_linkedin_uid': safe_extract(account_data, 'linkedin_uid'),
                    'loop_account_founded_year': safe_extract(account_data, 'founded_year'),
                    'loop_account_publicly_traded_symbol': safe_extract(account_data, 'publicly_traded_symbol'),
                    'loop_account_publicly_traded_exchange': safe_extract(account_data, 'publicly_traded_exchange'),
                    'loop_account_logo_url': safe_extract(account_data, 'logo_url'),
                    'loop_account_crunchbase_url': safe_extract(account_data, 'crunchbase_url'),
                    'loop_account_primary_domain': safe_extract(account_data, 'primary_domain'),
                    'loop_account_domain': safe_extract(account_data, 'domain'),
                    'loop_account_team_id': safe_extract(account_data, 'team_id'),
                    'loop_account_organization_id': safe_extract(account_data, 'organization_id'),
                    'loop_account_sanitized_phone': safe_extract(account_data, 'sanitized_phone'),
                })
                # Phone number data
                phone_data = safe_extract(match, 'phone_numbers', 0) or {}
                loop_data.update({
                    'loop_phone_number_raw_number': safe_extract(phone_data, 'raw_number'),
                    'loop_phone_number_sanitized_number': safe_extract(phone_data, 'sanitized_number'),
                    'loop_phone_number_type': safe_extract(phone_data, 'type'),
                    'loop_phone_number_position': safe_extract(phone_data, 'position'),
                    'loop_phone_number_status': safe_extract(phone_data, 'status'),
                })
                for col, value in loop_data.items():
                    df_final.at[current_row, col] = value        
                current_row += 1
            # Extract rate limits from headers
            rate_limits = {
                'x_rate_limit_minute': int(response_headers.get('x-rate-limit-minute', '0')),
                'x_minute_usage': int(response_headers.get('x-minute-usage', '0')),
                'x_minute_requests_left': int(response_headers.get('x-minute-requests-left', '0')),
                'x_rate_limit_hourly': int(response_headers.get('x-rate-limit-hourly', '0')),
                'x_hourly_usage': int(response_headers.get('x-hourly-usage', '0')),
                'x_hourly_requests_left': int(response_headers.get('x-hourly-requests-left', '0')),
                'x_rate_limit_24_hour': int(response_headers.get('x-rate-limit-24-hour', '0')),
                'x_24_hour_usage': int(response_headers.get('x-24-hour-usage', '0')),
                'x_24_hour_requests_left': int(response_headers.get('x-24-hour-requests-left', '0')),
            }
            next_batch_size = 10 if (index + 1) < len(batches) else len(df) % 10
            if rate_limits['x_24_hour_requests_left'] <= 1:
                print('Rate limit for day reached. Stopping further requests.')
                break
            elif rate_limits['x_hourly_requests_left'] <= 1:
                print('Rate limit for hour reached. Stopping further requests.')
                break
            elif rate_limits['x_minute_requests_left'] <= 1:
                print('Rate limit for minute reached, sleeping for 60 seconds.')
                time.sleep(60)
            elif rate_limits['x_hourly_requests_left'] < next_batch_size or rate_limits['x_24_hour_requests_left'] < next_batch_size:
                print(f'Not enough credits to process the next batch of size {next_batch_size}. Stopping further requests.')
                break
            elif rate_limits['x_minute_requests_left'] < next_batch_size:
                print(f'Approaching the per-minute request limit. Waiting for 60 seconds.')
                time.sleep(60)
        df_final = df_final.iloc[:current_row]
        df_final = pd.concat([df_original, df_final], axis=1)
        df_final.rename(columns=all_columns, inplace=True)
        phone_columns = [
            'Organization - Primary phone number',
            'Organization - Phone',
            'Account - Primary phone number',
            'Phone number - Raw number',
            'Phone number - Sanitized number'
        ]
        for column in phone_columns:
            if column in df_final.columns:
                df_final[column] = df_final[column].apply(add_quote_to_plus)
        
        '''
        print("x_rate_limit_minute: " + str(x_rate_limit_minute))
        print("x_minute_usage: " + str(x_minute_usage))
        print("x_minute_requests_left: " + str(x_minute_requests_left))
        print("x_rate_limit_hourly: " + str(x_rate_limit_hourly))
        print("x_hourly_usage: " + str(x_hourly_usage))
        print("x_hourly_requests_left: " + str(x_hourly_requests_left))
        print("x_rate_limit_24_hour: " + str(x_rate_limit_24_hour))
        print("x_24_hour_usage:" + str(x_24_hour_usage))
        print("x_24_hour_requests_left: " + str(x_24_hour_requests_left))
        '''

        return df_final

def apollo_company_enrichment(api_key, df, domains_column_name):
        df_original = df.copy()
        API_ENDPOINT = 'https://api.apollo.io/api/v1/organizations/bulk_enrich'
        headers = {'Content-Type': 'application/json', 'Cache-Control': 'no-cache',}
        def send_batch_request(batch):
            json_data = {'api_key': api_key, 'domains': batch}
            response = requests.post(API_ENDPOINT, headers=headers, json=json_data)
            return response.headers, response.json()
        columns = [
            'loop_record_found', 'loop_id', 'loop_name', 'loop_website_url', 'loop_blog_url',
            'loop_angellist_url', 'loop_linkedin_url', 'loop_twitter_url', 'loop_facebook_url',
            'loop_primary_phone_number', 'loop_primary_phone_source', 'loop_languages_values',
            'loop_alexa_ranking', 'loop_phone', 'loop_linkedin_uid', 'loop_founded_year',
            'loop_publicly_traded_symbol', 'loop_publicly_traded_exchange', 'loop_logo_url',
            'loop_crunchbase_url', 'loop_primary_domain', 'loop_industry', 'loop_keywords_values',
            'loop_estimated_num_employees', 'loop_snippets_loaded', 'loop_retail_location_count',
            'loop_raw_address', 'loop_street_address', 'loop_city', 'loop_state', 'loop_country',
            'loop_postal_code', 'loop_account_id', 'loop_account_domain', 'loop_account_name',
            'loop_account_phone', 'loop_account_sanitized_phone'
        ]
        total_rows = len(df)
        df_final = pd.DataFrame(index=range(total_rows), columns=columns)
        batches = [df.iloc[i:i+10] for i in range(0, len(df), 10)]
        current_row = 0
        for batch in tqdm(batches, desc="Processing company batches"):
            domains_list = batch[domains_column_name].tolist()
            response_headers, response_data = send_batch_request(domains_list)
            organizations = response_data.get('organizations', [])
            for idx, organization in enumerate(organizations):
                loop_record_found = 'Found' if organization else 'Not found'
                org_data = {
                    'loop_record_found': loop_record_found,
                    'loop_id': safe_extract(organization, 'id'),
                    'loop_name': safe_extract(organization, 'name'),
                    'loop_website_url': safe_extract(organization, 'website_url'),
                    'loop_blog_url': safe_extract(organization, 'blog_url'),
                    'loop_angellist_url': safe_extract(organization, 'angellist_url'),
                    'loop_linkedin_url': safe_extract(organization, 'linkedin_url'),
                    'loop_twitter_url': safe_extract(organization, 'twitter_url'),
                    'loop_facebook_url': safe_extract(organization, 'facebook_url'),
                    'loop_primary_phone_number': safe_extract(organization, 'primary_phone', 'number'),
                    'loop_primary_phone_source': safe_extract(organization, 'primary_phone', 'source'),
                    'loop_languages_values': ', '.join(safe_extract(organization, 'languages') or []),
                    'loop_alexa_ranking': safe_extract(organization, 'alexa_ranking'),
                    'loop_phone': safe_extract(organization, 'phone'),
                    'loop_linkedin_uid': safe_extract(organization, 'linkedin_uid'),
                    'loop_founded_year': safe_extract(organization, 'founded_year'),
                    'loop_publicly_traded_symbol': safe_extract(organization, 'publicly_traded_symbol'),
                    'loop_publicly_traded_exchange': safe_extract(organization, 'publicly_traded_exchange'),
                    'loop_logo_url': safe_extract(organization, 'logo_url'),
                    'loop_crunchbase_url': safe_extract(organization, 'crunchbase_url'),
                    'loop_primary_domain': domains_list[idx] if not organization else safe_extract(organization, 'primary_domain'),
                    'loop_industry': safe_extract(organization, 'industry'),
                    'loop_keywords_values': ', '.join(safe_extract(organization, 'keywords') or []),
                    'loop_estimated_num_employees': safe_extract(organization, 'estimated_num_employees'),
                    'loop_snippets_loaded': safe_extract(organization, 'snippets_loaded'),
                    'loop_retail_location_count': safe_extract(organization, 'retail_location_count'),
                    'loop_raw_address': safe_extract(organization, 'raw_address'),
                    'loop_street_address': safe_extract(organization, 'street_address'),
                    'loop_city': safe_extract(organization, 'city'),
                    'loop_state': safe_extract(organization, 'state'),
                    'loop_country': safe_extract(organization, 'country'),
                    'loop_postal_code': safe_extract(organization, 'postal_code')
                }
                account_data = safe_extract(organization, 'account') or {}
                org_data.update({
                    'loop_account_id': safe_extract(account_data, 'account_id'),
                    'loop_account_domain': safe_extract(account_data, 'domain'),
                    'loop_account_name': safe_extract(account_data, 'name'),
                    'loop_account_phone': safe_extract(account_data, 'phone'),
                    'loop_account_sanitized_phone': safe_extract(account_data, 'sanitized_phone')
                })
                for col, value in org_data.items():
                    df_final.at[current_row, col] = value
                current_row += 1
            rate_limits = {
                'x_rate_limit_minute': int(response_headers.get('x-rate-limit-minute', '0')),
                'x_minute_requests_left': int(response_headers.get('x-minute-requests-left', '0')),
                'x_rate_limit_hourly': int(response_headers.get('x-rate-limit-hourly', '0')),
                'x_hourly_requests_left': int(response_headers.get('x-hourly-requests-left', '0')),
                'x_rate_limit_24_hour': int(response_headers.get('x-rate-limit-24-hour', '0')),
                'x_24_hour_requests_left': int(response_headers.get('x-24-hour-requests-left', '0'))
            }
            next_batch_size = 10 if (idx + 1) < len(batches) else len(df) % 10
            if rate_limits['x_24_hour_requests_left'] <= 1:
                print('Rate limit for the day reached. Stopping further requests.')
                break
            elif rate_limits['x_hourly_requests_left'] <= 1:
                print('Rate limit for the hour reached. Stopping further requests.')
                break
            elif rate_limits['x_minute_requests_left'] <= 1:
                print('Rate limit for the minute reached, sleeping for 60 seconds.')
                time.sleep(60)
            elif rate_limits['x_hourly_requests_left'] < next_batch_size or rate_limits['x_24_hour_requests_left'] < next_batch_size:
                print(f'Not enough credits to process the next batch of size {next_batch_size}. Stopping further requests.')
                break
            elif rate_limits['x_minute_requests_left'] < next_batch_size:
                print(f'Approaching the per-minute request limit. Waiting for 60 seconds.')
                time.sleep(60)
        phone_columns = ['loop_primary_phone_number', 'loop_phone', 'loop_account_phone', 'loop_account_sanitized_phone']
        for column in phone_columns:
            if column in df_final.columns:
                df_final[column] = df_final[column].apply(lambda x: "'" + str(x) if pd.notna(x) and str(x).startswith('+') else x)
        rename_dict = {
            'loop_record_found': 'Record found?',
            'loop_id': 'Organization - ID',
            'loop_name': 'Organization - Name',
            'loop_website_url': 'Organization - Website URL',
            'loop_blog_url': 'Organization - Blog URL',
            'loop_angellist_url': 'Organization - Angellist URL',
            'loop_linkedin_url': 'Organization - LinkedIn URL',
            'loop_twitter_url': 'Organization - Twitter URL',
            'loop_facebook_url': 'Organization - Facebook URL',
            'loop_primary_phone_number': 'Organization - Primary phone number',
            'loop_primary_phone_source': 'Organization - Primary phone source',
            'loop_languages_values': 'Organization - Languages',
            'loop_alexa_ranking': 'Organization - Alexa ranking',
            'loop_phone': 'Organization - Phone',
            'loop_linkedin_uid': 'Organization - LinkedIn UID',
            'loop_founded_year': 'Organization - Founded year',
            'loop_publicly_traded_symbol': 'Organization - Publicly traded symbol',
            'loop_publicly_traded_exchange': 'Organization - Publicly traded exchange',
            'loop_logo_url': 'Organization - Logo URL',
            'loop_crunchbase_url': 'Organization - Crunchbase URL',
            'loop_primary_domain': 'Organization - Primary domain',
            'loop_industry': 'Organization - Industry',
            'loop_keywords_values': 'Organization - Keywords',
            'loop_estimated_num_employees': 'Organization - Estimated number of employees',
            'loop_snippets_loaded': 'Organization - Snippets loaded?',
            'loop_retail_location_count': 'Organization - Retail location count',
            'loop_raw_address': 'Organization - Raw address',
            'loop_street_address': 'Organization - Street address',
            'loop_city': 'Organization - City',
            'loop_state': 'Organization - State',
            'loop_country': 'Organization - Country',
            'loop_postal_code': 'Organization - Postal code',
            'loop_account_id': 'Account - ID',
            'loop_account_domain': 'Account - Domain',
            'loop_account_name': 'Account - Name',
            'loop_account_phone': 'Account - Phone',
            'loop_account_sanitized_phone': 'Account - Sanitized phone'
        }
        df_final.rename(columns=rename_dict, inplace=True)
        df_final = pd.concat([df_original, df_final.reset_index(drop=True)], axis=1)
        return df_final