import json
import os
import pandas as pd
import re
import requests
import streamlit as st
import sys
sys.path.append(os.path.abspath('../scripts/helper_scripts'))
from scripts.helper_scripts import *
from stqdm import stqdm

def company_linkedin_url_search_using_serper(dataframe, columnName, apiKey, streamlit_execution=False):
    columnName_values = dataframe[columnName].tolist()
    df_final = pd.DataFrame()
    for columnName_value in stqdm(columnName_values, desc="Processing"):
        request_base_url = "https://google.serper.dev/search"
        payload = json.dumps({
            "q": f'"{columnName_value}" site:linkedin.com',
            "location": "United States"
        })
        headers = {
            'X-API-KEY': apiKey,
            'Content-Type': 'application/json'
        }
        response = requests.request("POST", request_base_url, headers=headers, data=payload)
        response_json = response.json()
        query = columnName_value
        error = None
        df_loop_final = pd.DataFrame()
        organic_results = safe_extract(response_json, 'organic')
        if organic_results:
            df_loop = pd.DataFrame()
            for organic_result in organic_results:
                found_linkedin_url = False
                pos = safe_extract(organic_result, 'position')
                url = safe_extract(organic_result, 'link')
                desc = safe_extract(organic_result, 'snippet')
                title = safe_extract(organic_result, 'title')
                #Lookup for the first match
                linkedin_company_pattern = r'/company/'
                if re.search(linkedin_company_pattern, url):
                    all_variables = locals()
                    selected_vars = {var: [all_variables[var]] for var in ["pos", "url", "desc", "title"]}
                    df_loop = pd.DataFrame(selected_vars)
                    df_loop_final = pd.DataFrame({'query': [query], 'error': [error]})
                    df_loop_final = pd.concat([df_loop_final, df_loop], axis=1)
                    df_final = pd.concat([df_final, df_loop_final])
                    found_linkedin_url = True
                    break
            if not found_linkedin_url:
                pos = url = desc = title = url_shown = pos_overall = None
                all_variables = locals()
                selected_vars = {var: [all_variables[var]] for var in ["pos", "url", "desc", "title"]}
                df_loop = pd.DataFrame(selected_vars)
                error = "No LinkedIn URL found!"
                df_loop_final = pd.DataFrame({'query': [query], 'error': [error]})
                df_loop_final = pd.concat([df_loop_final, df_loop], axis=1)
                df_final = pd.concat([df_final, df_loop_final])
                pass
        else:
            pos = url = desc = title = url_shown = pos_overall = None
            all_variables = locals()
            selected_vars = {var: [all_variables[var]] for var in ["pos", "url", "desc", "title"]}
            df_loop = pd.DataFrame(selected_vars)
            error = "No results found!"
            df_loop_final = pd.DataFrame({'query': [query], 'error': [error]})
            df_loop_final = pd.concat([df_loop_final, df_loop], axis=1)
            df_final = pd.concat([df_final, df_loop_final])
            pass
    return df_final