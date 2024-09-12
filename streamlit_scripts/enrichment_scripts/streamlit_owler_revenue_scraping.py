import asyncio
import os
import streamlit as st
import sys
sys.path.append(os.path.abspath('../scripts/helper_scripts'))
from scripts.helper_scripts import *
sys.path.append(os.path.abspath('../scripts/enrichment_scripts'))
from scripts.enrichment_scripts.owler_revenue_scraping import *

zenrowsApiKey = st.secrets["ZENROWS_API_KEY"]["value"]
OWLER_PC_cookie = st.secrets["OWLER_PC_COOKIE"]["value"]
key_dict = dict(st.secrets["GOOGLE_CLOUD_CREDENTIALS"])
key_dict["private_key"] = key_dict["private_key"].replace("\\n", "\n")

st.title("Owler revenue scripts")
def reset_inputs():
    st.session_state["spreadsheet_url"] = ""
    st.session_state["sheet_name"] = ""
    st.session_state["column_name"] = ""
    st.session_state["sheet_name_result"] = ""
    st.session_state["domainColumnName"] = ""
    st.session_state["owlerColumnName"] = ""
    st.session_state["sheetNameResult"] = ""
if "previous_option" not in st.session_state:
    st.session_state["previous_option"] = "Select one Owler revenue script"
owler_revenue_option = st.selectbox(
    "Select one Owler revenue script",
    ("Select one Owler revenue script",
     "Search Owler URLs & Scraping Owler URLs",
     "Scraping Owler URLs",
     )
)
if owler_revenue_option != st.session_state["previous_option"]:
    reset_inputs()
st.session_state["previous_option"] = owler_revenue_option
if owler_revenue_option != "Select one Owler revenue script":
    st.write("Before executing any script, please ensure that you share your spreadsheet with this account, granting editor permissions: kalungi-google-colab@invertible-now-393117.iam.gserviceaccount.com")
if owler_revenue_option == "Search Owler URLs & Scraping Owler URLs":
    spreadsheet_url = st.text_input("Spreadsheet URL (Here you should paste the spreadsheet URL which you are going to use)", key="spreadsheet_url")
    sheet_name = st.text_input("Sheet name (Here you should write the name of the sheet where your data is)", key="sheet_name")
    column_name = st.text_input("Domain column name (Here you should write the column name where the domains are)", key="column_name")
    sheet_name_result = st.text_input("Result sheet name (Here you should write the name of the sheet where your scraped data will be pasted)", key="sheet_name_result")
if owler_revenue_option == "Scraping Owler URLs":
    spreadsheetUrl = st.text_input("Spreadsheet URL (Here you should paste the spreadsheet URL which you are going to use)", key="spreadsheetUrl")
    sheetName = st.text_input("Sheet name (Here you should write the name of the sheet where your data is)", key="sheetName")
    domainColumnName = st.text_input("Domain column name (Here you should write the column name where the domains are)", key="domainColumnName")
    owlerColumnName = st.text_input("Owler URL column name (Here you should write the column name where the Owler URLs are.)", key="owlerColumnName")
    sheetNameResult = st.text_input("Result sheet name (Here you should write the name of the sheet where your scraped data will be pasted)", key="sheetNameResult")

if owler_revenue_option != "Select one Owler revenue script":
    if st.button("Start scraping"):
        if not spreadsheet_url:
            st.error("Please fill spreadsheet URL.")
        if spreadsheet_url:
            try:
                dataframe_input = retrieve_spreadsheet(spreadsheet_url, sheet_name, key_dict)
                if dataframe_input is not None and not dataframe_input.empty:
                    if owler_revenue_option == "Search Owler URLs & Scraping Owler URLs":
                        search_owler_urls_and_scraping_owler_urls(OWLER_PC_cookie, dataframe_input, column_name, spreadsheet_url, sheet_name, key_dict, zenrowsApiKey, sheet_name_result)
                    if owler_revenue_option == "Scraping Owler URLs":
                        search_owler_urls_and_scraping_owler_urls(OWLER_PC_cookie, dataframe_input, column_name, spreadsheet_url, sheet_name, key_dict, zenrowsApiKey, sheet_name_result)
                    st.success("Scraping completed!")
            except Exception as e:
                st.error(f"An error occurred: {e}")
                st.exception(e)