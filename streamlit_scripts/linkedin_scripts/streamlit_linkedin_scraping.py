import asyncio
import os
import streamlit as st
import sys
sys.path.append(os.path.abspath('../scripts/helper_scripts'))
from scripts.helper_scripts import *
sys.path.append(os.path.abspath('../scripts/linkedin_scripts'))
from scripts.linkedin_scripts.linkedin_scraping import *

key_dict = dict(st.secrets["GOOGLE_CLOUD_CREDENTIALS"])
key_dict["private_key"] = key_dict["private_key"].replace("\\n", "\n")

st.title("LinkedIn scraping scripts")
def reset_inputs():
    st.session_state["li_at"] = ""
    st.session_state["spreadsheet_url"] = ""
    st.session_state["sheet_name"] = ""
    st.session_state["column_name"] = ""
    st.session_state["location_count"] = ""
if "previous_option" not in st.session_state:
    st.session_state["previous_option"] = "Select one LinkedIn scraping script"
linkedin_scraping_option = st.selectbox(
    "Select one LinkedIn scraping script",
    ("Select one LinkedIn scraping script",
     "Sales Navigator lead search export",
     "Sales Navigator account export",
     "LinkedIn account scrape",
     "LinkedIn lead scrape",
     "LinkedIn account activity scrape",
     "LinkedIn lead activity scrape",
     "LinkedIn post commenters scrape",
     "LinkedIn job offers scrape",
     "LinkedIn job offer details scrape",
     )
)
if linkedin_scraping_option != st.session_state["previous_option"]:
    reset_inputs()
st.session_state["previous_option"] = linkedin_scraping_option
if linkedin_scraping_option != "Select one LinkedIn scraping script":
    st.write("Before executing any script, please ensure that you share your spreadsheet with this account, granting editor permissions: kalungi-google-colab@invertible-now-393117.iam.gserviceaccount.com")
if linkedin_scraping_option == "Sales Navigator lead search export":
    st.write("Use LinkedIn Sales Nave as a database and download any search results list from Sales Navigator. Use this when scraping contacts from a LinkedIn Sales Navigator query.")
    st.write("[Tutorial >](https://www.loom.com/looms/videos)")
    li_at = st.text_input("li_at (LinkedIn authentication cookie)", key="li_at")
    spreadsheet_url = st.text_input("Spreadsheet URL (URL of the spreadsheet where the LinkedIn company links are located)", key="spreadsheet_url")
    sheet_name = st.text_input("Sheet name (Name of the sheet where the LinkedIn profile links are located)", key="sheet_name")
    column_name = st.text_input("Column name (Name of the column where the LinkedIn profile links are located)", key="column_name")
if linkedin_scraping_option == "Sales Navigator account export":
    li_at = st.text_input("li_at (LinkedIn authentication cookie)", key="li_at")
    spreadsheet_url = st.text_input("Spreadsheet URL (URL of the spreadsheet where the LinkedIn company links are located)", key="spreadsheet_url")
    sheet_name = st.text_input("Sheet name (Name of the sheet where the LinkedIn profile links are located)", key="sheet_name")
    column_name = st.text_input("Column name (Name of the column where the LinkedIn profile links are located)", key="column_name")
if linkedin_scraping_option == "LinkedIn account scrape":
    li_at = st.text_input("li_at (LinkedIn authentication cookie)", key="li_at")
    spreadsheet_url = st.text_input("Spreadsheet URL (URL of the spreadsheet where the LinkedIn company links are located)", key="spreadsheet_url")    
    sheet_name = st.text_input("Sheet name (Name of the sheet where the LinkedIn company links are located)", key="sheet_name")
    column_name = st.text_input("Column name (Name of the column where the LinkedIn company links are located)", key="column_name")
    location_count = st.text_input("Location count (Number of locations to be scraped - choose between 0 and 100)", key="location_count")
if linkedin_scraping_option == "LinkedIn lead scrape":
    li_at = st.text_input("li_at (LinkedIn authentication cookie)", key="li_at")
    spreadsheet_url = st.text_input("Spreadsheet URL (URL of the spreadsheet where the LinkedIn profile links are located)", key="spreadsheet_url")
    sheet_name = st.text_input("Sheet name (Name of the sheet where the LinkedIn profile links are located)", key="sheet_name")
    column_name = st.text_input("Column name (Name of the column where the LinkedIn profile links are located)", key="column_name")
if linkedin_scraping_option == "LinkedIn account activity scrape":
    li_at = st.text_input("li_at (LinkedIn authentication cookie)", key="li_at")
    spreadsheet_url = st.text_input("Spreadsheet URL (URL of the spreadsheet where the LinkedIn company links are located)", key="spreadsheet_url")
    sheet_name = st.text_input("Sheet name (Name of the sheet where the LinkedIn company links are located)", key="sheet_name")
    column_name = st.text_input("Column name (Name of the column where the LinkedIn company links are located)", key="column_name")
if linkedin_scraping_option == "LinkedIn lead activity scrape":
    li_at = st.text_input("li_at (LinkedIn authentication cookie)", key="li_at")
    spreadsheet_url = st.text_input("Spreadsheet URL (URL of the spreadsheet where the LinkedIn profile links are located)", key="spreadsheet_url")
    sheet_name = st.text_input("Sheet name (Name of the sheet where the LinkedIn profile links are located)", key="sheet_name")
    column_name = st.text_input("Column name (Name of the column where the LinkedIn profile links are located)", key="column_name")
if linkedin_scraping_option == "LinkedIn post commenters scrape":
    li_at = st.text_input("li_at (LinkedIn authentication cookie)", key="li_at")
    spreadsheet_url = st.text_input("Spreadsheet URL (URL of the spreadsheet where the LinkedIn activity links are located)", key="spreadsheet_url")
    sheet_name = st.text_input("Sheet name (Name of the sheet where the LinkedIn activity links are located)", key="sheet_name")
    column_name = st.text_input("Column name (Name of the column where the LinkedIn activity links are located)", key="column_name")
if linkedin_scraping_option == "LinkedIn job offers scrape":
    li_at = st.text_input("li_at (LinkedIn authentication cookie)", key="li_at")
    spreadsheet_url = st.text_input("Spreadsheet URL (URL of the spreadsheet where the LinkedIn company links are located)", key="spreadsheet_url")
    sheet_name = st.text_input("Sheet name (Name of the sheet where the LinkedIn company links are located)", key="sheet_name")
    column_name = st.text_input("Column name (Name of the column where the LinkedIn company links are located)", key="column_name")
if linkedin_scraping_option == "LinkedIn job offer details scrape":
    li_at = st.text_input("li_at (LinkedIn authentication cookie)", key="li_at")
    spreadsheet_url = st.text_input("Spreadsheet URL (URL of the spreadsheet where the IDs of the job offers are located)", key="spreadsheet_url")
    sheet_name = st.text_input("Sheet name (Name of the sheet where the IDs of the job offers are located)", key="sheet_name")
    column_name = st.text_input("Column name (Name of the column where the IDs of the job offers are located)", key="column_name")

if linkedin_scraping_option != "Select one LinkedIn scraping script":
    if st.button("Start scraping"):
        if not li_at:
            st.error("Please fill li_at.")
        if not spreadsheet_url:
            st.error("Please fill spreadsheet URL.")
        if li_at and spreadsheet_url:
            try:
                dataframe_input = retrieve_spreadsheet(spreadsheet_url, sheet_name, key_dict)
                if dataframe_input is not None and not dataframe_input.empty:
                    JSESSIONID, li_a, csrf_token, cookies_dict = asyncio.run(retrieve_tokens(li_at))
                    if linkedin_scraping_option == "Sales Navigator lead search export":
                        dataframe_result = sales_navigator_lead_export(li_at, JSESSIONID, li_a, csrf_token, dataframe_input, column_name)
                    if linkedin_scraping_option == "Sales Navigator account export":
                        dataframe_result = sales_navigator_account_export(li_at, JSESSIONID, li_a, csrf_token, dataframe_input, column_name)
                    if linkedin_scraping_option == "LinkedIn account scrape":
                        dataframe_result = linkedin_account(li_at, JSESSIONID, li_a, csrf_token, dataframe_input, column_name, cookies_dict, location_count)
                    if linkedin_scraping_option == "LinkedIn lead scrape":
                        dataframe_result = linkedin_lead(csrf_token, dataframe_input, column_name, cookies_dict)
                    if linkedin_scraping_option == "LinkedIn account activity scrape":
                        dataframe_result = company_activity_extractor(csrf_token, dataframe_input, column_name, cookies_dict)
                    if linkedin_scraping_option == "LinkedIn lead activity scrape":
                        dataframe_result = profile_activity_extractor(csrf_token, dataframe_input, column_name, cookies_dict)
                    if linkedin_scraping_option == "LinkedIn post commenters scrape":
                        dataframe_result = post_commenters_extractor(csrf_token, dataframe_input, column_name, cookies_dict)
                    if linkedin_scraping_option == "LinkedIn job offers scrape":
                        dataframe_result = job_offers_extractor(csrf_token, dataframe_input, column_name, cookies_dict)
                    if linkedin_scraping_option == "LinkedIn job offer details scrape":
                        dataframe_result = job_offers_details_extractor(csrf_token, dataframe_input, column_name, cookies_dict)  
                    write_into_spreadsheet(spreadsheet_url, sheet_name, dataframe_result, key_dict)
                    st.success("Scraping completed!")
            except Exception as e:
                st.error(f"An error occurred: {e}")
                st.exception(e)
