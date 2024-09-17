import os
import streamlit as st
import sys
sys.path.append(os.path.abspath('../scripts/helper_scripts'))
from scripts.helper_scripts import *
sys.path.append(os.path.abspath('../scripts/enrichment_scripts'))
from scripts.enrichment_scripts import company_linkedin_url_search_using_serper

key_dict = dict(st.secrets["GOOGLE_CLOUD_CREDENTIALS"])
key_dict["private_key"] = key_dict["private_key"].replace("\\n", "\n")
streamlit_execution = True

st.title("Company LinkedIn URL search using Serper")
st.write("Before executing any script, please ensure that you share your spreadsheet with this account, granting editor permissions: kalungi-google-colab@invertible-now-393117.iam.gserviceaccount.com")
spreadsheet_url = st.text_input("Spreadsheet URL (Name of the spreadsheet where the domains are located)", key="spreadsheet_url")
sheet_name = st.text_input("Sheet name (Name of the sheet where the domains are located)", key="sheet_name")
column_name = st.text_input("Domain column name (Name of the column where the domains are located)", key="column_name")
serper_api_key = st.text_input("Serper API key (Serper API key from serper.dev)", key="serper_api_key")

if st.button("Start searching"):
        if not spreadsheet_url:
            st.error("Please fill spreadsheet URL.")
        if not serper_api_key:
             st.error("Please fill Seper API key.")
        if spreadsheet_url and serper_api_key:
            try:
                dataframe_input = retrieve_spreadsheet(spreadsheet_url, sheet_name, key_dict)
                if dataframe_input is not None and not dataframe_input.empty:
                    dataframe_result = company_linkedin_url_search_using_serper(dataframe_input, column_name, serper_api_key, streamlit_execution)
                    write_into_spreadsheet(spreadsheet_url, sheet_name, dataframe_result, key_dict)
                    st.success("Searching completed!")
            except Exception as e:
                st.error(f"An error occurred: {e}")
                st.exception(e)