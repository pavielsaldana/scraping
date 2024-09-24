import anvil.server
import streamlit as st
import sys
import os
sys.path.append(os.path.abspath('../scripts/helper_scripts'))
from scripts.helper_scripts import *

key_dict = dict(st.secrets["GOOGLE_CLOUD_CREDENTIALS"])
key_dict["private_key"] = key_dict["private_key"].replace("\\n", "\n")

# Connect to Anvil Uplink
anvil.server.connect("server_DHRQTXHJAMIELJIFKJJXFHLT-7GHUM6DVFBDW5NPJ")

# Streamlit app UI
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
        st.error("Please fill Serper API key.")
    if spreadsheet_url and serper_api_key:
        try:
            # Retrieve spreadsheet data
            dataframe_input = retrieve_spreadsheet(spreadsheet_url, sheet_name, key_dict)
            
            # Start background task in Anvil
            if dataframe_input is not None and not dataframe_input.empty:
                task_id = anvil.server.call('start_linkedin_search_task', dataframe_input, column_name, serper_api_key)
                st.session_state['task_id'] = task_id
                st.write(f"Task {task_id} started...")
        except Exception as e:
            st.error(f"An error occurred: {e}")
            st.exception(e)

# Check task status if running
if 'task_id' in st.session_state:
    task_id = st.session_state['task_id']
    task_status = anvil.server.call('check_task_status', task_id)

    st.write(f"Task Status: {task_status}")

    if task_status == "In Progress":
        st.info("Task is still running...")
    elif task_status == "Failed":
        st.error("Task failed.")
    else:
        st.success("Task completed!")
        # Optionally, display the dataframe or download results
        st.write(task_status)
