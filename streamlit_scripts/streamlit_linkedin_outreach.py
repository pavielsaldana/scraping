import asyncio
import os
import streamlit as st
import sys
sys.path.append(os.path.abspath('../helper_scripts'))
from helper_scripts.helper_scripts import *
sys.path.append(os.path.abspath('../linkedin_scripts'))
from linkedin_scripts.linkedin_outreach import *

key_dict = dict(st.secrets["GOOGLE_CLOUD_CREDENTIALS"])
key_dict["private_key"] = key_dict["private_key"].replace("\\n", "\n")

st.title("LinkedIn outreach scripts")
def reset_inputs():
    st.session_state["li_at"] = ""
    st.session_state["spreadsheet_url"] = ""
    st.session_state["sheet_name"] = ""
    st.session_state["min_waiting_time"] = "5"
    st.session_state["max_waiting_time"] = "10"
    st.session_state["column_name"] = ""
    st.session_state["result_column_name"] = ""
    st.session_state["action"] = "Select action"
    st.session_state["invitation_id_column_name"] = ""
    st.session_state["invitation_shared_secret_column_name"] = ""
    st.session_state["vmid_column_name"] = ""
    st.session_state["message_column_name"] = ""

if "previous_option" not in st.session_state:
    st.session_state["previous_option"] = "Select one LinkedIn outreach script"
linkedin_outreach_option = st.selectbox(
    "Select one LinkedIn outreach script",
    ("Select one LinkedIn outreach script",
     "Obtain the current user profile",
     "Get all connections",
     "Get all connection requests",
     "Get all sent connection requests",
     "Get the last 20 conversations",
     "Get all conversations with connections",
     "Get all messages from conversations",
     "Mark as seen conversations",
     "Remove connections",
     "Accept or ignore connection requests",
     "Withdraw connection requests",
     "Follow or unfollow leads (must be a connection)",
     "Send connection requests",
     "Send message",
     )
)
if linkedin_outreach_option != st.session_state["previous_option"]:
    reset_inputs()
st.session_state["previous_option"] = linkedin_outreach_option
if linkedin_outreach_option != "Select one LinkedIn outreach script":
    st.write("Before executing any script, please ensure that you share your spreadsheet with this account, granting editor permissions: kalungi-google-colab@invertible-now-393117.iam.gserviceaccount.com")
if linkedin_outreach_option == "Obtain the current user profile":
    st.write("LinkedIn authentication cookie.")
    li_at = st.text_input("li_at", key="li_at")
    st.write("URL of the spreadsheet where all the information will be printed.")
    spreadsheet_url = st.text_input("Spreadsheet URL", key="spreadsheet_url")    
    st.write("Name of the sheet where all the information will be printed.")
    sheet_name = st.text_input("Sheet name", key="sheet_name")
if linkedin_outreach_option == "Get all connections":
    st.write("LinkedIn authentication cookie.")
    li_at = st.text_input("li_at", key="li_at")
    st.write("URL of the spreadsheet where all the connections will be printed.")
    spreadsheet_url = st.text_input("Spreadsheet URL", key="spreadsheet_url")    
    st.write("Name of the sheet where all the connections will be printed.")
    sheet_name = st.text_input("Sheet name", key="sheet_name")
if linkedin_outreach_option == "Get all connection requests":
    st.write("LinkedIn authentication cookie.")
    li_at = st.text_input("li_at", key="li_at")
    st.write("URL of the spreadsheet where all the connection requests will be printed.")
    spreadsheet_url = st.text_input("Spreadsheet URL", key="spreadsheet_url")    
    st.write("Name of the sheet where all the connection requests will be printed.")
    sheet_name = st.text_input("Sheet name", key="sheet_name")
if linkedin_outreach_option == "Get all sent connection requests":
    st.write("LinkedIn authentication cookie.")
    li_at = st.text_input("li_at", key="li_at")
    st.write("URL of the spreadsheet where all the sent connection requests will be printed.")
    spreadsheet_url = st.text_input("Spreadsheet URL", key="spreadsheet_url")    
    st.write("Name of the sheet where all the sent connection requests will be printed.")
    sheet_name = st.text_input("Sheet name", key="sheet_name")
if linkedin_outreach_option == "Get the last 20 conversations":
    st.write("LinkedIn authentication cookie.")
    li_at = st.text_input("li_at", key="li_at")
    st.write("URL of the spreadsheet where all the conversations will be printed.")
    spreadsheet_url = st.text_input("Spreadsheet URL", key="spreadsheet_url")    
    st.write("Name of the sheet where all the conversations will be printed.")
    sheet_name = st.text_input("Sheet name", key="sheet_name")
if linkedin_outreach_option == "Get all conversations with connections":
    st.write("LinkedIn authentication cookie.")
    li_at = st.text_input("li_at", key="li_at")
    st.write("URL of the spreadsheet where all the conversations with connections will be printed.")
    spreadsheet_url = st.text_input("Spreadsheet URL", key="spreadsheet_url")    
    st.write("Name of the sheet where all the conversations with connections will be printed.")
    sheet_name = st.text_input("Sheet name", key="sheet_name")
    st.write("Minimum waiting time in seconds for each conversation id lookup.")
    min_waiting_time = int(st.text_input("Minimum waiting time", "0", key="min_waiting_time"))
    st.write("Maximum waiting time in seconds for each conversation id lookup.")
    max_waiting_time = int(st.text_input("Maximum waiting time", "0", key="max_waiting_time"))
if linkedin_outreach_option == "Get all messages from conversations":
    st.write("LinkedIn authentication cookie.")
    li_at = st.text_input("li_at", key="li_at")
    st.write("URL of the spreadsheet where all the conversation ids are located.")
    spreadsheet_url = st.text_input("Spreadsheet URL", key="spreadsheet_url")    
    st.write("Name of the sheet where all the conversation ids are located.")
    sheet_name = st.text_input("Sheet name", key="sheet_name")
    st.write("Name of the column where all the conversation ids are located.")
    column_name = st.text_input("Column name", key="column_name")
if linkedin_outreach_option == "Mark as seen conversations":
    st.write("LinkedIn authentication cookie.")
    li_at = st.text_input("li_at", key="li_at")
    st.write("URL of the spreadsheet where all the conversation ids are located.")
    spreadsheet_url = st.text_input("Spreadsheet URL", key="spreadsheet_url")    
    st.write("Name of the sheet where all the conversation ids are located.")
    sheet_name = st.text_input("Sheet name", key="sheet_name")
    st.write("Name of the column where all the conversation ids are located.")
    column_name = st.text_input("Column name", key="column_name")
    st.write("Name of the column where all the results will be printed.")
    result_column_name = st.text_input("Result column name", key="result_column_name")
    st.write("Minimum waiting time in seconds for each conversation marked as seen.")
    min_waiting_time = int(st.text_input("Minimum waiting time", "0", key="min_waiting_time"))
    st.write("Maximum waiting time in seconds for each conversation marked as seen.")
    max_waiting_time = int(st.text_input("Maximum waiting time", "0", key="max_waiting_time"))
if linkedin_outreach_option == "Remove connections":
    st.write("LinkedIn authentication cookie.")
    li_at = st.text_input("li_at", key="li_at")
    st.write("URL of the spreadsheet where all the vmids or universal names are located.")
    spreadsheet_url = st.text_input("Spreadsheet URL", key="spreadsheet_url")    
    st.write("Name of the sheet where all the vmids or universal names are located.")
    sheet_name = st.text_input("Sheet name", key="sheet_name")
    st.write("Name of the column where all the vmids or universal names are located.")
    column_name = st.text_input("Column name", key="column_name")
    st.write("Name of the column where all the results will be printed.")
    result_column_name = st.text_input("Result column name", key="result_column_name")
    st.write("Minimum waiting time in seconds for each connection removed.")
    min_waiting_time = int(st.text_input("Minimum waiting time", "0", key="min_waiting_time"))
    st.write("Maximum waiting time in seconds for each connection removed.")
    max_waiting_time = int(st.text_input("Maximum waiting time", "0", key="max_waiting_time"))
if linkedin_outreach_option == "Accept or ignore connection requests":
    st.write("LinkedIn authentication cookie.")
    li_at = st.text_input("li_at", key="li_at")
    st.write("URL of the spreadsheet where all the invitation ids and invitation shared secrets are located.")
    spreadsheet_url = st.text_input("Spreadsheet URL", key="spreadsheet_url")    
    st.write("Name of the sheet where all the invitation ids and invitation shared secrets are located.")
    sheet_name = st.text_input("Sheet name", key="sheet_name")
    st.write('"accept" if you want to accept all the connection requests and "ignore" if you want to ignore all the connection requests.')
    action = st.selectbox("accept", options=["accept", "ignore"], key="action")
    st.write("Name of the column where all the invitation ids are located.")
    invitation_id_column_name = st.text_input("Invitation id column Name", key="invitation_id_column_name")
    st.write("Name of the column where all the invitation shared secrets are located.")
    invitation_shared_secret_column_name = st.text_input("Invitation shared secret column name", key="invitation_shared_secret_column_name")    
    st.write("Name of the column where all the results will be printed.")
    result_column_name = st.text_input("Result column name", key="result_column_name")
    st.write("Minimum waiting time in seconds for each connection request accepted/ignored.")
    min_waiting_time = int(st.text_input("Minimum waiting time", "0", key="min_waiting_time"))
    st.write("Maximum waiting time in seconds for each connection request accepted/ignored.")
    max_waiting_time = int(st.text_input("Maximum waiting time", "0", key="max_waiting_time"))
if linkedin_outreach_option == "Withdraw connection requests":
    st.write("LinkedIn authentication cookie.")
    li_at = st.text_input("li_at", key="li_at")
    st.write("URL of the spreadsheet where all the the invitation ids are located.")
    spreadsheet_url = st.text_input("Spreadsheet URL", key="spreadsheet_url")    
    st.write("Name of the sheet where all the the invitation ids are located.")
    sheet_name = st.text_input("Sheet name", key="sheet_name")
    st.write("Name of the column where all the the invitation ids are located.")
    column_name = st.text_input("Column name", key="column_name")
    st.write("Name of the column where all the results will be printed.")
    result_column_name = st.text_input("Result column name", key="result_column_name")
    st.write("Minimum waiting time in seconds for each connection request withdrawed.")
    min_waiting_time = int(st.text_input("Minimum waiting time", "0", key="min_waiting_time"))
    st.write("Maximum waiting time in seconds for each connection request withdrawed.")
    max_waiting_time = int(st.text_input("Maximum waiting time", "0", key="max_waiting_time"))
if linkedin_outreach_option == "Follow or unfollow leads (must be a connection)":
    st.write("LinkedIn authentication cookie.")
    li_at = st.text_input("li_at", key="li_at")
    st.write("URL of the spreadsheet where all the vmids are located are located.")
    spreadsheet_url = st.text_input("Spreadsheet URL", key="spreadsheet_url")    
    st.write("Name of the sheet where all the vmids are located are located.")
    sheet_name = st.text_input("Sheet name", key="sheet_name")
    st.write('"follow" if you want to follow all the profiles and "unfollow" if you want to unfollow all the profiles.')
    action = st.selectbox("Select action", options=["follow", "unfollow"], key="action")
    st.write("Name of the column where all the vmids ids are located.")
    column_name = st.text_input("vmid column name", key="column_name")    
    st.write("Name of the column where all the results will be printed.")
    result_column_name = st.text_input("Result column name", key="result_column_name")
    st.write("Minimum waiting time in seconds for each connection request accepted/ignored.")
    min_waiting_time = int(st.text_input("Minimum waiting time", "0", key="min_waiting_time"))
    st.write("Maximum waiting time in seconds for each connection request accepted/ignored.")
    max_waiting_time = int(st.text_input("Maximum waiting time", "0", key="max_waiting_time"))
if linkedin_outreach_option == "Send connection requests":
    st.write("LinkedIn authentication cookie.")
    li_at = st.text_input("li_at", key="li_at")
    st.write("URL of the spreadsheet where all the vmids and messages are located.")
    spreadsheet_url = st.text_input("Spreadsheet URL", key="spreadsheet_url")    
    st.write("Name of the sheet where where all the vmids and messages are located.")
    sheet_name = st.text_input("Sheet name", key="sheet_name")
    st.write("Name of the column where all the vmids are located.")
    vmid_column_name = st.text_input("vmid column name", key="vmid_column_name")    
    st.write("Name of the column where all the messages are located.")
    message_column_name = st.text_input("Message column name", key="message_column_name")
    st.write("Name of the column where all the results will be printed.")
    result_column_name = st.text_input("Result column name", key="result_column_name")
    st.write("Minimum waiting time in seconds for each connection request sent.")
    min_waiting_time = int(st.text_input("Minimum waiting time", "0", key="min_waiting_time"))
    st.write("Maximum waiting time in seconds for each connection request sent.")
    max_waiting_time = int(st.text_input("Maximum waiting time", "0", key="max_waiting_time"))
if linkedin_outreach_option == "Send message":
    st.write("LinkedIn authentication cookie.")
    li_at = st.text_input("li_at", key="li_at")
    st.write("URL of the spreadsheet where all the vmids and messages are located.")
    spreadsheet_url = st.text_input("Spreadsheet URL", key="spreadsheet_url")    
    st.write("Name of the sheet where where all the vmids and messages are located.")
    sheet_name = st.text_input("Sheet name", key="sheet_name")
    st.write("Name of the column where all the vmids are located.")
    vmid_column_name = st.text_input("vmid column name", key="vmid_column_name")    
    st.write("Name of the column where all the messages are located (If the profile has a Sales Navigator subscription, ensure it's a maximum of 300 characters. If not, limit it to 200 characters)")
    message_column_name = st.text_input("Message column name", key="message_column_name")
    st.write("Name of the column where all the results will be printed.")
    result_column_name = st.text_input("Result column name", key="result_column_name")
    st.write("Minimum waiting time in seconds for each message sent.")
    min_waiting_time = int(st.text_input("Minimum waiting time", "0", key="min_waiting_time"))
    st.write("Maximum waiting time in seconds for each message sent.")
    max_waiting_time = int(st.text_input("Maximum waiting time", "0", key="max_waiting_time"))

if linkedin_outreach_option != "Select one LinkedIn outreach script":
    if st.button("Start outreach"):
        if not li_at:
            st.error("Please fill li_at.")
        if not spreadsheet_url:
            st.error("Please fill spreadsheet URL.")
        if li_at and spreadsheet_url:
            try:
                dataframe_input = retrieve_spreadsheet(spreadsheet_url, sheet_name, key_dict)
                if dataframe_input is not None and not dataframe_input.empty:
                    JSESSIONID, li_a, csrf_token, cookies_dict = asyncio.run(retrieve_tokens(li_at))
                    if linkedin_outreach_option == "Obtain the current user profile":
                        script_type = "obtain_current_user_profile"
                        dataframe_result = linkedin_outreach_scripts(csrf_token=csrf_token, cookies_dict=cookies_dict, script_type=script_type)
                    if linkedin_outreach_option == "Get all connections":
                        script_type = "get_all_connections_profiles"
                        dataframe_result = linkedin_outreach_scripts(csrf_token=csrf_token, cookies_dict=cookies_dict, script_type=script_type)
                    if linkedin_outreach_option == "Get all connection requests":
                        script_type = "get_all_connection_requests"
                        dataframe_result = linkedin_outreach_scripts(csrf_token=csrf_token, cookies_dict=cookies_dict, script_type=script_type)
                    if linkedin_outreach_option == "Get all sent connection requests":
                        script_type = "get_all_sent_connection_requests"
                        dataframe_result = linkedin_outreach_scripts(csrf_token=csrf_token, cookies_dict=cookies_dict, script_type=script_type)
                    if linkedin_outreach_option == "Get the last 20 conversations":
                        script_type = "get_last_20_conversations"
                        dataframe_result = linkedin_outreach_scripts(csrf_token=csrf_token, cookies_dict=cookies_dict, script_type=script_type)
                    if linkedin_outreach_option == "Get all conversations with connections":
                        script_type = "get_all_conversations_with_connections"
                        dataframe_result = linkedin_outreach_scripts(csrf_token=csrf_token, cookies_dict=cookies_dict, script_type=script_type)
                    if linkedin_outreach_option == "Get all messages from conversations":
                        script_type = 'get_all_messages_from_conversation'
                        dataframe_result = linkedin_outreach_scripts(csrf_token=csrf_token, cookies_dict=cookies_dict, script_type=script_type)
                    if linkedin_outreach_option == "Mark as seen conversation":
                        script_type = 'mark_conversation_as_seen_using_conversation_id'
                        dataframe_result = linkedin_outreach_scripts(csrf_token=csrf_token, cookies_dict=cookies_dict, script_type=script_type)
                    if linkedin_outreach_option == "Remove connections":
                        script_type = 'remove_connections'
                        dataframe_result = linkedin_outreach_scripts(csrf_token=csrf_token, cookies_dict=cookies_dict, script_type=script_type)
                    if linkedin_outreach_option == "Accept or ignore connection requests":
                        script_type = "accept_or_remove_connection_requests"
                        dataframe_result = linkedin_outreach_scripts(csrf_token=csrf_token, cookies_dict=cookies_dict, script_type=script_type)
                    if linkedin_outreach_option == "Withdraw connection requests":
                        script_type = "withdraw_connection_requests"
                        dataframe_result = linkedin_outreach_scripts(csrf_token=csrf_token, cookies_dict=cookies_dict, script_type=script_type)
                    if linkedin_outreach_option == "Follow or unfollow leads (must be a connection)":
                        script_type = "follow_or_unfollow_profiles"
                        dataframe_result = linkedin_outreach_scripts(csrf_token=csrf_token, cookies_dict=cookies_dict, script_type=script_type)
                    if linkedin_outreach_option == "Send connection requests":
                        script_type = 'send_connection_requests'
                        dataframe_result = linkedin_outreach_scripts(csrf_token=csrf_token, cookies_dict=cookies_dict, script_type=script_type)
                    if linkedin_outreach_option == "Send message":
                        script_type = 'send_message_using_vmid'
                        dataframe_result = linkedin_outreach_scripts(csrf_token=csrf_token, cookies_dict=cookies_dict, script_type=script_type)
                                       
                    write_into_spreadsheet(spreadsheet_url, sheet_name, dataframe_result, key_dict)
                    st.success("Scraping completed!")
            except Exception as e:
                st.error(f"An error occurred: {e}")
                st.exception(e)
