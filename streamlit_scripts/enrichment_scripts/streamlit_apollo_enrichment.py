import os
import streamlit as st
import sys
sys.path.append(os.path.abspath('../scripts/helper_scripts'))
from scripts.helper_scripts import *
sys.path.append(os.path.abspath('../scripts/enrichment_scripts'))
from scripts.enrichment_scripts.apollo_enrichment import *

key_dict = dict(st.secrets["GOOGLE_CLOUD_CREDENTIALS"])
key_dict["private_key"] = key_dict["private_key"].replace("\\n", "\n")
streamlit_execution = True

st.title("Apollo enrichment scripts")
def reset_inputs():
    st.session_state["api_key_option"] = "Select an API key:"
    st.session_state["spreadsheet_url"] = ""
    st.session_state["sheet_name"] = ""
    st.session_state["first_name_column_name"] = ""
    st.session_state["last_name_column_name"] = ""
    st.session_state["name_column_name"] = ""
    st.session_state["email_column_name"] = ""
    st.session_state["organization_name_column_name"] = ""
    st.session_state["domain_column_name"] = ""
if "previous_option" not in st.session_state:
    st.session_state["previous_option"] = "Select one Apollo enrichment script"
apollo_enrichment_option = st.selectbox(
    "Select one Apollo enrichment script",
    ("Select one Apollo enrichment script",
     "Contact enrichment",
     "Company enrichment",
     )
)
if apollo_enrichment_option != st.session_state["previous_option"]:
    reset_inputs()
st.session_state["previous_option"] = apollo_enrichment_option
if apollo_enrichment_option != "Select one Apollo enrichment script":
    st.write("Before executing any script, please ensure that you share your spreadsheet with this account, granting editor permissions: kalungi-google-colab@invertible-now-393117.iam.gserviceaccount.com")
    st.write("You can enrich 900 contacts/companies per hour using each API key.")
if apollo_enrichment_option == "Contact enrichment":
    api_key_option = st.selectbox("Select an API key:", [
        'API key 1', 
        'API key 2', 
        'API key 3',
        'API key 4',
    ])
    if api_key_option == 'API key 1':
        api_key = st.secrets["APOLLO_API_KEY_OCC"]["value"]
    elif api_key_option == 'API key 2':
        api_key = st.secrets["APOLLO_API_KEY_A360"]["value"]
    elif api_key_option == 'API key 3':
        api_key = st.secrets["APOLLO_API_KEY_N"]["value"]
    elif api_key_option == 'API key 4':
        api_key = st.secrets["APOLLO_API_KEY_S"]["value"]
    spreadsheet_url = st.text_input("Spreadsheet URL", key="spreadsheet_url")
    sheet_name = st.text_input("Sheet name", key="sheet_name")
    st.write("All the following columns are optional, leave them empty if you do not have them.")
    first_name_column_name = st.text_input("First name column name (The person's first name)", key="first_name_column_name")
    last_name_column_name = st.text_input("Last name column name (The person's last name)", key="last_name_column_name")
    name_column_name = st.text_input("Full name column name (The person's full name)", key="name_column_name")
    email_column_name = st.text_input("Email column name (The person's email)", key="email_column_name")
    organization_name_column_name = st.text_input("Company name column name (The person's company name)", key="organization_name_column_name")
    domain_column_name = st.text_input("Domain column name (The person's company domain)", key="domain_column_name")
if apollo_enrichment_option == "Company enrichment":
    api_key_option = st.selectbox("Select an API key:", [
        'API key 1', 
        'API key 2', 
        'API key 3',
        'API key 4',
    ])
    if api_key_option == 'API key 1':
        api_key = st.secrets["APOLLO_API_KEY_OCC"]["value"]
    elif api_key_option == 'API key 2':
        api_key = st.secrets["APOLLO_API_KEY_A360"]["value"]
    elif api_key_option == 'API key 3':
        api_key = st.secrets["APOLLO_API_KEY_N"]["value"]
    elif api_key_option == 'API key 4':
        api_key = st.secrets["APOLLO_API_KEY_S"]["value"]
    domain_column_name = st.text_input("Domain column name (The person's company domain)", key="domain_column_name")

if apollo_enrichment_option != "Select one Apollo enrichment script":
    if st.button("Start enrichment"):
        if not api_key:
            st.error("Please select an API key.")
        if not spreadsheet_url:
            st.error("Please fill spreadsheet URL.")
        if spreadsheet_url:
            try:
                dataframe_input = retrieve_spreadsheet(spreadsheet_url, sheet_name, key_dict)
                if dataframe_input is not None and not dataframe_input.empty:
                    if apollo_enrichment_option == "Contact enrichment":
                        dataframe_result = apollo_contact_enrichment(api_key, dataframe_input, first_name_column_name, last_name_column_name, name_column_name, email_column_name, organization_name_column_name, domain_column_name, streamlit_execution)
                        write_into_spreadsheet(spreadsheet_url, sheet_name, dataframe_result, key_dict)
                    if apollo_enrichment_option == "Company enrichment":
                        dataframe_result = apollo_company_enrichment(api_key, dataframe_input, domain_column_name, streamlit_execution=False)
                        st.write("Still not implemented.")
                    st.success("Enrichment completed!")
            except Exception as e:
                st.error(f"An error occurred: {e}")
                st.exception(e)