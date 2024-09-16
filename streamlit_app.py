import os
import hmac
import streamlit as st
import importlib
import sys
import traceback

st.set_page_config(page_title="ABM App", page_icon="https://media.licdn.com/dms/image/v2/C4E0BAQEUNQJN0rf-yQ/company-logo_200_200/company-logo_200_200/0/1630648936722/kalungi_inc_logo?e=2147483647&v=beta&t=4vrP50CSK9jEFI7xtF7DzTlSMZdjmq__F0eG8IJwfN8")

def check_password():
    """Returns `True` if the user had the correct password."""

    def password_entered():
        """Checks whether a password entered by the user is correct."""
        if hmac.compare_digest(st.session_state["password"], st.secrets["APP_PASSWORD"]["value"]):
            st.session_state["password_correct"] = True
            del st.session_state["password"]  # Don't store the password.
        else:
            st.session_state["password_correct"] = False

    # Return True if the password is validated.
    if st.session_state.get("password_correct", False):
        return True

    # Show input for password.
    with st.form("password_form"):
        st.text_input("Password", type="password", key="password")
        submit_button = st.form_submit_button("Submit")
        
    if submit_button:
        password_entered()
        if st.session_state["password_correct"]:
            st.rerun()  # Rerun the script to remove the password form
        else:
            st.error("😕 Password incorrect")
    
    return False

def load_module(module_path):
    try:
        module = importlib.import_module(module_path)
        return module
    except Exception as e:
        st.error(f"Error importing module {module_path}: {str(e)}")
        st.code(traceback.format_exc())
        return None

def execute_module(module):
    try:
        exec(module.__file__, module.__dict__)
    except Exception as e:
        st.error(f"Error executing module {module.__name__}: {str(e)}")
        st.code(traceback.format_exc())

if check_password():
    # Your existing app code goes here
    if not os.path.exists("/home/appuser/.cache/ms-playwright"):
        os.system("playwright install")
    
    st.sidebar.title("Navigation")
    page = st.sidebar.radio("Go to", ["Welcome", "LinkedIn Scraping", "LinkedIn Search", "LinkedIn Outreach", "AI QA", "Owler Revenue", "AI Title Cleaning"])

    module_path = {
        "Welcome": "streamlit_scripts.streamlit_welcome",
        "LinkedIn Scraping": "streamlit_scripts.linkedin_scripts.streamlit_linkedin_scraping",
        "LinkedIn Search": "streamlit_scripts.linkedin_scripts.streamlit_linkedin_search",
        "LinkedIn Outreach": "streamlit_scripts.linkedin_scripts.streamlit_linkedin_outreach",
        "AI QA": "streamlit_scripts.enrichment_scripts.streamlit_ai_qa",
        "Owler Revenue": "streamlit_scripts.enrichment_scripts.streamlit_owler_revenue_scraping",
        "AI Title Cleaning": "streamlit_scripts.data_cleaning_scripts.streamlit_ai_title_cleaning"
    }

    module = load_module(module_path[page])
    if module:
        execute_module(module)
    
    st.sidebar.image("https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcSYXyGNY0mQSyCRUKXrXWI4-O31kspcM0eVLg&s", use_column_width=True)
    st.sidebar.markdown("Kalungi ABM App [V1.0](https://docs.google.com/document/d/1armsOtBlHntK4YUWpPH3tTLYlo53ZkzyY-yDW_Nu1x8/edit)")
else:
    st.stop()  # Don't run the rest of the app if password is incorrect.