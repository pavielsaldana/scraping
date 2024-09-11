'''
import streamlit as st

welcome_page = st.Page("streamlit_scripts/streamlit_welcome.py",
                       title="Welcome",)
linkedin_scraping_page = st.Page("streamlit_scripts/streamlit_linkedin_scraping.py",
                                title="LinkedIn scraping",)
linkedin_search_page = st.Page("streamlit_scripts/streamlit_linkedin_search.py",
                               title = "LinkedIn search",)
linkedin_outreach_page = st.Page("streamlit_scripts/streamlit_linkedin_outreach.py",
                                 title = "LinkedIn outreach",)
pg = st.navigation(
    {
        "Welcome": [welcome_page,],
        "LinkedIn scripts": [linkedin_scraping_page, linkedin_search_page, linkedin_outreach_page],
    }
)
pg.run()
'''
import streamlit as st
import subprocess
import os
from playwright.sync_api import sync_playwright

# Function to check if Chromium is installed
def check_chromium_installed():
    try:
        with sync_playwright() as p:
            chromium_path = p.chromium.executable_path
            if not os.path.exists(chromium_path):
                return False
            return True
    except Exception:
        return False

# Ensure that Playwright is installed
if not check_chromium_installed():
    st.info("Installing Playwright browsers, please wait...")
    subprocess.run(["playwright", "install", "chromium"], check=True)
    st.success("Playwright browsers installed successfully!")

# Now proceed with the rest of the Streamlit app
welcome_page = st.Page("streamlit_scripts/streamlit_welcome.py", title="Welcome")
linkedin_scraping_page = st.Page("streamlit_scripts/streamlit_linkedin_scraping.py", title="LinkedIn scraping")
linkedin_search_page = st.Page("streamlit_scripts/streamlit_linkedin_search.py", title="LinkedIn search")
linkedin_outreach_page = st.Page("streamlit_scripts/streamlit_linkedin_outreach.py", title="LinkedIn outreach")

pg = st.navigation(
    {
        "Welcome": [welcome_page],
        "LinkedIn scripts": [linkedin_scraping_page, linkedin_search_page, linkedin_outreach_page],
    }
)
pg.run()
