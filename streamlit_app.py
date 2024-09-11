import streamlit as st
import subprocess
import sys
import os
from playwright.sync_api import sync_playwright

def install_playwright_browsers():
    try:
        result = subprocess.run([sys.executable, "-m", "playwright", "install", "chromium"], capture_output=True, text=True, check=True)
        st.success("Playwright browsers installed successfully!")
        st.code(result.stdout, language="bash")
    except subprocess.CalledProcessError as e:
        st.error(f"Failed to install Playwright browsers. Error: {e}")
        st.code(e.output, language="bash")
        st.stop()

def check_playwright_installation():
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch()
            browser.close()
        st.success("Playwright browsers are correctly installed and working.")
    except Exception as e:
        st.warning(f"Playwright browsers are not installed or there was an error: {e}")
        install_playwright_browsers()

def init_app():
    with st.spinner("Initializing app and checking dependencies..."):
        check_playwright_installation()
    
    # Uncomment these lines if you want to display debug information
    # st.write("Environment Variables:")
    # st.json(dict(os.environ))
    # st.write("Python Executable:", sys.executable)
    # st.write("Python Version:", sys.version)
    # st.write("Current Working Directory:", os.getcwd())
    # st.write("Contents of current directory:")
    # st.code("\n".join(os.listdir()))

# Initialize the app
init_app()

# Define your pages
welcome_page = st.Page("streamlit_scripts/streamlit_welcome.py", title="Welcome")
linkedin_scraping_page = st.Page("streamlit_scripts/streamlit_linkedin_scraping.py", title="LinkedIn scraping")
linkedin_search_page = st.Page("streamlit_scripts/streamlit_linkedin_search.py", title="LinkedIn search")
linkedin_outreach_page = st.Page("streamlit_scripts/streamlit_linkedin_outreach.py", title="LinkedIn outreach")

# Set up navigation
pg = st.navigation(
    {
        "Welcome": [welcome_page],
        "LinkedIn scripts": [linkedin_scraping_page, linkedin_search_page, linkedin_outreach_page],
    }
)

# Run the app
pg.run()