import streamlit as st
import subprocess
import sys
import os
from playwright.sync_api import sync_playwright
import subprocess
import sys

def install_playwright_deps():
    try:
        result = subprocess.run(["sudo", "playwright", "install-deps"], capture_output=True, text=True, check=True)
        st.success("Playwright dependencies installed successfully!")
        st.code(result.stdout, language="bash")
    except subprocess.CalledProcessError as e:
        st.error(f"Failed to install Playwright dependencies. Error: {e}")
        st.code(e.output, language="bash")

# Call this function in your init_app() function
install_playwright_deps()

def install_system_deps():
    try:
        cmd = [
            "sudo", "apt-get", "install", "-y",
            "libnss3", "libnspr4", "libatk1.0-0", "libatk-bridge2.0-0",
            "libcups2", "libdrm2", "libxkbcommon0", "libatspi2.0-0",
            "libxcomposite1", "libxdamage1", "libxfixes3", "libxrandr2",
            "libgbm1", "libpango-1.0-0", "libcairo2", "libasound2"
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        st.success("System dependencies installed successfully!")
        st.code(result.stdout, language="bash")
    except subprocess.CalledProcessError as e:
        st.error(f"Failed to install system dependencies. Error: {e}")
        st.code(e.output, language="bash")

# Call this function in your init_app() function if install_playwright_deps() fails
install_system_deps()

def install_playwright_browsers():
    try:
        result = subprocess.run([sys.executable, "-m", "playwright", "install", "chromium"], capture_output=True, text=True, check=True)
        st.success("Playwright browsers installed successfully!")
        st.code(result.stdout, language="bash")
    except subprocess.CalledProcessError as e:
        st.error(f"Failed to install Playwright browsers. Error: {e}")
        st.code(e.output, language="bash")
        try:
            st.warning("Attempting to install with sudo...")
            result = subprocess.run(["sudo", sys.executable, "-m", "playwright", "install", "chromium"], capture_output=True, text=True, check=True)
            st.success("Playwright browsers installed successfully with sudo!")
            st.code(result.stdout, language="bash")
        except subprocess.CalledProcessError as e:
            st.error(f"Failed to install Playwright browsers with sudo. Error: {e}")
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
        install_playwright_deps()
        install_system_deps()
        check_playwright_installation()
    
    # ... rest of your init_app function ...
    
    st.write("Environment Variables:")
    st.json(dict(os.environ))
    st.write("Python Executable:", sys.executable)
    st.write("Python Version:", sys.version)
    st.write("Current Working Directory:", os.getcwd())
    st.write("Contents of current directory:")
    st.code("\n".join(os.listdir()))

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