import os
import streamlit as st

if not os.path.exists("/home/appuser/.cache/ms-playwright"):
    os.system("playwright install")

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