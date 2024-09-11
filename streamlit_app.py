import os
import streamlit as st

if not os.path.exists("/home/appuser/.cache/ms-playwright"):
    os.system("playwright install")

st.set_page_config(page_title="ABM App", page_icon="https://media.licdn.com/dms/image/v2/C4E0BAQEUNQJN0rf-yQ/company-logo_200_200/company-logo_200_200/0/1630648936722/kalungi_inc_logo?e=2147483647&v=beta&t=4vrP50CSK9jEFI7xtF7DzTlSMZdjmq__F0eG8IJwfN8")

welcome_page = st.Page("streamlit_scripts/streamlit_welcome.py",
                       title="Welcome",)
linkedin_scraping_page = st.Page("streamlit_scripts/streamlit_linkedin_scraping.py",
                                title="LinkedIn scraping",)
linkedin_search_page = st.Page("streamlit_scripts/streamlit_linkedin_search.py",
                               title = "LinkedIn search",)
linkedin_outreach_page = st.Page("streamlit_scripts/streamlit_linkedin_outreach.py",
                                 title = "LinkedIn outreach",)
ai_qa_page = st.Page("streamlit_scripts/streamlit_ai_qa.py",
                                 title = "AI QA",)
pg = st.navigation(
    {
        "Welcome": [welcome_page,],
        "LinkedIn scripts": [linkedin_scraping_page, linkedin_search_page, linkedin_outreach_page],
        "Enrichment scripts": [ai_qa_page],
    }
)

st.logo("https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcSYXyGNY0mQSyCRUKXrXWI4-O31kspcM0eVLg&s")
st.sidebar.markdown("Kalungi ABM App [V1.0](https://docs.google.com/document/d/1armsOtBlHntK4YUWpPH3tTLYlo53ZkzyY-yDW_Nu1x8/edit)")

pg.run()