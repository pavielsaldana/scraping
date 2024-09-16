import os
import hmac
import streamlit as st

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
            st.experimental_rerun()  # Rerun the script to remove the password form
        else:
            st.error("😕 Password incorrect")
    
    return False

if check_password():
    # Your existing app code goes here
    if not os.path.exists("/home/appuser/.cache/ms-playwright"):
        os.system("playwright install")
    
    welcome_page = st.Page("streamlit_scripts/streamlit_welcome.py",
                           title="Welcome",
                           icon=":material/account_circle:")
    # ... (rest of your pages)
    
    pg = st.navigation(
        {
            "Welcome": [welcome_page,],
            "LinkedIn scripts": [linkedin_scraping_page, linkedin_search_page, linkedin_outreach_page,],
            "Enrichment scripts": [ai_qa_page, owler_revenue_page,],
            "Data cleaning": [ai_title_cleaning_page,],
        }
    )
    
    st.logo("https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcSYXyGNY0mQSyCRUKXrXWI4-O31kspcM0eVLg&s")
    st.sidebar.markdown("Kalungi ABM App [V1.0](https://docs.google.com/document/d/1armsOtBlHntK4YUWpPH3tTLYlo53ZkzyY-yDW_Nu1x8/edit)")
    pg.run()
else:
    st.stop()  # Don't run the rest of the app if password is incorrect.