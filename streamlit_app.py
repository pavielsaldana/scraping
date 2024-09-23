import os
import hmac
import streamlit as st
import time

# Page configuration
st.set_page_config(
    page_title="ABM App", 
    page_icon="https://media.licdn.com/dms/image/v2/C4E0BAQEUNQJN0rf-yQ/company-logo_200_200/company-logo_200_200/0/1630648936722/kalungi_inc_logo?e=2147483647&v=beta&t=4vrP50CSK9jEFI7xtF7DzTlSMZdjmq__F0eG8IJwfN8"
)

# Check password function
def check_password():
    def password_entered():
        if hmac.compare_digest(st.session_state["password"], st.secrets["APP_PASSWORD"]["value"]):
            st.session_state["password_correct"] = True
            del st.session_state["password"]
        else:
            st.session_state["password_correct"] = False
    
    if st.session_state.get("password_correct", False):
        return True

    st.text_input("Password", type="password", on_change=password_entered, key="password")
    
    if "password_correct" in st.session_state:
        st.error("Password incorrect")
    
    return False

# If password is correct, run the app
if check_password():
    
    # Session state initialization for keep-alive
    if 'keep_alive' not in st.session_state:
        st.session_state.keep_alive = True
    
    # Keep the session alive using rerun without threading
    if st.session_state.keep_alive:
        time.sleep(10)
        st.experimental_rerun()
    
    # Navigation menu logic
    st.sidebar.title("Navigation")
    page = st.sidebar.selectbox("Select a page", ["Welcome", "LinkedIn scripts", "Enrichment scripts", "Data cleaning"])
    
    if page == "Welcome":
        st.write("Welcome to the ABM App!")
        st.write("This is the welcome page content.")
    
    elif page == "LinkedIn scripts":
        linkedin_page = st.sidebar.selectbox("LinkedIn scripts", ["Scraping", "Search", "Outreach"])
        if linkedin_page == "Scraping":
            st.write("LinkedIn scraping page")
        elif linkedin_page == "Search":
            st.write("LinkedIn search page")
        elif linkedin_page == "Outreach":
            st.write("LinkedIn outreach page")
    
    elif page == "Enrichment scripts":
        enrichment_page = st.sidebar.selectbox("Enrichment scripts", ["AI QA", "Owler revenue", "Company LinkedIn URL search using Serper", "Apollo enrichment"])
        if enrichment_page == "AI QA":
            st.write("AI QA page")
        elif enrichment_page == "Owler revenue":
            st.write("Owler revenue page")
        elif enrichment_page == "Company LinkedIn URL search using Serper":
            st.write("Company LinkedIn URL search using Serper page")
        elif enrichment_page == "Apollo enrichment":
            st.write("Apollo enrichment page")
    
    elif page == "Data cleaning":
        st.write("AI title cleaning page")
    
    # Display the Kalungi logo and link to the documentation
    st.sidebar.image("https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcSYXyGNY0mQSyCRUKXrXWI4-O31kspcM0eVLg&s", use_column_width=True)
    st.sidebar.markdown("[Kalungi ABM App V1.0 Documentation](https://docs.google.com/document/d/1armsOtBlHntK4YUWpPH3tTLYlo53ZkzyY-yDW_Nu1x8/edit)")

else:
    st.stop()
