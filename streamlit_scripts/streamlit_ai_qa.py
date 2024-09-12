import streamlit as st
import os
import openai
import sys
sys.path.append(os.path.abspath('../enrichment_scripts'))
from enrichment_scripts.ai_qa import *

key_dict = dict(st.secrets["GOOGLE_CLOUD_CREDENTIALS"])
key_dict["private_key"] = key_dict["private_key"].replace("\\n", "\n")

OPENAI_API_KEY = st.secrets["OPENAI_API_KEY"]["value"]
openai.api_key = OPENAI_API_KEY
zenrowsApiKey = st.secrets["ZENROWS_API_KEY"]["value"]

st.title("QA with Searching Keyword")

spreadsheet_url = st.text_input("URL de Google Sheets", "https://docs.google.com/spreadsheets/d/1WdRriLXggLZlz1dIoyiGMEdu13YVWibJLp7u5-Z6Gjo/edit?gid=352666901#gid=352666901")
sheet_name = st.text_input("Nombre de la hoja", "Test")
column_name = st.text_input("Nombre de la columna", "domain")
serper_API = st.text_input("Seleccione un API de Serper", "81ead61f8203d7445b4c38d383d58422eb6963ae")

keywords = st.text_area("Introduce las keywords separadas por comas", "Delivery, Shipping, last mile, White Glove, final mile")
keywords_list = [keyword.strip() for keyword in keywords.split(',')]
keywords_final = ['"' + keyword + '"' for keyword in keywords_list]
formatted_keywords = " | ".join(keywords_final)
st.write("Keywords formateadas:", formatted_keywords)

prompt = st.text_area("Introduce el prompt", "Assess if the company is a manufacturer or provides any delivery or shipping of Chemical products or derivatives by searching for terms or phrases indicating this kind of services  including but not limited to 'Chemical Distributors', 'Chemical Manuufacturers', 'Shipping', 'Delivery'. Respond in the following manner: Yes. Provide a brief explanation (no more than 300 characters) on why it qualifies. No. Provide a brief explanation (no more than 300 characters) on why it does not qualify. Maybe. If the information is ambiguous or insufficient, briefly explain (no more than 300 characters) why it's not possible to determine.")

if st.button("Iniciar procesamiento"):
    if not spreadsheet_url or not serper_API:
        st.error("Please enter both the Spreadsheet URL and the Serper API key")
    else:
        with st.spinner("Running the scraper. This could take a few minutes depending on the list size..."):
            try:
                progress_bar = st.progress(0)
                result, totalcost = process_data(spreadsheet_url, sheet_name, column_name, formatted_keywords, prompt, serper_API, progress_bar)
                st.success("Scraping completed!")
                st.dataframe(result)
                st.write(f"El costo total fue: ${totalcost:.6f}")
            except Exception as e:
                st.error(f"An error occurred: {str(e)}")
                st.exception(e)
