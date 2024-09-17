import http.client
import streamlit as st
import json
import requests
import gspread
import pandas as pd
import re

from gspread_dataframe import set_with_dataframe
from bs4 import BeautifulSoup
from langchain.text_splitter import CharacterTextSplitter
from langchain.embeddings import OpenAIEmbeddings
from langchain_community.vectorstores import FAISS
from langchain.chains.question_answering import load_qa_chain
from langchain.callbacks import get_openai_callback
from langchain.chat_models import ChatOpenAI
from zenrows import ZenRowsClient
from google.oauth2.service_account import Credentials

OPENAI_API_KEY = st.secrets["OPENAI_API_KEY"]["value"]
zenrowsApiKey = st.secrets["ZENROWS_API_KEY"]["value"]
key_dict = dict(st.secrets["GOOGLE_CLOUD_CREDENTIALS"])
key_dict["private_key"] = key_dict["private_key"].replace("\\n", "\n")

#faiss-cpu
#langchain==0.0.349
#openai==0.28.1
#tiktoken==0.7.0


def check_for_keywords(text, keywords):
    regex_pattern = '|'.join([rf'\b{k}\b' for k in keywords])
    if pd.notna(text) and re.search(regex_pattern, text, re.IGNORECASE):
        return True
    else:
        return False

def process_vertical_input(input_text):
    vertical_dict = {}
    lines = input_text.split('\n')
    for line in lines:
        if ':' in line:
            vertical, keywords = line.split(':', 1)
            keywords = [kw.strip().strip('"') for kw in keywords.split(',')]
            vertical_dict[vertical.strip()] = keywords
    return vertical_dict


def buscar_enlaces_organicos(keywords, row, serper_api):
    conn = http.client.HTTPSConnection("google.serper.dev")
    payload = json.dumps({
      'q': f' {keywords} site:{row}',
      "num": 8
    })
    headers = {
      'X-API-KEY': serper_api,
      'Content-Type': 'application/json'
    }
    conn.request("POST", "/search", payload, headers)
    res = conn.getresponse()
    data = res.read()
    data_dict = json.loads(data.decode("utf-8"))
    organic_results = data_dict.get('organic', [])
    links = [result['link'].replace("https://", "http://") for result in organic_results if 'link' in result and not result['link'].lower().endswith('.pdf')]
    links.append(f'http://{row}')
    return links[:3]

def get_text_from_url(url):
    client = ZenRowsClient(zenrowsApiKey)
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        for script_or_style in soup(['script', 'style']):
            script_or_style.extract()
        text = soup.get_text()
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        return '\n'.join(chunk for chunk in chunks if chunk)
    except requests.RequestException as e:
        pass
    try:
        response = client.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        for script_or_style in soup(['script', 'style']):
            script_or_style.extract()
        text = soup.get_text()
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        return '\n'.join(chunk for chunk in chunks if chunk)
    except requests.RequestException as e:
        pass
    try:
        params = {"js_render": "true"}
        response = client.get(url, params=params)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        for script_or_style in soup(['script', 'style']):
            script_or_style.extract()
        text = soup.get_text()
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        return '\n'.join(chunk for chunk in chunks if chunk)
    except requests.exceptions.RequestException as e:
        return ""

def process_url_data(urls):
    combined_text = ""
    for url in urls:
        text = get_text_from_url(url)
        if not text == "":
            combined_text += text + " "
    if combined_text == "":
        return "Error 422"
    return combined_text

def get_text_chunks(text):
    text_splitter = CharacterTextSplitter(separator="\n", chunk_size=1000, chunk_overlap=200, length_function=len)
    return text_splitter.split_text(text)

def get_vectors(text_chunks, OPENAI_API_KEY):
    embeddings = OpenAIEmbeddings(openai_api_key=OPENAI_API_KEY)
    return FAISS.from_texts(texts=text_chunks, embedding=embeddings)

def get_response_from_chain(vectorstore, search_question, llm_question, OPENAI_API_KEY):
    docs = vectorstore.similarity_search(search_question)
    llm = ChatOpenAI(api_key=OPENAI_API_KEY, temperature=0.9, max_tokens= 4000, model="gpt-4o-mini")
    chain = load_qa_chain(llm, chain_type="stuff")
    return chain.run(input_documents=docs, question=llm_question)

def split_text(text):
    global error_message
    if text == error_message:
        return error_message, None
    if pd.isna(text) or not text:
        return None, None
    parts = re.split(r'\. |, ', text, 1)
    QA = parts[0].split()[0] if len(parts) > 0 else None
    reason = parts[1] if len(parts) > 1 else None
    return QA, reason

def format_keywords(input_string):
    keywords_list = [keyword.strip() for keyword in input_string.split(",")]
    keywords_final = ['"' + keyword + '"' for keyword in keywords_list]
    return " | ".join(keywords_final)

#KEYWORDS FUNCTIONS
def check_for_error(response):
    error_keywords = [r'\berror\b',r'\btimeout\b',r'\b403\b']
    regex_pattern = '|'.join(error_keywords)
    if pd.notna(response) and re.search(regex_pattern, response, re.IGNORECASE):
        return True
    else:
        return False

error_message = "Error 422"

def process_data(spreadsheet_url, sheet_name, column_name, formatted_keywords, prompt, serper_api, progress_bar, key_dict, OPENAI_API_KEY, vertical_dict):
    cost_per_prompt_token = 0.000015 / 1000
    cost_per_completion_token = 0.0006 / 1000
    totalcost = 0
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    credentials = Credentials.from_service_account_info(key_dict, scopes=scope)
    client = gspread.authorize(credentials)
    spreadsheet = client.open_by_url(spreadsheet_url)
    worksheet = spreadsheet.worksheet(sheet_name)
    data = worksheet.get_all_values()
    dataframe = pd.DataFrame(data[1:], columns=data[0])
    num_rows = dataframe.shape[0]
        
    for index, row in dataframe.iterrows():
        try:
            domain = row[column_name]
            links_obtenidos = buscar_enlaces_organicos(formatted_keywords, domain, serper_api)
            text = process_url_data(links_obtenidos)
            if text != error_message:
                text_chunks = get_text_chunks(text)
                if text_chunks:
                    vectorstore = get_vectors(text_chunks, OPENAI_API_KEY)
                    search_question = "Chemical, Shipping, Delivery"
                    llm_question = prompt
                    with get_openai_callback() as cb:
                        response = get_response_from_chain(vectorstore, search_question, llm_question, OPENAI_API_KEY)
                        error = check_for_error(response)
                        dataframe.at[index, 'Error'] = error
                        prompt_cost = cb.prompt_tokens * cost_per_prompt_token
                        output_cost = cb.completion_tokens * cost_per_completion_token
                        total_cost = prompt_cost + output_cost
                        totalcost += total_cost
                        dataframe.at[index, 'result'] = response
                else:
                    dataframe.at[index, 'result'] = ""
            else:
                dataframe.at[index, 'result'] = error_message
                
            for vertical, keywords in vertical_dict.items():
                found = check_for_keywords(text, keywords)
                dataframe.at[index, vertical] = found
                
        except Exception as e:
            error_message = str(e)
            dataframe.at[index, 'QA'] = error_message
            print(f"Error processing row {index}: {error_message}")
        progress_bar.progress((index + 1) / num_rows)
    df_final = dataframe
    if 'Error' not in df_final.columns:
        df_final['Error'] = ''
    df_final['QA'], df_final['Reason'] = zip(*df_final['result'].apply(split_text))
    df_final = df_final[[column_name, 'QA', 'Reason', 'result', 'Error'] + list(vertical_dict.keys())]
    worksheet.clear()
    set_with_dataframe(worksheet, df_final, include_index=False, resize=True, allow_formulas=True)
    return df_final, totalcost
#STREAMLIT BEGIN
st.title("QA with Searching Keyword")

option = st.selectbox(
    "Select a Client ICP",
    ("Select Client ICP","New ICP Fit QA","Headlight Solutions (Chemical)")
)

if option == "New ICP Fit QA":
    keywords_input= "Keyword1_to_search, Keyword2_to_search, Keyword3_to_search..."
    prompt_input= "Assess if the company is a XXXXXX by searching for terms or phrases indicating this kind of services  including but not limited to XXXXXX. Respond in the following manner: Yes. Provide a brief explanation (no more than 300 characters) on why it qualifies. No. Provide a brief explanation (no more than 300 characters) on why it does not qualify. Maybe. If the information is ambiguous or insufficient, briefly explain (no more than 300 characters) why it's not possible to determine."
    verticals_input= "Vertical1"

elif option == "Headlight Solutions (Chemical)":
    keywords_input= "Delivery, Shipping, last mile, White Glove, final mile"
    prompt_input= "Assess if the company is a manufacturer or provides any delivery or shipping of Chemical products or derivatives by searching for terms or phrases indicating this kind of services  including but not limited to 'Chemical Distributors', 'Chemical Manuufacturers', 'Shipping', 'Delivery'. Respond in the following manner: Yes. Provide a brief explanation (no more than 300 characters) on why it qualifies. No. Provide a brief explanation (no more than 300 characters) on why it does not qualify. Maybe. If the information is ambiguous or insufficient, briefly explain (no more than 300 characters) why it's not possible to determine."
    verticals_input= "Shipping: \"Shipping\", \"Delivery\", \"Logistics\", \"Freight\"\n"
                               "Chemicals: \"Chemical\", \"Chemicals\", \"Hazardous Materials\""

if option != "Select Client ICP":   
    st.write("Use the IA QA tool when you have a list of domains that you need to do QA to check if the companies are fit with the ICP, you can also check if there are mention of certain keywords in the webpages.")
    st.write("[Tutorial >](https://www.loom.com/looms/videos)")
    
    spreadsheet_url = st.text_input("Select a Google Sheets URL", "https://docs.google.com/spreadsheets/d/1WdRriLXggLZlz1dIoyiGMEdu13YVWibJLp7u5-Z6Gjo/edit?gid=352666901#gid=352666901")
    sheet_name = st.text_input("Select the Sheet Name", "Test")
    column_name = st.text_input("Select the Column Name", "domain")
    serper_api = st.text_input("Select a Serper API", "091de71c94b24d78f85f38e527c370ae6c2f2f59")
    
    keywords = st.text_area("Enter keywords separated by commas", keywords_input)
    keywords_list = [keyword.strip() for keyword in keywords.split(',')]
    keywords_final = ['"' + keyword + '"' for keyword in keywords_list]
    formatted_keywords = " | ".join(keywords_final)
    st.write("Formatted Keywords:", formatted_keywords)
    
    prompt = st.text_area("Enter the prompt", prompt_input)
    
    verticals = st.text_area("Enter the verticals and their keywords", verticals_input)
    
    if verticals:
        vertical_dict = process_vertical_input(verticals)

    if st.button("Start processing"):
        if not spreadsheet_url or not serper_api:
            st.error("Please enter both the Spreadsheet URL and the Serper API key")
        else:
            with st.spinner("Running the scraper. This could take a few minutes depending on the list size..."):
                try:
                    progress_bar = st.progress(0)
                    result, totalcost = process_data(spreadsheet_url, sheet_name, column_name, formatted_keywords, prompt, serper_api, progress_bar, key_dict, OPENAI_API_KEY, vertical_dict)
                    st.success("Scraping completed!")
                    st.dataframe(result)
                    st.write(f"El costo total fue: ${totalcost:.6f}")
                except Exception as e:
                    st.error(f"An error occurred: {str(e)}")
                    st.exception(e)
