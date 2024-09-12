import http.client
import streamlit as st

openai_api_key = st.secrets["OPENAI_API_KEY"]["value"]
zenrowsApiKey = st.secrets["ZENROWS_API_KEY"]["value"]
serper_api= '81ead61f8203d7445b4c38d383d58422eb6963ae'
key_dict = dict(st.secrets["GOOGLE_CLOUD_CREDENTIALS"])
key_dict["private_key"] = key_dict["private_key"].replace("\\n", "\n")

def buscar_enlaces_organicos(keywords, row):
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
    headers = {'User-Agent': 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)'}
    try:
        response = client.get(url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            for script_or_style in soup(['script', 'style']):
                script_or_style.extract()
            text = soup.get_text()
            lines = (line.strip() for line in text.splitlines())
            chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
            return '\n'.join(chunk for chunk in chunks if chunk)
        else:
            return f"Error {response.status_code}"
    except requests.exceptions.RequestException as e:
        return f"Error de solicitud: {e}"

def process_url_data(urls):
    combined_text = ''
    for url in urls:
        text = get_text_from_url(url)
        if not text.startswith("Error"):
            combined_text += text + " "
    return combined_text

def get_text_chunks(text):
    text_splitter = CharacterTextSplitter(separator="\n", chunk_size=1000, chunk_overlap=200, length_function=len)
    return text_splitter.split_text(text)

def get_vectors(text_chunks, openai_api_key):
    embeddings = OpenAIEmbeddings(openai_api_key=openai_api_key)
    return FAISS.from_texts(texts=text_chunks, embedding=embeddings)

def get_response_from_chain(vectorstore, search_question, llm_question):
    docs = vectorstore.similarity_search(search_question)
    llm = ChatOpenAI(temperature=0.9, max_tokens= 4000, model="gpt-4o-mini")
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

import json
import requests
import gspread
import pandas as pd
import re

from gspread_dataframe import set_with_dataframe
from bs4 import BeautifulSoup
from langchain.text_splitter import CharacterTextSplitter
from langchain.embeddings import OpenAIEmbeddings
from langchain.vectorstores import FAISS
from langchain.chains.question_answering import load_qa_chain
from langchain.callbacks import get_openai_callback
from langchain.chat_models import ChatOpenAI
from zenrows import ZenRowsClient

import streamlit as st
import pandas as pd
from google.oauth2.service_account import Credentials

def process_data(spreadsheet_url, sheet_name, column_name, formatted_keywords, prompt, serper_API, progress_bar, openai_api_key):
    error_message = "Error 422"
    cost_per_prompt_token = 0.000015 / 1000
    cost_per_completion_token = 0.0006 / 1000
    totalcost = 0

    # Authentication for Google Sheets API
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credentials = Credentials.from_service_account_info(key_dict, scopes=scope)
    client = gspread.authorize(credentials)
    spreadsheet = client.open_by_url(spreadsheet_url)
    worksheet = spreadsheet.worksheet(sheet_name)
    
    # Extract data from the worksheet
    data = worksheet.get_all_values()
    dataframe = pd.DataFrame(data[1:], columns=data[0])
    num_rows = dataframe.shape[0]
    
    # Iterate over rows and process each
    for index, row in dataframe.iterrows():
        try:
            domain = row[column_name]
            # Fetch links and extract text
            links_obtenidos = buscar_enlaces_organicos(formatted_keywords, domain)
            text = process_url_data(links_obtenidos)
            
            if text != error_message:  # No error
                text_chunks = get_text_chunks(text)
                if text_chunks:
                    vectorstore = get_vectors(text_chunks, openai_api_key)
                    
                    # Define search and LLM questions
                    search_question = "Chemical, Shipping, delivery"
                    llm_question = prompt
                    
                    with get_openai_callback() as cb:
                        response = get_response_from_chain(vectorstore, search_question, llm_question)
                        error = check_for_error(response)
                        
                        # Update DataFrame with result
                        dataframe.at[index, 'Error'] = error
                        dataframe.at[index, 'result'] = response
                        
                        # Calculate costs
                        prompt_cost = cb.prompt_tokens * cost_per_prompt_token
                        output_cost = cb.completion_tokens * cost_per_completion_token
                        total_cost = prompt_cost + output_cost
                        totalcost += total_cost
                else:
                    dataframe.at[index, 'result'] = ""
            else:
                dataframe.at[index, 'result'] = error_message
        except Exception as e:
            error_message = str(e)
            dataframe.at[index, 'QA'] = error_message
            st.error(f"Error processing row {index}: {error_message}")
        
        # Update progress bar
        progress_bar.progress((index + 1) / num_rows)
    
    # Split and organize results
    df_final = dataframe
    df_final['QA'], df_final['Reason'] = zip(*df_final['result'].apply(split_text))
    df_final = df_final[[column_name, 'QA', 'Reason', 'result']]
    
    # Update Google Sheet with results
    worksheet.clear()
    set_with_dataframe(worksheet, df_final, include_index=False, resize=True, allow_formulas=True)
    
    return df_final, totalcost
