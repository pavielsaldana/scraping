import http.client
import streamlit as st
import json
import requests
import os
import google.auth
import gspread
import pandas as pd
import re
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe
from bs4 import BeautifulSoup
from langchain.text_splitter import CharacterTextSplitter
from langchain.embeddings import OpenAIEmbeddings
from langchain.vectorstores import FAISS
from langchain.chains import ConversationalRetrievalChain
from langchain.chains.question_answering import load_qa_chain
from langchain.llms import OpenAI
from langchain.callbacks import get_openai_callback
from openlimit.utilities import num_tokens_consumed_by_embedding_request
from langchain.chat_models import ChatOpenAI
from zenrows import ZenRowsClient

# Error message to identify faulty rows
error_message = "Error 422"

# API keys setup
openai_api_key = st.secrets["OPENAI_API_KEY"]["value"]
zenrowsApiKey = st.secrets["ZENROWS_API_KEY"]["value"]
key_dict = dict(st.secrets["GOOGLE_CLOUD_CREDENTIALS"])
key_dict["private_key"] = key_dict["private_key"].replace("\\n", "\n")
serper_api = st.text_input("Seleccione un API de Serper", "689a38f1e3cd679dbce702437c376783b5a24c85")

# Function to fetch organic links using Serper API
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

    # Convert the response to a Python dictionary
    data_dict = json.loads(data.decode("utf-8"))

    # Extract organic results
    organic_results = data_dict.get('organic', [])
    links = [result['link'].replace("https://", "http://") for result in organic_results if 'link' in result and not result['link'].lower().endswith('.pdf')]

    # Add the domain with HTTP
    links.append(f'http://{row}')

    return links[:3]

# Function to get text from URLs using ZenRowsClient
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

# Function to process data from a list of URLs
def process_url_data(urls):
    combined_text = ''
    for url in urls:
        text = get_text_from_url(url)
        if not text.startswith("Error"):
            combined_text += text + " "
    return combined_text

# Function to split text into manageable chunks
def get_text_chunks(text):
    text_splitter = CharacterTextSplitter(separator="\n", chunk_size=1000, chunk_overlap=200, length_function=len)
    return text_splitter.split_text(text)

# Function to create vectors from text chunks using FAISS
def get_vectors(text_chunks):
    embeddings = OpenAIEmbeddings(openai_api_key=openai_api_key)
    return FAISS.from_texts(texts=text_chunks, embedding=embeddings)

# Function to query FAISS for responses
def get_response_from_chain(vectorstore, search_question, llm_question):
    docs = vectorstore.similarity_search(search_question)
    llm = ChatOpenAI(temperature=0.9, max_tokens=4000, model="gpt-4o-mini")
    chain = load_qa_chain(llm, chain_type="stuff")
    return chain.run(input_documents=docs, question=llm_question)

# Function to split text into QA and reason
def split_text(text):
    if text == error_message:
        return error_message, None
    if pd.isna(text) or not text:
        return None, None
    parts = re.split(r'\. |, ', text, 1)
    QA = parts[0].split()[0] if len(parts) > 0 else None
    reason = parts[1] if len(parts) > 1 else None
    return QA, reason

# Main function to process the data from the Google Sheet and populate results
def process_data(spreadsheet_url, sheet_name, column_name, formatted_keywords, prompt, serper_API, progress_bar):
    cost_per_prompt_token = 0.000015 / 1000
    cost_per_completion_token = 0.0006 / 1000
    total_cost = 0
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
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
            links_obtenidos = buscar_enlaces_organicos(formatted_keywords, domain)
            text = process_url_data(links_obtenidos)
            if text != error_message:
                text_chunks = get_text_chunks(text)

                if text_chunks:
                    vectorstore = get_vectors(text_chunks)
                    search_question = "Chemical, Shipping, delivery"
                    llm_question = prompt
                    with get_openai_callback() as cb:
                        response = get_response_from_chain(vectorstore, search_question, llm_question)
                        dataframe.at[index, 'result'] = response
                        prompt_cost = cb.prompt_tokens * cost_per_prompt_token
                        output_cost = cb.completion_tokens * cost_per_completion_token
                        total_cost += (prompt_cost + output_cost)
                else:
                    dataframe.at[index, 'result'] = ""
            else:
                dataframe.at[index, 'result'] = error_message
        except Exception as e:
            dataframe.at[index, 'QA'] = str(e)

        progress_bar.progress((index + 1) / num_rows)

    df_final = dataframe
    df_final['QA'], df_final['Reason'] = zip(*df_final['result'].apply(split_text))
    df_final = df_final[[column_name, 'QA', 'Reason', 'result']]

    worksheet.clear()
    set_with_dataframe(worksheet, df_final, include_index=False, resize=True, allow_formulas=True)

    return df_final, total_cost

