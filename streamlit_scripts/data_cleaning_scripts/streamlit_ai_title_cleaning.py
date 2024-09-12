import openai
import pandas as pd
import re
import itertools
import streamlit as st
from tqdm import tqdm
from openai.embeddings_utils import get_embedding
from openai.embeddings_utils import cosine_similarity
from datetime import datetime
import os
import sys
sys.path.append(os.path.abspath('../scripts/helper_scripts'))
from scripts.helper_scripts import *

#openai==0.28.1
#matplotlib

OPENAI_API_KEY= st.secrets["OPENAI_API_KEY"]["value"]
openai.api_key = OPENAI_API_KEY
key_dict = dict(st.secrets["GOOGLE_CLOUD_CREDENTIALS"])
key_dict["private_key"] = key_dict["private_key"].replace("\\n", "\n")

def tc(spreadsheetUrl,sheetName,columnName_Title,spreadsheetUrl_DB,key_dict): 

  sheetName_Seniority = "Seniority"
  sheetName_Standard = "DB"
  sheetName_Chief= "Chief"
  sheetName_Function = "Function"
  sheetName_Database= "Database"
  columvar_Variation= "Variation"
  columnvar_Seniority = "Seniority"
  columnvar_Function = "Function"
  columnstand_DB = "Standard Title"
  columnName_Repo= "Title"
  columnName_RepoStandard= "Standard Title"


  #OpenAI: ABM Request
  spreadsheetUrl_request = "https://docs.google.com/spreadsheets/d/1t0dtznfuZqlChc-baRA5zg2_hjWN_LMFPU3fvxW9f2w/edit#gid=0"
  sheetName_request = "Control"
  df_request = retrieve_spreadsheet(spreadsheetUrl_request, sheetName_request, key_dict)

  #Spreadsheet donde estan los Titulos no limpios
  df= retrieve_spreadsheet(spreadsheetUrl, sheetName, key_dict)
  df_titulos = retrieve_spreadsheet(spreadsheetUrl_DB, sheetName_Database, key_dict)
  df_Standard= retrieve_spreadsheet(spreadsheetUrl_DB, sheetName_Standard, key_dict)
  df_Chief= retrieve_spreadsheet(spreadsheetUrl_DB, sheetName_Chief, key_dict)
  df_Seniority = retrieve_spreadsheet(spreadsheetUrl_DB, sheetName_Seniority, key_dict)
  df_Function = retrieve_spreadsheet(spreadsheetUrl_DB, sheetName_Function, key_dict)

  conocimiento_df = df_Standard
  conocimiento_df['Embedding'] = conocimiento_df[columnstand_DB].apply(lambda x: get_embedding(x, engine='text-embedding-ada-002'))

  def buscar(busqueda, datos, n_resultados=1):
      busqueda_embed = get_embedding(busqueda, engine="text-embedding-ada-002")
      datos["Similitud"] = datos['Embedding'].apply(lambda x: cosine_similarity(x, busqueda_embed))
      datos = datos.sort_values("Similitud", ascending=False)
      return datos.iloc[:n_resultados][[columnstand_DB, "Similitud"]]

  def Chief(title, df_Chief):
      for index, row in df_Chief.iterrows():
          variation = row[columvar_Variation]
          if re.search(r'\b' + re.escape(variation) + r'\b', title, re.IGNORECASE):
              return row['concat_title']  # Asume que esta es la columna que contiene el resultado deseado
      return None

  def Seniority(title, df_Seniority):
      matches = []
      for index, row in df_Seniority.iterrows():
          variation = row[columvar_Variation]
          if re.search(r'\b' + re.escape(variation) + r'\b', title, re.IGNORECASE):
              matches.append(row[columnvar_Seniority])
      return matches

  def Function(title, df_Function):
      matches = []
      for index, row in df_Function.iterrows():
          variation = row[columvar_Variation]
          if re.search(r'\b' + re.escape(variation) + r'\b', title, re.IGNORECASE):
              matches.append(row[columnvar_Function])
      return matches

  def generate_combinations(seniorities, functions):
      return [f"{seniority}{function}" for seniority, function in itertools.product(seniorities, functions)]

  def Keywords(text):
      keywords = [r'\bretired',r'\bassistant',r'\bconsultant',r'\bAdvisor to',r'\bProject Owner']
      regex_pattern = '|'.join(keywords)
      if pd.notna(text) and re.search(regex_pattern, text, re.IGNORECASE):
          return True
      else:
          return False

  #Buscando los titles en una DB con titulos limpios y suicios y haciendo merge
  df[columnName_Title] = df[columnName_Title]
  df_titulos = df_titulos.drop_duplicates(subset=[columnName_Repo]).copy()
  df_titulos[columnName_Repo] = df_titulos[columnName_Repo].apply(lambda x: str(x).strip())
  
  df = pd.merge(df, df_titulos[[columnName_Repo,'Standard']],
                left_on=columnName_Title,
                right_on=columnName_Repo,
                how='left')

  df['Standard_AI'] = df['Standard'].fillna('')
  df.drop(columns=[columnName_Repo,'Standard'], inplace=True)

  df['IA_QA'] = 'TRUE'
  filas_relevantes = df[df['Standard_AI'] == ''].shape[0]

  df['IA_QA'] = df.apply(lambda row: 'FALSE' if row['Standard_AI'] != '' else row['IA_QA'], axis=1)

  df = df.copy()

  # Inicializar listas para guardar los resultados
  indices_to_update = []
  standards = []
  similitudes = []

  num_rows = df.shape[0]
  df= df.drop_duplicates()

  st.write("Generating embbedings")
  index_progress = 0
  progress_bar = st.progress(0)

  print("Generating embbedings")
  for index, row in tqdm(df.iterrows(), total=filas_relevantes):
    if row['IA_QA'] == 'TRUE':
        title = row[columnName_Title]
        query = f"Tell me what Standard Title you think the following non-standard title should have: {title}"
        response = buscar(query, conocimiento_df)        
        # Guardar los resultados y los índices
        indices_to_update.append(index)
        standards.append(response.iloc[0][columnstand_DB])
        similitudes.append(response.iloc[0]['Similitud'])    
    # Increment index_progress
    index_progress += 1    
    # Update progress bar
    progress_value = min(index_progress / filas_relevantes, 1.0)
    progress_bar.progress(progress_value)
  
  # Actualizar solo las filas correspondientes en el DataFrame
  for i, idx in enumerate(indices_to_update):
      df.at[idx, 'Standard_AI'] = standards[i]
      df.at[idx, 'Similitud'] = similitudes[i]

  filas_relevantes_2 = df[df['IA_QA'] == 'TRUE'].shape[0]
  index_progress = 0
  st.write("Processing rows")
  progress_bar = st.progress(0)

  for index, row in tqdm(df[df['IA_QA'] == 'TRUE'].iterrows(), total=filas_relevantes_2):
      title = row[columnName_Title]
      Chief_result = Chief(title, df_Chief)
      # Reset de variables para cada fila
      keywords= Keywords(title)
      df.at[index, 'Keywords'] = keywords
      seniority_result, function_result, title_concat = None, None, None
      multiple_seniority, multiple_function = False, False
      if Chief_result is None:
          seniority_matches = Seniority(title, df_Seniority)
          function_matches = Function(title, df_Function)
          # Revisar si hay múltiples coincidencias
          multiple_seniority = len(seniority_matches) > 1
          multiple_function = len(function_matches) > 1
          df.at[index, 'Multiple Seniority'] = multiple_seniority
          df.at[index, 'Multiple Function'] = multiple_function
          # Generar todas las combinaciones posibles
          title_combinations = generate_combinations(seniority_matches, function_matches)
          # Buscar la primera coincidencia válida
          for title_concat in title_combinations:
              standard_title_row = df_Standard[df_Standard['concat_title'] == title_concat]
              if not standard_title_row.empty:
                  standard_title = standard_title_row['Standard Title'].iloc[0]
                  break  # Encontrar la primera coincidencia válida
          else:
              standard_title = "X"  # Usar "X" si no se encuentra una coincidencia
      else:
          title_concat = Chief_result
          df.at[index, 'Chief'] = True
          standard_title_row = df_Standard[df_Standard['concat_title'] == title_concat]
          standard_title = standard_title_row['Standard Title'].iloc[0]
      index_progress += 1    
      # Update progress bar
      progress_value = min(index_progress / filas_relevantes_2, 1.0)
      progress_bar.progress(progress_value)
      # Actualizar el DataFrame con los resultados encontrados
      df.at[index, 'Standard title'] = standard_title
  # Limpieza y preparación de los datos
  df['Standard_AI'] = df['Standard_AI'].apply(lambda x: str(x).rstrip(' .'))
  df_Standard[columnstand_DB] = df_Standard[columnstand_DB].apply(lambda x: str(x).strip())

  #Merge
  df = pd.merge(df, df_Standard[[columnstand_DB, 'Seniority', 'Function', 'Persona']],
                left_on='Standard title',
                right_on=columnstand_DB,
                how='left')
  # Manejo de valores nulos para las nuevas columnas
  df['Seniority'] = df['Seniority'].fillna('').apply(lambda x: str(x).rstrip(' .'))
  df['Function'] = df['Function'].fillna('').apply(lambda x: str(x).rstrip(' .'))
  df['Persona'] = df['Persona'].fillna('').apply(lambda x: str(x).rstrip(' .'))
  df.drop(columns=[columnstand_DB], inplace=True)

  df_test=df

  df['fit'] = ''
  df_test.loc[df_test['IA_QA'] == 'FALSE', 'fit'] = True
  #df_test.loc[df_test['Chief'] == True, 'fit'] = True
  df_test.loc[df_test['Standard title'] == df_test['Standard_AI'], 'fit'] = True
  df_test.loc[df_test['Keywords'] == True, 'fit'] = False
  df_test.loc[df_test['Standard title'] == 'X', 'fit'] = False
  df_test.loc[df_test['IA_QA'] == 'FALSE', 'Standard title'] = df_test['Standard_AI']
  df_test.loc[df_test['fit'] == False, 'Standard title'] = 'X'

  df_test = df_test[['title', 'Standard title', 'fit','IA_QA']]

  write_into_spreadsheet(spreadsheetUrl, sheetName, df_test, key_dict)

  #OpenAI: ABM Request
  nuevo_registro = {
      'User': "Streamlit",
      'Request Date': datetime.now(),
      'Script Name': "Title Cleanin QA Script 4.0",
      'Script Url': "https://colab.research.google.com/drive/1PQOBmxcdBEvqLsOxCanD59P7Uxjli8zv#scrollTo=Da5oMfExbFoC",
      'Nº request': filas_relevantes_2,
      'API Key': "EdGuj"

  }
  nuevo_registro_df = pd.DataFrame([nuevo_registro])
  df_request = pd.concat([df_request, nuevo_registro_df], ignore_index=True)

  # Selecciona la hoja de cálculo por nombre
  sheetName_request = 'Control'
  write_into_spreadsheet(spreadsheetUrl_request, sheetName_request, df_request, key_dict)
  return df_test

st.title("Title Cleaning")

option = st.selectbox(
    "Select a Client ICP",
    ("Select Scraper Type","Onfleet (DSP)","Onfleet (Resellers)")
)

# Conditional inputs based on the selected scraper type
if option == "Onfleet (DSP)":
  st.write("Use this tool when you need to make Title Cleaning standarization for a list of contact. to use this tool you should have created a title repository and a database with your standardized titles, below is a video on how to do it. It is important to note that you will also have to share your spreadsheet with this account, granting editor permissions: kalungi-google-colab@invertible-now-393117.iam.gserviceaccount.com.")
  st.write("[Tutorial >](https://www.loom.com/looms/videos)")
  spreadsheet_url = st.text_input("Select a spreadsheet Url", "https://docs.google.com/spreadsheets/d/19hsZxx29AuBJ4zGBh8iB7ImqbT_lWZHKeXFd3mExkb0/edit?gid=0#gid=0")
  sheet_name = st.text_input("Select Sheet Name", "TC")
  column_name = st.text_input("Select Column Name", "title")
  spreadsheetUrl_DB = st.text_input("Select Title DB Url", "https://docs.google.com/spreadsheets/d/173_FgevHCEA9jTOlHyp16hYsTXxNzwclZiLjUbcMl4Q/edit#gid=87246784")

elif option == "Onfleet (Resellers)":
  st.write("Use This tool when you need to make Title Cleaning standarization for a list of contact. to use this tool you should have created a title repository and a database with your standardized titles, below is a video on how to do it. It is important to note that you will also have to share your spreadsheet with this account, granting editor permissions: kalungi-google-colab@invertible-now-393117.iam.gserviceaccount.com.")
  st.write("[Tutorial >](https://www.loom.com/looms/videos)")
  spreadsheet_url = st.text_input("Select a spreadsheet Url", "https://docs.google.com/spreadsheets/d/19hsZxx29AuBJ4zGBh8iB7ImqbT_lWZHKeXFd3mExkb0/edit?gid=0#gid=0")
  sheet_name = st.text_input("Select Sheet Name", "TC")
  column_name = st.text_input("Select Column Name", "title")
  spreadsheetUrl_DB = st.text_input("Select Title DB Url", "https://docs.google.com/spreadsheets/d/1tbNbX1y-FEbE4hZXB6B2jfHEN3Xa2c3BA7PLGNKu0-s/edit#gid=0")

if option != "Select Client ICP":
  if st.button("Iniciar procesamiento"):
      if not spreadsheet_url or not spreadsheetUrl_DB:
          st.error("Please enter both the Spreadsheet URL and a Title DB Url")
      else:
          with st.spinner("Running the TC Tool. This could take a few minutes depending on the list size..."):
              try:
                  result = tc(spreadsheet_url,sheet_name,column_name,spreadsheetUrl_DB,key_dict)
                  st.success("TC completed!")
                  st.dataframe(result)
              except Exception as e:
                  st.error(f"An error occurred: {str(e)}")
                  st.exception(e)