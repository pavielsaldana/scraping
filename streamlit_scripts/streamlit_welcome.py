import streamlit as st

st.title("Welcome!")

from langchain.embeddings import OpenAIEmbeddings
from langchain.vectorstores import FAISS
openai_api_key = st.secrets["OPENAI_API_KEY"]["value"]
def test_faiss_vectorstore():
    try:
        # Initialize embeddings
        embeddings = OpenAIEmbeddings(openai_api_key=openai_api_key)
        # Sample text
        text_samples = ["sample text one", "sample text two"]
        # Generate vectorstore
        vectorstore = FAISS.from_texts(text_samples, embedding=embeddings)
        st.write(vectorstore)
        st.write("FAISS vectorstore created successfully!")
        print(vectorstore)
        return vectorstore
    except Exception as e:
        st.error(f"Error in FAISS setup: {str(e)}")

test_faiss_vectorstore()