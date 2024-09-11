import streamlit as st

st.title("Welcome!")
st.write(st.secrets["OPENAI_API_KEY"]["value"])