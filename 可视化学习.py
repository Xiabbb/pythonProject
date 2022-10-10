import pandas as pd
import streamlit as st

st.write('Hello world!')

df_data = pd.read_excel('审核列表 (17).xlsx')

st.dataframe(df_data)
