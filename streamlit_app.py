import streamlit as st
from common.hello import say_hello
import snowflake.connector
from snowflake.snowpark.context import get_active_session
from dotenv import load_dotenv
import os
load_dotenv()

try:
    st.session_state.CONN = get_active_session()
except:
    if 'CONN' not in st.session_state or st.session_state.CONN is None:
        st.session_state.CONN = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            host=os.getenv('SNOWFLAKE_HOST'),
            port=443,
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            role=os.getenv('SNOWFLAKE_ROLE'),
        )

st.title(f"Example streamlit app. {say_hello()}")
