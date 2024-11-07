import streamlit as st
from common.hello import say_hello
import snowflake.connector
from snowflake.snowpark.context import get_active_session
from dotenv import load_dotenv
import os
load_dotenv()

try:
    session = get_active_session()
except:
    session = Session.builder.configs({
            "user": os.getenv('SNOWFLAKE_USER'),
            "password": os.getenv('SNOWFLAKE_PASSWORD'),
            "account": os.getenv('SNOWFLAKE_ACCOUNT'),
            "host": os.getenv('SNOWFLAKE_HOST'),
            "port": 443,
            "warehouse": os.getenv('SNOWFLAKE_WAREHOUSE'),
            "role": os.getenv('SNOWFLAKE_ROLE'),
        }).create()

st.title(f"Example streamlit app. {say_hello()} Alvin")
