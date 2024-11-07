from snowflake.snowpark import Session
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



def fetch_databases():
    return [row[1] for row in session.sql("SHOW DATABASES").collect()]


def fetch_schemas(db):
    return [row[1] for row in session.sql(f"SHOW SCHEMAS IN {db}").collect()]


def fetch_tables(db, schema):
    table_list = [row[1] for row in session.sql(f"SHOW TABLES IN {db}.{schema}").collect()]
    view_list = [row[1] for row in session.sql(f"SHOW VIEWS IN {db}.{schema}").collect()]
    return table_list + view_list

def fetch_stages():
    stage_list = [row[1] for row in session.sql(f"SHOW STAGES IN STREAMLIT_DB.PUBLIC").collect()]
    return stage_list
def fetch_files(stage):
    file_list = [row[0].split('/')[-1] for row in session.sql(f"LIST @{stage}").collect()]
    return file_list

def create_report(session, report, report_name):
    stage_name = "@STREAMLIT_DB.PUBLIC.STREAMLIT_APP_FILES"
    # Create a file-like object in memory from the html_report string
    report.to_file(f"{report_name}.html")
    session.file.put(f"{report_name}.html", stage_name, auto_compress=False)
    
    file_sql = f"select GET_PRESIGNED_URL(@STREAMLIT_DB.PUBLIC.STREAMLIT_APP_FILES, '{report_name}.html', 3600) as signed_url;"
    signed_url = session.sql(file_sql).collect()[0]['SIGNED_URL']
    return f"{report_name}.html", signed_url