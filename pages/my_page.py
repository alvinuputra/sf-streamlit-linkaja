import streamlit as st
from common.utils import *
import streamlit.components.v1 as components
from ydata_profiling import ProfileReport
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import datetime
import snowflake.connector
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark import Session
from dotenv import load_dotenv
import os
load_dotenv()

# Define UI colors
colors = {
    "primary": "#00BFFF",  # Light Blue (Snowflake logo color)
    "secondary": "#1ABC9C",  # Bright Green (hover effects)
    "background": "#000000",  # Black background
    "sidebar_bg": "#1C1C1C",  # Sidebar background
    "widget_bg": "#333333",  # Widget background
    "widget_text": "#ffffff",  # Widget text color
    "label_text": "#ffffff"  # Label text color
}

# Apply custom styling for the UI
st.markdown(
    f"""
    <style>
    .stApp {{ 
        background-color: {colors["background"]} !important; 
        color: white !important; 
    }}

    [data-testid=stSidebar] {{ 
        background-color: {colors["sidebar_bg"]} !important; 
        color: white !important; 
    }}

    [data-testid=stSidebar] h1, [data-testid=stSidebar] h2 {{ 
        color: {colors["primary"]}; 
        font-weight: bold; 
    }}

    h1 {{ 
        color: {colors["primary"]}; 
        font-size: 2.5rem; font-weight: 600; 
    }}

    h3 {{ 
        color: {colors["primary"]}; 
    }}

    .stButton>button {{
        color: white; background-color: {colors["primary"]};
        border: none; border-radius: 12px; font-size: 18px;
        padding: 12px 24px; cursor: pointer;
        box-shadow: 0px 5px 10px rgba(255, 255, 255, 0.2);
        transition: all 0.3s ease-in-out;
    }}
    .stButton>button:hover {{
        background-color: {colors["secondary"]};
        box-shadow: 0px 10px 20px rgba(255, 255, 255, 0.25);
        transform: translateY(-2px);
    }}

    .stSelectbox, .stTextInput, .stMultiSelect, .stDataFrame, .stEditable {{
        background-color: {colors["widget_bg"]} !important;
        color: {colors["widget_text"]} !important;
        border: 2px solid {colors["primary"]}; 
        border-radius: 8px; 
        padding: 10px; 
        font-size: 16px;
    }}
    .stSelectbox:hover, .stTextInput:hover, .stMultiSelect:hover {{ 
        border-color: {colors["secondary"]}; 
    }}

    .stSelectbox label, .stTextInput label, .stMultiSelect label {{ 
        color: {colors["label_text"]} !important; 
    }}

    [data-baseweb="checkbox"] [data-testid="stWidgetLabel"] p {{
    /* Styles for the label text for checkbox and toggle */
        background-color: {colors["widget_bg"]} !important;
        color: {colors["label_text"]} !important; 
        font-size: 16px;
        border: 2px solid {colors["primary"]}; 
        border-radius: 8px; 
        padding: 10px; 
        margin-bottom: 20px; 
    }}

    [data-testid="stCheckbox"] label span {{
    /* Styles the checkbox */
        height: 4rem;
        width: 4rem;
    }}

    .stSpinner {{
        color: {colors["label_text"]} !important;
    }}

    .stTextInput, .stSelectbox, .stCheckbox, .stButton, .stMultiSelect {{ 
        margin-bottom: 20px; 
    }}

    hr {{ 
        border: none; 
        border-top: 
        4px solid {colors["primary"]}; 
        margin: 20px 0; 
    }}

    /* Hover effect for image thumbnails */
    img:hover {{
        transform: scale(1.05);  /* Slight zoom on hover */
        transition: transform 0.2s ease-in-out;
    }}
    </style>


    """,
    unsafe_allow_html=True
)

# Customize font sizes, font style, and figure appearance for Seaborn
plt.rcParams.update({
                    'axes.titlesize': 16,  # Title size
                    'axes.labelsize': 11,  # Axis label size
                    'xtick.labelsize': 11,  # X-axis tick size
                    'ytick.labelsize': 11,  # Y-axis tick size
                    'legend.fontsize': 11,  # Legend font size
                    'figure.figsize': (12, 6),  # Customize figure size for better readability
                    'axes.facecolor': 'black',  # Set background color to white
                    'axes.edgecolor': 'black'  # Set axis edge color to black for contrast
                })
plt.xticks(rotation=45, ha='right')

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

# Sidebar: Database, Schema, and Table Selection

st.sidebar.title("Database Configuration")
# Load database, schema, and table into session state if not already set
if "selected_db" not in st.session_state:
    st.session_state.selected_db = None
if "selected_schema" not in st.session_state:
    st.session_state.selected_schema = None
if "selected_table" not in st.session_state:
    st.session_state.selected_table = None
if "html_report" not in st.session_state:
    st.session_state.html_report = None
if "send_email" not in st.session_state:
    st.session_state.send_email = False
if "send_report" not in st.session_state:
    st.session_state.send_report = False

# Fetch and select the database
databases = fetch_databases()
st.session_state.selected_db = st.sidebar.selectbox("Select a Database:", databases, index=None)

# Fetch and select the schema if a database is selected
if st.session_state.selected_db:
    schemas = fetch_schemas(st.session_state.selected_db)
    # st.session_state.selected_schema = st.sidebar.selectbox("Select a Schema:", schemas, index=schemas.index(st.session_state.selected_schema) if st.session_state.selected_schema else 0)
    st.session_state.selected_schema = st.sidebar.selectbox("Select a Schema:", schemas, index=None)


# Fetch and select the table if both a database and schema are selected
if st.session_state.selected_schema:
    tables = fetch_tables(st.session_state.selected_db, st.session_state.selected_schema)
    # st.session_state.selected_table = st.sidebar.selectbox("Select a Table:", tables, index=tables.index(st.session_state.selected_table) if st.session_state.selected_table else 0)
    selected_table = st.sidebar.selectbox("Select a Table:", tables, index=None)
    if selected_table != st.session_state.selected_table:
        st.session_state.html_report = None
        st.session_state.selected_table = selected_table
        if "data_df" in st.session_state:
            del st.session_state["data_df"]



# If a table is selected, fetch and display data
if st.session_state.selected_table:
    try:
        st.title("📝 Advanced Data Editor")
        st.markdown("<hr>", unsafe_allow_html=True)

        # Fetch data from Snowflake into a Pandas DataFrame
        data_df = session.table(
            f"{st.session_state.selected_db}.{st.session_state.selected_schema}.{st.session_state.selected_table}").to_pandas()

        for column in data_df.columns:
            # Check if the column can be converted to datetime
            if not pd.api.types.is_numeric_dtype(data_df[column]):
                try:
                    data_df[column] = pd.to_datetime(data_df[column])
                except:
                    pass

        # Use session state to manage the editable dataframe
        if "data_df" not in st.session_state:
            st.session_state.data_df = data_df.copy()

        # View/Edit Data section
        st.subheader("🖊️ View/Edit Data")

        # Display the data editor for editing
        st.session_state.data_df = st.data_editor(st.session_state.data_df, num_rows="dynamic",
                                                    use_container_width=True)

        # Manage Data (Add/Delete Columns, Undo, Overwrite, Create New Table)

        st.markdown("<hr>", unsafe_allow_html=True)
        st.subheader("⚙️ Tools")
        with st.expander("**Open/Closed Tools**", expanded=True):

            col1, col2 = st.columns(2, gap="large")

            # First column: Add/Delete Columns
            with col1:

                st.subheader("➕ Add New Columns")
                col3, col4 = st.columns([2, 1.5])
                with col3:
                    new_columns = st.text_input("New column names (comma-separated):",
                                                placeholder="e.g., Height, Weight")

                with col4:
                    col_type = st.selectbox("New columns type", options=["String", "Integer", "Float"])

                # Confirmation button for adding new columns
                if st.button("Confirm Add Columns"):
                    if new_columns:
                        new_column_list = [col.strip().upper() for col in new_columns.split(",") if
                                            col.strip()]  # Uppercase

                        if new_column_list:
                            # Add the new columns based on selected type
                            for new_column_name in new_column_list:
                                if new_column_name not in st.session_state.data_df.columns:
                                    if col_type == "String":
                                        st.session_state.data_df[new_column_name] = ''
                                    elif col_type == "Integer":
                                        st.session_state.data_df[new_column_name] = 0
                                    elif col_type == "Float":
                                        st.session_state.data_df[new_column_name] = 0.0

                            # Update the DataFrame with new columns
                            st.success("New columns added successfully.")
                            st.rerun()
                        else:
                            st.warning("Please enter valid column names.")
                    else:
                        st.warning("No columns were entered.")

                # Delete columns
                st.subheader("❌ Delete Columns")
                columns_to_delete = st.multiselect("Select columns to delete:", st.session_state.data_df.columns)

                if st.button("Confirm Delete Columns"):
                    if columns_to_delete:
                        st.session_state.data_df.drop(columns=columns_to_delete, inplace=True)
                        st.success("Selected columns deleted successfully.")
                        st.rerun()
                    else:
                        st.warning("Please select columns to delete.")

            # Second column: Undo Changes, Overwrite Data, Create New Table
            with col2:
                st.subheader("🔄 Reset Changes")
                if st.button("Reset Changes"):
                    st.session_state.data_df = session.table(
                        f"{st.session_state.selected_db}.{st.session_state.selected_schema}.{st.session_state.selected_table}").to_pandas()
                    st.success("Changes reset successfully.")
                    st.rerun()

                st.subheader("📤 Overwrite Data")
                if st.button("Overwrite Data"):
                    snowpark_df = session.create_dataframe(st.session_state.data_df)
                    snowpark_df.write.mode("overwrite").save_as_table(
                        f"{st.session_state.selected_db}.{st.session_state.selected_schema}.{st.session_state.selected_table}")
                    st.success(f"Data successfully overwritten in {st.session_state.selected_table}.")
                    st.rerun()

                st.subheader("🆕 Create New Table")
                new_table_name = st.text_input("Enter new table name:")
                if st.button("Create New Table"):
                    if new_table_name:
                        snowpark_df = session.create_dataframe(st.session_state.data_df)
                        snowpark_df.write.mode("overwrite").save_as_table(
                            f"{st.session_state.selected_db}.{st.session_state.selected_schema}.{new_table_name}")
                        st.success(
                            f"New table {new_table_name} created successfully in {st.session_state.selected_schema}.")
                        st.rerun()

        st.markdown("<hr>", unsafe_allow_html=True)
        # Summary Statistics
        st.subheader("📊 Summary Statistics")
        if not st.session_state.data_df.empty:
            summary_df = st.session_state.data_df.describe().transpose()
            available_stats = summary_df.columns.intersection(
                ['count', 'mean', 'std', 'min', '25%', '50%', '75%', 'max'])
            if not available_stats.empty:
                st.dataframe(summary_df[available_stats], use_container_width=True)
            else:
                st.warning("Warning: No summary statistics available for this data.", icon="⚠️")

        st.markdown("<hr>", unsafe_allow_html=True)
        # Data Plotting
        st.subheader("📊 Plot Numeric Data")

        plot_type = st.selectbox("Choose Plot Type", options=["Bar Chart", "Line Chart", "None"])
        numeric_columns = st.session_state.data_df.select_dtypes(include=['int', 'float']).columns
        with plt.style.context('dark_background'):
            if plot_type != "None" and numeric_columns.any():
                col_x_axis, col_y_axis = st.columns(2, gap="large")
                with col_x_axis:
                    x_axis = st.selectbox("X-axis", options=st.session_state.data_df.columns)
                with col_y_axis:
                    y_axis = st.selectbox("Y-axis", options=numeric_columns)

                if plot_type == "Bar Chart":
                    fig, ax = plt.subplots()
                    st.session_state.data_df.groupby(x_axis)[y_axis].mean().plot(kind='bar', ax=ax)

                    ax.set_facecolor('black')  # Set the background color of the plot area to black
                    ax.grid(True, color='gray', linestyle='--', linewidth=0.5)  # Adjust grid lines
                    st.pyplot(fig)

                    fig, ax = plt.subplots()
                    st.session_state.data_df.groupby(x_axis)[y_axis].mean().plot(kind='barh', ax=ax)  # Change to horizontal bar chart

                    ax.set_facecolor('black')  # Set the background color of the plot area to black
                    ax.grid(True, color='gray', linestyle='--', linewidth=0.5)  # Adjust grid lines
                    st.pyplot(fig)
                elif plot_type == "Line Chart":
                    fig, ax = plt.subplots()
                    st.session_state.data_df.plot(x=x_axis, y=y_axis, kind='line', ax=ax)

                    ax.set_facecolor('black')  # Set the background color of the plot area to black
                    ax.grid(True, color='gray', linestyle='--', linewidth=0.5)  # Adjust grid lines
                    st.pyplot(fig)
            else:
                st.warning(f"Warning: No numeric column available for this data.", icon="⚠️")

        st.markdown("<hr>", unsafe_allow_html=True)
        st.subheader("📊 Summary Report")
        if not st.session_state.html_report:
            with st.spinner(f"Creating Summary Report..."):
                # Use seaborn-white style and adjust fonts and ticks for a cleaner look
                with plt.style.context('seaborn-whitegrid'):
                    # Generate the profiling report
                    report = ProfileReport(
                        st.session_state.data_df,
                        title="Enhanced Data Summary Report",
                        tsmode=True,
                        explorative=True,  # Enable explorative analysis (includes interaction features)
                        correlations={
                            "pearson": {"calculate": True},
                            "spearman": {"calculate": True},  # Extra correlations
                            "kendall": {"calculate": True}},
                        progress_bar=True  # Show a progress bar during report generation
                    )

                # Generating the HTML report as a string
                st.session_state.html_report = report.to_html()

        # Displaying the HTML report in Streamlit
        components.html(html=st.session_state.html_report, width=None, height=800, scrolling=True)
        if st.button("Send Report To Email"):
            st.session_state.send_report = True

        if st.session_state.send_report:
            st.session_state.to_email = st.text_input("Recipient Email:")
            st.session_state.email_subject = st.text_input("Email Subject:",
                                                            value=f"Snowflake Report - {datetime.datetime.utcnow().strftime('%Y-%m-%d')}")
            if st.button("Send Email"):
                st.session_state.send_email = True
            if st.session_state.send_email:
                if st.session_state.to_email:  # Check if email is provided
                    st.session_state.file_name, st.session_state.file_url = create_report(session,
                                                                                            st.session_state.report,
                                                                                            st.session_state.email_subject)
                    st.session_state.send_email_query = f"""
                                CALL SYSTEM$SEND_EMAIL(
                                    'my_email_int', 
                                    '{st.session_state.to_email}', 
                                    '{st.session_state.email_subject}', 
                                    'Attached report for your reference <a href="{st.session_state.file_url}">{st.session_state.file_name}</a>', 
                                    'text/html');
                                """
                    session.sql(st.session_state.send_email_query).collect()
                    st.success('Email sent successfully!')
                    st.session_state.send_report = False  # Reset to avoid sending again
                    st.session_state.send_email = False
                else:
                    st.error("Please enter a recipient email.")

    except Exception as e:
        st.error(f"Error loading data from table: {str(e)}")
