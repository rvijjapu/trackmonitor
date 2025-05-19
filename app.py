# TrendTrack Monitor 360 View Dashboard
# Deployment Instructions for Streamlit Community Cloud:
#
# **Step 1: Update GitHub Repository**:
# 1. Go to https://github.com/your-username/trendtrack-monitor.
# 2. Edit `app.py` and replace its content with this file.
# 3. Update `requirements.txt` with the new dependencies.
# 4. Commit with message: "Added support for all databases with dynamic parameters".
#
# **Step 2: Redeploy on Streamlit Community Cloud**:
# 1. Log in at https://streamlit.io/cloud.
# 2. Find your app (`trendtrack-monitor`) and click "Manage app".
# 3. Click "Redeploy" to rebuild the app with the updated `app.py`.
# 4. Access the app at the provided URL (e.g., https://trendtrack-monitor.streamlit.app).
#
# **Step 3: Test the Dashboard**:
# - Test database connections with various databases.
# - Switch between "360 View" and "Trends Comparison" tabs.
# - Verify connection success messages, data loading, and dashboard functionality.
!pip install streamlit 
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
from datetime import datetime, timedelta, date
import time
from sqlalchemy import create_engine
import os
import warnings
import logging
import threading
import re

# Optional database libraries
try:
    from supabase import create_client, Client
except ImportError:
    create_client = Client = None

try:
    from google.cloud import bigquery
except ImportError:
    bigquery = None

try:
    from pymongo import MongoClient
except ImportError:
    MongoClient = None

try:
    import cx_Oracle
except ImportError:
    cx_Oracle = None

try:
    from snowflake.sqlalchemy import URL
except ImportError:
    URL = None

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore')

# Streamlit page configuration
st.set_page_config(page_title="TrendTrack Monitor - Modern Dashboard", layout="wide")

# Apply Tailwind CSS and custom styles via Streamlit markdown
st.markdown("""
    <link href="https://cdn.jsdelivr.net/npm/tailwindcss@2.2.19/dist/tailwind.min.css" rel="stylesheet">
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
    <style>
        body {
            font-family: 'Inter', sans-serif;
            background: linear-gradient(135deg, #f7fafc, #edf2f7);
            min-height: 100vh;
            margin: 0;
            padding: 0;
        }
        .container {
            max-width: 1500px;
            margin: 0 auto;
            padding: 2.5rem;
        }
        .kpi-card {
            background: white;
            border-radius: 12px;
            box-shadow: 0 4px 20px rgba(0, 0, 0, 0.05);
            padding: 1.5rem;
            text-align: center;
            transition: transform 0.3s ease, box-shadow 0.3s ease;
        }
        .kpi-card:hover {
            transform: translateY(-5px);
            box-shadow: 0 8px 30px rgba(0, 0, 0, 0.1);
        }
        .kpi-card h3 {
            color: #4a5568;
            font-size: 0.9rem;
            font-weight: 600;
            margin-bottom: 0.5rem;
        }
        .kpi-card p {
            color: #3B82F6;
            font-size: 1.2rem;
            font-weight: 700;
        }
        .filter-section {
            background: white;
            border-radius: 12px;
            box-shadow: 0 4px 20px rgba(0, 0, 0, 0.05);
            padding: 1.5rem;
            margin-bottom: 2rem;
        }
        .chart-container {
            background: white;
            border-radius: 12px;
            box-shadow: 0 4px 20px rgba(0, 0, 0, 0.05);
            padding: 1.5rem;
            margin-bottom: 2rem;
            opacity: 0;
            animation: fadeIn 0.5s ease forwards;
            transition: transform 0.3s ease, box-shadow 0.3s ease;
        }
        .chart-container:hover {
            transform: translateY(-5px);
            box-shadow: 0 8px 30px rgba(0, 0, 0, 0.1);
        }
        @keyframes fadeIn {
            from { opacity: 0; transform: translateY(10px); }
            to { opacity: 1; transform: translateY(0); }
        }
        .error {
            color: #EF4444;
            text-align: center;
            margin: 1rem 0;
        }
        .footer {
            color: #6B7280;
            font-size: 0.9rem;
            text-align: center;
            margin-top: 2rem;
        }
        .change-indicator-up {
            color: #10B981;
            font-weight: 600;
        }
        .change-indicator-down {
            color: #EF4444;
            font-weight: 600;
        }
        .stTabs [data-baseweb="tab"] {
            background: white;
            border-radius: 8px 8px 0 0;
            padding: 0.75rem 1.5rem;
            cursor: pointer;
            transition: background-color 0.3s ease;
        }
        .stTabs [data-baseweb="tab"]:hover {
            background-color: #F3F4F6;
        }
        .stTabs [data-baseweb="tab"][aria-selected="true"] {
            background-color: #A3BFFA;
            color: white;
        }
        .stTabs [data-baseweb="tab-panel"] {
            padding-top: 1rem;
        }
        .stSelectbox, .stMultiSelect {
            border: 1px solid #e2e8f0;
            border-radius: 8px;
            padding: 0.5rem;
            width: 100%;
        }
        .stDateInput input {
            border: 1px solid #e2e8f0;
            border-radius: 8px;
            padding: 0.5rem;
            width: 100%;
        }
        .checkbox-container {
            max-height: 100px;
            overflow-y: auto;
            border: 1px solid #e2e8f0;
            border-radius: 8px;
            padding: 0.25rem;
            background: #fff;
        }
        .checkbox-container label {
            display: block;
            padding: 0.1rem 0;
            font-size: 0.8rem;
            color: #4a5568;
        }
        .checkbox-container input[type="checkbox"] {
            margin-right: 0.3rem;
        }
        .custom-date-range {
            margin-top: 1rem;
        }
        .chart-container {
            min-height: 450px;
        }
    </style>
""", unsafe_allow_html=True)

# Check for environment variables (for Streamlit Community Cloud)
DATABASE_URL = os.getenv("DATABASE_URL")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# UI Form for Data Source Selection
st.sidebar.header("Data Source Configuration")
data_sources = [
    "Dummy Data", "PostgreSQL", "MySQL", "Microsoft SQL Server", "Snowflake",
    "SQLite", "MongoDB", "Oracle", "Neon", "Supabase", "BigQuery"
]
data_source = st.sidebar.selectbox("Select Data Source", data_sources)

# Initialize session state for connection parameters and data
if 'connection_params' not in st.session_state:
    st.session_state.connection_params = {}
if 'data_fetched' not in st.session_state:
    st.session_state.data_fetched = False
if 'df' not in st.session_state:
    st.session_state.df = None
if 'error_message' not in st.session_state:
    st.session_state.error_message = ""
if 'connection_established' not in st.session_state:
    st.session_state.connection_established = False
if 'connection_objects' not in st.session_state:
    st.session_state.connection_objects = {}
if 'churn_triggers' not in st.session_state:
    st.session_state.churn_triggers = None
if 'top_promotions' not in st.session_state:
    st.session_state.top_promotions = None
if 'top_coupons' not in st.session_state:
    st.session_state.top_coupons = None

# Supabase WebSocket setup (if available)
if create_client and SUPABASE_URL and SUPABASE_KEY:
    try:
        @st.experimental_singleton
        def init_supabase():
            return create_client(SUPABASE_URL, SUPABASE_KEY)

        supabase = init_supabase()

        def subscribe_to_updates():
            def listen(*args):
                st.cache_data.clear()
            supabase.table("subscriptions").on("*", listen).subscribe()
            logger.info("Subscribed to Supabase real-time updates")

        # Run subscription in a separate thread
        threading.Thread(target=subscribe_to_updates, daemon=True).start()
    except Exception as e:
        logger.error(f"Failed to set up Supabase WebSocket: {str(e)}")
else:
    supabase = None

# Database parameter requirements
db_param_requirements = {
    "PostgreSQL": ["host", "port", "database", "username", "password"],
    "MySQL": ["host", "port", "database", "username", "password"],
    "Microsoft SQL Server": ["server", "database", "username", "password", "driver"],
    "Snowflake": ["account", "user", "password", "role", "warehouse", "database", "schema"],
    "SQLite": ["database_path"],
    "MongoDB": ["host", "port", "database", "username", "password"],
    "Oracle": ["host", "port", "service_name", "username", "password"],
    "Neon": ["host", "port", "database", "username", "password"],
    "Supabase": ["host", "port", "database", "username", "password"],
    "BigQuery": ["project_id", "dataset_id", "table_id", "credential_path"]
}

# Default values for parameters
db_param_defaults = {
    "PostgreSQL": {"host": "db.your-project.supabase.co", "port": "5432", "database": "postgres", "username": "postgres"},
    "MySQL": {"host": "localhost", "port": "3306", "database": "mydb", "username": "root"},
    "Microsoft SQL Server": {"server": "localhost", "database": "mydb", "username": "sa", "driver": "ODBC Driver 17 for SQL Server"},
    "Snowflake": {"account": "", "user": "", "role": "", "warehouse": "", "database": "", "schema": ""},
    "SQLite": {"database_path": "/path/to/database.db"},
    "MongoDB": {"host": "localhost", "port": "27017", "database": "mydb", "username": "admin"},
    "Oracle": {"host": "localhost", "port": "1521", "service_name": "orcl", "username": "system"},
    "Neon": {"host": "your-neon-host.neon.tech", "port": "5432", "database": "neondb", "username": "neonuser"},
    "Supabase": {"host": "db.your-project.supabase.co", "port": "5432", "database": "postgres", "username": "postgres"},
    "BigQuery": {"project_id": "", "dataset_id": "trendtrack", "table_id": "subscriptions", "credential_path": ""}
}

# Dynamic parameter prompts
if data_source != "Dummy Data":
    params = db_param_requirements.get(data_source, [])
    for param in params:
        default_value = db_param_defaults.get(data_source, {}).get(param, "")
        if param == "password":
            st.session_state.connection_params[param] = st.sidebar.text_input(param.capitalize(), type="password", key=param)
        elif param in ["port"]:
            st.session_state.connection_params[param] = st.sidebar.text_input(param.capitalize(), value=default_value, key=param)
        elif param == "database_path":
            st.session_state.connection_params[param] = st.sidebar.text_input("Database Path (e.g., /path/to/database.db)", value=default_value, key=param)
        elif param == "credential_path":
            st.session_state.connection_params[param] = st.sidebar.text_input("Service Account JSON Path", value=default_value, key=param)
        else:
            st.session_state.connection_params[param] = st.sidebar.text_input(param.capitalize(), value=default_value, key=param)

    # Connect button with validation
    if st.sidebar.button("Connect"):
        # Basic parameter validation
        missing_params = [param for param in params if not st.session_state.connection_params.get(param, "").strip()]
        if missing_params:
            st.session_state.error_message = f"Missing required parameters: {', '.join(missing_params)}"
            st.sidebar.error(st.session_state.error_message)
            logger.error(st.session_state.error_message)
        else:
            # Clean up existing connections
            if 'connection_objects' in st.session_state:
                for conn_type, conn in st.session_state.connection_objects.items():
                    try:
                        if conn_type in ["PostgreSQL", "MySQL", "Microsoft SQL Server", "Snowflake", "SQLite", "Oracle", "Neon", "Supabase", "RenderDB"] and conn:
                            conn.dispose()
                        elif conn_type == "MongoDB" and conn:
                            conn.close()
                    except:
                        pass
            st.session_state.connection_objects = {}

            try:
                if data_source in ["PostgreSQL", "Neon", "Supabase"]:
                    connection_string = DATABASE_URL if DATABASE_URL else \
                        f"postgresql+psycopg2://{st.session_state.connection_params['username']}:{st.session_state.connection_params['password']}@{st.session_state.connection_params['host']}:{st.session_state.connection_params['port']}/{st.session_state.connection_params['database']}"
                    engine = create_engine(connection_string)
                    df = fetch_sql_data(engine, st.session_state.get('refresh_key', 0))
                    st.session_state.connection_objects[data_source] = engine
                elif data_source == "MySQL":
                    connection_string = f"mysql+mysqlclient://{st.session_state.connection_params['username']}:{st.session_state.connection_params['password']}@{st.session_state.connection_params['host']}:{st.session_state.connection_params['port']}/{st.session_state.connection_params['database']}"
                    engine = create_engine(connection_string)
                    df = fetch_sql_data(engine, st.session_state.get('refresh_key', 0))
                    st.session_state.connection_objects['MySQL'] = engine
                elif data_source == "Microsoft SQL Server":
                    connection_string = f"mssql+pyodbc://{st.session_state.connection_params['username']}:{st.session_state.connection_params['password']}@{st.session_state.connection_params['server']}/{st.session_state.connection_params['database']}?driver={st.session_state.connection_params['driver']}"
                    engine = create_engine(connection_string)
                    df = fetch_sql_data(engine, st.session_state.get('refresh_key', 0))
                    st.session_state.connection_objects['MSSQL'] = engine
                elif data_source == "Snowflake":
                    if not URL:
                        raise ImportError("Snowflake-SQLAlchemy is not installed.")
                    connection_string = URL(
                        account=st.session_state.connection_params['account'],
                        user=st.session_state.connection_params['user'],
                        password=st.session_state.connection_params['password'],
                        database=st.session_state.connection_params['database'],
                        schema=st.session_state.connection_params['schema'],
                        warehouse=st.session_state.connection_params['warehouse'],
                        role=st.session_state.connection_params['role']
                    )
                    engine = create_engine(connection_string)
                    df = fetch_sql_data(engine, st.session_state.get('refresh_key', 0))
                    st.session_state.connection_objects['Snowflake'] = engine
                elif data_source == "SQLite":
                    connection_string = f"sqlite:///{st.session_state.connection_params['database_path']}"
                    engine = create_engine(connection_string)
                    df = fetch_sql_data(engine, st.session_state.get('refresh_key', 0))
                    st.session_state.connection_objects['SQLite'] = engine
                elif data_source == "MongoDB":
                    if not MongoClient:
                        raise ImportError("pymongo is not installed.")
                    client = MongoClient(
                        host=st.session_state.connection_params['host'],
                        port=int(st.session_state.connection_params['port']),
                        username=st.session_state.connection_params['username'],
                        password=st.session_state.connection_params['password']
                    )
                    db = client[st.session_state.connection_params['database']]
                    collection = db['subscriptions']
                    df = pd.DataFrame(list(collection.find()))
                    if 'Date' in df.columns:
                        df['Date'] = pd.to_datetime(df['Date'])
                    st.session_state.connection_objects['MongoDB'] = client
                elif data_source == "Oracle":
                    if not cx_Oracle:
                        raise ImportError("cx_Oracle is not installed.")
                    connection_string = f"oracle+cx_oracle://{st.session_state.connection_params['username']}:{st.session_state.connection_params['password']}@{st.session_state.connection_params['host']}:{st.session_state.connection_params['port']}/{st.session_state.connection_params['service_name']}"
                    engine = create_engine(connection_string)
                    df = fetch_sql_data(engine, st.session_state.get('refresh_key', 0))
                    st.session_state.connection_objects['Oracle'] = engine
                elif data_source == "BigQuery":
                    if not bigquery:
                        raise ImportError("google-cloud-bigquery is not installed.")
                    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = st.session_state.connection_params['credential_path']
                    client = bigquery.Client(project=st.session_state.connection_params['project_id'])
                    query = f"SELECT * FROM `{st.session_state.connection_params['project_id']}.{st.session_state.connection_params['dataset_id']}.{st.session_state.connection_params['table_id']}`"
                    df = client.query(query).to_dataframe()
                    if 'Date' in df.columns:
                        df['Date'] = pd.to_datetime(df['Date'])
                    st.session_state.connection_objects['BigQuery'] = client
                st.session_state.df = df
                st.session_state.data_fetched = True
                st.session_state.connection_established = True
                st.session_state.error_message = ""
                st.sidebar.success(f"Connected to {data_source} successfully!")
                logger.info(f"Connected to {data_source} database")
            except Exception as e:
                st.session_state.error_message = f"Connection failed: {str(e)}. Reverted to dummy data."
                st.session_state.data_fetched = False
                st.session_state.connection_established = False
                df = generate_dummy_data(st.session_state.get('refresh_key', 0))
                st.session_state.df = df
                st.sidebar.error(st.session_state.error_message)
                logger.error(f"Database connection failed: {str(e)}")

# Generate dummy data (matching HTML code)
@st.cache_data(ttl=10)
def generate_dummy_data(_refresh_key):
    np.random.seed(int(time.time()))
    start_date = pd.to_datetime("2023-01-01")
    end_date = pd.to_datetime("2025-04-06")
    dates = pd.date_range(start=start_date, end=end_date, freq="D")
    regions = ["North America", "South America", "Europe", "Africa", "Asia", "Australia"]
    skus = ["SKU005", "SKU002", "SKU018", "SKU036", "SKU001"]
    payment_methods = ["App Billing", "Google Wallet", "Pay Pal", "Roku Payment", "Debit Card", "Credit Card"]
    statuses = ["Paid", "Active", "Free Trial", "Registered"]
    clients = [
        "1001", "AHA", "ATT", "Antel", "ABSCBN", "Astro Sooka", "Astro NJOI", "Astro PayTV",
        "BBCAsia", "Britbox", "Cignal", "Etisalat", "Etv", "Exxen", "FOXUSA", "Kocowa",
        "Lightbox", "MongolTV", "Marquee", "MK Ooredoo", "NBA", "NEWS9", "PLDT", "Pilipinas",
        "Sinclair", "Sony", "SimpleTv", "Shahid", "TRT World", "TV3", "TV ASAHI", "VIKI",
        "One31", "Gotham"
    ]

    # Subscriptions data
    subscriptions_data = []
    for date in dates:
        for region in regions:
            for sku in skus:
                for client in clients:
                    for status in statuses:
                        subscriptions_data.append({
                            "Date": date,
                            "Region": region,
                            "SKU": sku,
                            "Client": client,
                            "Status": status,
                            "Subscribers": np.random.randint(500, 5000),
                            "Revenue": np.random.randint(10000, 100000),
                            "PaymentMethod": np.random.choice(payment_methods),
                            "FreeTrials": np.random.randint(100, 500),
                            "NewOrders": np.random.randint(50, 200),
                            "Conversions": np.random.randint(100, 500),
                            "Redemptions": np.random.randint(20, 100),
                            "Registrations": np.random.randint(200, 600),
                            "ActivePaid": np.random.randint(300, 4000),
                            "Renewals": np.random.randint(100, 300),
                            "PaymentAmount": np.random.randint(5000, 20000),
                            "RefundAmount": np.random.randint(100, 1000),
                            "InvoluntaryChurn": np.random.randint(50, 200),
                            "VoluntaryChurn": np.random.randint(50, 200),
                            "Winbacks": np.random.randint(10, 100)
                        })
    subscriptions_df = pd.DataFrame(subscriptions_data)

    # Churn triggers
    churn_triggers = []
    triggers = [
        "Got all the content needed already",
        "Was too expensive",
        "Technical issues",
        "Stopped subscribing to bundling partner",
        "After trial is expired, decided not to continue"
    ]
    for client in clients:
        for trigger in triggers:
            churn_triggers.append({
                "Trigger": trigger,
                "ChurnRate": np.random.uniform(5, 25),
                "Client": client
            })
    churn_triggers_df = pd.DataFrame(churn_triggers)

    # Top promotions
    top_promotions = []
    promos = ["Spring Deal", "20% OFF Combo", "AppleTV Offer", "Credit Card Offer", "Package10%OFF"]
    for client in clients:
        for promo in promos:
            top_promotions.append({
                "Promotion": promo,
                "ProfitMargin": np.random.uniform(18, 35),
                "Client": client
            })
    top_promotions_df = pd.DataFrame(top_promotions)

    # Top coupons
    top_coupons = []
    coupons = ["FLAT25", "MOVIE999", "PREMIERE", "SAVE50", "FESTIVE10", "OFFER999", "FIRST50"]
    for client in clients:
        for coupon in coupons:
            top_coupons.append({
                "Coupon": coupon,
                "Count": np.random.randint(50, 87),
                "Client": client
            })
    top_coupons_df = pd.DataFrame(top_coupons)

    logger.info("Generated dummy data")
    return subscriptions_df, churn_triggers_df, top_promotions_df, top_coupons_df

# Fetch data from SQL database
@st.cache_data(ttl=10)
def fetch_sql_data(_engine, _refresh_key):
    query = """
    SELECT id, date, region, sku, client, status, subscribers, revenue, payment_method,
           free_trials, new_orders, conversions, redemptions, registrations, active_paid,
           renewals, payment_amount, refund_amount, involuntary_churn, voluntary_churn, winbacks
    FROM subscriptions
    """
    try:
        df = pd.read_sql(query, _engine).assign(date=lambda x: pd.to_datetime(x['date']))
        logger.info("Fetched data from SQL database")
        return df
    except Exception as e:
        logger.error(f"SQL query failed: {str(e)}")
        raise

# Initialize data
if data_source == "Dummy Data" or not st.session_state.data_fetched:
    subscriptions_df, churn_triggers_df, top_promotions_df, top_coupons_df = generate_dummy_data(st.session_state.get('refresh_key', 0))
else:
    subscriptions_df = st.session_state.df
    # For database mode, generate churn triggers, promotions, and coupons as dummy data if not fetched
    if st.session_state.churn_triggers is None or st.session_state.top_promotions is None or st.session_state.top_coupons is None:
        _, churn_triggers_df, top_promotions_df, top_coupons_df = generate_dummy_data(st.session_state.get('refresh_key', 0))
    else:
        churn_triggers_df = st.session_state.churn_triggers
        top_promotions_df = st.session_state.top_promotions
        top_coupons_df = st.session_state.top_coupons

# Store additional data in session state
st.session_state.churn_triggers = churn_triggers_df
st.session_state.top_promotions = top_promotions_df
st.session_state.top_coupons = top_coupons_df

# Initialize refresh key for real-time updates
if 'refresh_key' not in st.session_state:
    st.session_state.refresh_key = 0

# Auto-refresh for real-time updates
if 'last_refresh' not in st.session_state:
    st.session_state.last_refresh = time.time()

if time.time() - st.session_state.last_refresh > 10:  # Refresh every 10 seconds (unless WebSocket is active)
    st.session_state.refresh_key += 1
    st.cache_data.clear()
    if data_source == "Dummy Data" or not st.session_state.data_fetched:
        subscriptions_df, churn_triggers_df, top_promotions_df, top_coupons_df = generate_dummy_data(st.session_state.refresh_key)
    else:
        try:
            if DATABASE_URL:
                engine = create_engine(DATABASE_URL)
                subscriptions_df = fetch_sql_data(engine, st.session_state.refresh_key)
                st.session_state.connection_objects['RenderDB'] = engine
            elif data_source in ["PostgreSQL", "Neon", "Supabase"]:
                engine = create_engine(
                    f"postgresql+psycopg2://{st.session_state.connection_params['username']}:{st.session_state.connection_params['password']}@{st.session_state.connection_params['host']}:{st.session_state.connection_params['port']}/{st.session_state.connection_params['database']}"
                )
                subscriptions_df = fetch_sql_data(engine, st.session_state.refresh_key)
                st.session_state.connection_objects[data_source] = engine
            elif data_source == "MySQL":
                engine = create_engine(
                    f"mysql+mysqlclient://{st.session_state.connection_params['username']}:{st.session_state.connection_params['password']}@{st.session_state.connection_params['host']}:{st.session_state.connection_params['port']}/{st.session_state.connection_params['database']}"
                )
                subscriptions_df = fetch_sql_data(engine, st.session_state.refresh_key)
                st.session_state.connection_objects['MySQL'] = engine
            elif data_source == "Microsoft SQL Server":
                engine = create_engine(
                    f"mssql+pyodbc://{st.session_state.connection_params['username']}:{st.session_state.connection_params['password']}@{st.session_state.connection_params['server']}/{st.session_state.connection_params['database']}?driver={st.session_state.connection_params['driver']}"
                )
                subscriptions_df = fetch_sql_data(engine, st.session_state.refresh_key)
                st.session_state.connection_objects['MSSQL'] = engine
            elif data_source == "Snowflake":
                connection_string = URL(
                    account=st.session_state.connection_params['account'],
                    user=st.session_state.connection_params['user'],
                    password=st.session_state.connection_params['password'],
                    database=st.session_state.connection_params['database'],
                    schema=st.session_state.connection_params['schema'],
                    warehouse=st.session_state.connection_params['warehouse'],
                    role=st.session_state.connection_params['role']
                )
                engine = create_engine(connection_string)
                subscriptions_df = fetch_sql_data(engine, st.session_state.refresh_key)
                st.session_state.connection_objects['Snowflake'] = engine
            elif data_source == "SQLite":
                engine = create_engine(f"sqlite:///{st.session_state.connection_params['database_path']}")
                subscriptions_df = fetch_sql_data(engine, st.session_state.refresh_key)
                st.session_state.connection_objects['SQLite'] = engine
            elif data_source == "MongoDB":
                client = MongoClient(
                    host=st.session_state.connection_params['host'],
                    port=int(st.session_state.connection_params['port']),
                    username=st.session_state.connection_params['username'],
                    password=st.session_state.connection_params['password']
                )
                db = client[st.session_state.connection_params['database']]
                collection = db['subscriptions']
                subscriptions_df = pd.DataFrame(list(collection.find()))
                if 'Date' in subscriptions_df.columns:
                    subscriptions_df['Date'] = pd.to_datetime(subscriptions_df['Date'])
                st.session_state.connection_objects['MongoDB'] = client
            elif data_source == "Oracle":
                engine = create_engine(
                    f"oracle+cx_oracle://{st.session_state.connection_params['username']}:{st.session_state.connection_params['password']}@{st.session_state.connection_params['host']}:{st.session_state.connection_params['port']}/{st.session_state.connection_params['service_name']}"
                )
                subscriptions_df = fetch_sql_data(engine, st.session_state.refresh_key)
                st.session_state.connection_objects['Oracle'] = engine
            elif data_source == "BigQuery":
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = st.session_state.connection_params['credential_path']
                client = bigquery.Client(project=st.session_state.connection_params['project_id'])
                query = f"SELECT * FROM `{st.session_state.connection_params['project_id']}.{st.session_state.connection_params['dataset_id']}.{st.session_state.connection_params['table_id']}`"
                subscriptions_df = client.query(query).to_dataframe()
                if 'Date' in subscriptions_df.columns:
                    subscriptions_df['Date'] = pd.to_datetime(subscriptions_df['Date'])
                st.session_state.connection_objects['BigQuery'] = client
            # Regenerate churn triggers, promotions, and coupons as they may not be in the database
            _, churn_triggers_df, top_promotions_df, top_coupons_df = generate_dummy_data(st.session_state.refresh_key)
        except Exception as e:
            st.session_state.error_message = f"Connection lost: {str(e)}. Reverted to dummy data."
            subscriptions_df, churn_triggers_df, top_promotions_df, top_coupons_df = generate_dummy_data(st.session_state.refresh_key)
            logger.error(f"Connection lost during refresh: {str(e)}")
    st.session_state.df = subscriptions_df
    st.session_state.churn_triggers = churn_triggers_df
    st.session_state.top_promotions = top_promotions_df
    st.session_state.top_coupons = top_coupons_df
    st.session_state.last_refresh = time.time()
    st.experimental_rerun()

# Dashboard title
st.markdown('<h1 class="text-4xl font-bold text-center text-gray-800 mb-8">TrendTrack Monitor</h1>', unsafe_allow_html=True)

# Tabs
tab1, tab2 = st.tabs(["360 View", "Trends Comparison"])

# 360 View Tab (Single Track)
with tab1:
    # Filters
    st.markdown('<div class="filter-section">', unsafe_allow_html=True)
    col1, col2, col3 = st.columns(3)
    with col1:
        clients = sorted(subscriptions_df['Client'].unique())
        track_360 = st.selectbox("Track Name", ["Select a track"] + clients, key="track_360")
    with col2:
        regions = ["All"] + sorted(subscriptions_df['Region'].unique())
        region_360 = st.selectbox("Region", regions, key="region_360")
    with col3:
        time_periods = ["Last 7 Days", "Last 30 Days", "Last 90 Days", "Last 6 Months", "Last Year", "Custom Range"]
        time_period_360 = st.selectbox("Time Period", time_periods, key="time_period_360")

    # Custom date range
    if time_period_360 == "Custom Range":
        st.markdown('<div class="custom-date-range">', unsafe_allow_html=True)
        col4, col5 = st.columns(2)
        with col4:
            start_date_360 = st.date_input("Start Date", value=datetime.now() - timedelta(days=30), max_value=date(2025, 4, 6), key="start_date_360")
        with col5:
            end_date_360 = st.date_input("End Date", value=date(2025, 4, 6), max_value=date(2025, 4, 6), key="end_date_360")
        st.markdown('</div>', unsafe_allow_html=True)
    else:
        end_date_360 = date(2025, 4, 6)
        if time_period_360 == "Last 7 Days":
            start_date_360 = (datetime(2025, 4, 6) - timedelta(days=7)).date()
        elif time_period_360 == "Last 30 Days":
            start_date_360 = (datetime(2025, 4, 6) - timedelta(days=30)).date()
        elif time_period_360 == "Last 90 Days":
            start_date_360 = (datetime(2025, 4, 6) - timedelta(days=90)).date()
        elif time_period_360 == "Last 6 Months":
            start_date_360 = (datetime(2025, 4, 6) - pd.offsets.MonthBegin(6)).date()
        elif time_period_360 == "Last Year":
            start_date_360 = (datetime(2025, 4, 6) - pd.offsets.YearBegin(1)).date()
    st.markdown('</div>', unsafe_allow_html=True)

    # Error Message
    error_message_360 = st.empty()

    if track_360 == "Select a track":
        error_message_360.markdown('<div class="error">Please select a track to proceed.</div>', unsafe_allow_html=True)
    else:
        error_message_360.markdown('')
        filtered_df = subscriptions_df[subscriptions_df['Client'] == track_360]
        if region_360 != "All":
            filtered_df = filtered_df[filtered_df['Region'] == region_360]
        filtered_df = filtered_df[(filtered_df['Date'] >= pd.to_datetime(start_date_360)) & (filtered_df['Date'] <= pd.to_datetime(end_date_360))]

        # KPI Cards
        total_subscribers = filtered_df['Subscribers'].sum()
        kpi_metrics = {
            "Revenue": f"${round(filtered_df['Revenue'].sum() / 1000000, 1)}M",
            "Subscribers": f"{round(total_subscribers / 1000)}K",
            "Registrations": f"{round(filtered_df['Registrations'].sum() / 1000)}K",
            "Conversions": f"{round(filtered_df['Conversions'].sum() / 1000)}K (Paid)",
            "Free Trials": f"{round(filtered_df['FreeTrials'].sum() / 1000)}K",
            "New Orders": f"{round(filtered_df['NewOrders'].sum() / 1000)}K",
            "Active Paid": f"{round(filtered_df['ActivePaid'].sum() / 1000)}K",
            "Coupon Redemptions": f"{round(filtered_df['Redemptions'].sum() / 1000)}K",
            "Renewals": f"{round(filtered_df['Renewals'].sum() / 1000)}K",
            "Payment Amount": f"${round(filtered_df['PaymentAmount'].sum() / 1000000, 1)}M",
            "Refund Amount": f"${round(filtered_df['RefundAmount'].sum() / 1000, 1)}K",
            "Involuntary Churn": f"{round(filtered_df['InvoluntaryChurn'].sum() / 1000)}K",
            "Voluntary Churn": f"{round(filtered_df['VoluntaryChurn'].sum() / 1000)}K",
            "Winbacks": f"{round(filtered_df['Winbacks'].sum() / 1000)}K",
            "ARPU": f"${round(filtered_df['Revenue'].sum() / total_subscribers, 2) if total_subscribers else 0}"
        }
        kpi_cols = st.columns(5)
        for i, (metric, value) in enumerate(kpi_metrics.items()):
            with kpi_cols[i % 5]:
                st.markdown(f'<div class="kpi-card"><h3>{metric}</h3><p>{value}</p></div>', unsafe_allow_html=True)

        # Visualizations
        time_period_text = time_period_360.replace("Last ", "").replace(" Days", "D").replace(" Months", "M").replace(" Year", "Y")
        col6, col7 = st.columns(2)

        # Subscribers by Region (Choropleth)
        region_subs = filtered_df.groupby('Region')['Subscribers'].sum().reset_index()
        region_to_iso = {
            "North America": "USA",
            "South America": "BRA",
            "Europe": "DEU",
            "Africa": "ZAF",
            "Asia": "CHN",
            "Australia": "AUS"
        }
        fig1 = go.Figure(data=go.Choropleth(
            locations=region_subs['Region'].map(region_to_iso),
            z=region_subs['Subscribers'],
            text=region_subs['Region'],
            colorscale=[[0, '#A3BFFA'], [1, '#C4B5FD']],
            colorbar_title="Subscribers",
            colorbar_tickformat='s'
        ))
        fig1.update_layout(
            title=f"Subscribers by Region ({time_period_text})",
            title_font=dict(size=18, color='#1f2937', family='Inter'),
            geo=dict(showframe=False, showcoastlines=True, projection_type='equirectangular'),
            margin=dict(t=80, b=50, l=50, r=50),
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            height=450
        )
        with col6:
            st.markdown('<div class="chart-container">', unsafe_allow_html=True)
            st.plotly_chart(fig1, use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)

        # Revenue by Region (Funnel)
        region_revenue = filtered_df.groupby('Region')['Revenue'].sum().reset_index().sort_values('Revenue', ascending=False)
        fig2 = go.Figure(go.Funnel(
            y=region_revenue['Region'],
            x=region_revenue['Revenue'],
            text=[f"${(rev / 1000000):.2f}M" for rev in region_revenue['Revenue']],
            textinfo='text',
            marker=dict(color=['#A3BFFA', '#B5F5EC', '#C4B5FD', '#FED7AA', '#FBB6CE', '#D1D5DB'])
        ))
        fig2.update_layout(
            title=f"Revenue by Region ({time_period_text})",
            title_font=dict(size=18, color='#1f2937', family='Inter'),
            margin=dict(t=80, b=50, l=100, r=50),
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            xaxis=dict(tickformat='s', showgrid=False, showticklabels=False),
            yaxis=dict(showgrid=False),
            height=450
        )
        with col7:
            st.markdown('<div class="chart-container">', unsafe_allow_html=True)
            st.plotly_chart(fig2, use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)

        # Subscribers by SKU (Bar)
        sku_subs = filtered_df.groupby('SKU')['Subscribers'].sum().reset_index()
        fig3 = px.bar(sku_subs, x='SKU', y='Subscribers',
                      color_discrete_sequence=['#B5F5EC', '#A3BFFA', '#FED7AA', '#C4B5FD', '#FBB6CE'])
        fig3.update_layout(
            title=f"Subscribers by SKU ({time_period_text})",
            title_font=dict(size=18, color='#1f2937', family='Inter'),
            margin=dict(t=80, b=80, l=60, r=50),
            xaxis=dict(tickfont=dict(size=10, color='#718096'), tickangle=-45, automargin=True, showgrid=False),
            yaxis=dict(
                title='Subscribers',
                titlefont=dict(size=14, color='#1f2937'),
                tickfont=dict(size=8, color='#718096'),
                showticklabels=False,
                ticks='',
                automargin=True,
                showgrid=False
            ),
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            height=450
        )
        with col6:
            st.markdown('<div class="chart-container">', unsafe_allow_html=True)
            st.plotly_chart(fig3, use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)

        # Revenue by SKU (Pie)
        sku_revenue = filtered_df.groupby('SKU')['Revenue'].sum().reset_index()
        fig4 = px.pie(sku_revenue, names='SKU', values='Revenue',
                      color_discrete_sequence=['#A3BFFA', '#B5F5EC', '#FED7AA', '#C4B5FD', '#FBB6CE'])
        fig4.update_layout(
            title=f"Revenue by SKU ({time_period_text})",
            title_font=dict(size=18, color='#1f2937', family='Inter'),
            margin=dict(t=80, b=50, l=50, r=50),
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            height=450
        )
        with col7:
            st.markdown('<div class="chart-container">', unsafe_allow_html=True)
            st.plotly_chart(fig4, use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)

        # Churn Triggers (Bar)
        churn_filtered = churn_triggers_df[churn_triggers_df['Client'] == track_360]
        fig5 = px.bar(churn_filtered, x='ChurnRate', y='Trigger', orientation='h',
                      color_discrete_sequence=['#FBB6CE', '#A3BFFA', '#B5F5EC', '#FED7AA', '#C4B5FD'])
        fig5.update_layout(
            title=f"Churn Triggers ({time_period_text})",
            title_font=dict(size=18, color='#1f2937', family='Inter'),
            margin=dict(t=80, b=50, l=220, r=50),
            xaxis=dict(
                title='Churn Rate (%)',
                titlefont=dict(size=14, color='#1f2937'),
                tickfont=dict(size=8, color='#718096'),
                tickformat='.1f',
                dtick=5,
                automargin=True,
                showgrid=False,
                showticklabels=False,
                ticks=''
            ),
            yaxis=dict(tickfont=dict(size=8, color='#718096'), automargin=True, showgrid=False),
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            height=450
        )
        with col6:
            st.markdown('<div class="chart-container">', unsafe_allow_html=True)
            st.plotly_chart(fig5, use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)

        # Subscribers by Status (Bar)
        status_subs = filtered_df.groupby('Status')['Subscribers'].sum().reset_index()
        fig6 = px.bar(status_subs, x='Subscribers', y='Status', orientation='h',
                      color_discrete_sequence=['#B5F5EC', '#A3BFFA', '#FED7AA', '#C4B5FD'])
        fig6.update_layout(
            title=f"Subscribers by Status ({time_period_text})",
            title_font=dict(size=18, color='#1f2937', family='Inter'),
            margin=dict(t=80, b=50, l=120, r=50),
            xaxis=dict(
                title='Subscribers',
                titlefont=dict(size=14, color='#1f2937'),
                tickfont=dict(size=8, color='#718096'),
                showticklabels=False,
                ticks='',
                automargin=True,
                showgrid=False
            ),
            yaxis=dict(tickfont=dict(size=8, color='#718096'), automargin=True, showgrid=False),
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            height=450
        )
        with col7:
            st.markdown('<div class="chart-container">', unsafe_allow_html=True)
            st.plotly_chart(fig6, use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)

        # Revenue by Payment Method (Pie)
        payment_revenue = filtered_df.groupby('PaymentMethod')['Revenue'].sum().reset_index()
        fig7 = px.pie(payment_revenue, names='PaymentMethod', values='Revenue',
                      color_discrete_sequence=['#A3BFFA', '#B5F5EC', '#FED7AA', '#C4B5FD', '#FBB6CE', '#D1D5DB'])
        fig7.update_layout(
            title=f"Revenue by Payment Method ({time_period_text})",
            title_font=dict(size=18, color='#1f2937', family='Inter'),
            margin=dict(t=80, b=50, l=50, r=50),
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            height=450
        )
        with col6:
            st.markdown('<div class="chart-container">', unsafe_allow_html=True)
            st.plotly_chart(fig7, use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)

        # Top Promotions (Funnel)
        promo_filtered = top_promotions_df[top_promotions_df['Client'] == track_360].sort_values('ProfitMargin', ascending=False)
        fig8 = go.Figure(go.Funnel(
            y=promo_filtered['Promotion'],
            x=promo_filtered['ProfitMargin'],
            textinfo='value+percent initial',
            marker=dict(color=['#C4B5FD', '#A3BFFA', '#B5F5EC', '#FED7AA', '#FBB6CE'])
        ))
        fig8.update_layout(
            title=f"Top Promotions by Profit Margin ({time_period_text})",
            title_font=dict(size=18, color='#1f2937', family='Inter'),
            margin=dict(t=80, b=50, l=100, r=50),
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            xaxis=dict(tickformat='.1f', showgrid=False, showticklabels=False),
            yaxis=dict(showgrid=False),
            height=450
        )
        with col7:
            st.markdown('<div class="chart-container">', unsafe_allow_html=True)
            st.plotly_chart(fig8, use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)

        # Top Coupons (Bar)
        coupon_filtered = top_coupons_df[top_coupons_df['Client'] == track_360].sort_values('Count', ascending=False)
        fig9 = px.bar(coupon_filtered, x='Count', y='Coupon', orientation='h',
                      color_discrete_sequence=['#FED7AA', '#FED7AA', '#FED7AA', '#FED7AA', '#FED7AA', '#FED7AA', '#FED7AA'])
        fig9.update_layout(
            title=f"Top Coupons by Count ({time_period_text})",
            title_font=dict(size=18, color='#1f2937', family='Inter'),
            margin=dict(t=80, b=50, l=120, r=50),
            xaxis=dict(
                title='Count',
                titlefont=dict(size=14, color='#1f2937'),
                tickfont=dict(size=8, color='#718096'),
                showticklabels=False,
                ticks='',
                automargin=True,
                showgrid=False
            ),
            yaxis=dict(tickfont=dict(size=8, color='#718096'), automargin=True, showgrid=False),
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            height=450
        )
        with col6:
            st.markdown('<div class="chart-container">', unsafe_allow_html=True)
            st.plotly_chart(fig9, use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)

        # Churned Customers Over Time (Line)
        churn_data = filtered_df.groupby('Date')[['InvoluntaryChurn', 'VoluntaryChurn']].sum().reset_index()
        churn_data['TotalChurn'] = churn_data['InvoluntaryChurn'] + churn_data['VoluntaryChurn']
        fig10 = px.line(churn_data, x='Date', y='TotalChurn',
                        line_shape='linear', color_discrete_sequence=['#6366F1'])
        fig10.update_traces(
            mode='lines+markers',
            marker=dict(size=6, color='#FBB6CE', line=dict(width=1, color='#ffffff')),
            line=dict(width=2)
        )
        fig10.update_layout(
            title=f"Churned Customers Over Time ({time_period_text})",
            title_font=dict(size=18, color='#1f2937', family='Inter'),
            margin=dict(t=80, b=80, l=60, r=50),
            xaxis=dict(
                title='Date',
                titlefont=dict(size=14, color='#1f2937'),
                tickfont=dict(size=8, color='#718096'),
                tickangle=-45,
                automargin=True,
                showgrid=False
            ),
            yaxis=dict(
                title='Churned Customers',
                titlefont=dict(size=14, color='#1f2937'),
                tickfont=dict(size=8, color='#718096'),
                tickformat='s',
                dtick=500,
                automargin=True,
                showgrid=False
            ),
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            height=450
        )
        st.markdown('<div class="chart-container">', unsafe_allow_html=True)
        st.plotly_chart(fig10, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

        # Active Customers Over Time (Line)
        active_data = filtered_df.groupby('Date')['ActivePaid'].sum().reset_index()
        fig11 = px.line(active_data, x='Date', y='ActivePaid',
                        line_shape='linear', color_discrete_sequence=['#3B82F6'])
        fig11.update_traces(
            mode='lines+markers',
            marker=dict(size=6, color='#A3BFFA', line=dict(width=1, color='#ffffff')),
            line=dict(width=2)
        )
        fig11.update_layout(
            title=f"Active Customers Over Time ({time_period_text})",
            title_font=dict(size=18, color='#1f2937', family='Inter'),
            margin=dict(t=80, b=80, l=60, r=50),
            xaxis=dict(
                title='Date',
                titlefont=dict(size=14, color='#1f2937'),
                tickfont=dict(size=8, color='#718096'),
                tickangle=-45,
                automargin=True,
                showgrid=False
            ),
            yaxis=dict(
                title='Active Customers',
                titlefont=dict(size=14, color='#1f2937'),
                tickfont=dict(size=8, color='#718096'),
                tickformat='s',
                dtick=1000,
                automargin=True,
                showgrid=False
            ),
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            height=450
        )
        st.markdown('<div class="chart-container">', unsafe_allow_html=True)
        st.plotly_chart(fig11, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

# Trends Comparison Tab (Multiple Tracks)
with tab2:
    # Filters
    st.markdown('<div class="filter-section">', unsafe_allow_html=True)
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.markdown('<label class="block text-sm font-medium text-gray-700 mb-1">Track Name(s)</label>', unsafe_allow_html=True)
        st.markdown('<div class="checkbox-container">', unsafe_allow_html=True)
        selected_tracks = st.multiselect("", clients, key="tracks_trends", label_visibility="collapsed")
        st.markdown('</div>', unsafe_allow_html=True)
    with col2:
        st.markdown('<label class="block text-sm font-medium text-gray-700 mb-1">Metric(s)</label>', unsafe_allow_html=True)
        st.markdown('<div class="checkbox-container">', unsafe_allow_html=True)
        metrics = [
            "Subscribers", "Revenue", "TotalChurn", "FreeTrials", "NewOrders", "Conversions",
            "Redemptions", "Registrations", "ActivePaid", "Renewals", "PaymentAmount",
            "RefundAmount", "InvoluntaryChurn", "VoluntaryChurn", "Winbacks"
        ]
        selected_metrics = st.multiselect("", metrics, key="metrics_trends", label_visibility="collapsed")
        st.markdown('</div>', unsafe_allow_html=True)
    with col3:
        comparison_options = [
            ("Yesterday vs. Today", "yesterday-today"),
            ("Last Week vs. This Week", "lastweek-thisweek"),
            ("Last Month vs. This Month", "lastmonth-thismonth"),
            ("Last Quarter vs. This Quarter", "lastquarter-thisquarter"),
            ("Last Half-Year vs. This Half-Year", "lasthalfyear-thishalfyear"),
            ("Last Year vs. This Year", "lastyear-thisyear")
        ]
        comparison_type = st.selectbox("Duration Comparison", [opt[0] for opt in comparison_options], key="comparison_trends")
        comparison_value = next(value for label, value in comparison_options if label == comparison_type)
    with col4:
        graph_types = ["Bar", "Line", "Scatter", "Area", "Pie", "Donut"]
        graph_type = st.selectbox("Graph Type", graph_types, key="graph_type_trends")
    st.markdown('</div>', unsafe_allow_html=True)

    # Error Message
    error_message_trends = st.empty()

    if not selected_tracks:
        error_message_trends.markdown('<div class="error">Please select at least one track to proceed.</div>', unsafe_allow_html=True)
    elif not selected_metrics:
        error_message_trends.markdown('<div class="error">Please select at least one metric to proceed.</div>', unsafe_allow_html=True)
    else:
        error_message_trends.markdown('')

        # Date ranges for Trends Comparison
        def get_date_ranges(comparison_type):
            today = datetime(2025, 4, 6)
            period1_start, period1_end, period2_start, period2_end, period1_label, period2_label = today, today, today, today, '', ''
            if comparison_type == "yesterday-today":
                period2_end = today
                period2_start = today
                period1_end = today - timedelta(days=1)
                period1_start = period1_end
                period1_label = "Yesterday"
                period2_label = "Today"
            elif comparison_type == "lastweek-thisweek":
                period2_end = today
                period2_start = today - timedelta(days=today.weekday())
                period1_end = period2_start - timedelta(days=1)
                period1_start = period1_end - timedelta(days=6)
                period1_label = "Last Week"
                period2_label = "This Week"
            elif comparison_type == "lastmonth-thismonth":
                period2_end = today
                period2_start = today.replace(day=1)
                period1_end = period2_start - timedelta(days=1)
                period1_start = period1_end.replace(day=1)
                period1_label = "Last Month"
                period2_label = "This Month"
            elif comparison_type == "lastquarter-thisquarter":
                period2_end = today
                current_quarter = (today.month - 1) // 3 + 1
                period2_start = datetime(today.year, (current_quarter - 1) * 3 + 1, 1)
                period1_end = period2_start - timedelta(days=1)
                period1_start = datetime(period1_end.year, ((period1_end.month - 1) // 3 - 1) * 3 + 4, 1)
                period1_label = "Last Quarter"
                period2_label = "This Quarter"
            elif comparison_type == "lasthalfyear-thishalfyear":
                period2_end = today
                current_half_year = 1 if today.month < 7 else 2
                period2_start = datetime(today.year, 1 if current_half_year == 1 else 7, 1)
                period1_end = period2_start - timedelta(days=1)
                period1_start = datetime(period1_end.year, 7 if period1_end.month < 7 else 1, 1)
                period1_label = "Last Half-Year"
                period2_label = "This Half-Year"
            elif comparison_type == "lastyear-thisyear":
                period2_end = today
                period2_start = datetime(today.year, 1, 1)
                period1_end = period2_start - timedelta(days=1)
                period1_start = datetime(period1_end.year, 1, 1)
                period1_label = "Last Year"
                period2_label = "This Year"
            return period1_start, period1_end, period2_start, period2_end, period1_label, period2_label

        period1_start, period1_end, period2_start, period2_end, period1_label, period2_label = get_date_ranges(comparison_value)
        table_rows = []
        colors = ['#A3BFFA', '#FBB6CE', '#B5F5EC', '#FED7AA', '#D1D5DB', '#C4B5FD']
        line_colors = ['#6366F1', '#3B82F6']
        marker_colors = ['#FBB6CE', '#A3BFFA']

        # Create one chart per metric
        col8, col9 = st.columns(2)
        for metric_idx, metric in enumerate(selected_metrics):
            all_values = []
            bar_data = []
            for track_idx, track in enumerate(selected_tracks):
                filtered_data = subscriptions_df[subscriptions_df['Client'] == track]
                period1_data = filtered_data[(filtered_data['Date'] >= pd.to_datetime(period1_start)) & (filtered_data['Date'] <= pd.to_datetime(period1_end))]
                period2_data = filtered_data[(filtered_data['Date'] >= pd.to_datetime(period2_start)) & (filtered_data['Date'] <= pd.to_datetime(period2_end))]
                period1_value = 0
                period2_value = 0
                if metric == "TotalChurn":
                    period1_value = period1_data['InvoluntaryChurn'].sum() + period1_data['VoluntaryChurn'].sum()
                    period2_value = period2_data['InvoluntaryChurn'].sum() + period2_data['VoluntaryChurn'].sum()
                else:
                    period1_value = period1_data[metric].sum()
                    period2_value = period2_data[metric].sum()

                all_values.extend([period1_value, period2_value])

                value_change = period2_value - period1_value
                percent_change = ((value_change / period1_value) * 100) if period1_value else 0

                short_metric = metric.replace("TotalChurn", "Churn").replace("FreeTrials", "Trials").replace("NewOrders", "Orders").replace("Conversions", "Conv").replace("Redemptions", "Redemp").replace("Registrations", "Reg").replace("ActivePaid", "Active").replace("Renewals", "Renew").replace("PaymentAmount", "PayAmt").replace("RefundAmount", "RefAmt").replace("InvoluntaryChurn", "InvChurn").replace("VoluntaryChurn", "VolChurn").replace("Winbacks", "Winback")
                short_period1 = period1_label.replace("Yesterday", "Yest").replace("Today", "Today").replace("Last Week", "LW").replace("This Week", "TW").replace("Last Month", "LM").replace("This Month", "TM").replace("Last Quarter", "LQ").replace("This Quarter", "TQ").replace("Last Half-Year", "LHY").replace("This Half-Year", "THY").replace("Last Year", "LY").replace("This Year", "TY")
                short_period2 = period2_label.replace("Yesterday", "Yest").replace("Today", "Today").replace("Last Week", "LW").replace("This Week", "TW").replace("Last Month", "LM").replace("This Month", "TM").replace("Last Quarter", "LQ").replace("This Quarter", "TQ").replace("Last Half-Year", "LHY").replace("This Half-Year", "THY").replace("Last Year", "LY").replace("This Year", "TY")

                if graph_type.lower() in ["pie", "donut"]:
                    fig = go.Figure(data=go.Pie(
                        labels=[f"{track} ({short_period1})", f"{track} ({short_period2})"],
                        values=[period1_value, period2_value],
                        marker=dict(colors=[colors[(metric_idx * len(selected_tracks) + track_idx) % len(colors)] for _ in range(2)]),
                        hole=0.4 if graph_type.lower() == "donut" else 0,
                        name=track
                    ))
                else:
                    if graph_type.lower() == "line":
                        fig = go.Figure()
                        fig.add_trace(go.Scatter(
                            x=[f"{track} ({short_period1})", f"{track} ({short_period2})"],
                            y=[period1_value, period2_value],
                            mode='lines+markers',
                            name=track,
                            line=dict(width=2, color=line_colors[track_idx % len(line_colors)]),
                            marker=dict(size=6, color=marker_colors[track_idx % len(marker_colors)], line=dict(width=1, color='#ffffff'))
                        ))
                    elif graph_type.lower() == "scatter":
                        fig = go.Figure()
                        fig.add_trace(go.Scatter(
                            x=[f"{track} ({short_period1})", f"{track} ({short_period2})"],
                            y=[period1_value, period2_value],
                            mode='markers',
                            name=track,
                            marker=dict(size=8, color=colors[(metric_idx * len(selected_tracks) + track_idx) % len(colors)])
                        ))
                    elif graph_type.lower() == "area":
                        fig = go.Figure()
                        fig.add_trace(go.Scatter(
                            x=[f"{track} ({short_period1})", f"{track} ({short_period2})"],
                            y=[period1_value, period2_value],
                            mode='lines',
                            fill='tozeroy',
                            name=track,
                            line=dict(width=2, color=colors[(metric_idx * len(selected_tracks) + track_idx) % len(colors)])
                        ))
                    else:  # Bar
                        fig = go.Figure()
                        fig.add_trace(go.Bar(
                            x=[f"{track} ({short_period1})", f"{track} ({short_period2})"],
                            y=[period1_value, period2_value],
                            name=track,
                            marker_color=colors[(metric_idx * len(selected_tracks) + track_idx) % len(colors)],
                            width=0.1
                        ))

                min_value = min(all_values)
                max_value = max(all_values)
                padding = (max_value - min_value) * 0.1
                y_axis_range = [max(0, min_value - padding), max_value + padding]
                range_diff = max_value - min_value
                y_axis_dtick = 10000 if range_diff > 50000 else (5000 if range_diff > 10000 else 1000)

                # Compute the chart title outside the f-string to avoid backslash in f-string
                transformed_metric = metric.replace('TotalChurn', 'Churn')
                transformed_metric = re.sub(r'([A-Z])', r' \1', transformed_metric).strip()
                chart_title = f"{transformed_metric} Comparison"

                layout = {
                    "title": chart_title,
                    "titlefont": dict(size=18, color='#1f2937', family='Inter'),
                    "margin": dict(t=80, b=80, l=60, r=50),
                    "plot_bgcolor": 'rgba(0,0,0,0)',
                    "paper_bgcolor": 'rgba(0,0,0,0)',
                    "legend": dict(x=1, y=1, bgcolor='rgba(255,255,255,0.8)'),
                    "height": 450
                }
                if graph_type.lower() == "bar":
                    layout["barmode"] = "group"
                    layout["xaxis"] = dict(tickfont=dict(size=10, color='#718096'), tickangle=-45, automargin=True, showgrid=False)
                    layout["yaxis"] = dict(
                        title='Value',
                        titlefont=dict(size=14, color='#1f2937'),
                        tickfont=dict(size=8, color='#718096'),
                        showticklabels=False,
                        ticks='',
                        range=[0, y_axis_range[1]],
                        automargin=True,
                        showgrid=False
                    )
                elif graph_type.lower() == "line":
                    layout["xaxis"] = dict(tickfont=dict(size=8, color='#718096'), tickangle=-45, automargin=True, showgrid=False)
                    layout["yaxis"] = dict(
                        title='Value',
                        titlefont=dict(size=14, color='#1f2937'),
                        tickfont=dict(size=8, color='#718096'),
                        tickformat='s',
                        dtick=y_axis_dtick,
                        range=y_axis_range,
                        automargin=True,
                        showgrid=False
                    )
                elif graph_type.lower() == "scatter":
                    layout["xaxis"] = dict(tickfont=dict(size=10, color='#718096'), tickangle=-45, automargin=True, showgrid=False)
                    layout["yaxis"] = dict(
                        title='Value',
                        titlefont=dict(size=14, color='#1f2937'),
                        tickfont=dict(size=8, color='#718096'),
                        range=y_axis_range,
                        automargin=True,
                        showgrid=False
                    )
                elif graph_type.lower() == "area":
                    layout["xaxis"] = dict(tickfont=dict(size=10, color='#718096'), tickangle=-45, automargin=True, showgrid=False)
                    layout["yaxis"] = dict(
                        title='Value',
                        titlefont=dict(size=14, color='#1f2937'),
                        tickfont=dict(size=8, color='#718096'),
                        range=y_axis_range,
                        automargin=True,
                        showgrid=False
                    )
                elif graph_type.lower() in ["pie", "donut"]:
                    layout["xaxis"] = dict(visible=False)
                    layout["yaxis"] = dict(visible=False)

                fig.update_layout(**layout)

                with col8 if metric_idx % 2 == 0 else col9:
                    st.markdown('<div class="chart-container">', unsafe_allow_html=True)
                    st.plotly_chart(fig, use_container_width=True)
                    st.markdown('</div>', unsafe_allow_html=True)

                table_rows.append({
                    "track": track,
                    "metric": metric,
                    "period1_value": period1_value,
                    "period2_value": period2_value,
                    "value_change": value_change,
                    "percent_change": percent_change
                })

        # Summary Table
        st.markdown('<div class="chart-container"><h2 class="text-xl font-semibold text-gray-800 mb-4">Summary of Changes</h2>', unsafe_allow_html=True)
        table_html = f"""
        <table class="min-w-full divide-y divide-gray-200">
            <thead class="bg-gray-50">
                <tr>
                    <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Track</th>
                    <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Metric</th>
                    <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">{period1_label}</th>
                    <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">{period2_label}</th>
                    <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Change</th>
                    <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">% Change</th>
                </tr>
            </thead>
            <tbody class="bg-white divide-y divide-gray-200">
        """
        for row in table_rows:
            # Format metric name for display in the table
            display_metric = row['metric'].replace('TotalChurn', 'Churn')
            display_metric = re.sub(r'([A-Z])', r' \1', display_metric).strip()
            table_html += f"""
            <tr>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-900">{row['track']}</td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-900">{display_metric}</td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-900">{row['period1_value']:,}</td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-900">{row['period2_value']:,}</td>
                <td class="px-6 py-4 whitespace-nowrap text-sm {'change-indicator-up' if row['value_change'] >= 0 else 'change-indicator-down'}">
                    {'+' if row['value_change'] >= 0 else ''}{row['value_change']:,}
                </td>
                <td class="px-6 py-4 whitespace-nowrap text-sm {'change-indicator-up' if row['percent_change'] >= 0 else 'change-indicator-down'}">
                    {'+' if row['percent_change'] >= 0 else ''}{row['percent_change']:.2f}%
                </td>
            </tr>
            """
        table_html += """
            </tbody>
        </table>
        """
        st.markdown(table_html, unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

# Footer
st.markdown('<div class="footer">Last Updated: April 06, 2025 | Powered by: Plotly.js</div>', unsafe_allow_html=True)

# Clean up connections on app shutdown (simulated via session state cleanup)
def cleanup():
    if 'connection_objects' in st.session_state:
        for conn_type, conn in st.session_state.connection_objects.items():
            try:
                if conn_type in ["PostgreSQL", "MySQL", "Microsoft SQL Server", "Snowflake", "SQLite", "Oracle", "Neon", "Supabase", "RenderDB"] and conn:
                    conn.dispose()
                elif conn_type == "MongoDB" and conn:
                    conn.close()
            except:
                pass
        st.session_state.connection_objects = {}

if st.session_state.get('shutdown', False):
    cleanup()
