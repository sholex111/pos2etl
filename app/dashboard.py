# app/dashboard.py

import streamlit as st
import pandas as pd
from utils import get_db_engine, setup_logging

# Setup logger for this module
logger = setup_logging()

# --- Page Configuration ---
st.set_page_config(
    page_title="E-Commerce Dashboard",
    page_icon="🛒",
    layout="wide"
)

st.title("🛒 E-Commerce Sales Dashboard")
st.markdown("This dashboard displays deduplicated data from the `core_ecomm_sales` table.")

# --- Database Connection Caching ---

@st.cache_resource
def get_engine():
    """Get a cached database engine."""
    logger.info("Dashboard: Creating new database engine connection.")
    return get_db_engine()

@st.cache_data(ttl=300)
def load_data():
    """Loads all data from the core_ecomm_sales table."""
    try:
        logger.info("Dashboard: Cache miss. Loading data from core_ecomm_sales.")
        engine = get_engine()
        # Query the new core table
        query = "SELECT * FROM core_ecomm_sales ORDER BY invoicedate DESC;"
        df = pd.read_sql(query, con=engine)
        
        # Ensure 'invoicedate' is a datetime object for charting
        if 'invoicedate' in df.columns:
            df['invoicedate'] = pd.to_datetime(df['invoicedate'])
        
        logger.info(f"Dashboard: Loaded {len(df)} rows.")
        return df
    except Exception as e:
        logger.error(f"Dashboard: Error loading data: {e}")
        st.error(f"Error loading data from database. Please check logs. Error: {e}")
        return pd.DataFrame() # Return empty DataFrame on error

# --- Main Dashboard Logic ---
try:
    df = load_data()

    if df.empty:
        st.warning("No data found in the data warehouse. Please run the ETL process or add CSV files.")
        st.stop()

    # --- KPIs ---
    st.header("Key Metrics")
    
    # Calculate total sale per line item
    df['total_sale'] = df['quantity'] * df['price']
    
    # Calculate high-level metrics
    total_revenue = df['total_sale'].sum()
    total_profit = df['profit'].sum()
    total_orders = df['invoice'].nunique() # Count unique invoices
    avg_margin = df['margin'].mean()

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Revenue", f"${total_revenue:,.2f}")
    col2.metric("Total Profit", f"${total_profit:,.2f}")
    col3.metric("Total Orders", f"{total_orders:,}")
    col4.metric("Average Margin", f"{avg_margin:.2%}") # Format as percentage

    st.markdown("---")

    # --- Charts ---
    st.header("Visualizations")
    
    col_chart1, col_chart2 = st.columns(2)

    with col_chart1:
        # Sales and Profit over time
        st.subheader("Daily Revenue & Profit")
        if 'invoicedate' in df.columns:
            # Resample by day
            sales_over_time = df.set_index('invoicedate').resample('D')[['total_sale', 'profit']].sum()
            st.line_chart(sales_over_time)
        else:
            st.warning("InvoiceDate column not found for time series chart.")

    with col_chart2:
        # Top products
        st.subheader("Top 10 Products (by Quantity)")
        top_products = df.groupby('description')['quantity'].sum().nlargest(10)
        st.bar_chart(top_products)

    col_chart3, col_chart4 = st.columns(2)

    with col_chart3:
        # Sales by Country
        st.subheader("Top 10 Countries (by Revenue)")
        sales_by_country = df.groupby('country')['total_sale'].sum().nlargest(10)
        st.bar_chart(sales_by_country)

    with col_chart4:
        # Sales by Category
        st.subheader("Revenue by Category")
        sales_by_category = df.groupby('category')['total_sale'].sum().nlargest(10)
        st.bar_chart(sales_by_category)


    # --- Raw Data ---
    st.header("Raw Data Explorer")
    st.dataframe(df)
    
    # Add a button to refresh data
    if st.button('Refresh Data'):
        st.cache_data.clear() # Clear the data cache
        st.rerun()

except Exception as e:
    logger.error(f"An error occurred in the Streamlit dashboard: {e}")
    st.error("An error occurred while rendering the dashboard. Please check the logs.")