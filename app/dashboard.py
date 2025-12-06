# app/dashboard.py

import streamlit as st
import pandas as pd
import altair as alt
from utils import get_db_engine, setup_logging
import time 

# Setup logger for this module
logger = setup_logging()

# --- Page Configuration ---
st.set_page_config(
    page_title="E-Commerce Dashboard",
    page_icon="🛒",
    layout="wide"
)

# --- Streamlit Session State Initialization ---
# This ensures that the page reloads cleanly when the cache is cleared.
if 'data_refreshed' not in st.session_state:
    st.session_state.data_refreshed = False

# --- Database Connection Caching ---
@st.cache_resource
def get_engine():
    """Get a cached database engine."""
    logger.info("Dashboard: Creating new database engine connection.")
    return get_db_engine()

# --- Data Loading Function (CACHED) ---
@st.cache_data(ttl=300)
def load_data():
    """Loads all data from the core_ecomm_sales table."""
    try:
        logger.info("Dashboard: Cache miss or manually cleared. Loading data from core_ecomm_sales.")
        
        # Simulate a database load time for better UX when refreshing
        time.sleep(0.5) 

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


# --- Refresh Button Callback ---
def refresh_data_callback():
    """Clears the data cache and sets state to force a full rerun."""
    st.cache_data.clear()
    st.session_state.data_refreshed = True
    st.rerun()

# --- HEADER and REFRESH UI ---
col_title, col_button = st.columns([0.8, 0.2])

with col_title:
    st.title("🛒 E-Commerce Sales Dashboard")
    st.markdown("This dashboard displays deduplicated data from the `core_ecomm_sales` table.")

with col_button:
    # Use a callback function for clean cache clearing
    st.button(
        'Force Data Refresh', 
        on_click=refresh_data_callback, 
        type="primary",
        help="Click to clear the data cache and pull fresh data from the PostgreSQL warehouse."
    )
    if st.session_state.data_refreshed:
        st.success("Data cache cleared! Displaying the latest data from the ETL run.")
        st.session_state.data_refreshed = False # Reset state after message is displayed


# --- Main Dashboard Logic ---
try:
    df = load_data()

    if df.empty:
        st.warning("No data found in the data warehouse. Please run the ETL process or add CSV files.")
        st.stop()

    # --- Pre-Calculations ---
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
        # 1. Daily Revenue and Profit (Keep)
        st.subheader("Daily Revenue & Profit")
        if 'invoicedate' in df.columns:
            sales_over_time = df.set_index('invoicedate').resample('D')[['total_sale', 'profit']].sum().reset_index()
            
            # Use Altair for the dual line chart
            chart = alt.Chart(sales_over_time).mark_line().encode(
                x=alt.X('invoicedate:T', title='Date'),
                y=alt.Y('total_sale:Q', title='Revenue'),
                color=alt.value('darkblue')
            ).interactive() + alt.Chart(sales_over_time).mark_line().encode(
                x='invoicedate:T',
                y=alt.Y('profit:Q', title='Profit'),
                color=alt.value('green')
            ).interactive()
            st.altair_chart(chart, use_container_width=True)

        else:
            st.warning("InvoiceDate column not found for time series chart.")

    with col_chart2:
        # 2. Top 10 Products by Qty (Filter NaN)
        st.subheader("Top 10 Products (by Quantity)")
        # Filter out NaN descriptions before grouping
        df_filtered_products = df.dropna(subset=['description'])
        
        top_products = df_filtered_products.groupby('description')['quantity'].sum().nlargest(10).reset_index(name='Total Quantity')
        chart = alt.Chart(top_products).mark_bar().encode(
            y=alt.Y('description', sort='-x', title='Product'),
            x=alt.X('Total Quantity', title='Quantity Sold'),
            tooltip=['description', 'Total Quantity']
        ).properties(height=350)
        st.altair_chart(chart, use_container_width=True)


    col_chart3, col_chart4 = st.columns(2)

    with col_chart3:
        # 3. Top 10 Countries by Profit (Changed from Revenue)
        st.subheader("Top 10 Countries (by Profit)")
        # Group by Profit
        sales_by_country = df.groupby('country')['profit'].sum().nlargest(10).reset_index(name='Total Profit')
        chart = alt.Chart(sales_by_country).mark_bar().encode(
            x=alt.X('Total Profit', title='Profit ($)'),
            y=alt.Y('country', sort='-x', title='Country'),
            color=alt.value('#E879F9'),
            tooltip=['country', alt.Tooltip('Total Profit', format='$,.0f')]
        ).properties(height=350)
        st.altair_chart(chart, use_container_width=True)

    with col_chart4:
        # 4. Horizontal Bar Chart: Categories by Revenue (Filter 'unclassified')
        st.subheader("Top Categories (by Revenue)")
        
        # Filter out 'unclassified' and NaN categories
        df_filtered_category = df[(df['category'] != 'unclassified') & (df['category'].notna())]
        
        # Aggregate revenue and get top 10
        sales_by_category = df_filtered_category.groupby('category')['total_sale'].sum().nlargest(10).reset_index(name='Total Revenue')
        
        if not sales_by_category.empty:
            # Create Altair Horizontal Bar Chart, ordered largest to smallest
            chart = alt.Chart(sales_by_category).mark_bar().encode(
                # Use 'category' for Y-axis (vertical) and sort it by the Revenue value
                y=alt.Y('category', sort='-x', title='Category'), 
                x=alt.X('Total Revenue', title='Revenue ($)'),
                color=alt.value('#10B981'), # Tailwind green color
                tooltip=['category', alt.Tooltip('Total Revenue', format='$,.0f')]
            ).properties(height=350)
            st.altair_chart(chart, use_container_width=True)
        else:
            st.warning("No categorized data found (after excluding 'unclassified').")


    # --- Raw Data ---
    st.header("Raw Data Explorer")
    st.dataframe(df, use_container_width=True)
    

except Exception as e:
    logger.error(f"An error occurred in the Streamlit dashboard: {e}")
    st.error("An error occurred while rendering the dashboard. Please check the logs.")