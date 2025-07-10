import streamlit as st
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from db_connect import run_query

st.set_page_config(page_title="📊 PhonePe Dashboard", layout="wide")

st.sidebar.title("🔎 Filter Data")

# ------------------------------------
# Fetch dynamic filter options
# ------------------------------------

years = run_query("SELECT DISTINCT year FROM aggregated_transaction ORDER BY year;")['year'].dropna().astype(int).tolist()
quarters = run_query("SELECT DISTINCT quarter FROM aggregated_transaction ORDER BY quarter;")['quarter'].tolist()
states = run_query("SELECT DISTINCT state FROM aggregated_transaction WHERE state <> 'All-India' ORDER BY state;")['state'].tolist()

# Add "All" option
years.insert(0, "All")
quarters.insert(0, "All")
states.insert(0, "All")

# ------------------------------------
# Sidebar filters
# ------------------------------------

year_filter = st.sidebar.selectbox("📅 Year", years)
quarter_filter = st.sidebar.selectbox("🗓️ Quarter", quarters)
state_filter = st.sidebar.selectbox("🌍 State", states)

# Build WHERE clause
where_clauses = []
params = []

if year_filter != "All":
    where_clauses.append("year = %s")
    params.append(year_filter)

if quarter_filter != "All":
    where_clauses.append("quarter = %s")
    params.append(quarter_filter)

if state_filter != "All":
    where_clauses.append("state = %s")
    params.append(state_filter)

where_sql = ""
if where_clauses:
    where_sql = "WHERE " + " AND ".join(where_clauses)

st.title("📊 PhonePe Dashboard (Filtered View)")

# ------------------------------------
# 1. Top 10 States by Transaction Value
# ------------------------------------

query1 = f"""
SELECT
    state,
    SUM(count) AS total_transactions,
    SUM(amount) AS total_value
FROM aggregated_transaction
WHERE state <> 'All-India'
{(' AND ' + ' AND '.join(where_clauses)) if where_clauses else ''}
GROUP BY state
ORDER BY total_value DESC
LIMIT 10;
"""

df1 = run_query(query1, params)
if not df1.empty:
    fig1, ax1 = plt.subplots(figsize=(8,5))
    sns.barplot(x='total_value', y='state', data=df1, palette='viridis', ax=ax1)
    ax1.set_title("Top 10 States by Transaction Value")
    st.pyplot(fig1)

# ------------------------------------
# 2. Top 10 States by Insurance Premium
# ------------------------------------

query2 = f"""
SELECT
    state,
    SUM(transaction_amount) AS total_premium
FROM map_insurance_hover
WHERE state <> 'All-India'
{(' AND ' + ' AND '.join(where_clauses)) if where_clauses else ''}
GROUP BY state
ORDER BY total_premium DESC
LIMIT 10;
"""

df2 = run_query(query2, params)
if not df2.empty:
    fig2, ax2 = plt.subplots(figsize=(8,8))
    ax2.pie(df2['total_premium'], labels=df2['state'], autopct='%1.1f%%', startangle=140)
    ax2.set_title("Insurance Premium Distribution (Top 10 States)")
    st.pyplot(fig2)

# ------------------------------------
# 3. App Opens Trend Over Time
# ------------------------------------

query3 = f"""
SELECT
    year,
    quarter,
    SUM(app_opens) AS total_app_opens
FROM map_user
{where_sql}
GROUP BY year, quarter
ORDER BY year, quarter;
"""

df3 = run_query(query3, params)
if not df3.empty:
    df3['period'] = df3['year'].astype(str) + " " + df3['quarter']
    fig3, ax3 = plt.subplots(figsize=(10,4))
    sns.lineplot(x='period', y='total_app_opens', data=df3, marker='o', ax=ax3)
    ax3.set_title("App Opens Over Time")
    ax3.tick_params(axis='x', rotation=45)
    st.pyplot(fig3)

# ------------------------------------
# 4. Top 10 Districts by Transaction Value
# ------------------------------------

query4 = f"""
SELECT
    district,
    SUM(transaction_amount) AS total_value
FROM map_transaction
{where_sql}
GROUP BY district
ORDER BY total_value DESC
LIMIT 10;
"""

df4 = run_query(query4, params)
if not df4.empty:
    fig4, ax4 = plt.subplots(figsize=(8,5))
    sns.barplot(x='total_value', y='district', data=df4, palette='magma', ax=ax4)
    ax4.set_title("Top 10 Districts by Transaction Value")
    st.pyplot(fig4)

# ------------------------------------
# 5. Top 10 Districts by Registered Users
# ------------------------------------

query5 = f"""
SELECT
    district,
    SUM(registered_users) AS total_users
FROM map_user
{where_sql}
GROUP BY district
ORDER BY total_users DESC
LIMIT 10;
"""

df5 = run_query(query5, params)
if not df5.empty:
    fig5, ax5 = plt.subplots(figsize=(8,5))
    sns.barplot(x='total_users', y='district', data=df5, palette='coolwarm', ax=ax5)
    ax5.set_title("Top 10 Districts by Registered Users")
    st.pyplot(fig5)

st.success(" Dashboard loaded with filters!")
