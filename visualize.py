from db_connect import run_query
import matplotlib.pyplot as plt
import seaborn as sns
import os

# Create plots/ folder if not exists
os.makedirs("plots", exist_ok=True)

# -------------------------------
# 1. Top 10 States by Transaction Value
# -------------------------------

query1 = """
SELECT
    state,
    SUM(count) AS total_transactions,
    SUM(amount) AS total_value
FROM aggregated_transaction
WHERE state <> 'All-India'
GROUP BY state
ORDER BY total_value DESC
LIMIT 10;
"""

df1 = run_query(query1)
print("\nTop 10 States by Transaction Value:\n", df1)

plt.figure(figsize=(10,6))
sns.barplot(x='total_value', y='state', data=df1, palette='viridis')
plt.title('Top 10 States by Total Transaction Value')
plt.xlabel('Total Transaction Value (₹)')
plt.ylabel('State')
plt.tight_layout()
plt.savefig("plots/top_states_transaction_value.png")
plt.show()

# -------------------------------
# 2. Top 10 States by Insurance Premium Collected
# -------------------------------

query2 = """
SELECT
    state,
    SUM(transaction_amount) AS total_premium
FROM map_insurance_hover
WHERE state <> 'All-India'
GROUP BY state
ORDER BY total_premium DESC
LIMIT 10;
"""

df2 = run_query(query2)
print("\nTop 10 States by Insurance Premium Collected:\n", df2)

plt.figure(figsize=(8,8))
plt.pie(df2['total_premium'], labels=df2['state'], autopct='%1.1f%%', startangle=140)
plt.title('Insurance Premium Distribution (Top 10 States)')
plt.savefig("plots/top_states_insurance_premium.png")
plt.show()

# -------------------------------
# 3. App Opens Trend Over Time
# -------------------------------

query3 = """
SELECT
    year,
    quarter,
    SUM(app_opens) AS total_app_opens
FROM map_user
GROUP BY year, quarter
ORDER BY year, quarter;
"""

df3 = run_query(query3)
print("\nApp Opens Trend Over Time:\n", df3)

df3['period'] = df3['year'].astype(str) + " " + df3['quarter']

plt.figure(figsize=(12,5))
sns.lineplot(x='period', y='total_app_opens', data=df3, marker='o')
plt.title('App Opens Trend Over Time')
plt.xlabel('Period')
plt.ylabel('Total App Opens')
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("plots/app_opens_trend.png")
plt.show()

# -------------------------------
# 4. Top 10 Districts by Transaction Value
# -------------------------------

query4 = """
SELECT
    district,
    SUM(transaction_amount) AS total_value
FROM map_transaction
GROUP BY district
ORDER BY total_value DESC
LIMIT 10;
"""

df4 = run_query(query4)
print("\nTop 10 Districts by Transaction Value:\n", df4)

plt.figure(figsize=(10,6))
sns.barplot(x='total_value', y='district', data=df4, palette='magma')
plt.title('Top 10 Districts by Transaction Value')
plt.xlabel('Total Transaction Value (₹)')
plt.ylabel('District')
plt.tight_layout()
plt.savefig("plots/top_districts_transaction_value.png")
plt.show()

# -------------------------------
# 5. Top 10 Districts by Registered Users
# -------------------------------

query5 = """
SELECT
    district,
    SUM(registered_users) AS total_users
FROM map_user
GROUP BY district
ORDER BY total_users DESC
LIMIT 10;
"""

df5 = run_query(query5)
print("\nTop 10 Districts by Registered Users:\n", df5)

plt.figure(figsize=(10,6))
sns.barplot(x='total_users', y='district', data=df5, palette='coolwarm')
plt.title('Top 10 Districts by Registered Users')
plt.xlabel('Total Registered Users')
plt.ylabel('District')
plt.tight_layout()
plt.savefig("plots/top_districts_registered_users.png")
plt.show()

print("\n✅ All plots generated and saved in /plots folder (excluding 'All-India').")
