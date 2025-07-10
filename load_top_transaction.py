import os
import json
import mysql.connector

# ---------- CONFIG ----------

db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Munda403@',
    'database': 'phonepe'
}

base_path = r"C:\Users\ASUS\Desktop\PhonPe\pulse\data\top\transaction\country\india"

conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

insert_query = """
    INSERT INTO top_transaction
    (state, year, quarter, district, transaction_count, transaction_amount)
    VALUES (%s, %s, %s, %s, %s, %s)
"""

# ---------- LOAD NATIONAL DATA ----------

for year in os.listdir(base_path):
    year_path = os.path.join(base_path, year)
    if not os.path.isdir(year_path):
        continue

    for file in os.listdir(year_path):
        if file.endswith(".json") and file.replace('.json','') in ['2','3','4']:
            json_path = os.path.join(year_path, file)
            quarter = file.strip(".json")

            with open(json_path, 'r') as f:
                data = json.load(f)

            districts = data.get('data', {}).get('districts', [])

            if districts and isinstance(districts, list):
                for district_data in districts:
                    district = district_data.get('entityName', None)
                    metric = district_data.get('metric', {})
                    count = metric.get('count', None)
                    amount = metric.get('amount', None)

                    values = (
                        "All-India",
                        int(year),
                        f"Q{quarter}",
                        district,
                        int(count) if count is not None else None,
                        float(amount) if amount is not None else None
                    )

                    print("Inserting National:", values)
                    cursor.execute(insert_query, values)
            else:
                print(f"🚫 No district data found in: {json_path}")

# ---------- LOAD STATE DATA ----------

state_path_root = os.path.join(base_path, "state")
if os.path.exists(state_path_root):
    for state in os.listdir(state_path_root):
        state_path = os.path.join(state_path_root, state)
        if not os.path.isdir(state_path):
            continue

        for year in os.listdir(state_path):
            year_path = os.path.join(state_path, year)
            if not os.path.isdir(year_path):
                continue

            for file in os.listdir(year_path):
                if file.endswith(".json") and file.replace('.json','') in ['2','3','4']:
                    json_path = os.path.join(year_path, file)
                    quarter = file.strip(".json")

                    with open(json_path, 'r') as f:
                        data = json.load(f)

                    districts = data.get('data', {}).get('districts', [])

                    if districts and isinstance(districts, list):
                        for district_data in districts:
                            district = district_data.get('entityName', None)
                            metric = district_data.get('metric', {})
                            count = metric.get('count', None)
                            amount = metric.get('amount', None)

                            values = (
                                state,
                                int(year),
                                f"Q{quarter}",
                                district,
                                int(count) if count is not None else None,
                                float(amount) if amount is not None else None
                            )

                            print("Inserting State:", values)
                            cursor.execute(insert_query, values)
                    else:
                        print(f" No district data found in: {json_path}")

conn.commit()
cursor.close()
conn.close()
print(" top_transaction data load complete.")
