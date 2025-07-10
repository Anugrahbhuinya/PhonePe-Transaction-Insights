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

base_path = r"C:\Users\ASUS\Desktop\PhonPe\pulse\data\map\transaction\hover\country\india"

conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

insert_query = """
    INSERT INTO map_transaction
    (state, year, quarter, district, transaction_count, transaction_amount)
    VALUES (%s, %s, %s, %s, %s, %s)
"""

# ---------- LOAD NATIONAL DATA ----------

for year in os.listdir(base_path):
    year_path = os.path.join(base_path, year)
    if not os.path.isdir(year_path):
        continue

    for file in os.listdir(year_path):
        if file.endswith(".json") and file.replace('.json','') in ['1','2','3','4']:
            json_path = os.path.join(year_path, file)
            quarter = file.strip(".json")

            with open(json_path, 'r') as f:
                data = json.load(f)

            hover_data_list = data.get('data', {}).get('hoverDataList', [])

            if hover_data_list and isinstance(hover_data_list, list):
                for district_data in hover_data_list:
                    district = district_data.get('name', None)
                    
                    metric_list = district_data.get('metric', [])
                    if isinstance(metric_list, list) and len(metric_list) > 0:
                        metric = metric_list[0]
                    else:
                        metric = {}

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
                print(f"🚫 No hover data found in: {json_path}")

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
                if file.endswith(".json") and file.replace('.json','') in ['1','2','3','4']:
                    json_path = os.path.join(year_path, file)
                    quarter = file.strip(".json")

                    with open(json_path, 'r') as f:
                        data = json.load(f)

                    hover_data_list = data.get('data', {}).get('hoverDataList', [])

                    if hover_data_list and isinstance(hover_data_list, list):
                        for district_data in hover_data_list:
                            district = district_data.get('name', None)
                            
                            metric_list = district_data.get('metric', [])
                            if isinstance(metric_list, list) and len(metric_list) > 0:
                                metric = metric_list[0]
                            else:
                                metric = {}

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
                        print(f" No hover data found in: {json_path}")

conn.commit()
cursor.close()
conn.close()
print(" map_transaction data load complete.")
