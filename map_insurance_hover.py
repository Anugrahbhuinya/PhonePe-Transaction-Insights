import os
import json
import mysql.connector

db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Munda403@',
    'database': 'phonepe'
}

base_path = r"C:\Users\ASUS\Desktop\PhonPe\pulse\data\map\insurance\hover\country\india"

conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

insert_query = """
    INSERT INTO map_insurance_hover
    (state, year, quarter, district, transaction_count, transaction_amount)
    VALUES (%s, %s, %s, %s, %s, %s)
"""

# ---------- NATIONAL LEVEL ----------

for year in os.listdir(base_path):
    year_path = os.path.join(base_path, year)
    if not os.path.isdir(year_path):
        continue

    for file in os.listdir(year_path):
        if file.endswith(".json"):
            json_path = os.path.join(year_path, file)
            quarter = file.strip(".json")

            with open(json_path, 'r') as f:
                data = json.load(f)

            hover_data = data.get('data', {}).get('hoverDataList', [])

            if hover_data and isinstance(hover_data, list):
                for item in hover_data:
                    district = item.get('name')
                    metric_list = item.get('metric', [])

                    if isinstance(metric_list, list) and len(metric_list) > 0:
                        metric = metric_list[0]
                        count = metric.get('count')
                        amount = metric.get('amount')
                    else:
                        count = None
                        amount = None

                    values = (
                        "All-India",
                        int(year),
                        f"Q{quarter}",
                        district,
                        int(count) if count is not None else None,
                        float(amount) if amount is not None else None
                    )
                    print("Inserting National Hover:", values)
                    cursor.execute(insert_query, values)

# ---------- STATE LEVEL ----------

state_root = os.path.join(base_path, "state")
if os.path.exists(state_root):
    for state in os.listdir(state_root):
        state_path = os.path.join(state_root, state)
        if not os.path.isdir(state_path):
            continue

        for year in os.listdir(state_path):
            year_path = os.path.join(state_path, year)
            if not os.path.isdir(year_path):
                continue

            for file in os.listdir(year_path):
                if file.endswith(".json"):
                    json_path = os.path.join(year_path, file)
                    quarter = file.strip(".json")

                    with open(json_path, 'r') as f:
                        data = json.load(f)

                    hover_data = data.get('data', {}).get('hoverDataList', [])

                    if hover_data and isinstance(hover_data, list):
                        for item in hover_data:
                            district = item.get('name')
                            metric_list = item.get('metric', [])

                            if isinstance(metric_list, list) and len(metric_list) > 0:
                                metric = metric_list[0]
                                count = metric.get('count')
                                amount = metric.get('amount')
                            else:
                                count = None
                                amount = None

                            values = (
                                state,
                                int(year),
                                f"Q{quarter}",
                                district,
                                int(count) if count is not None else None,
                                float(amount) if amount is not None else None
                            )
                            print("Inserting State Hover:", values)
                            cursor.execute(insert_query, values)

conn.commit()
cursor.close()
conn.close()
print(" map_insurance_hover data load complete.")
