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

base_path = r"C:\Users\ASUS\Desktop\PhonPe\pulse\data\top\insurance\country\india"

conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

insert_query = """
    INSERT INTO top_insurance
    (state, year, quarter, entity_level, entity_name, transaction_count, transaction_amount)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
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

            data_section = data.get('data', {})

            for entity_level in ['districts', 'pincodes']:
                entity_list = data_section.get(entity_level, [])
                if entity_list and isinstance(entity_list, list):
                    for entity_data in entity_list:
                        name = entity_data.get('entityName', None)
                        metric = entity_data.get('metric', {})
                        count = metric.get('count', None)
                        amount = metric.get('amount', None)

                        values = (
                            "All-India",
                            int(year),
                            f"Q{quarter}",
                            entity_level[:-1],  # e.g. 'district'
                            name,
                            int(count) if count is not None else None,
                            float(amount) if amount is not None else None
                        )
                        print("Inserting National:", values)
                        cursor.execute(insert_query, values)

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

                    data_section = data.get('data', {})

                    for entity_level in ['districts', 'pincodes']:
                        entity_list = data_section.get(entity_level, [])
                        if entity_list and isinstance(entity_list, list):
                            for entity_data in entity_list:
                                name = entity_data.get('entityName', None)
                                metric = entity_data.get('metric', {})
                                count = metric.get('count', None)
                                amount = metric.get('amount', None)

                                values = (
                                    state,
                                    int(year),
                                    f"Q{quarter}",
                                    entity_level[:-1],  # e.g. 'district'
                                    name,
                                    int(count) if count is not None else None,
                                    float(amount) if amount is not None else None
                                )
                                print("Inserting State:", values)
                                cursor.execute(insert_query, values)

conn.commit()
cursor.close()
conn.close()
print("✅ top_insurance data load complete.")
