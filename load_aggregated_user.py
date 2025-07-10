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

base_path = r"C:\Users\ASUS\Desktop\PhonPe\pulse\data\aggregated\user\country\india"

conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

insert_query = """
    INSERT INTO aggregated_user
    (state, year, quarter, brand, count, percentage)
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

            user_data = data.get('data', {}).get('usersByDevice', [])

            if user_data and isinstance(user_data, list):
                for user in user_data:
                    brand = user.get('brand', None)
                    count = user.get('count', None)
                    percentage = user.get('percentage', None)

                    values = (
                        "All-India",
                        int(year),
                        f"Q{quarter}",
                        brand,
                        int(count) if count is not None else None,
                        float(percentage) if percentage is not None else None
                    )

                    print("Inserting National:", values)
                    cursor.execute(insert_query, values)
            else:
                print(f"🚫 No user data found in: {json_path}")

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

                    user_data = data.get('data', {}).get('usersByDevice', [])

                    if user_data and isinstance(user_data, list):
                        for user in user_data:
                            brand = user.get('brand', None)
                            count = user.get('count', None)
                            percentage = user.get('percentage', None)

                            values = (
                                state,
                                int(year),
                                f"Q{quarter}",
                                brand,
                                int(count) if count is not None else None,
                                float(percentage) if percentage is not None else None
                            )

                            print("Inserting State:", values)
                            cursor.execute(insert_query, values)
                    else:
                        print(f" No user data found in: {json_path}")

conn.commit()
cursor.close()
conn.close()
print(" aggregated_user data load complete.")
