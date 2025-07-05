import os
import json
import mysql.connector

db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Munda403@',
    'database': 'phonepe'
}

base_path = r"C:\Users\ASUS\Desktop\PhonPe\pulse\data\map\insurance\country\india"

conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

insert_query = """
    INSERT INTO map_insurance_grid
    (state, year, quarter, district, latitude, longitude, metric)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
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

            rows = data.get('data', {}).get('data', {}).get('data', [])

            if rows and isinstance(rows, list):
                for row in rows:
                    lat = row[0]
                    lng = row[1]
                    metric = row[2]
                    district = row[3]

                    values = (
                        "All-India",
                        int(year),
                        f"Q{quarter}",
                        district,
                        float(lat),
                        float(lng),
                        float(metric)
                    )
                    print("Inserting National Grid:", values)
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

                    rows = data.get('data', {}).get('data', {}).get('data', [])

                    if rows and isinstance(rows, list):
                        for row in rows:
                            lat = row[0]
                            lng = row[1]
                            metric = row[2]
                            district = row[3]

                            values = (
                                state,
                                int(year),
                                f"Q{quarter}",
                                district,
                                float(lat),
                                float(lng),
                                float(metric)
                            )
                            print("Inserting State Grid:", values)
                            cursor.execute(insert_query, values)

conn.commit()
cursor.close()
conn.close()
print("✅ map_insurance_grid data load complete.")
