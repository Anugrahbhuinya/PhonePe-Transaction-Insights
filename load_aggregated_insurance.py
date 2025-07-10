import os
import json
import mysql.connector
import datetime

db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Munda403@',
    'database': 'phonepe'
}

# ✅ Correct path as per your screenshot:
base_path = r"C:\Users\ASUS\Desktop\PhonPe\pulse\data\aggregated\insurance\country\india"

conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

insert_query = """
    INSERT INTO aggregated_insurance
    (state, year, quarter, insurance_type, count, amount)
    VALUES (%s, %s, %s, %s, %s, %s)
"""

# ---------- NATIONAL LEVEL DATA ----------

for year in ['2020', '2021', '2022', '2023', '2024']:
    year_path = os.path.join(base_path, year)
    if not os.path.isdir(year_path):
        continue

    for file in os.listdir(year_path):
        if file.endswith(".json") and file.replace('.json','') in ['2','3','4']:
            json_path = os.path.join(year_path, file)
            quarter = file.strip(".json")

            with open(json_path, 'r') as f:
                data = json.load(f)

            transaction_data = data['data'].get('transactionData', [])

            for tx in transaction_data:
                insurance_type = tx.get('name', None)
                instruments = tx.get('paymentInstruments', [])

                count = None
                amount = None

                if instruments:
                    count = instruments[0].get('count', None)
                    amount = instruments[0].get('amount', None)

                values = (
                    "All-India",
                    int(year),
                    f"Q{quarter}",
                    insurance_type,
                    int(count) if count is not None else None,
                    float(amount) if amount is not None else None
                )

                print("Inserting National:", values)
                cursor.execute(insert_query, values)

# ---------- STATE LEVEL DATA ----------

state_base = os.path.join(base_path, "state")
for state in os.listdir(state_base):
    state_path = os.path.join(state_base, state)
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

                transaction_data = data['data'].get('transactionData', [])

                for tx in transaction_data:
                    insurance_type = tx.get('name', None)
                    instruments = tx.get('paymentInstruments', [])

                    count = None
                    amount = None

                    if instruments:
                        count = instruments[0].get('count', None)
                        amount = instruments[0].get('amount', None)

                    values = (
                        state,
                        int(year),
                        f"Q{quarter}",
                        insurance_type,
                        int(count) if count is not None else None,
                        float(amount) if amount is not None else None
                    )

                    print("Inserting State:", values)
                    cursor.execute(insert_query, values)

conn.commit()
cursor.close()
conn.close()
print(" aggregated_insurance data load complete.")
