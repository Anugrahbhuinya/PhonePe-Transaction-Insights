import os
import json
import mysql.connector

db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Munda403@',
    'database': 'phonepe'
}

base_path = r"C:\Users\ASUS\Desktop\PhonPe\pulse\data\aggregated\transaction\country\india"

conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

insert_query = """
    INSERT INTO aggregated_transaction
    (state, year, quarter, transaction_type, count, amount)
    VALUES (%s, %s, %s, %s, %s, %s)
"""

# ---------- LOAD ALL-INDIA DATA ----------

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

            transaction_data = data['data'].get('transactionData', [])

            for tx in transaction_data:
                tx_type = tx.get('name', None)
                count = None
                amount = None
                instruments = tx.get('paymentInstruments', [])
                if instruments:
                    count = instruments[0].get('count', None)
                    amount = instruments[0].get('amount', None)

                values = (
                    "All-India",
                    int(year),
                    f"Q{quarter}",
                    tx_type,
                    int(count) if count is not None else None,
                    float(amount) if amount is not None else None
                )

                print("Inserting National:", values)
                cursor.execute(insert_query, values)

# ---------- LOAD STATE-LEVEL DATA ----------

state_path_root = os.path.join(base_path, "state")
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

                transaction_data = data['data'].get('transactionData', [])

                for tx in transaction_data:
                    tx_type = tx.get('name', None)
                    count = None
                    amount = None
                    instruments = tx.get('paymentInstruments', [])
                    if instruments:
                        count = instruments[0].get('count', None)
                        amount = instruments[0].get('amount', None)

                    values = (
                        state,
                        int(year),
                        f"Q{quarter}",
                        tx_type,
                        int(count) if count is not None else None,
                        float(amount) if amount is not None else None
                    )

                    print("Inserting State:", values)
                    cursor.execute(insert_query, values)

conn.commit()
cursor.close()
conn.close()
print(" aggregated_transaction data load complete.")
