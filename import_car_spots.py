import csv
import sqlite3
from pathlib import Path

DB_PATH = Path("railcars.db")
CSV_PATH = Path("data/car_spots.csv")

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

mode = input("Replace existing car spots or append? [R/A]: ").strip().upper()
if mode not in ("R", "A"):
    raise RuntimeError("Invalid choice. Enter R or A.")

if mode == "R":
    print("⚠️ Replacing existing car spots...")
    cur.execute("DELETE FROM car_spots")
    cur.execute("DELETE FROM industries")
    cur.execute("DELETE FROM sqlite_sequence WHERE name IN ('car_spots','industries')")
    conn.commit()

with open(CSV_PATH, newline="", encoding="utf-8-sig") as f:
    reader = csv.DictReader(f)
    print("CSV columns detected:", reader.fieldnames)

    for row in reader:
        # --- Insert industry type ---
        industry_type = row["industry_type"].strip()
        cur.execute(
            "INSERT OR IGNORE INTO industry_types (industry_type_name) VALUES (?)",
            (industry_type,)
        )
        cur.execute(
            "SELECT industry_type_id FROM industry_types WHERE industry_type_name = ?",
            (industry_type,)
        )
        industry_type_id = cur.fetchone()[0]

        # --- Insert industry ---
        industry_name = row["industry_name"].strip()
        cur.execute(
            "INSERT OR IGNORE INTO industries (industry_name, industry_type_id) VALUES (?, ?)",
            (industry_name, industry_type_id)
        )
        cur.execute(
            "SELECT industry_id FROM industries WHERE industry_name = ?",
            (industry_name,)
        )
        industry_id = cur.fetchone()[0]

        # --- Insert car spot ---
        spot_name = row["spot_name"].strip()
        capacity = int(row["capacity"])
        service_frequency = float(row["service_frequency"]) if row["service_frequency"] else None

        cur.execute(
            """
            INSERT OR IGNORE INTO car_spots
            (spot_id, spot_name, industry_id, capacity, service_frequency)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                int(row["spot_id"]),  # use CSV spot_id
                spot_name,
                industry_id,
                capacity,
                service_frequency
            )
        )

        # --- Allowed car types ---
        if row["allowed_car_types"]:
            for ct in row["allowed_car_types"].split("|"):
                ct = ct.strip()
                if not ct:
                    continue
                cur.execute(
                    "INSERT OR IGNORE INTO car_types (car_type_name) VALUES (?)",
                    (ct,)
                )
                cur.execute(
                    "SELECT car_type_id FROM car_types WHERE car_type_name = ?",
                    (ct,)
                )
                car_type_id = cur.fetchone()[0]

                cur.execute(
                    "INSERT OR IGNORE INTO spot_allowed_car_types (spot_id, car_type_id) VALUES (?, ?)",
                    (int(row["spot_id"]), car_type_id)
                )

conn.commit()
conn.close()
print("✅ Car spots imported successfully.")
