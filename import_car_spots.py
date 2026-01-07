import csv
import sqlite3

DB_PATH = "railcars.db"
CSV_PATH = "data/car_spots.csv"

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

with open(CSV_PATH, newline="", encoding="utf-8-sig") as f:
    reader = csv.DictReader(f)
    print("CSV columns detected:", reader.fieldnames)


    for row in reader:
        # Ensure industry type exists
        cur.execute(
            "INSERT OR IGNORE INTO industry_types (industry_type_name) VALUES (?)",
            (row["industry_type"],)
        )

        cur.execute(
            "SELECT industry_type_id FROM industry_types WHERE industry_type_name = ?",
            (row["industry_type"],)
        )
        industry_type_id = cur.fetchone()[0]

        # Ensure industry exists
        cur.execute("""
            INSERT OR IGNORE INTO industries (industry_name, industry_type_id)
            VALUES (?, ?)
        """, (row["industry_name"], industry_type_id))

        cur.execute(
            "SELECT industry_id FROM industries WHERE industry_name = ?",
            (row["industry_name"],)
        )
        industry_id = cur.fetchone()[0]

        # Create car spot
        cur.execute("""
            INSERT INTO car_spots
            (spot_name, industry_id, capacity, service_frequency)
            VALUES (?, ?, ?, ?)
        """, (
            row["spot_name"],
            industry_id,
            int(row["capacity"]),
            float(row["service_frequency"]) if row["service_frequency"] else None
        ))

        spot_id = cur.lastrowid

        # Allowed car types
        if row["allowed_car_types"]:
            for ct in row["allowed_car_types"].split("|"):
                cur.execute(
                    "INSERT OR IGNORE INTO car_types (car_type_name) VALUES (?)",
                    (ct.strip(),)
                )

                cur.execute(
                    "SELECT car_type_id FROM car_types WHERE car_type_name = ?",
                    (ct.strip(),)
                )
                car_type_id = cur.fetchone()[0]

                cur.execute("""
                    INSERT INTO spot_allowed_car_types (spot_id, car_type_id)
                    VALUES (?, ?)
                """, (spot_id, car_type_id))

conn.commit()
conn.close()
print("Car spots imported successfully.")
