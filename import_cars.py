import csv
import sqlite3
from pathlib import Path

DB_PATH = Path("railcars.db")
CSV_PATH = Path("data/cars.csv")

OFF_LAYOUT_SPOT_NAME = "OFF_LAYOUT"

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

with open(CSV_PATH, newline="", encoding="utf-8-sig") as f:
    reader = csv.DictReader(f)
    print("CSV columns detected:", reader.fieldnames)

    for row in reader:
        car_number = row["car_number"].strip()
        car_type = row["car_type"].strip()
        build_year = int(row["build_year"])
        road_name = row["road_name"].strip()
        status = row["status"].strip()

        # --- Determine spot ---
        raw_spot = row.get("spot_id", "").strip().upper()
        if raw_spot in ("", "STAGING", "OFF_LAYOUT", "OFF-LAYOUT"):
            target_spot = OFF_LAYOUT_SPOT_NAME
        else:
            target_spot = raw_spot

        # Lookup spot_id
        cur.execute(
            "SELECT spot_id FROM car_spots WHERE UPPER(TRIM(spot_name)) = ?",
            (target_spot.strip().upper(),)
        )
        result = cur.fetchone()

        # If not found, assign OFF_LAYOUT (must exist)
        if result is None:
            print(f"⚠️ Spot '{raw_spot}' not found. Assigning to OFF_LAYOUT.")
            cur.execute(
                "SELECT spot_id FROM car_spots WHERE spot_name = ?",
                (OFF_LAYOUT_SPOT_NAME,)
            )
            result = cur.fetchone()
            if result is None:
                raise RuntimeError("OFF_LAYOUT spot is missing from car_spots table")
        spot_id = result[0]

        # --- Insert car_type if needed ---
        cur.execute(
            "INSERT OR IGNORE INTO car_types (car_type_name) VALUES (?)",
            (car_type,)
        )
        cur.execute(
            "SELECT car_type_id FROM car_types WHERE car_type_name = ?",
            (car_type,)
        )
        car_type_id = cur.fetchone()[0]

        # --- Insert car ---
        cur.execute(
            """
            INSERT INTO cars (
                car_number, car_type_id, build_year, road_name, status, spot_id
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (car_number, car_type_id, build_year, road_name, status, spot_id)
        )

conn.commit()
conn.close()
print("✅ Cars imported successfully.")
