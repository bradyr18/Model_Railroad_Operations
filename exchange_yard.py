import sqlite3
from pathlib import Path

DB_PATH = Path("railcars.db")

def exchange_offlayout_to_yard(yard_spot_name: str, num_cars: int):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # --- 1. Get OFF_LAYOUT spot_id ---
    cur.execute("SELECT spot_id FROM car_spots WHERE spot_name = 'OFF_LAYOUT'")
    off_layout_row = cur.fetchone()
    if not off_layout_row:
        raise RuntimeError("OFF_LAYOUT spot not found in car_spots")
    off_layout_id = off_layout_row[0]

    # --- 2. Get target Yard spot_id ---
    cur.execute("""
        SELECT cs.spot_id
        FROM car_spots cs
        JOIN industries i ON cs.industry_id = i.industry_id
        JOIN industry_types it ON i.industry_type_id = it.industry_type_id
        WHERE UPPER(cs.spot_name) = UPPER(?) AND it.industry_type_name = 'Yard'
    """, (yard_spot_name,))
    yard_row = cur.fetchone()
    if not yard_row:
        raise RuntimeError(f"Yard spot '{yard_spot_name}' not found or not a Yard")
    yard_id = yard_row[0]

    # --- 3. Move all cars currently on the Yard track to OFF_LAYOUT ---
    cur.execute("""
        SELECT c.car_number, ct.car_type_name, c.road_name
        FROM cars c
        JOIN car_types ct ON c.car_type_id = ct.car_type_id
        WHERE c.spot_id = ?
    """, (yard_id,))
    current_yard_cars = cur.fetchall()
    if current_yard_cars:
        for car_number, car_type, road_name in current_yard_cars:
            cur.execute(
                "UPDATE cars SET spot_id = ? WHERE car_number = ?",
                (off_layout_id, car_number)
            )
            print(f"Moved car {road_name}{car_number} from Yard '{yard_spot_name}' → OFF_LAYOUT")
    else:
        print(f"No cars currently in Yard '{yard_spot_name}'")

    # --- 4. Pull specified number of cars from OFF_LAYOUT ---
    cur.execute("""
        SELECT c.car_number, ct.car_type_name, c.road_name
        FROM cars c
        JOIN car_types ct ON c.car_type_id = ct.car_type_id
        WHERE c.spot_id = ?
        LIMIT ?
    """, (off_layout_id, num_cars))
    off_layout_cars_to_move = cur.fetchall()

    if not off_layout_cars_to_move:
        print("No cars available in OFF_LAYOUT to move.")
    else:
        for car_number, car_type, road_name in off_layout_cars_to_move:
            cur.execute(
                "UPDATE cars SET spot_id = ? WHERE car_number = ?",
                (yard_id, car_number)
            )
            print(f"Moved car {road_name}{car_number} from OFF_LAYOUT → Yard '{yard_spot_name}'")

    conn.commit()
    conn.close()
    print("✅ Exchange complete.")

# --- Example usage ---
if __name__ == "__main__":
    yard_track_name = input("Enter Yard spot name: ").strip()
    num_to_move = int(input("Enter number of cars to pull from OFF_LAYOUT: ").strip())
    exchange_offlayout_to_yard(yard_track_name, num_to_move)
