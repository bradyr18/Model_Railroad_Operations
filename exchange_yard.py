import sqlite3
from pathlib import Path
import argparse
from typing import List

DB_PATH = Path("railcars.db")


def exchange_offlayout_to_yard(yard_spot_name: str, num_cars: int, industry_types_only: bool = False):
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

    # --- 3. Get capacity for Yard spot ---
    cur.execute("SELECT capacity FROM car_spots WHERE spot_id = ?", (yard_id,))
    cap_row = cur.fetchone()
    if not cap_row:
        raise RuntimeError(f"Capacity not found for Yard spot '{yard_spot_name}'")
    capacity = cap_row[0]

    # --- 4. Move all cars currently on the Yard track to OFF_LAYOUT ---
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
            print(f"Moved car {road_name} {car_number} from Yard '{yard_spot_name}' → OFF_LAYOUT")
    else:
        print(f"No cars currently in Yard '{yard_spot_name}'")

    # Recompute occupancy and determine how many cars we can pull in without exceeding capacity
    # After moving current yard cars to OFF_LAYOUT above the yard will be empty
    # so available slots equal the yard capacity.
    available_slots = capacity
    if available_slots <= 0:
        print(f"Yard '{yard_spot_name}' is at capacity ({capacity}); no cars pulled from OFF_LAYOUT.")
        conn.commit()
        conn.close()
        return

    to_move = min(num_cars, available_slots)
    if to_move < num_cars:
        print(f"Only {to_move} of requested {num_cars} will be moved due to capacity ({capacity}).")

    # --- 5. Pull up to `to_move` cars from OFF_LAYOUT at random ---
    if industry_types_only:
        # find car_type_ids that are allowed by any Industry spot
        cur.execute("""
            SELECT DISTINCT sat.car_type_id
            FROM spot_allowed_car_types sat
            JOIN car_spots cs ON sat.spot_id = cs.spot_id
            JOIN industries i ON cs.industry_id = i.industry_id
            JOIN industry_types it ON i.industry_type_id = it.industry_type_id
            WHERE it.industry_type_name = 'Industry'
        """)
        allowed_types = [r[0] for r in cur.fetchall()]
        if not allowed_types:
            print("No industry-used car types found; no cars will be pulled from OFF_LAYOUT.")
            conn.commit()
            conn.close()
            return
        placeholders = ",".join(["?" for _ in allowed_types])
        sql = f"""
            SELECT c.car_number, ct.car_type_name, c.road_name
            FROM cars c
            JOIN car_types ct ON c.car_type_id = ct.car_type_id
            WHERE c.spot_id = ? AND c.car_type_id IN ({placeholders})
            ORDER BY RANDOM()
            LIMIT ?
        """
        params: List = [off_layout_id] + allowed_types + [to_move]
        cur.execute(sql, params)
        off_layout_cars_to_move = cur.fetchall()
    else:
        cur.execute("""
            SELECT c.car_number, ct.car_type_name, c.road_name
            FROM cars c
            JOIN car_types ct ON c.car_type_id = ct.car_type_id
            WHERE c.spot_id = ?
            ORDER BY RANDOM()
            LIMIT ?
        """, (off_layout_id, to_move))
        off_layout_cars_to_move = cur.fetchall()

    if not off_layout_cars_to_move:
        print("No cars available in OFF_LAYOUT to move.")
    else:
        for car_number, car_type, road_name in off_layout_cars_to_move:
            cur.execute(
                "UPDATE cars SET spot_id = ? WHERE car_number = ?",
                (yard_id, car_number)
            )
            print(f"Moved car {road_name} {car_number} from OFF_LAYOUT → Yard '{yard_spot_name}'")

    conn.commit()
    conn.close()
    print("✅ Exchange complete.")

# --- Example usage ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Exchange cars between OFF_LAYOUT and a Yard spot')
    parser.add_argument('--yard', help='Yard spot name to pull cars into')
    parser.add_argument('--count', type=int, help='Number of cars to pull from OFF_LAYOUT')
    parser.add_argument('--industry-types-only', action='store_true', help='Only pull OFF_LAYOUT cars whose types are used by Industries')
    args = parser.parse_args()

    yard_track_name = args.yard or input("Enter Yard spot name: ").strip()
    if args.count is None:
        num_to_move = int(input("Enter number of cars to pull from OFF_LAYOUT: ").strip())
    else:
        num_to_move = args.count

    exchange_offlayout_to_yard(yard_track_name, num_to_move, industry_types_only=args.industry_types_only)
