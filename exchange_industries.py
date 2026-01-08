import sqlite3
import argparse
import random
from pathlib import Path
from typing import List, Dict, Tuple

DB_PATH = Path("railcars.db")


def choose_from_list(prompt: str, options: List[str]) -> int:
    print(prompt)
    for i, opt in enumerate(options, start=1):
        print(f"  {i}. {opt}")
    while True:
        choice = input("Enter number: ").strip()
        if not choice.isdigit():
            print("Please enter a number from the list.")
            continue
        idx = int(choice)
        if 1 <= idx <= len(options):
            return idx - 1
        print("Choice out of range.")


def fetch_yard_spots(cur) -> List[Tuple[int, str, int]]:
    cur.execute("""
        SELECT cs.spot_id, cs.spot_name, cs.capacity
        FROM car_spots cs
        JOIN industries i ON cs.industry_id = i.industry_id
        JOIN industry_types it ON i.industry_type_id = it.industry_type_id
        WHERE it.industry_type_name = 'Yard'
        ORDER BY cs.spot_name
    """)
    return cur.fetchall()


def fetch_yard_cars(cur, yard_id: int) -> List[Tuple[str, int, str]]:
    cur.execute("""
        SELECT c.car_number, c.car_type_id, c.road_name
        FROM cars c
        WHERE c.spot_id = ?
        ORDER BY c.road_name, c.car_number
    """, (yard_id,))
    return cur.fetchall()


def fetch_industry_spots(cur) -> List[Tuple[int, str, str, int, int]]:
    # Returns spot_id, spot_name, industry_name, capacity, occupancy
    cur.execute("""
        SELECT cs.spot_id, cs.spot_name, i.industry_name, cs.capacity, COUNT(c.car_number) as occupancy
        FROM car_spots cs
        JOIN industries i ON cs.industry_id = i.industry_id
        JOIN industry_types it ON i.industry_type_id = it.industry_type_id
        LEFT JOIN cars c ON c.spot_id = cs.spot_id
        WHERE it.industry_type_name = 'Industry'
        GROUP BY cs.spot_id
        ORDER BY i.industry_name, cs.spot_name
    """)
    return cur.fetchall()


def fetch_spot_allowed_types(cur) -> Dict[int, List[int]]:
    cur.execute("SELECT spot_id, car_type_id FROM spot_allowed_car_types")
    rows = cur.fetchall()
    d: Dict[int, List[int]] = {}
    for spot_id, ct_id in rows:
        d.setdefault(spot_id, []).append(ct_id)
    return d


def fetch_car_type_names(cur) -> Dict[int, str]:
    cur.execute("SELECT car_type_id, car_type_name FROM car_types")
    return {r[0]: r[1] for r in cur.fetchall()}


def find_occupant_of_spot(cur, spot_id: int):
    cur.execute("SELECT car_number FROM cars WHERE spot_id = ? LIMIT 1", (spot_id,))
    row = cur.fetchone()
    return row[0] if row else None


def exchange_from_yard(db_path: str, yard_spot_name: str = None, num_to_move: int = None):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Choose yard (mirror exchange_yard inputs)
    yards = fetch_yard_spots(cur)
    if not yards:
        print("No yard spots found in database.")
        conn.close()
        return

    # If not provided via args, prompt like exchange_yard
    if not yard_spot_name:
        yard_spot_name = input("Enter Yard spot name: ").strip()

    # find matching yard by name (case-insensitive)
    matches = [i for i, y in enumerate(yards) if y[1].lower() == yard_spot_name.lower()]
    if not matches:
        print(f"Yard '{yard_spot_name}' not found. Available yards:")
        for i, (_id, name, cap) in enumerate(yards, start=1):
            print(f"  {i}. {name} (capacity={cap})")
        selected_idx = choose_from_list("Select a yard:", [f"{n} (capacity={c})" for (_id, n, c) in yards])
    else:
        selected_idx = matches[0]

    yard_id, yard_name, yard_capacity = yards[selected_idx]

    # Fetch cars currently in yard
    yard_cars = fetch_yard_cars(cur, yard_id)
    if not yard_cars:
        print(f"No cars currently in Yard '{yard_name}'.")
        conn.close()
        return

    print(f"Cars currently in Yard '{yard_name}':")
    for i, (car_number, car_type_id, road_name) in enumerate(yard_cars, start=1):
        print(f"  {i}. {road_name} {car_number}")

    if num_to_move is None:
        choice = input("Enter number of cars to pull from yard: ").strip().lower()
        if choice == 'all':
            num = len(yard_cars)
        else:
            try:
                num = int(choice)
            except ValueError:
                print("Invalid number. Exiting.")
                conn.close()
                return
        num_to_move = max(0, min(num, len(yard_cars)))

    if num_to_move <= 0:
        print("Nothing to move.")
        conn.close()
        return

    cars_to_move = yard_cars[:num_to_move]

    # Load industry spots, allowed types, car type names
    spots = fetch_industry_spots(cur)
    allowed = fetch_spot_allowed_types(cur)
    car_type_names = fetch_car_type_names(cur)

    # Current yard occupancy
    cur.execute("SELECT COUNT(*) FROM cars WHERE spot_id = ?", (yard_id,))
    yard_current_count = cur.fetchone()[0]

    # We'll perform updates within a transaction
    print(f"Preparing to move {len(cars_to_move)} car(s) from Yard '{yard_name}'.")
    moved: List[Tuple[str, str, str, str]] = []
    moved_to_spot_ids: List[int] = []
    displaced_to_yard: List[Tuple[str, str, str]] = []
    replaced_from_industries: List[Tuple[str, str, str]] = []

    for car_number, car_type_id, road_name in cars_to_move:
        # Removing this car will free a slot in yard
        yard_current_count -= 1
        yard_free_slots = yard_capacity - yard_current_count

        # Try to find a spot with free capacity that allows this car type
        target_spot = None
        for spot_id, spot_name, industry_name, capacity, occupancy in spots:
            spot_allowed = allowed.get(spot_id)
            accepts = True if (spot_allowed is None or len(spot_allowed) == 0) else (car_type_id in spot_allowed)
            if not accepts:
                continue
            if occupancy < capacity:
                target_spot = (spot_id, spot_name, industry_name)
                # reserve slot locally
                # update occupancy in our local list
                for idx, s in enumerate(spots):
                    if s[0] == spot_id:
                        spots[idx] = (s[0], s[1], s[2], s[3], s[4] + 1)
                        break
                break

        if target_spot:
            # Move car into the free spot
            cur.execute("UPDATE cars SET spot_id = ? WHERE car_number = ?", (target_spot[0], car_number))
            moved.append((car_number, road_name, yard_name, target_spot[2] + ' / ' + target_spot[1]))
            moved_to_spot_ids.append(target_spot[0])
            print(f"Assigned {road_name} {car_number} → {target_spot[2]} / {target_spot[1]}")
            continue

        # No free spot found; try to find an occupiable spot by displacing occupant (capacity==1)
        displaced = False
        for spot_id, spot_name, industry_name, capacity, occupancy in spots:
            spot_allowed = allowed.get(spot_id)
            accepts = True if (spot_allowed is None or len(spot_allowed) == 0) else (car_type_id in spot_allowed)
            if not accepts:
                continue
            if capacity == 1 and occupancy >= 1:
                # can displace occupant if yard has free slot
                if yard_free_slots <= 0:
                    # cannot accept displaced car
                    continue
                occupant = find_occupant_of_spot(cur, spot_id)
                if not occupant:
                    continue
                # capture occupant road_name for summary
                cur.execute("SELECT road_name FROM cars WHERE car_number = ?", (occupant,))
                occ_row = cur.fetchone()
                occ_road = occ_row[0] if occ_row else ""
                # Move occupant to yard
                cur.execute("UPDATE cars SET spot_id = ? WHERE car_number = ?", (yard_id, occupant))
                # Update local occupancies
                for idx, s in enumerate(spots):
                    if s[0] == spot_id:
                        spots[idx] = (s[0], s[1], s[2], s[3], s[4] - 1)
                        break
                yard_current_count += 1
                yard_free_slots = yard_capacity - yard_current_count
                displaced_to_yard.append((occupant, occ_road, industry_name + ' / ' + spot_name))
                print(f"Displaced {occupant} from {industry_name} / {spot_name} → Yard '{yard_name}'")

                # Now place our car into spot
                cur.execute("UPDATE cars SET spot_id = ? WHERE car_number = ?", (spot_id, car_number))
                moved.append((car_number, road_name, yard_name, industry_name + ' / ' + spot_name))
                moved_to_spot_ids.append(spot_id)
                # adjust occupancy
                for idx, s in enumerate(spots):
                    if s[0] == spot_id:
                        spots[idx] = (s[0], s[1], s[2], s[3], s[4] + 1)
                        break
                displaced = True
                break

        if displaced:
            continue

        # If we reach here, no placement possible
        print(f"No available spot for {road_name} {car_number}; it remains in Yard '{yard_name}'.")
        # restore the yard_current_count change we made earlier since the car stays
        yard_current_count += 1

    # Determine how many actual yard->industry moves occurred
    moved_count = len(moved)

    # Now randomly select the same number of cars from Industries to move into the yard as replacements
    if moved_count > 0:
        # compute available yard slots
        cur.execute("SELECT COUNT(*) FROM cars WHERE spot_id = ?", (yard_id,))
        yard_count_after = cur.fetchone()[0]
        available_slots = yard_capacity - yard_count_after
        # choose a random replacement count between moved_count-2 and moved_count+2
        low = max(0, moved_count - 2)
        high = moved_count + 2
        desired_replacements = random.randint(low, high)
        print(f"Attempting to replace {desired_replacements} cars (range {low}-{high})")
        to_replace = min(desired_replacements, available_slots)
        if to_replace <= 0:
            print("No available yard capacity to accept replacements from industries.")
        else:
            # build exclusion lists: avoid spots we just filled and avoid the cars we moved from yard
            moved_car_numbers = [m[0] for m in moved]
            params: List = []
            not_in_spots_clause = ""
            if moved_to_spot_ids:
                placeholders = ",".join(["?" for _ in moved_to_spot_ids])
                not_in_spots_clause = f"AND c.spot_id NOT IN ({placeholders})\n                "
                params.extend(moved_to_spot_ids)

            not_in_cars_clause = ""
            if moved_car_numbers:
                placeholders_c = ",".join(["?" for _ in moved_car_numbers])
                not_in_cars_clause = f"AND c.car_number NOT IN ({placeholders_c})\n                "
                params.extend(moved_car_numbers)

            params.append(to_replace)

            sql = f"""
                SELECT c.car_number, ct.car_type_name, c.road_name, c.spot_id, cs.spot_name, i.industry_name
                FROM cars c
                JOIN car_types ct ON c.car_type_id = ct.car_type_id
                JOIN car_spots cs ON c.spot_id = cs.spot_id
                JOIN industries i ON cs.industry_id = i.industry_id
                JOIN industry_types it ON i.industry_type_id = it.industry_type_id
                WHERE it.industry_type_name = 'Industry'
                {not_in_spots_clause}
                {not_in_cars_clause}
                ORDER BY RANDOM()
                LIMIT ?
            """

            cur.execute(sql, tuple(params))
            replacement_rows = cur.fetchall()
            if not replacement_rows:
                print("No suitable industry cars found to move to yard.")
            else:
                replaced = []
                for car_number, car_type_name, road_name, spot_id, spot_name, industry_name in replacement_rows:
                    cur.execute("UPDATE cars SET spot_id = ? WHERE car_number = ?", (yard_id, car_number))
                    replaced_from_industries.append((car_number, road_name, industry_name + ' / ' + spot_name))
                    print(f"Moved {road_name} {car_number} from {industry_name} / {spot_name} → Yard '{yard_name}'")

    conn.commit()
    conn.close()

    print("\nDone. Summary of moves:")
    if not moved:
        print("  No cars were moved from the yard to industries.")
    else:
        for car_number, road, from_yard, to_spot in moved:
            print(f"  {road} {car_number}: {from_yard} → {to_spot}")

    # Summary of cars moved from Industries into the Yard
    print("\nMoved to Yard from Industries:")
    if not displaced_to_yard and not replaced_from_industries:
        print("  No cars were moved from industries to the yard.")
    else:
        for car_number, road, origin in displaced_to_yard:
            print(f"  {road} {car_number}: {origin} → {yard_name}")
        for car_number, road, origin in replaced_from_industries:
            print(f"  {road} {car_number}: {origin} → {yard_name}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Move cars from a Yard to Industry spots respecting types and capacities')
    parser.add_argument('--db', default=str(DB_PATH), help='Path to SQLite DB (default: railcars.db)')
    parser.add_argument('--yard', help='Yard spot name to operate on')
    parser.add_argument('--num', type=int, help='Number of cars to move (default: prompt)')
    args = parser.parse_args()

    exchange_from_yard(args.db, yard_spot_name=args.yard, num_to_move=args.num)
