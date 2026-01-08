import sqlite3
import argparse
import sys


def summarize_car_locations(
    db_path,
    show_yards=True,
    show_industries=True
):
    if not show_yards and not show_industries:
        print("Nothing to display (yards and industries both disabled).")
        return

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    filters = []
    # Always exclude Off-Layout / Staging
    filters.append("it.industry_type_name != 'Off-Layout'")

    if show_yards and not show_industries:
        filters.append("it.industry_type_name = 'Yard'")
    elif show_industries and not show_yards:
        filters.append("it.industry_type_name = 'Industry'")
    else:
        filters.append("it.industry_type_name IN ('Yard','Industry')")

    where_clause = "WHERE " + " AND ".join(filters)

    cur.execute(f"""
        SELECT
            it.industry_type_name,
            i.industry_name,
            cs.spot_name,
            c.road_name,
            c.car_number
        FROM cars c
        JOIN car_spots cs ON c.spot_id = cs.spot_id
        JOIN industries i ON cs.industry_id = i.industry_id
        JOIN industry_types it ON i.industry_type_id = it.industry_type_id
        {where_clause}
        ORDER BY
            it.industry_type_name,
            i.industry_name,
            cs.spot_name,
            c.road_name,
            c.car_number
    """)

    rows = cur.fetchall()
    conn.close()

    if not rows:
        print("No cars found for the selected filters.")
        return

    current_section = None
    current_industry = None
    current_spot = None

    print("\n=== CAR LOCATION SUMMARY ===\n")

    for industry_type, industry, spot, road, number in rows:
        if industry_type != current_section:
            current_section = industry_type
            current_industry = None
            current_spot = None
            print(f"\n--- {industry_type.upper()} ---")

        if industry != current_industry:
            current_industry = industry
            current_spot = None
            print(f"\n{industry}:")

        if spot != current_spot:
            current_spot = spot
            print(f"  Spot {spot}:")

        print(f"    {road} {number}")

    print("\n============================\n")


def main():
    parser = argparse.ArgumentParser(
        description="Summarize locations of railcars on the layout"
    )
    parser.add_argument(
        "--db",
        default="railcars.db",
        help="Path to SQLite database (default: railcars.db)"
    )

    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--yards",
        action="store_true",
        help="Show only yard cars"
    )
    group.add_argument(
        "--industries",
        action="store_true",
        help="Show only industry cars"
    )

    args = parser.parse_args()

    show_yards = True
    show_industries = True

    if args.yards:
        show_industries = False
    elif args.industries:
        show_yards = False

    summarize_car_locations(
        db_path=args.db,
        show_yards=show_yards,
        show_industries=show_industries
    )


if __name__ == "__main__":
    main()
