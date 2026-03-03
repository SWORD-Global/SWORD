"""
SWORD Maintenance - Fix River Name Formatting

Applies automated fixes to river_name or river_name_local field:
- Normalizes separators to standard '; ' (semicolon + space)
- Splits, deduplicates, and sorts multi-name strings
- Trims whitespace and redundant spaces
- Removes trailing truncation artifacts (e.g., ' or ', ' - ')
"""

import argparse
import sys
from pathlib import Path
import re

import duckdb
import pandas as pd


def fix_river_name(name: str) -> str:
    """Normalize a single river_name value."""
    if not name or name == "NODATA":
        return name

    # 1. Standardize separators (replace / , | with ;)
    # We leave ; alone for now to avoid splitting twice
    normalized = re.sub(r"[/,|]", ";", name)

    # 2. Split by semicolon
    parts = [p.strip() for p in normalized.split(";")]

    # 3. Clean each part
    cleaned_parts = []
    for p in parts:
        if not p:
            continue

        # Remove redundant whitespace
        p = re.sub(r" +", " ", p)

        # Remove trailing fragments like ' or', ' -', ' /'
        p = re.sub(r" (or|and|-) *$", "", p, flags=re.IGNORECASE)
        p = p.strip(" -/,|")

        if p:
            cleaned_parts.append(p)

    if not cleaned_parts:
        return "NODATA"

    # 4. Deduplicate and Sort
    unique_sorted = sorted(set(cleaned_parts))

    # 5. Rejoin
    return "; ".join(unique_sorted)


def main():
    parser = argparse.ArgumentParser(
        description="Fix river_name formatting in SWORD DuckDB"
    )
    parser.add_argument("--db", required=True, help="Path to DuckDB database")
    parser.add_argument(
        "--column",
        default="river_name",
        help="Column to fix (river_name or river_name_local)",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Report changes without applying"
    )
    parser.add_argument("--region", help="Limit fix to specific region")
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"Error: Database {db_path} not found")
        sys.exit(1)

    conn = duckdb.connect(str(db_path))

    where_clause = f"AND region = '{args.region}'" if args.region else ""

    print(f"Fetching reaches from {db_path} (column: {args.column})...")
    reaches = conn.execute(f"""
        SELECT reach_id, region, {args.column} as name
        FROM reaches
        WHERE {args.column} IS NOT NULL
          AND {args.column} != 'NODATA'
          {where_clause}
    """).fetchdf()

    print(f"Analyzing {len(reaches)} reaches...")

    updates = []
    for idx, row in reaches.iterrows():
        original = row["name"]
        fixed = fix_river_name(original)

        if fixed != original:
            updates.append(
                {
                    "reach_id": row["reach_id"],
                    "region": row["region"],
                    "old_name": original,
                    "new_name": fixed,
                }
            )

    if not updates:
        print(f"No formatting issues found in {args.column}.")
        sys.exit(0)

    print(f"Found {len(updates)} reaches with formatting issues in {args.column}.")

    # Show some examples
    print("\nSample changes:")
    for up in updates[:10]:
        print(
            f"  {up['reach_id']} ({up['region']}): '{up['old_name']}' -> '{up['new_name']}'"
        )

    if args.dry_run:
        print("\nDry run – no changes applied.")
    else:
        print(f"\nApplying {len(updates)} updates to {args.column}...")

        # Create temporary table for batch update
        temp_table = f"{args.column}_updates"
        conn.execute(
            f"CREATE TEMP TABLE {temp_table} (reach_id BIGINT, region VARCHAR, new_name VARCHAR)"
        )

        # Chunk the data for insertion
        updates_df = pd.DataFrame(updates)[["reach_id", "region", "new_name"]]

        # Insert into temp table
        conn.execute(f"INSERT INTO {temp_table} SELECT * FROM updates_df")

        # Perform the update
        conn.execute(f"""
            UPDATE reaches
            SET {args.column} = rnu.new_name
            FROM {temp_table} rnu
            WHERE reaches.reach_id = rnu.reach_id
              AND reaches.region = rnu.region
        """)

        print("Done.")

    conn.close()


if __name__ == "__main__":
    main()
