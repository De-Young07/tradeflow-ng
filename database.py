# Run in Anaconda Prompt or terminal
# pip install psycopg2-binary sqlalchemy first

import sqlite3
import pandas as pd
from sqlalchemy import create_engine, text

SQLITE_PATH  = r"C:\Users\USER\Projects\TradeFlow\data\tradeflow.db"
DATABASE_URL = "postgresql://postgres:Adebc4real.com@db.lheuohaztwtpouhhulyl.supabase.co:5432/postgres"

sqlite_conn = sqlite3.connect(SQLITE_PATH)
pg_engine   = create_engine(DATABASE_URL)

# Tables to migrate (in dependency order)
tables = [
    "states", "markets", "commodities", "vehicle_types",
    "corridors", "agents", "raw_submissions", "cleaned_prices",
    "transport_costs", "forecasts", "optimization_runs",
    "optimization_recommendations", "actual_outcomes", "pipeline_logs"
]

for table in tables:
    try:
        df = pd.read_sql(f"SELECT * FROM {table}", sqlite_conn)
        if df.empty:
            print(f"  {table}: empty — skipping")
            continue
        df.to_sql(table, pg_engine, if_exists="append", index=False)
        print(f"  {table}: {len(df)} rows migrated ✓")
    except Exception as e:
        print(f"  {table}: FAILED — {e}")

sqlite_conn.close()
print("\nMigration complete.")