# TradeFlow NG — Supabase Database Migration Script (Archived)
# Now using db_adapter.py for unified database access via environment variable.
# Reference: This shows the correct Supabase pooler connection string format.

import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres.lheuohaztwtpouhhulyl:YOUR_PASSWORD@aws-0-eu-west-1.pooler.supabase.com:6543/postgres"
)

# ⚠️ IMPORTANT: Use pooler connection (port 6543) for Streamlit Cloud and restricted networks
# Direct connection (port 5432) will NOT work in those environments

if "6543" not in DATABASE_URL:
    print("⚠️  WARNING: Not using pooler connection (port 6543)")
    print("   Recommended: Use pooler for cloud deployment")

print("""\n═══════════════════════════════════════════════════════════════
TradeFlow NG — Supabase Setup Guide
═══════════════════════════════════════════════════════════════

1. SET DATABASE_URL in environment
   
   Option A: Local development (.env file)
   ────────────────────────────────────────
   DATABASE_URL=postgresql://postgres.lheuohaztwtpouhhulyl:YOUR_PASSWORD@aws-0-eu-west-1.pooler.supabase.com:6543/postgres
   
   Option B: Streamlit Cloud (secrets.toml)
   ────────────────────────────────────────
   [database]
   DATABASE_URL = "postgresql://postgres.lheuohaztwtpouhhulyl:YOUR_PASSWORD@aws-0-eu-west-1.pooler.supabase.com:6543/postgres"
   
   Option C: Environment variable
   ──────────────────────────────────
   export DATABASE_URL="postgresql://postgres.lheuohaztwtpouhhulyl:YOUR_PASSWORD@aws-0-eu-west-1.pooler.supabase.com:6543/postgres"

2. All database operations now use db_adapter.py
   No hardcoded connection strings!
   
3. Connection flow:
   └─ os.environ.get(DATABASE_URL)
   └─ db_adapter.py detects Supabase (postgresql://)
   └─ Routes to psycopg2 with pooler connection
   
═══════════════════════════════════════════════════════════════
""")
