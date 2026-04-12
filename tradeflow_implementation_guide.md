# TradeFlow NG — Implementation & Git Setup Guide

---

## PART 1: GIT INITIALIZATION (Do This First)

### Step 1 — Install Git (if not installed)
```bash
# Check if Git is installed
git --version

# If not installed:
# Windows: Download from https://git-scm.com
# Mac:
brew install git
# Ubuntu/Linux:
sudo apt install git
```

### Step 2 — Configure Git identity (one-time setup)
```bash
git config --global user.name "Your Full Name"
git config --global user.email "your@email.com"
```

### Step 3 — Create your project folder and initialize Git
```bash
mkdir tradeflow-ng
cd tradeflow-ng
git init
```

### Step 4 — Create the full folder structure
```bash
mkdir -p data/raw data/processed notebooks src dashboard tests
touch src/ingestion.py src/cleaning.py src/forecasting.py src/optimization.py
touch dashboard/app.py
touch requirements.txt README.md .gitignore
```

### Step 5 — Create your .gitignore file
Open `.gitignore` and paste this content:
```
# Python
__pycache__/
*.py[cod]
*.pyo
.env
venv/
env/

# Data (never commit raw data to Git)
data/raw/
data/processed/
*.csv
*.xlsx
*.db
*.sqlite

# Jupyter
.ipynb_checkpoints/

# OS
.DS_Store
Thumbs.db

# Secrets
config.ini
secrets.py
*.key
```

### Step 6 — Create your virtual environment
```bash
# Create environment
python -m venv venv

# Activate it
# Windows:
venv\Scripts\activate
# Mac/Linux:
source venv/bin/activate
```

### Step 7 — Install all required packages
```bash
pip install streamlit pandas prophet pulp sqlalchemy plotly \
            requests python-dotenv apscheduler openpyxl
pip freeze > requirements.txt
```

### Step 8 — Make your first commit
```bash
git add .
git commit -m "Initial project structure — TradeFlow NG"
```

### Step 9 — Push to GitHub
```bash
# Create a new repo on github.com (do NOT initialize with README)
# Then run:
git remote add origin https://github.com/YOUR_USERNAME/tradeflow-ng.git
git branch -M main
git push -u origin main
```

---

## PART 2: DATABASE IMPLEMENTATION

### Step 1 — Create the database initializer script

Create `src/database.py`:

```python
"""
TradeFlow NG — Database Initializer
Run this once to create your SQLite database from schema.sql
"""

import sqlite3
import os

# Paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH  = os.path.join(BASE_DIR, "data", "tradeflow.db")
SQL_PATH = os.path.join(BASE_DIR, "tradeflow_schema.sql")

def init_database():
    """Create the database and all tables from schema file."""
    print(f"Creating database at: {DB_PATH}")

    # Create data directory if it doesn't exist
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")  # Enforce foreign keys in SQLite

    with open(SQL_PATH, "r") as f:
        sql_script = f.read()

    conn.executescript(sql_script)
    conn.commit()
    conn.close()
    print("Database initialized successfully.")
    print("Tables created with seed data.")

def get_connection():
    """Return a live database connection. Use this everywhere."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.row_factory = sqlite3.Row  # Returns rows as dicts, not tuples
    return conn

def test_connection():
    """Verify tables exist and seed data loaded correctly."""
    conn = get_connection()
    cursor = conn.cursor()

    tables = ["states", "markets", "commodities", "agents",
              "raw_submissions", "cleaned_prices", "forecasts",
              "optimization_runs", "optimization_recommendations",
              "actual_outcomes", "pipeline_logs"]

    print("\n--- DATABASE VERIFICATION ---")
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"  {table:<35} → {count} rows")

    conn.close()
    print("----------------------------\n")

if __name__ == "__main__":
    init_database()
    test_connection()
```

### Step 2 — Run the database initializer
```bash
# Make sure your virtual environment is active
# Make sure tradeflow_schema.sql is in your project root
python src/database.py
```

Expected output:
```
Creating database at: .../data/tradeflow.db
Database initialized successfully.

--- DATABASE VERIFICATION ---
  states                              → 8 rows
  markets                             → 0 rows
  commodities                         → 9 rows
  agents                              → 0 rows
  raw_submissions                     → 0 rows
  cleaned_prices                      → 0 rows
  forecasts                           → 0 rows
  optimization_runs                   → 0 rows
  optimization_recommendations        → 0 rows
  actual_outcomes                     → 0 rows
  pipeline_logs                       → 0 rows
----------------------------
```

### Step 3 — Populate markets manually (do this now)

Add your known markets in Python or directly in a Jupyter notebook:

```python
import sqlite3

conn = sqlite3.connect("data/tradeflow.db")

markets = [
    # (state_id, name, city)
    (1, "Mile 12 Market",    "Lagos"),
    (1, "Oyingbo Market",    "Lagos"),
    (2, "Bodija Market",     "Ibadan"),
    (2, "Oje Market",        "Ibadan"),
    (3, "Sagamu Market",     "Sagamu"),
    (4, "Oja-Oba Market",    "Ilorin"),
    (5, "Ganaja Market",     "Lokoja"),
    (6, "Wuse Market",       "Abuja"),
    (6, "Karu Market",       "Abuja"),
    (7, "Lafia Market",      "Lafia"),
    (8, "Minna Market",      "Minna"),
]

conn.executemany(
    "INSERT INTO markets (state_id, name, city) VALUES (?, ?, ?)",
    markets
)
conn.commit()
conn.close()
print("Markets inserted.")
```

### Step 4 — Populate corridors

```python
import sqlite3

conn = sqlite3.connect("data/tradeflow.db")

# (origin_state_id, dest_state_id, distance_km, avg_travel_hours, road_quality)
corridors = [
    (8, 6, 230,  3.5, "Good"),    # Niger → Abuja
    (7, 6, 100,  1.5, "Good"),    # Nasarawa → Abuja
    (5, 6, 180,  3.0, "Fair"),    # Kogi → Abuja
    (6, 4, 300,  5.0, "Fair"),    # Abuja → Kwara
    (4, 2, 130,  2.5, "Fair"),    # Kwara → Oyo
    (5, 2, 280,  5.0, "Poor"),    # Kogi → Oyo
    (2, 1, 120,  2.0, "Good"),    # Oyo → Lagos
    (2, 3,  80,  1.5, "Good"),    # Oyo → Ogun
    (3, 1,  60,  1.0, "Good"),    # Ogun → Lagos
    (8, 2, 580,  9.0, "Fair"),    # Niger → Oyo (long haul)
    (7, 2, 480,  8.0, "Fair"),    # Nasarawa → Oyo
    (6, 1, 520,  8.5, "Good"),    # Abuja → Lagos
]

conn.executemany("""
    INSERT INTO corridors 
    (origin_state_id, dest_state_id, distance_km, avg_travel_hours, road_quality)
    VALUES (?, ?, ?, ?, ?)
""", corridors)
conn.commit()
conn.close()
print("Corridors inserted.")
```

---

## PART 3: DUMMY DATA FOR TESTING

Run this in a Jupyter notebook to generate fake prices so your pipeline has something to work with before real Kobo data arrives:

```python
import sqlite3
import pandas as pd
import numpy as np
from datetime import date, timedelta

conn = sqlite3.connect("data/tradeflow.db")

np.random.seed(42)

# Base prices per commodity per state (NGN per unit) — rough estimates
base_prices = {
    # commodity_id: {state_id: base_price}
    1: {6: 18000, 7: 16000, 8: 17000, 4: 20000, 5: 21000, 2: 25000, 1: 28000, 3: 26000},  # Yam
    2: {6: 28000, 7: 26000, 8: 27000, 4: 30000, 5: 31000, 2: 35000, 1: 38000, 3: 36000},  # Maize
    3: {6: 45000, 7: 43000, 8: 44000, 4: 47000, 5: 48000, 2: 52000, 1: 55000, 3: 53000},  # Rice
    4: {6:  3500, 7:  3200, 8:  3300, 4:  3800, 5:  4000, 2:  5500, 1:  6500, 3:  6000},  # Tomato
}

records = []
start_date = date.today() - timedelta(weeks=8)

for week in range(8):
    price_date = start_date + timedelta(weeks=week)
    for commodity_id, state_prices in base_prices.items():
        for state_id, base in state_prices.items():
            # Add realistic weekly noise
            noise = np.random.normal(0, base * 0.05)
            price = round(max(base + noise, base * 0.7), 2)
            records.append((
                state_id, commodity_id, price,
                price / 100,   # price_per_kg approx
                price_date,
                False, True    # not outlier, confirmed
            ))

conn.executemany("""
    INSERT OR IGNORE INTO cleaned_prices
    (state_id, commodity_id, price_per_unit, price_per_kg,
     price_date, is_outlier, is_confirmed)
    VALUES (?, ?, ?, ?, ?, ?, ?)
""", records)

conn.commit()
print(f"Inserted {len(records)} dummy price records.")
conn.close()
```

---

## PART 4: COMMIT EVERYTHING

```bash
# After each phase, always commit
git add .
git commit -m "Phase 1 complete: Database schema + initializer + seed data"
git push
```

---

## RECOMMENDED COMMIT CADENCE

| Milestone                          | Commit Message                                      |
|------------------------------------|-----------------------------------------------------|
| Folder structure created           | `chore: initial project structure`                  |
| Schema finalized                   | `feat: database schema v1`                          |
| DB initializer working             | `feat: database initializer and connection module`  |
| Markets & corridors populated      | `data: seed markets and corridors`                  |
| Dummy data generated               | `test: dummy price data for pipeline testing`       |
| Ingestion script working           | `feat: Kobo ingestion pipeline`                     |
| Cleaning pipeline done             | `feat: data cleaning and outlier detection`         |
| Prophet model working              | `feat: Prophet forecasting module`                  |
| PuLP optimizer working             | `feat: transportation optimization core`            |
| Streamlit dashboard MVP            | `feat: dashboard MVP`                               |

---

## NEXT STEPS (In Order)

1. Run `src/database.py` — confirm tables exist
2. Insert markets and corridors using the scripts above
3. Generate dummy data in Jupyter
4. Open a notebook and run a simple query:
   ```python
   import sqlite3, pandas as pd
   conn = sqlite3.connect("data/tradeflow.db")
   df = pd.read_sql("SELECT * FROM cleaned_prices LIMIT 20", conn)
   print(df)
   ```
5. If you see 20 rows — your foundation is solid. Move to ingestion.
