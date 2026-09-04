"""
TradeFlow NG — Optimization Engine (Phase 4)
Linear Programming via PuLP to maximize profit across trade corridors.
All DB access goes through db_adapter — no direct conn.execute calls.
"""

import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta, timezone
import warnings
warnings.filterwarnings("ignore")
import os

from db_adapter import query as db_query, execute as db_execute, get_connection

try:
    import pulp
except ImportError:
    raise ImportError("PuLP not installed. Run: pip install pulp")

# ── Weights ────────────────────────────────────────────────
PROFIT_WEIGHT = 0.6
MARGIN_WEIGHT = 0.4

# ── Perishability ──────────────────────────────────────────
PERISHABLE  = "Perishable"
SEMI_PERISH = "Semi-Perishable"
DURABLE     = "Durable"

INCOMPATIBLE_PAIRS = {
    (PERISHABLE, DURABLE),
    (DURABLE,    PERISHABLE),
}

# ── Staleness / freshness ──────────────────────────────────
# Forecasts whose training data was too old are marked STALE_TRAINING by the
# forecasting stage. When SUPPRESS_STALE_RECS is on, routes built on such
# forecasts are dropped from the recommendation set (never optimized/saved).
SUPPRESS_STALE_RECS = True
STALE_MARKER        = "STALE_TRAINING"   # keep in sync with forecasting.STALE_MARKER
FORECAST_FRESH_DAYS = 2                   # warn if the latest forecast batch is older
# A shock-flagged forecast whose confidence band has collapsed to ~zero width is
# the fingerprint of clamp-railing: an over-extrapolated trend clipped to the
# historical band, so predicted == lower == upper at the rail. It carries no real
# signal, so the optimizer skips it rather than let an artificial extreme headline
# a recommendation. Forecasting now uses flat growth for far-out targets, so these
# should be rare — this is defense in depth.
RAILED_BAND_RATIO   = 0.001

# ── Interim transport-cost estimate ────────────────────────
# Used ONLY when transport_costs has no valid row for a route. Real cost rows
# always take precedence, so once the table is populated this is bypassed with
# no code change. The per-vehicle-km rate is a placeholder — tune it.
TRANSPORT_COST_NGN_PER_KM       = 300.0   # ₦ per vehicle-km
DEFAULT_ESTIMATE_CAPACITY_UNITS = 30      # fallback if vehicle_types is unavailable
DEFAULT_ESTIMATE_CAPACITY_KG    = 3000


def _utc_today():
    """Canonical pipeline date (UTC) so reads line up with forecasting writes."""
    return datetime.now(timezone.utc).date()


def pick_estimate_vehicle(vehicle_types):
    """Choose the vehicle assumed for interim distance-based transport
    estimates. Prefers a mid-size 'Medium' truck; falls back to the row whose
    capacity is closest to the default, then to module constants. Avoids
    hard-coding a vehicle_type id that may differ in the live DB."""
    fallback = {
        "id": None,
        "capacity_units": DEFAULT_ESTIMATE_CAPACITY_UNITS,
        "capacity_kg": DEFAULT_ESTIMATE_CAPACITY_KG,
        "name": "estimate",
    }
    if vehicle_types is None or vehicle_types.empty:
        return fallback
    vt = vehicle_types.copy()
    med = (vt[vt["name"].str.contains("Medium", case=False, na=False)]
           if "name" in vt.columns else vt.iloc[0:0])
    if not med.empty:
        r = med.iloc[0]
    elif "capacity_kg" in vt.columns:
        vt["_d"] = (pd.to_numeric(vt["capacity_kg"], errors="coerce")
                    - DEFAULT_ESTIMATE_CAPACITY_KG).abs()
        r = vt.sort_values("_d").iloc[0]
    else:
        return fallback
    cu = r.get("capacity_units")
    ck = r.get("capacity_kg")
    return {
        "id": int(r["id"]) if pd.notna(r.get("id")) else None,
        "capacity_units": float(cu) if pd.notna(cu) and cu else DEFAULT_ESTIMATE_CAPACITY_UNITS,
        "capacity_kg": float(ck) if pd.notna(ck) and ck else DEFAULT_ESTIMATE_CAPACITY_KG,
        "name": r["name"] if "name" in r and pd.notna(r.get("name")) else "estimate",
    }


# ═══════════════════════════════════════════════════════════
# 1. DATA LOADERS — all via db_adapter
# ═══════════════════════════════════════════════════════════

def load_forecasts(forecast_date=None):
    if forecast_date is None:
        forecast_date = str(_utc_today() + timedelta(days=1))
    # Use the LATEST generated batch for the target date rather than
    # `generated_on = CURRENT_DATE`. That equality broke whenever the writer's
    # local date (WAT) disagreed with the server's UTC date, hiding a fresh
    # batch entirely. MAX(generated_on) is timezone-proof.
    df = db_query("""
        SELECT f.state_id, f.commodity_id,
               s.name               AS state_name,
               c.name               AS commodity_name,
               c.perishability_class,
               c.avg_weight_kg,
               c.unit_of_measure,
               f.predicted_price,
               f.lower_bound,
               f.upper_bound,
               f.is_shock_flagged,
               f.shock_reason,
               f.generated_on
        FROM   forecasts    f
        JOIN   states       s ON f.state_id     = s.id
        JOIN   commodities  c ON f.commodity_id = c.id
        WHERE  f.forecast_date = ?
          AND  f.generated_on  = (
                   SELECT MAX(f2.generated_on)
                   FROM   forecasts f2
                   WHERE  f2.forecast_date = ?
               )
        ORDER BY c.name, s.name
    """, (forecast_date, forecast_date))
    print(f"  Loaded {len(df)} forecasts for {forecast_date}.")

    # Freshness guard — warn (don't block) if the batch is old.
    if not df.empty and "generated_on" in df.columns:
        batch = pd.to_datetime(df["generated_on"]).max().date()
        age   = (_utc_today() - batch).days
        if age > FORECAST_FRESH_DAYS:
            print(f"  ⚠ Latest forecast batch is {age}d old "
                  f"(generated {batch}); recommendations may be outdated.")
    return df


def load_supply_prices():
    df = db_query("""
        SELECT cp.state_id, cp.commodity_id,
               s.name              AS state_name,
               c.name              AS commodity_name,
               cp.price_per_unit   AS buy_price,
               cp.quantity_available,
               cp.price_date
        FROM   cleaned_prices cp
        JOIN   states      s ON cp.state_id     = s.id
        JOIN   commodities c ON cp.commodity_id = c.id
        WHERE  cp.price_date = (
                   SELECT MAX(cp2.price_date)
                   FROM   cleaned_prices cp2
                   WHERE  cp2.state_id     = cp.state_id
                     AND  cp2.commodity_id = cp.commodity_id
                     AND  cp2.is_outlier   = FALSE
               )
          AND  cp.is_outlier   = FALSE
          AND  cp.is_confirmed = TRUE
    """)
    print(f"  Loaded {len(df)} current supply prices.")
    return df


def load_corridors():
    df = db_query("""
        SELECT co.id              AS corridor_id,
               co.origin_state_id,
               co.dest_state_id,
               co.distance_km,
               co.avg_travel_hours,
               co.road_quality,
               so.name            AS origin_name,
               sd.name            AS dest_name
        FROM   corridors co
        JOIN   states so ON co.origin_state_id = so.id
        JOIN   states sd ON co.dest_state_id   = sd.id
        WHERE  co.is_active = 1
    """)
    print(f"  Loaded {len(df)} active corridors.")
    return df


def load_transport_costs():
    df = db_query("""
        SELECT tc.corridor_id, tc.commodity_id,
               tc.cost_per_unit, tc.vehicle_type_id,
               vt.capacity_kg,
               vt.name AS vehicle_name
        FROM   transport_costs tc
        LEFT JOIN vehicle_types vt ON tc.vehicle_type_id = vt.id
        WHERE  (tc.expiry_date IS NULL OR tc.expiry_date >= CURRENT_DATE)
          AND  tc.effective_date <= CURRENT_DATE
        ORDER BY tc.effective_date DESC
    """)
    df = df.drop_duplicates(subset=["corridor_id", "commodity_id"], keep="first")
    print(f"  Loaded {len(df)} transport cost records.")
    return df


def load_vehicle_types():
    return db_query("SELECT * FROM vehicle_types ORDER BY capacity_kg")


# ═══════════════════════════════════════════════════════════
# 2. BUILD PROFIT MATRIX
# ═══════════════════════════════════════════════════════════

def is_railed_artifact(forecast_row):
    """True when a shock-flagged forecast's confidence band has collapsed to
    ~zero width — the clamp-rail signature (see RAILED_BAND_RATIO). A legitimate
    shock keeps a wide band, so it is not caught here."""
    if not bool(forecast_row["is_shock_flagged"]):
        return False
    pred = float(forecast_row["predicted_price"] or 0)
    if pred <= 0:
        return False
    band = float(forecast_row["upper_bound"] or 0) - float(forecast_row["lower_bound"] or 0)
    return (band / pred) < RAILED_BAND_RATIO


def build_profit_matrix(forecasts, supply_prices, corridors, transport_costs,
                        est_vehicle=None):
    if est_vehicle is None:
        est_vehicle = {
            "id": None,
            "capacity_units": DEFAULT_ESTIMATE_CAPACITY_UNITS,
            "capacity_kg": DEFAULT_ESTIMATE_CAPACITY_KG,
            "name": "estimate",
        }
    rows = []
    missing_cost_warnings = []
    stale_suppressed  = 0
    railed_suppressed = 0

    for _, corridor in corridors.iterrows():
        origin_id   = corridor["origin_state_id"]
        dest_id     = corridor["dest_state_id"]
        corridor_id = corridor["corridor_id"]
        origin_name = corridor["origin_name"]
        dest_name   = corridor["dest_name"]

        origin_supply = supply_prices[supply_prices["state_id"] == origin_id]

        for _, supply_row in origin_supply.iterrows():
            commodity_id   = supply_row["commodity_id"]
            commodity_name = supply_row["commodity_name"]
            WHOLESALE_DISCOUNT = 0.18
            buy_price = supply_row["buy_price"] * (1 - WHOLESALE_DISCOUNT)

            dest_forecast = forecasts[
                (forecasts["state_id"]     == dest_id) &
                (forecasts["commodity_id"] == commodity_id)
            ]
            if dest_forecast.empty:
                continue

            forecast_row  = dest_forecast.iloc[0]
            sell_price    = forecast_row["predicted_price"]
            perishability = forecast_row["perishability_class"]
            is_shock      = bool(forecast_row["is_shock_flagged"])
            shock_reason  = forecast_row["shock_reason"]

            # Suppress recs built on stale forecasts (user policy). The
            # forecasting stage prefixes shock_reason with STALE_TRAINING.
            is_stale = bool(shock_reason) and str(shock_reason).startswith(STALE_MARKER)
            if SUPPRESS_STALE_RECS and is_stale:
                stale_suppressed += 1
                continue

            # Skip clamp-railed artifacts (collapsed-band shock forecasts) so an
            # over-extrapolated extreme can't headline a recommendation.
            if is_railed_artifact(forecast_row):
                railed_suppressed += 1
                continue

            cost_row = transport_costs[
                (transport_costs["corridor_id"]  == corridor_id) &
                (transport_costs["commodity_id"] == commodity_id)
            ]

            if cost_row.empty:
                # No real cost yet → interim distance-based estimate (flagged).
                # Trip cost spread across the assumed vehicle's unit capacity.
                distance_km       = float(corridor["distance_km"] or 0)
                cap_units         = est_vehicle["capacity_units"] or DEFAULT_ESTIMATE_CAPACITY_UNITS
                trip_cost         = distance_km * TRANSPORT_COST_NGN_PER_KM
                transport_cost    = (trip_cost / cap_units) if cap_units else 0.0
                missing_cost_flag = True
                vehicle_type_id   = est_vehicle["id"]
                vehicle_capacity  = est_vehicle["capacity_kg"] or DEFAULT_ESTIMATE_CAPACITY_KG
                missing_cost_warnings.append(
                    f"{commodity_name}: {origin_name} → {dest_name} "
                    f"(est ₦{transport_cost:,.0f}/unit @ {distance_km:.0f}km)"
                )
            else:
                transport_cost    = float(cost_row.iloc[0]["cost_per_unit"])
                missing_cost_flag = False
                vt_id             = cost_row.iloc[0]["vehicle_type_id"]
                vehicle_type_id   = int(vt_id) if pd.notna(vt_id) else None
                cap               = cost_row.iloc[0]["capacity_kg"]
                vehicle_capacity  = float(cap) if cap else DEFAULT_ESTIMATE_CAPACITY_KG

            profit_per_unit = sell_price - buy_price - transport_cost
            margin_pct      = (
                (profit_per_unit / sell_price * 100) if sell_price > 0 else 0
            )
            norm_profit     = profit_per_unit / 10000
            norm_margin     = margin_pct / 100
            objective_score = PROFIT_WEIGHT * norm_profit + MARGIN_WEIGHT * norm_margin

            rows.append({
                "corridor_id":         corridor_id,
                "origin_state_id":     origin_id,
                "dest_state_id":       dest_id,
                "origin_name":         origin_name,
                "dest_name":           dest_name,
                "commodity_id":        commodity_id,
                "commodity_name":      commodity_name,
                "perishability":       perishability,
                "buy_price":           buy_price,
                "sell_price":          sell_price,
                "transport_cost":      transport_cost,
                "vehicle_type_id":     vehicle_type_id,
                "profit_per_unit":     profit_per_unit,
                "margin_pct":          margin_pct,
                "objective_score":     objective_score,
                "vehicle_capacity_kg": vehicle_capacity,
                "avg_weight_kg":       float(forecast_row["avg_weight_kg"] or 100),
                "is_shock_flagged":    is_shock,
                "shock_reason":        shock_reason,
                "missing_cost_flag":   missing_cost_flag,
            })

    matrix = pd.DataFrame(rows)

    if stale_suppressed:
        print(f"  ⓘ Suppressed {stale_suppressed} route(s) built on stale "
              f"forecasts (SUPPRESS_STALE_RECS=on).")

    if railed_suppressed:
        print(f"  ⓘ Skipped {railed_suppressed} route(s) built on clamp-railed "
              f"forecast artifacts (collapsed-band shocks).")

    if missing_cost_warnings:
        print(f"\n  ⚠ Estimated transport for {len(missing_cost_warnings)} route(s) "
              f"(no real cost yet — auto-switches when transport_costs is loaded):")
        for w in missing_cost_warnings[:5]:
            print(f"    - {w}")
        if len(missing_cost_warnings) > 5:
            print(f"    ... and {len(missing_cost_warnings)-5} more.")

    print(f"  Built profit matrix: {len(matrix)} route-commodity combinations.")
    return matrix, stale_suppressed


# ═══════════════════════════════════════════════════════════
# 3. PERISHABILITY CHECK
# ═══════════════════════════════════════════════════════════

def can_share_truck(pa, pb):
    return (pa, pb) not in INCOMPATIBLE_PAIRS


# ═══════════════════════════════════════════════════════════
# 4. PuLP OPTIMIZATION
# ═══════════════════════════════════════════════════════════

def run_optimization(matrix, supply_prices, forecasts):
    if matrix.empty:
        print("  ✗ Profit matrix is empty.")
        return None, None, None, "Infeasible"

    viable = matrix[matrix["profit_per_unit"] > 0].copy()
    if viable.empty:
        print("  ✗ No profitable routes found.")
        return None, None, None, "Infeasible"

    print(f"  Viable routes (profit > 0): {len(viable)}")

    prob = pulp.LpProblem("TradeFlow_NG_Optimization", pulp.LpMaximize)

    Q = {}
    for idx, row in viable.iterrows():
        var_name = f"Q_{row['origin_state_id']}_{row['dest_state_id']}_{row['commodity_id']}"
        Q[idx] = pulp.LpVariable(var_name, lowBound=0, cat="Continuous")

    prob += pulp.lpSum(
        Q[idx] * row["objective_score"]
        for idx, row in viable.iterrows()
    ), "Weighted_Profit_Margin"

    # Supply constraints
    for (origin_id, commodity_id), group in viable.groupby(
        ["origin_state_id", "commodity_id"]
    ):
        supply_row = supply_prices[
            (supply_prices["state_id"]     == origin_id) &
            (supply_prices["commodity_id"] == commodity_id)
        ]
        if supply_row.empty:
            continue
        available = supply_row.iloc[0]["quantity_available"]
        if pd.isna(available) or available <= 0:
            available = 100
        prob += (
            pulp.lpSum(Q[idx] for idx in group.index) <= available,
            f"Supply_{origin_id}_{commodity_id}"
        )

    # Demand constraints
    for (dest_id, commodity_id), group in viable.groupby(
        ["dest_state_id", "commodity_id"]
    ):
        prob += (
            pulp.lpSum(Q[idx] for idx in group.index) <= 120,
            f"Demand_{dest_id}_{commodity_id}"
        )

    # Truck capacity constraints
    for corridor_id, group in viable.groupby("corridor_id"):
        vehicle_cap_kg = group.iloc[0]["vehicle_capacity_kg"]
        prob += (
            pulp.lpSum(
                Q[idx] * row["avg_weight_kg"]
                for idx, row in group.iterrows()
            ) <= vehicle_cap_kg,
            f"TruckCap_{corridor_id}"
        )

    # Perishability mixing constraints
    for corridor_id, group in viable.groupby("corridor_id"):
        perishables = group[group["perishability"] == PERISHABLE]
        durables    = group[group["perishability"] == DURABLE]
        if perishables.empty or durables.empty:
            continue
        M = 10000
        y = pulp.LpVariable(f"mix_{corridor_id}", cat="Binary")
        prob += (
            pulp.lpSum(Q[idx] for idx in perishables.index) <= M * y,
            f"PerishMix_P_{corridor_id}"
        )
        prob += (
            pulp.lpSum(Q[idx] for idx in durables.index) <= M * (1 - y),
            f"PerishMix_D_{corridor_id}"
        )

    solver = pulp.PULP_CBC_CMD(msg=0)
    prob.solve(solver)
    status = pulp.LpStatus[prob.status]
    print(f"  Solver status: {status}")
    return prob, Q, viable, status


# ═══════════════════════════════════════════════════════════
# 5. EXTRACT RECOMMENDATIONS
# ═══════════════════════════════════════════════════════════

def extract_recommendations(prob, Q, viable, run_id):
    recommendations = []
    for idx, var in Q.items():
        quantity = pulp.value(var)
        if quantity is None or quantity < 0.01:
            continue
        row    = viable.loc[idx]
        profit = quantity * row["profit_per_unit"]
        qty_kg = quantity * float(row.get("avg_weight_kg") or 0)
        vt_id  = row.get("vehicle_type_id")
        recommendations.append({
            "run_id":               run_id,
            "corridor_id":          int(row["corridor_id"]),
            "commodity_id":         int(row["commodity_id"]),
            "vehicle_type_id":      (int(vt_id) if pd.notna(vt_id) else None),
            "origin":               row["origin_name"],
            "destination":          row["dest_name"],
            "commodity":            row["commodity_name"],
            "perishability":        row["perishability"],
            "recommended_quantity": round(quantity, 2),
            "recommended_quantity_kg": round(qty_kg, 2),
            "buy_price":            round(row["buy_price"], 2),
            "sell_price":           round(row["sell_price"], 2),
            "transport_cost":       round(row["transport_cost"], 2),
            "profit_per_unit":      round(row["profit_per_unit"], 2),
            "expected_profit_ngn":  round(profit, 2),
            "profit_margin_pct":    round(row["margin_pct"], 2),
            "is_shock_flagged":     bool(row["is_shock_flagged"]),
            "missing_cost_flag":    bool(row["missing_cost_flag"]),
            "shock_reason":         row["shock_reason"],
            "is_backhaul":          False,
            "backhaul_note":        None,
            "status":               "Pending",
        })
    return pd.DataFrame(recommendations)


# ═══════════════════════════════════════════════════════════
# 6. BACKHAULING
# ═══════════════════════════════════════════════════════════

def suggest_backhaul(recommendations):
    if recommendations.empty:
        return recommendations
    recommendations = recommendations.copy()
    if "is_backhaul" not in recommendations.columns:
        recommendations["is_backhaul"]   = False
        recommendations["backhaul_note"] = None

    for i, out_row in recommendations.iterrows():
        origin = out_row["origin"]
        dest   = out_row["destination"]
        comm   = out_row["commodity"]
        return_routes = recommendations[
            (recommendations["origin"]      == dest) &
            (recommendations["destination"] == origin) &
            (recommendations["commodity"]   != comm)
        ]
        if not return_routes.empty:
            ret = return_routes.iloc[0]
            recommendations.at[i, "is_backhaul"]   = True
            recommendations.at[i, "backhaul_note"] = (
                f"Return load: {ret['commodity']} "
                f"({dest} → {origin}, "
                f"₦{ret['profit_per_unit']:,.0f}/unit profit)"
            )

    backhaul_count = int(recommendations["is_backhaul"].sum())
    if backhaul_count > 0:
        print(f"  ✓ {backhaul_count} backhaul opportunities identified.")
    return recommendations


# ═══════════════════════════════════════════════════════════
# 7. SAVE TO DATABASE — all via db_execute
# ═══════════════════════════════════════════════════════════

def save_optimization_run(total_profit, status, week_start, week_end):
    """Insert a new optimization run record and return its ID."""
    db_execute("""
        INSERT INTO optimization_runs
            (run_date, week_start, week_end, solver_status, total_profit_ngn)
        VALUES (?, ?, ?, ?, ?)
    """, (str(_utc_today()), str(week_start), str(week_end),
          status, round(float(total_profit), 2)))

    # Retrieve the last inserted ID
    result = db_query(
        "SELECT id FROM optimization_runs ORDER BY id DESC LIMIT 1"
    )
    return int(result.iloc[0]["id"]) if not result.empty else None


def save_recommendations(recommendations, run_id):
    if recommendations.empty:
        return 0
    inserted = 0
    for _, row in recommendations.iterrows():
        try:
            vt_id = row.get("vehicle_type_id")
            db_execute("""
                INSERT INTO optimization_recommendations (
                    run_id, corridor_id, commodity_id, vehicle_type_id,
                    recommended_quantity, recommended_quantity_kg,
                    buy_price, sell_price, transport_cost,
                    expected_profit_ngn, profit_margin_pct,
                    is_shock_flagged, missing_cost_flag, shock_reason,
                    is_backhaul, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                run_id,
                int(row["corridor_id"]),
                int(row["commodity_id"]),
                (int(vt_id) if pd.notna(vt_id) else None),
                float(row["recommended_quantity"]),
                float(row.get("recommended_quantity_kg") or 0),
                float(row["buy_price"]),
                float(row["sell_price"]),
                float(row["transport_cost"]),
                float(row["expected_profit_ngn"]),
                float(row["profit_margin_pct"]),
                bool(row["is_shock_flagged"]),
                bool(row["missing_cost_flag"]),
                row.get("shock_reason"),
                bool(row["is_backhaul"]),
                "Pending",
            ))
            inserted += 1
        except Exception as e:
            print(f"    Skipped recommendation: {e}")
    return inserted


def update_run_total(run_id, total_profit):
    db_execute(
        "UPDATE optimization_runs SET total_profit_ngn = ? WHERE id = ?",
        (round(float(total_profit), 2), run_id)
    )


def log_run(status, records_in=0, records_out=0, error=None, duration=None):
    try:
        db_execute("""
            INSERT INTO pipeline_logs
                (run_type, status, records_in, records_out,
                 error_message, duration_secs)
            VALUES (?, ?, ?, ?, ?, ?)
        """, ("Optimization", status, records_in, records_out,
              error, duration))
    except Exception as e:
        print(f"  Warning: could not write to pipeline_logs: {e}")


# ═══════════════════════════════════════════════════════════
# 8. PRINT RESULTS
# ═══════════════════════════════════════════════════════════

def print_recommendations(recommendations):
    if recommendations.empty:
        print("\n  No recommendations generated.")
        return
    print(f"\n{'='*80}")
    print(f"  TRADEFLOW NG — WEEKLY RECOMMENDATIONS")
    print(f"  Generated: {_utc_today()}")
    print(f"{'='*80}")
    for _, row in recommendations.sort_values(
        "expected_profit_ngn", ascending=False
    ).iterrows():
        flags = []
        if row.get("is_shock_flagged"):    flags.append("⚠ HIGH-RISK")
        if row.get("missing_cost_flag"):   flags.append("⚠ EST. TRANSPORT")
        if row.get("is_backhaul"):         flags.append("↩ BACKHAUL")
        flag_str = "  " + " | ".join(flags) if flags else ""
        print(
            f"\n  {row.get('commodity','?')} | "
            f"{row.get('origin','?')} → {row.get('destination','?')}{flag_str}"
        )
        print(f"  {'─'*60}")
        print(f"  Quantity:       {row['recommended_quantity']:>8.1f} units")
        print(f"  Buy price:      ₦{row['buy_price']:>10,.0f} / unit")
        print(f"  Sell price:     ₦{row['sell_price']:>10,.0f} / unit")
        print(f"  Transport cost: ₦{row['transport_cost']:>10,.0f} / unit")
        print(f"  Profit/unit:    ₦{row['profit_per_unit']:>10,.0f}")
        print(f"  Total profit:   ₦{row['expected_profit_ngn']:>10,.0f}")
        print(f"  Margin:         {row['profit_margin_pct']:>8.1f}%")
        if row.get("is_backhaul") and row.get("backhaul_note"):
            print(f"  {row['backhaul_note']}")

    total = recommendations["expected_profit_ngn"].sum()
    print(f"\n{'='*80}")
    print(f"  TOTAL EXPECTED PROFIT:  ₦{total:,.0f}")
    print(f"  RECOMMENDATIONS:        {len(recommendations)}")
    print(f"  HIGH-RISK ROUTES:       {int(recommendations['is_shock_flagged'].sum())}")
    print(f"  BACKHAUL OPPORTUNITIES: {int(recommendations['is_backhaul'].sum())}")
    print(f"{'='*80}\n")


# ═══════════════════════════════════════════════════════════
# 9. MAIN PIPELINE
# ═══════════════════════════════════════════════════════════

def run_optimization_pipeline():
    start      = datetime.now()
    week_start = _utc_today()
    week_end   = _utc_today() + timedelta(days=7)

    print(f"\n{'='*52}")
    print(f"  OPTIMIZATION PIPELINE — {start.strftime('%Y-%m-%d %H:%M')}")
    print(f"  Week: {week_start} to {week_end}")
    print(f"{'='*52}\n")

    try:
        print("[1/6] Loading data...")
        forecasts       = load_forecasts()
        supply_prices   = load_supply_prices()
        corridors       = load_corridors()
        transport_costs = load_transport_costs()

        if forecasts.empty:
            print("  ✗ No forecasts. Run forecasting pipeline first.")
            log_run("Failed", 0, 0, error="No forecasts available")
            return None

        print("\n[2/6] Building profit matrix...")
        vehicle_types = load_vehicle_types()
        est_vehicle   = pick_estimate_vehicle(vehicle_types)
        matrix, stale_suppressed = build_profit_matrix(
            forecasts, supply_prices, corridors, transport_costs,
            est_vehicle=est_vehicle,
        )
        if matrix.empty:
            # An empty matrix after suppression is an EXPECTED state (training
            # data too old), not a pipeline failure. Record the run honestly
            # and log "Skipped" so it doesn't page anyone.
            if stale_suppressed:
                msg = f"No viable routes (stale: {stale_suppressed} suppressed)"
                run_id = save_optimization_run(0, msg, week_start, week_end)
                print(f"  ⓘ {msg}.")
                print(f"     Forecasts are built on stale training data; "
                      f"refresh confirmed prices to restore recommendations.")
                log_run("Skipped", len(forecasts), 0, duration=round(
                    (datetime.now() - start).total_seconds(), 2))
                return None
            log_run("Failed", 0, 0, error="Empty profit matrix")
            return None

        print("[3/6] Running PuLP optimizer...")
        prob, Q, viable, status = run_optimization(
            matrix, supply_prices, forecasts
        )
        if prob is None:
            log_run("Failed", len(matrix), 0, error="Infeasible")
            return None

        print("\n[4/6] Extracting recommendations...")
        run_id = save_optimization_run(0, status, week_start, week_end)
        recommendations = extract_recommendations(prob, Q, viable, run_id)
        print(f"  {len(recommendations)} profitable routes recommended.")

        print("\n[5/6] Identifying backhaul opportunities...")
        recommendations = suggest_backhaul(recommendations)

        print("\n[6/6] Saving to database...")
        saved        = save_recommendations(recommendations, run_id)
        total_profit = (
            float(recommendations["expected_profit_ngn"].sum())
            if not recommendations.empty else 0.0
        )
        update_run_total(run_id, total_profit)

        duration = (datetime.now() - start).total_seconds()
        log_run("Success", len(matrix), saved, duration=round(duration, 2))

        print_recommendations(recommendations)
        print(f"  ✓ Optimization complete in {round(duration,1)}s")
        print(f"  Run ID: {run_id} | Saved: {saved} recommendations\n")
        return recommendations

    except Exception as e:
        duration = (datetime.now() - start).total_seconds()
        log_run("Failed", 0, 0, error=str(e), duration=round(duration, 2))
        print(f"\n  ✗ Optimization failed: {e}\n")
        raise


if __name__ == "__main__":
    run_optimization_pipeline()
