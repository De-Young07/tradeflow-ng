"""
TradeFlow NG — Pydantic Schemas
All request and response models for FastAPI routes.
"""

from pydantic import BaseModel
from typing import Optional, List, Any
from datetime import date, datetime


# ── Generic API wrapper ───────────────────────────────────────
class APIResponse(BaseModel):
    data: Any = None
    error: Optional[str] = None
    status: str = "ok"


# ── Pipeline log ──────────────────────────────────────────────
class PipelineLog(BaseModel):
    id: int
    run_type: str
    status: str
    records_in: Optional[int] = None
    records_out: Optional[int] = None
    error_message: Optional[str] = None
    duration_secs: Optional[float] = None
    run_at: Optional[datetime] = None


# ── Overview ──────────────────────────────────────────────────
class OverviewResponse(BaseModel):
    total_profit: float
    n_recommendations: int
    n_agents: int
    n_price_records: int
    last_forecast_date: Optional[str] = None
    last_optimization_date: Optional[str] = None
    pipeline_logs: List[PipelineLog] = []
    avg_margin: float = 0.0
    n_backhauls: int = 0
    n_shock_flags: int = 0


# ── Recommendations ───────────────────────────────────────────
class Recommendation(BaseModel):
    id: int
    run_id: int
    corridor_id: int
    commodity_id: int
    commodity_name: Optional[str] = None
    origin: Optional[str] = None
    destination: Optional[str] = None
    recommended_quantity: float = 0.0
    buy_price: float = 0.0
    sell_price: float = 0.0
    transport_cost: float = 0.0
    profit_per_unit: float = 0.0
    expected_profit_ngn: float = 0.0
    profit_margin_pct: float = 0.0
    is_shock_flagged: bool = False
    is_backhaul: bool = False
    missing_cost_flag: Optional[bool] = None
    shock_reason: Optional[str] = None
    backhaul_note: Optional[str] = None
    status: Optional[str] = None


# ── Forecasts ─────────────────────────────────────────────────
class ForecastPoint(BaseModel):
    date: str
    predicted_price: float
    lower_bound: float
    upper_bound: float
    is_shock_flagged: bool = False
    is_forecast: bool = True


class HistoricalPoint(BaseModel):
    date: str
    price: float
    is_forecast: bool = False


class ForecastResponse(BaseModel):
    state: str
    commodity: str
    historical: List[HistoricalPoint] = []
    forecast: List[ForecastPoint] = []
    next_week_avg: float = 0.0
    price_range_low: float = 0.0
    price_range_high: float = 0.0
    high_risk_days: int = 0


# ── Prices ────────────────────────────────────────────────────
class PricePoint(BaseModel):
    state: str
    commodity: str
    price: float
    price_date: str
    market: Optional[str] = None


class PriceTrendPoint(BaseModel):
    date: str
    state: str
    price: float


# ── Agents ────────────────────────────────────────────────────
class Agent(BaseModel):
    id: int
    full_name: str
    agent_id: str
    phone: Optional[str] = None
    state: Optional[str] = None
    market: Optional[str] = None
    is_active: bool = True
    submission_count: int = 0


class CreateAgentRequest(BaseModel):
    full_name: str
    agent_id: str
    password: str
    phone: Optional[str] = None
    state_id: int
    market_id: Optional[int] = None
    role: str = "Reporter"


# ── Price submission ──────────────────────────────────────────
class SubmitPriceRequest(BaseModel):
    commodity_id: int
    market_id: Optional[int] = None
    reported_price: float
    quantity_available: Optional[float] = None
    quality_grade: Optional[str] = None
    availability: Optional[str] = None
    road_condition: Optional[str] = None
    obs_date: Optional[date] = None
    notes: Optional[str] = None


# ── Trade feedback ────────────────────────────────────────────
class FeedbackRequest(BaseModel):
    recommendation_id: int
    actual_buy_price: float
    actual_sell_price: float
    actual_transport_cost: Optional[float] = None
    actual_quantity: Optional[float] = None
    trade_date: date
    notes: Optional[str] = None


# ── Tableau / profit matrix ───────────────────────────────────
class TableauCell(BaseModel):
    origin: str
    destination: str
    commodity: str
    profit_per_unit: float = 0.0
    margin_pct: float = 0.0
    is_profitable: bool = False


class TableauResponse(BaseModel):
    commodity: str
    cells: List[TableauCell] = []
    top_routes: List[Recommendation] = []


# ── Pipeline result ───────────────────────────────────────────
class PipelineResult(BaseModel):
    status: str
    records_cleaned: int = 0
    forecasts_written: int = 0
    recommendations_generated: int = 0
    duration_secs: float = 0.0
    error: Optional[str] = None
