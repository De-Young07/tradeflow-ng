"""
TradeFlow NG — Pydantic Schemas
"""
from pydantic import BaseModel
from typing import Optional, List
from datetime import date, datetime


# ── Auth ──────────────────────────────────────────────────
class AdminLoginRequest(BaseModel):
    username: str
    password: str

class AgentLoginRequest(BaseModel):
    agent_id: str
    password: str

class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"

class AgentData(BaseModel):
    id: int
    name: str
    state: str
    market: str
    agent_id: str
    state_id: Optional[int] = None
    market_id: Optional[int] = None

class AgentTokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    agent_data: AgentData


# ── Price Submission ──────────────────────────────────────
class PriceSubmitRequest(BaseModel):
    commodity_id: int
    market_id: int
    reported_price: float
    quantity_available: Optional[float] = None
    quality_grade: Optional[str] = None
    availability: Optional[str] = None
    road_condition: Optional[str] = None
    obs_date: str
    notes: Optional[str] = None


# ── Trade Outcome ─────────────────────────────────────────
class TradeOutcomeRequest(BaseModel):
    recommendation_id: int
    actual_buy_price: float
    actual_sell_price: float
    actual_transport_cost: float
    actual_quantity: float
    trade_date: str
    notes: Optional[str] = None


# ── Admin Feedback ────────────────────────────────────────
class AdminFeedbackRequest(BaseModel):
    recommendation_id: int
    actual_buy_price: float
    actual_sell_price: float
    actual_transport_cost: float
    actual_quantity: float
    trade_date: str
    notes: Optional[str] = None


# ── Create Agent ──────────────────────────────────────────
class CreateAgentRequest(BaseModel):
    full_name: str
    agent_id: str
    password: str
    phone: str
    state_id: int
    market_id: int
    role: Optional[str] = "Field Agent"


# ── Generic Response wrapper ──────────────────────────────
class APIResponse(BaseModel):
    data: Optional[object] = None
    error: Optional[str] = None
    status: str = "ok"
