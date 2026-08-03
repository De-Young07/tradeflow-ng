"""
TradeFlow NG — Authentication
JWT tokens for admin (username + password) and agent (agent_id + password).

ROUTES (after prefix /auth in main.py):
  POST /auth/admin/login  → admin JWT
  POST /auth/agent/login  → agent JWT + profile
"""

import os
from datetime import datetime, timedelta, timezone
from typing import Optional

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import JWTError, jwt
from pydantic import BaseModel

from models.database import get_db
from security import verify_password

router   = APIRouter()
security = HTTPBearer()

# ── Config ──────────────────────────────────────────────────
JWT_SECRET       = os.getenv("JWT_SECRET", "tradeflow-dev-secret-change-in-prod")
JWT_ALGORITHM    = "HS256"
JWT_EXPIRY_HOURS = 24

ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "admin")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "tradeflow2026")


# ── Schemas ─────────────────────────────────────────────────
class AdminLoginRequest(BaseModel):
    username: str
    password: str

class AgentLoginRequest(BaseModel):
    agent_id: str
    password:  str

class TokenResponse(BaseModel):
    access_token: str
    token_type:   str = "bearer"

class AgentTokenResponse(TokenResponse):
    agent_data: dict


# ── JWT helpers ─────────────────────────────────────────────
def create_token(payload: dict, role: str) -> str:
    data = payload.copy()
    data.update({
        "role": role,
        "exp":  datetime.now(timezone.utc) + timedelta(hours=JWT_EXPIRY_HOURS),
        "iat":  datetime.now(timezone.utc),
    })
    return jwt.encode(data, JWT_SECRET, algorithm=JWT_ALGORITHM)


def decode_token(token: str) -> dict:
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"},
        )


# ── Dependencies ────────────────────────────────────────────
def require_admin(
    credentials: HTTPAuthorizationCredentials = Depends(security),
) -> dict:
    payload = decode_token(credentials.credentials)
    if payload.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    return payload


def require_agent(
    credentials: HTTPAuthorizationCredentials = Depends(security),
) -> dict:
    payload = decode_token(credentials.credentials)
    if payload.get("role") not in ("agent", "admin"):
        raise HTTPException(status_code=403, detail="Agent access required")
    return payload


# ── Admin login ──────────────────────────────────────────────
# Full path: POST /auth/admin/login  (prefix /auth + this route /admin/login)
@router.post("/admin/login", response_model=TokenResponse)
async def admin_login(body: AdminLoginRequest):
    if body.username.strip() != ADMIN_USERNAME or body.password.strip() != ADMIN_PASSWORD:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
        )
    token = create_token(
        {"sub": body.username, "username": body.username},
        role="admin",
    )
    return {"access_token": token, "token_type": "bearer"}


# ── Agent login ──────────────────────────────────────────────
# Full path: POST /auth/agent/login  (prefix /auth + this route /agent/login)
# NOTE: was incorrectly /auth/agent/login causing double-prefix → /auth/auth/agent/login
@router.post("/agent/login", response_model=AgentTokenResponse)
async def agent_login(body: AgentLoginRequest, db: asyncpg.Connection = Depends(get_db)):
    aid = body.agent_id.strip().upper()
    pwd = body.password.strip()

    # Fetch by agent_id only, then verify the password in Python. This supports
    # both bcrypt-hashed and legacy plaintext passwords during migration.
    row = await db.fetchrow("""
        SELECT a.id, a.full_name, a.agent_id, a.phone, a.password,
               a.state_id, a.market_id,
               s.name AS state_name,
               m.name AS market_name
        FROM   agents a
        LEFT JOIN states  s ON a.state_id  = s.id
        LEFT JOIN markets m ON a.market_id = m.id
        WHERE  a.agent_id = $1
        AND  a.is_active IS NOT FALSE
    """, aid)

    if not row or not verify_password(pwd, row["password"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect Agent ID or password",
        )

    agent_data = {
        "id":        row["id"],
        "name":      row["full_name"],
        "agent_id":  row["agent_id"],
        "phone":     row["phone"],
        "state_id":  row["state_id"],
        "state":     row["state_name"],
        "market_id": row["market_id"],
        "market":    row["market_name"],
    }
    token = create_token(
        {
            "sub":         aid,
            "agent_db_id": row["id"],
            "state_id":    row["state_id"],
        },
        role="agent",
    )
    return {"access_token": token, "token_type": "bearer", "agent_data": agent_data}
