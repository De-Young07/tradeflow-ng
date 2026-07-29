"""
TradeFlow NG — SQLAlchemy Async Database
Compatible with FastAPI Depends(get_db)
"""

import os
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from sqlalchemy.orm import declarative_base

# -------------------------------------------------------------------
# Database URL
# -------------------------------------------------------------------

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL environment variable is not set.")

# -------------------------------------------------------------------
# Async Engine
# -------------------------------------------------------------------

engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    future=True,
    pool_pre_ping=True,
)

# -------------------------------------------------------------------
# Session Factory
# -------------------------------------------------------------------

AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)

# -------------------------------------------------------------------
# Base Model
# -------------------------------------------------------------------

Base = declarative_base()

# -------------------------------------------------------------------
# Dependency
# -------------------------------------------------------------------
