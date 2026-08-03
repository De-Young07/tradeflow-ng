"""
TradeFlow NG — Async Database Pool (asyncpg + PostgreSQL/Supabase)
"""

import os
import asyncpg
from typing import AsyncGenerator

pool: asyncpg.Pool | None = None

DATABASE_URL = os.environ.get("DATABASE_URL", "")


async def init_pool():
    global pool
    # Ensure correct URL scheme for asyncpg
    dsn = (
        DATABASE_URL.replace("postgres://", "postgresql://", 1)
        if DATABASE_URL.startswith("postgres://")
        else DATABASE_URL
    )
    pool = await asyncpg.create_pool(
        dsn=dsn,
        min_size=2,
        max_size=10,
        command_timeout=60,
        ssl="require",  # Required for cloud databases like Render & Supabase
        # Supabase's pooler (port 6543) runs pgbouncer in transaction mode,
        # which does not support prepared statements. Disable the statement
        # cache so asyncpg never issues named PREPARE calls. Safe for direct
        # (session-mode, port 5432) connections too.
        statement_cache_size=0,
    )


async def close_pool():
    global pool
    if pool:
        await pool.close()


async def fetch(sql: str, *args):
    if pool is None:
        raise RuntimeError("Database pool is not initialized.")
    async with pool.acquire() as conn:
        return await conn.fetch(sql, *args)


async def fetchrow(sql: str, *args):
    if pool is None:
        raise RuntimeError("Database pool is not initialized.")
    async with pool.acquire() as conn:
        return await conn.fetchrow(sql, *args)


async def fetchval(sql: str, *args):
    if pool is None:
        raise RuntimeError("Database pool is not initialized.")
    async with pool.acquire() as conn:
        return await conn.fetchval(sql, *args)


async def execute(sql: str, *args):
    if pool is None:
        raise RuntimeError("Database pool is not initialized.")
    async with pool.acquire() as conn:
        return await conn.execute(sql, *args)


def row_to_dict(record):
    if record is None:
        return None
    return dict(record)


def rows_to_list(records):
    return [dict(r) for r in records]


# FastApi dependency yielding an asyncpg connection from the pool
async def get_db() -> AsyncGenerator[asyncpg.Connection, None]:
    if pool is None:
        raise RuntimeError("Database pool is not initialized. Ensure lifespan calls init_pool().")
    async with pool.acquire() as conn:
        yield conn
        
