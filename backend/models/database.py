"""
TradeFlow NG — Async Database Pool (asyncpg + PostgreSQL/Supabase)
"""

import asyncpg
import os

pool: asyncpg.Pool | None = None

DATABASE_URL = os.environ.get("DATABASE_URL", "")


async def init_pool():
    global pool
    pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=2,
        max_size=10,
        command_timeout=60,
    )


async def close_pool():
    global pool
    if pool:
        await pool.close()


async def fetch(sql: str, *args):
    async with pool.acquire() as conn:
        return await conn.fetch(sql, *args)


async def fetchrow(sql: str, *args):
    async with pool.acquire() as conn:
        return await conn.fetchrow(sql, *args)


async def fetchval(sql: str, *args):
    async with pool.acquire() as conn:
        return await conn.fetchval(sql, *args)


async def execute(sql: str, *args):
    async with pool.acquire() as conn:
        return await conn.execute(sql, *args)


def row_to_dict(record):
    if record is None:
        return None
    return dict(record)


def rows_to_list(records):
    return [dict(r) for r in records]
