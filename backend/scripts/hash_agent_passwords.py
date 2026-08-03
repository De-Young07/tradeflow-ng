import os
from typing import TYPE_CHECKING, Optional

from backend.security import hash_password, is_hashed

if TYPE_CHECKING:
    import asyncpg


def should_hash_password(stored: str | None) -> bool:
    if not stored:
        return False
    return not is_hashed(stored)


async def hash_agent_passwords(conn: "asyncpg.Connection") -> int:
    """Hash plaintext agent passwords in place and return the number updated."""
    rows = await conn.fetch(
        "SELECT id, password FROM agents WHERE is_active IS NOT FALSE ORDER BY id"
    )

    updated = 0
    for row in rows:
        stored = row["password"]
        if not should_hash_password(stored):
            continue

        hashed = hash_password(stored)
        await conn.execute("UPDATE agents SET password = $1 WHERE id = $2", hashed, row["id"])
        updated += 1

    return updated


async def hash_agent_passwords_from_dsn(dsn: Optional[str] = None) -> int:
    import asyncpg

    resolved = dsn or os.getenv("DATABASE_URL", "")
    if not resolved:
        raise RuntimeError("DATABASE_URL is not set")

    if resolved.startswith("postgres://"):
        resolved = resolved.replace("postgres://", "postgresql://", 1)

    conn = await asyncpg.connect(resolved)
    try:
        return await hash_agent_passwords(conn)
    finally:
        await conn.close()


def main() -> None:
    import asyncio

    try:
        updated = asyncio.run(hash_agent_passwords_from_dsn())
    except RuntimeError as exc:
        raise SystemExit(str(exc)) from exc

    print(f"Hashed {updated} plaintext agent password(s).")


if __name__ == "__main__":
    main()
