"""
TradeFlow NG — Password hashing utilities.

Uses bcrypt directly (not via passlib) to avoid the passlib 1.7.4 / bcrypt 4+
incompatibility. Verification is backward-compatible: it accepts both bcrypt
hashes and legacy plaintext passwords, so the codebase can be deployed BEFORE
the stored passwords are migrated. Once all rows are hashed, the plaintext
branch simply never matches.
"""

import hmac
import bcrypt

# bcrypt only considers the first 72 bytes of a password. We encode+truncate
# explicitly so long inputs hash deterministically instead of raising.
_MAX_BCRYPT_BYTES = 72


def _prepare(password: str) -> bytes:
    return password.encode("utf-8")[:_MAX_BCRYPT_BYTES]


def is_hashed(stored: str) -> bool:
    """True if the stored value looks like a bcrypt hash ($2a$/$2b$/$2y$…)."""
    return isinstance(stored, str) and stored.startswith("$2") and len(stored) >= 55


def hash_password(password: str) -> str:
    """Return a bcrypt hash for the given plaintext password."""
    return bcrypt.hashpw(_prepare(password), bcrypt.gensalt()).decode("utf-8")


def verify_password(password: str, stored: str) -> bool:
    """
    Verify a plaintext password against a stored value.

    - If `stored` is a bcrypt hash, verify with bcrypt.
    - Otherwise treat `stored` as legacy plaintext and compare in constant time.
    """
    if not stored:
        return False
    if is_hashed(stored):
        try:
            return bcrypt.checkpw(_prepare(password), stored.encode("utf-8"))
        except ValueError:
            return False
    # Legacy plaintext row — constant-time compare, still accepts login.
    return hmac.compare_digest(password, stored)
