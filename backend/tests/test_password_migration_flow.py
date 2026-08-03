import os
import sys
import unittest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from backend.security import hash_password, is_hashed, verify_password
from backend.scripts.hash_agent_passwords import should_hash_password


class PasswordMigrationFlowTests(unittest.TestCase):
    def test_verify_password_accepts_plaintext_and_bcrypt(self):
        plaintext = "TradeFlow123!"
        self.assertTrue(verify_password(plaintext, plaintext))

        hashed = hash_password(plaintext)
        self.assertTrue(is_hashed(hashed))
        self.assertTrue(verify_password(plaintext, hashed))
        self.assertFalse(verify_password("wrong-password", hashed))

    def test_should_hash_password_only_for_plaintext_values(self):
        plaintext = "LegacyPass"
        self.assertTrue(should_hash_password(plaintext))

        hashed = hash_password(plaintext)
        self.assertFalse(should_hash_password(hashed))


if __name__ == "__main__":
    unittest.main()
