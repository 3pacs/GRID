#!/usr/bin/env python3
"""
Run once to generate the master password hash.
Sets GRID_MASTER_PASSWORD_HASH and GRID_JWT_SECRET in your .env file.

Usage: python scripts/setup_auth.py
"""

import re
import secrets
from pathlib import Path

from passlib.context import CryptContext

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def update_env(content: str, key: str, value: str) -> str:
    pattern = rf"^{key}=.*$"
    replacement = f"{key}={value}"
    if re.search(pattern, content, re.MULTILINE):
        return re.sub(pattern, replacement, content, flags=re.MULTILINE)
    return content + f"\n{key}={value}\n"


def main() -> None:
    print("=== GRID Authentication Setup ===\n")
    password = input("Set master password: ").strip()
    confirm = input("Confirm password: ").strip()

    if password != confirm:
        print("Passwords do not match.")
        exit(1)

    if len(password) < 12:
        print("Password must be at least 12 characters.")
        exit(1)

    hashed = pwd_context.hash(password)
    jwt_secret = secrets.token_hex(32)

    env_path = Path(".env")
    env_content = env_path.read_text() if env_path.exists() else ""

    env_content = update_env(env_content, "GRID_MASTER_PASSWORD_HASH", hashed)
    env_content = update_env(env_content, "GRID_JWT_SECRET", jwt_secret)
    env_content = update_env(env_content, "GRID_JWT_EXPIRE_HOURS", "168")
    env_content = update_env(
        env_content, "GRID_ALLOWED_ORIGINS", "https://grid.yourdomain.com"
    )

    env_path.write_text(env_content)
    print("\n✓ .env updated with hashed password and JWT secret.")
    print("✓ Never commit .env to version control.")
    print(
        "\nNext: start the API with: uvicorn api.main:app --host 0.0.0.0 --port 8000"
    )


if __name__ == "__main__":
    main()
