from pathlib import Path

import pytest
from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def env() -> dict[str, str]:
    """Values from .env.example, the documented defaults."""
    return {k: v for k, v in dotenv_values(ROOT / ".env.example").items() if v is not None}
