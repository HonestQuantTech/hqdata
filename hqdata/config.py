"""Configuration loading from .env file"""

from pathlib import Path
from dotenv import load_dotenv

# Load .env from current working directory first (user's project root)
# Then fall back to package directory (for development)
_cwd_env = Path.cwd() / ".env"
_pkg_env = Path(__file__).parent.parent / ".env"

if _cwd_env.exists():
    load_dotenv(_cwd_env)
elif _pkg_env.exists():
    load_dotenv(_pkg_env)
