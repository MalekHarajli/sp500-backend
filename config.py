import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is missing from environment variables")

if not POLYGON_API_KEY:
    raise RuntimeError("POLYGON_API_KEY is missing from environment variables")
