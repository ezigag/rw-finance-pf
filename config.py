import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

class Config:
    DATABASE_URL = os.getenv("DATABASE_URL")
    TRACKED_TICKERS = os.getenv("TRACKED_TICKERS", "")

config = Config()
