import os
from dotenv import load_dotenv

load_dotenv()

MASSIVE_API_KEY = os.getenv("MASSIVE_API_KEY", "")
MASSIVE_BASE_URL = "https://api.polygon.io"

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
APP_BASE_URL = os.getenv("APP_BASE_URL", "http://localhost:8000")

RESEND_API_KEY = os.getenv("RESEND_API_KEY", "")
RECIPIENT_EMAIL = os.getenv("RECIPIENT_EMAIL", "abhinavkhaitan@gmail.com")

# Database: if DATABASE_URL is set, use PostgreSQL (Supabase); otherwise SQLite
DATABASE_URL = os.getenv("DATABASE_URL", "")
DB_PATH = os.getenv("DB_PATH", "news_digest.db")

DEFAULT_STOCKS = {
    "AAPL": "Apple",
    "MSFT": "Microsoft",
    "GOOGL": "Alphabet",
    "AMZN": "Amazon",
    "NVDA": "NVIDIA",
    "META": "Meta Platforms",
    "TSLA": "Tesla",
    "JPM": "JPMorgan Chase",
    "V": "Visa",
    "JNJ": "Johnson & Johnson",
}

REFRESH_HOUR = 6  # Nightly refresh at 6 AM
ARTICLE_RETENTION_DAYS = 7
