# config/settings.py
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# -------------------------------------------------
# Local Data Folders
# -------------------------------------------------
CATALOG = "agentic_dq_local"
BRONZE_DIR = "data/bronze"
SILVER_DIR = "data/silver"
QUARANTINE_DIR = "data/quarantine"
HISTORY_FILE = "data/system/dq_history.json"

# -------------------------------------------------
# LangSmith Configuration (optional)
# -------------------------------------------------
os.environ["LANGCHAIN_TRACING_V2"] = os.getenv("LANGCHAIN_TRACING_V2", "false")
os.environ["LANGCHAIN_PROJECT"] = os.getenv("LANGCHAIN_PROJECT", "Agentic-DQ-Gemini")
os.environ["LANGCHAIN_ENDPOINT"] = os.getenv("LANGCHAIN_ENDPOINT", "https://api.langsmith.com")
os.environ["LANGCHAIN_API_KEY"] = os.getenv("LANGSMITH_API_KEY", "")

# -------------------------------------------------
# Gemini Configuration
# -------------------------------------------------
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if not GEMINI_API_KEY:
    raise ValueError("GEMINI_API_KEY environment variable is required")

# -------------------------------------------------
# Email Configuration
# -------------------------------------------------
EMAIL_SENDER = os.getenv("EMAIL_SENDER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
EMAIL_RECIPIENTS = os.getenv("EMAIL_RECIPIENTS", "").split(",") if os.getenv("EMAIL_RECIPIENTS") else []
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))

# Validate email configuration (optional - can be configured later)
EMAILS_CONFIGURED = bool(EMAIL_SENDER and EMAIL_PASSWORD)
if not EMAILS_CONFIGURED:
    import logging
    logger = logging.getLogger(__name__)
    logger.debug("Email configuration not set. Email alerts will be skipped. Set EMAIL_SENDER and EMAIL_PASSWORD to enable.")

