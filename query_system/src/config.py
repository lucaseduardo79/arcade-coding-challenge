"""Configuration for the LLM query system."""

import os

GROQ_MODEL = os.environ.get("GROQ_MODEL", "llama-3.3-70b-versatile")
GROQ_TEMPERATURE = 0

DB_PATH = os.environ.get("DB_PATH", "shared/db/financial_data.duckdb")

MAX_CONVERSATION_HISTORY = 10
MAX_QUERY_ROWS = 50
MAX_SQL_RETRIES = 3
