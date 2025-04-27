import os
from datetime import datetime, timedelta

GITHUB_API_URL = "https://api.github.com"
# !! IMPORTANT: Set this environment variable before running !!
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")

# --- Search Criteria ---
TARGET_LANGUAGE = "python"
MIN_STARS = 50
MIN_PUSH_DATE = (datetime.now() - timedelta(days=365 * 2)).strftime("%Y-%m-%d")
# Specify desired licenses (SPDX identifiers). Empty list means any license.
# Example: REQUIRED_LICENSES = ["mit", "apache-2.0", "bsd-3-clause"]
REQUIRED_LICENSES = [
    "mit",
    "apache-2.0",
    "gpl-2.0",
    "gpl-3.0",
    "lgpl-2.1",
    "lgpl-3.0",
    "bsd-3-clause",
    "bsd-2-clause",
]  # Common permissive/copyleft

# --- File Filtering Criteria ---
TARGET_EXTENSIONS = [".py"]
EXCLUDED_DIRS = [
    "site-packages",
    "node_modules",
    "vendor",
    ".git",
    "dist",
    "build",
    "__pycache__",
    "test",
    "tests",
    "example",
    "examples",
    "doc",
    "docs",
    ".venv",
    "env",
    "venv",
]
MAX_FILE_SIZE = 1024 * 1024  # 1 MB limit
MIN_FILE_LINES = 10

# --- Processing Limits ---
MAX_REPOS_TO_PROCESS = 20  # Max repos to attempt to fetch trees for
MAX_FILES_PER_REPO = 50  # Max files to fetch content for per repo
MAX_CONCURRENT_REQUESTS = 20  # Max simultaneous API requests

# --- Output ---
# Output from the initial crawl stage
RAW_OUTPUT_FILE = "raw_crawled_data.jsonl"
# Intermediate and final files for the pipeline
FILTERED_OUTPUT_FILE = "filtered_data.jsonl"
SCORED_OUTPUT_FILE = "scored_data.jsonl"
FINAL_PARQUET_FILE = "final_dataset.parquet"


# --- Rate Limiting ---
RATE_LIMIT_SLEEP_BUFFER = 5  # Add a small buffer to sleep time
REQUEST_DELAY = 0.05  # Minimum delay between requests (adjust based on observation)

# --- Pipeline Settings ---
# Deduplication scope ('file' or 'repo') - 'file' means unique content across all repos
DEDUPLICATION_SCOPE = "file"
