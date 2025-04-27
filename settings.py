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
REQUIRED_LICENSES = []

# --- File Filtering Criteria ---
TARGET_EXTENSIONS = [".py", ".md", ".txt"]
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
]
MAX_FILE_SIZE = 1024 * 1024  # 1 MB limit

# --- Processing Limits ---
MAX_REPOS_TO_PROCESS = 10  # Max repos to attempt to fetch trees for
MAX_FILES_PER_REPO = 20  # Max files to fetch content for per repo
MAX_CONCURRENT_REQUESTS = 15  # Max simultaneous API requests

# --- Output ---
OUTPUT_FILE = "crawled_data_async.jsonl"

# --- Rate Limiting ---
# Add a small buffer to sleep time after hitting rate limit
RATE_LIMIT_SLEEP_BUFFER = 5
# Minimum delay between requests to be polite (in seconds)
REQUEST_DELAY = 0.1
