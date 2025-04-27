import os
from datetime import datetime, timedelta

GITHUB_API_URL = "https://api.github.com"
# !! IMPORTANT: Set this environment variable before running !!
# On Linux/macOS: export GITHUB_TOKEN="your_personal_access_token"
# On Windows (cmd): set GITHUB_TOKEN="your_personal_access_token"
# On Windows (PowerShell): $env:GITHUB_TOKEN="your_personal_access_token"
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")

# Search Criteria
TARGET_LANGUAGE = "python"
MIN_STARS = 50
MIN_PUSH_DATE = (datetime.now() - timedelta(days=365 * 2)).strftime("%Y-%m-%d")


# File Filtering Criteria
TARGET_EXTENSIONS = [".py", ".md", ".txt"]
EXCLUDED_DIRS = [
    "site-packages",
    "node_modules",
    "vendor",
    ".git",
    "dist",
    "build",
    "__pycache__",
]
MAX_FILE_SIZE = 1024 * 1024

MAX_REPOS_TO_PROCESS = 10
MAX_FILES_PER_REPO = 10
