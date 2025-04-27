import base64
import json
import re
import time

import requests
from loguru import logger

import settings


def make_api_request(url, headers):
    """Makes an API request and handles basic error/rate limit checks."""
    try:
        response = requests.get(url, headers=headers)

        # Basic Rate Limit Info (Visible for debugging)
        remaining = response.headers.get("X-RateLimit-Remaining")
        reset_time = response.headers.get("X-RateLimit-Reset")
        if remaining is not None:
            logger.debug(f"Rate Limit Remaining: {remaining}")
            if int(remaining) == 0:
                reset_timestamp = int(reset_time)
                sleep_time = max(0, reset_timestamp - time.time()) + 5  # Add buffer
                logger.warning(
                    f"Rate limit exceeded. Sleeping for {sleep_time:.2f} seconds."
                )
                time.sleep(sleep_time)
                # Retry after sleeping (optional, could also just exit)
                response = requests.get(url, headers=headers)

        response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {e}")
        logger.error(f"URL: {url}")
        if hasattr(e, "response") and e.response is not None:
            logger.info(f"Response Status: {e.response.status_code}")
            logger.info(
                f"Response Text: {e.response.text[:500]}"
            )  # Print first 500 chars
        return None
    except json.JSONDecodeError as e:
        logger.error(f"Failed to decode JSON response: {e}")
        logger.error(f"URL: {url}")
        return None


def search_repositories(query, headers, per_page=30):
    """Searches repositories based on a query."""
    logger.info(f"Searching repos with query: {query}")
    search_url = f"{settings.GITHUB_API_URL}/search/repositories"
    params = {"q": query, "per_page": per_page, "sort": "stars", "order": "desc"}

    try:
        response = requests.get(search_url, headers=headers, params=params)
        response.raise_for_status()
        # Basic Rate Limit Info (Visible for debugging)
        remaining = response.headers.get("X-RateLimit-Remaining")
        if remaining is not None:
            logger.debug(f"Rate Limit Remaining: {remaining}")
        return response.json().get("items", [])
    except requests.exceptions.RequestException as e:
        logger.error(f"Search request failed: {e}")
        return []
    except json.JSONDecodeError as e:
        logger.error(f"Failed to decode search JSON response: {e}")
        return []


def get_repo_tree(owner, repo, headers):
    """Gets the recursive file tree for the default branch."""
    logger.info(f"Getting tree for {owner}/{repo}")
    # Get repo info to find default branch
    repo_info_url = f"{settings.GITHUB_API_URL}/repos/{owner}/{repo}"
    repo_info = make_api_request(repo_info_url, headers)
    if not repo_info:
        return None, None  # Return None for license too

    default_branch = repo_info.get("default_branch")
    license_info = repo_info.get("license")
    repo_license = (
        license_info.get("spdx_id", "NOASSERTION") if license_info else "NOASSERTION"
    )

    if not default_branch:
        logger.error(f"Could not determine default branch for {owner}/{repo}")
        return None, repo_license

    # Get the tree using the default branch name
    # NOTE: A more robust way involves getting the commit SHA for the branch first.
    tree_url = f"{settings.GITHUB_API_URL}/repos/{owner}/{repo}/git/trees/{default_branch}?recursive=1"
    tree_data = make_api_request(tree_url, headers)

    # Handle truncated results (very large repos)
    if tree_data and tree_data.get("truncated"):
        logger.warning(
            f"Tree data for {owner}/{repo} was truncated. Some files may be missed."
        )

    return tree_data.get("tree", []) if tree_data else [], repo_license


def get_blob_content(owner, repo, file_sha, headers):
    """Gets the content of a file blob."""
    logger.debug(f"Getting blob {file_sha[:7]} for {owner}/{repo}")
    blob_url = f"{settings.GITHUB_API_URL}/repos/{owner}/{repo}/git/blobs/{file_sha}"
    blob_data = make_api_request(blob_url, headers)
    if not blob_data:
        return None

    content = blob_data.get("content")
    encoding = blob_data.get("encoding")

    if content and encoding == "base64":
        try:
            return base64.b64decode(content).decode("utf-8", errors="replace")
        except Exception as e:
            logger.error(f"Failed to decode base64 content for blob {file_sha}: {e}")
            return None
    elif content:  # If not base64, assume it's utf-8 text (less common for API)
        return content
    return None


def is_file_relevant(file_info):
    """Checks if a file meets the filtering criteria."""
    path = file_info.get("path", "").lower()
    size = file_info.get("size")
    file_type = file_info.get("type")

    if file_type != "blob":
        return False

    if size is None or size > settings.MAX_FILE_SIZE:
        return False

    if not any(path.endswith(ext) for ext in settings.TARGET_EXTENSIONS):
        return False

    if any(excluded in path.split("/") for excluded in settings.EXCLUDED_DIRS):
        return False

    return True


def sanitize_content(content):
    """Performs basic checks for PII or secrets (Placeholder)."""
    findings = []
    # Very basic email regex
    emails = re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", content)
    if emails:
        findings.append({"type": "pii_email", "matches": emails})

    # Very basic private key check
    keys = re.findall(
        r"-----BEGIN (?:RSA|OPENSSH|EC|PGP) PRIVATE KEY BLOCK-----", content
    )
    if keys:
        findings.append({"type": "potential_private_key", "count": len(keys)})

    return findings
