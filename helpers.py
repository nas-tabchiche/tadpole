import asyncio
import base64
import json
import re
import time

import aiohttp
from loguru import logger

import settings

# --- Async API Request Handling ---


async def make_api_request(session, url, headers, semaphore):
    """Makes an asynchronous API request with rate limit handling and concurrency control."""
    async with semaphore:  # Acquire semaphore before making request
        # Add a small delay to be polite to the API
        await asyncio.sleep(settings.REQUEST_DELAY)

        try:
            logger.trace(f"Requesting URL: {url}")
            async with session.get(url, headers=headers) as response:
                remaining = response.headers.get("X-RateLimit-Remaining")
                reset_time = response.headers.get("X-RateLimit-Reset")

                if remaining is not None:
                    logger.trace(f"Rate Limit Remaining: {remaining}")
                    if int(remaining) == 0:
                        reset_timestamp = int(reset_time)
                        sleep_time = (
                            max(0, reset_timestamp - time.time())
                            + settings.RATE_LIMIT_SLEEP_BUFFER
                        )
                        logger.warning(
                            f"Rate limit 0 reached. Sleeping for {sleep_time:.2f} seconds."
                        )
                        await asyncio.sleep(sleep_time)
                        # Retry the request after sleeping
                        logger.info(
                            f"Retrying request to {url} after rate limit sleep."
                        )
                        # Need to release and re-acquire semaphore for the retry
                        # NOTE: This simple retry might fail if the rate limit persists.
                        # A more robust solution would involve a loop or backoff strategy.
                        return await make_api_request(session, url, headers, semaphore)

                response.raise_for_status()  # Raise AiohttpHttpProcessingError for bad status codes
                # Check if response is JSON before trying to decode
                if "application/json" in response.headers.get("Content-Type", ""):
                    return await response.json()
                else:
                    logger.warning(
                        f"Non-JSON response received from {url}. Content-Type: {response.headers.get('Content-Type')}"
                    )
                    return None

        except aiohttp.ClientResponseError as e:
            logger.error(f"HTTP Error: {e.status} for URL: {url}")
            logger.error(f"Message: {e.message}")
            # Specific handling for 404 Not Found might be useful
            if e.status == 404:
                logger.warning(f"Resource not found (404): {url}")
            # Specific handling for 403 Forbidden (often rate limits or auth issues)
            elif e.status == 403:
                logger.error(
                    f"Forbidden (403). Check token/permissions or potential secondary rate limit. URL: {url}"
                )
                # Check headers for rate limit info even on 403
                remaining = e.headers.get("X-RateLimit-Remaining")
                reset_time = e.headers.get("X-RateLimit-Reset")
                if remaining == "0":
                    reset_timestamp = int(reset_time)
                    sleep_time = (
                        max(0, reset_timestamp - time.time())
                        + settings.RATE_LIMIT_SLEEP_BUFFER
                    )
                    logger.warning(
                        f"Secondary rate limit likely hit. Sleeping for {sleep_time:.2f} seconds based on 403 header."
                    )
                    await asyncio.sleep(sleep_time)
            # NOTE: We might want to implement retries for transient server errors (5xx) here
            return None
        except aiohttp.ClientConnectionError as e:
            logger.error(f"Connection Error: {e} for URL: {url}")
            return None
        except asyncio.TimeoutError:
            logger.error(f"Request timed out for URL: {url}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error during API request to {url}: {e}")
            return None


async def search_repositories(session, query, headers, semaphore, per_page=100):
    """Searches repositories asynchronously."""
    # NOTE: GitHub search API has stricter rate limits (e.g., 30 reqs/min)
    logger.info(f"Searching repos with query: {query}")
    search_url = f"{settings.GITHUB_API_URL}/search/repositories"
    # Fetch more per page with async
    params = {"q": query, "per_page": per_page, "sort": "stars", "order": "desc"}

    # Use a separate semaphore or adjust delays if search rate limit is hit often
    async with semaphore:
        await asyncio.sleep(settings.REQUEST_DELAY)  # Polite delay
        try:
            async with session.get(
                search_url, headers=headers, params=params
            ) as response:
                remaining = response.headers.get("X-RateLimit-Remaining")
                if remaining is not None:
                    logger.trace(f"Search Rate Limit Remaining: {remaining}")
                    # Add specific sleep logic if search limit (0) is hit

                response.raise_for_status()
                if "application/json" in response.headers.get("Content-Type", ""):
                    data = await response.json()
                    return data.get("items", [])
                else:
                    logger.error(f"Non-JSON response from search API: {search_url}")
                    return []
        except aiohttp.ClientResponseError as e:
            logger.error(f"Search request failed: {e.status} - {e.message}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error during search: {e}")
            return []


async def get_repo_tree(session, owner, repo, headers, semaphore):
    """Gets the recursive file tree asynchronously."""
    logger.info(f"Getting tree for {owner}/{repo}")

    # Get repo info to find default branch and license
    repo_info_url = f"{settings.GITHUB_API_URL}/repos/{owner}/{repo}"
    repo_info = await make_api_request(session, repo_info_url, headers, semaphore)
    if not repo_info:
        logger.warning(f"Failed to get repo info for {owner}/{repo}")
        return None, None

    default_branch = repo_info.get("default_branch")
    license_info = repo_info.get("license")
    repo_license = (
        license_info.get("spdx_id", "NOASSERTION") if license_info else "NOASSERTION"
    )

    if not default_branch:
        logger.error(f"Could not determine default branch for {owner}/{repo}")
        return None, repo_license

    # Get the tree using the default branch name
    tree_url = f"{settings.GITHUB_API_URL}/repos/{owner}/{repo}/git/trees/{default_branch}?recursive=1"
    tree_data = await make_api_request(session, tree_url, headers, semaphore)

    if tree_data and tree_data.get("truncated"):
        logger.warning(
            f"Tree data for {owner}/{repo} was truncated. Some files may be missed."
        )

    # Check license against requirements
    if (
        settings.REQUIRED_LICENSES
        and repo_license.lower() not in settings.REQUIRED_LICENSES
        and repo_license.upper() != "NOASSERTION"
    ):
        logger.info(
            f"Skipping repo {owner}/{repo}: License '{repo_license}' not in required list {settings.REQUIRED_LICENSES}."
        )
        return None, repo_license  # Return None for tree to skip processing files

    return tree_data.get("tree", []) if tree_data else [], repo_license


async def get_blob_content(session, owner, repo, file_sha, headers, semaphore):
    """Gets the content of a file blob asynchronously."""
    logger.debug(f"Getting blob {file_sha[:7]} for {owner}/{repo}")
    blob_url = f"{settings.GITHUB_API_URL}/repos/{owner}/{repo}/git/blobs/{file_sha}"
    blob_data = await make_api_request(session, blob_url, headers, semaphore)
    if not blob_data:
        return None

    content = blob_data.get("content")
    encoding = blob_data.get("encoding")

    if content and encoding == "base64":
        try:
            # Decoding can be CPU-bound, run in executor for very large files if needed
            # For now, let's keep it simple.
            return base64.b64decode(content).decode("utf-8", errors="replace")
        except Exception as e:
            logger.error(f"Failed to decode base64 content for blob {file_sha}: {e}")
            return None
    elif content:
        # NOTE: Should generally be base64, but let's handle direct content just in case
        logger.warning(f"Blob {file_sha} for {owner}/{repo} was not base64 encoded.")
        return content
    return None


# --- Sync Helper Functions (Can remain synchronous as they are CPU-bound) ---


def is_file_relevant(file_info):
    """Checks if a file meets the filtering criteria."""
    path = file_info.get("path", "").lower()
    size = file_info.get("size")
    file_type = file_info.get("type")

    if file_type != "blob":
        return False

    if size is None or size == 0 or size > settings.MAX_FILE_SIZE:  # Skip empty files
        return False

    if not any(path.endswith(ext) for ext in settings.TARGET_EXTENSIONS):
        return False

    # Check against excluded directory patterns
    path_parts = path.split("/")
    if any(part in settings.EXCLUDED_DIRS for part in path_parts):
        return False
    # More specific check: is any *directory* in the path excluded?
    for i in range(len(path_parts) - 1):
        if path_parts[i] in settings.EXCLUDED_DIRS:
            return False

    return True


def sanitize_content(content):
    """Performs basic checks for PII or secrets (Placeholder)."""
    # NOTE: Consider moving regex compilation outside the function for performance
    # if called very frequently.
    email_regex = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")
    key_regex = re.compile(r"-----BEGIN (?:RSA|OPENSSH|EC|PGP) PRIVATE KEY")

    findings = []

    try:
        emails = email_regex.findall(content)
        if emails:
            findings.append(
                {"type": "pii_email", "matches": list(set(emails))}
            )  # Store unique emails

        keys = key_regex.findall(content)
        if keys:
            findings.append({"type": "potential_private_key", "count": len(keys)})
    except Exception as e:
        logger.error(f"Error during sanitization regex: {e}")
        # TODO: Decide how to handle content that causes regex errors

    # TODO: Add more checks here (e.g., common API key patterns)

    return findings
