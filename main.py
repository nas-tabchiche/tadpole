import asyncio
import json
import time
import aiohttp
from loguru import logger

import settings
from helpers import (
    get_blob_content,
    get_repo_tree,
    is_file_relevant,
    sanitize_content,
    search_repositories,
)

logger.remove()  # Remove default logger
logger.add("crawler_debug.log", rotation="10 MB", level="TRACE")  # Log debug to file
logger.add(lambda msg: print(msg, end=""), level="DEBUG", format="{message}")


async def process_repo(
    session, repo_info, headers, semaphore, output_file_handle
) -> int:
    """Fetches tree, filters files, fetches content, and writes to output for a single repo."""
    owner = repo_info["owner"]["login"]
    repo_name = repo_info["name"]
    repo_url = repo_info["html_url"]
    logger.info(f"Processing Repository: {owner}/{repo_name}")

    tree, repo_license = await get_repo_tree(
        session, owner, repo_name, headers, semaphore
    )

    if tree is None:
        # Reason for skipping (e.g., license mismatch) might be logged in get_repo_tree
        logger.warning(f"Skipping repo {owner}/{repo_name} (Tree invalid or filtered).")
        return 0

    files_to_process = []
    for file_info in tree:
        if len(files_to_process) >= settings.MAX_FILES_PER_REPO:
            logger.debug(
                f"Reached max files ({settings.MAX_FILES_PER_REPO}) for {owner}/{repo_name}"
            )
            break
        if is_file_relevant(file_info):
            files_to_process.append(file_info)

    if not files_to_process:
        logger.info(f"No relevant files found in {owner}/{repo_name}")
        return 0

    logger.info(
        f"Fetching content for {len(files_to_process)} files in {owner}/{repo_name}..."
    )

    # Create tasks to fetch blob content concurrently
    blob_tasks = []
    for file_info in files_to_process:
        task = asyncio.create_task(
            get_blob_content(
                session, owner, repo_name, file_info["sha"], headers, semaphore
            )
        )
        blob_tasks.append((task, file_info))  # Keep track of file_info with its task

    files_processed_count = 0
    for task, file_info in blob_tasks:
        try:
            # Timeout per blob fetch task can be added here if needed
            content = await asyncio.wait_for(task, timeout=60.0)

            if content:
                # CPU-bound sanitization - could run in executor if becomes bottleneck
                findings = sanitize_content(content)

                output_data = {
                    "repo_url": repo_url,
                    "path": file_info["path"],
                    "size": file_info.get("size"),
                    "license": repo_license,
                    "content_sha": file_info["sha"],
                    "findings": findings,
                    "content": content,  # Storing full content now
                }
                output_file_handle.write(json.dumps(output_data) + "\n")
                files_processed_count += 1
            else:
                logger.warning(
                    f"Could not fetch or decode content for {file_info['path']} in {owner}/{repo_name}"
                )
        except asyncio.TimeoutError:
            logger.error(
                f"Timeout fetching blob {file_info['sha']} for {file_info['path']} in {owner}/{repo_name}"
            )
        except Exception as e:
            logger.error(
                f"Error processing blob task for {file_info['path']} in {owner}/{repo_name}: {e}"
            )

    logger.info(
        f"Finished processing {owner}/{repo_name} ({files_processed_count} files written)"
    )
    return files_processed_count


async def main():
    """Main async function to run the crawler."""
    start_time = time.time()

    if not settings.GITHUB_TOKEN:
        logger.error("GITHUB_TOKEN environment variable not set.")
        exit(1)

    headers = {
        "Authorization": f"token {settings.GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    }

    # Create a semaphore to limit concurrent requests
    semaphore = asyncio.Semaphore(settings.MAX_CONCURRENT_REQUESTS)

    # Use a single session for connection pooling
    async with aiohttp.ClientSession(headers=headers) as session:
        # Construct search query
        license_query_part = ""
        if settings.REQUIRED_LICENSES:
            license_query_part = " ".join(
                [f"license:{lic}" for lic in settings.REQUIRED_LICENSES]
            )

        query = f"language:{settings.TARGET_LANGUAGE} stars:>{settings.MIN_STARS} pushed:>{settings.MIN_PUSH_DATE} {license_query_part}".strip()

        # Initial search
        # TODO: Handle paginated responses
        repositories = await search_repositories(
            session, query, headers, semaphore, per_page=100
        )

        if not repositories:
            logger.info("No repositories found matching the criteria.")
            return

        logger.info(
            f"Found {len(repositories)} potential repositories from initial search."
        )

        # Limit the number of repos we actually process fully
        repos_to_process = repositories[: settings.MAX_REPOS_TO_PROCESS]
        logger.info(
            f"Attempting to process details for {len(repos_to_process)} repositories."
        )

        with open(settings.OUTPUT_FILE, "w", encoding="utf-8") as f:
            # Create tasks to process repositories concurrently
            repo_tasks = [
                process_repo(session, repo, headers, semaphore, f)
                for repo in repos_to_process
            ]

            # Wait for all repository processing tasks to complete
            results = await asyncio.gather(*repo_tasks, return_exceptions=True)

            total_files_processed = 0
            processed_repo_count = 0
            for i, result in enumerate(results):
                repo_name = f"{repos_to_process[i]['owner']['login']}/{repos_to_process[i]['name']}"
                if isinstance(result, Exception):
                    logger.error(f"Error processing repository {repo_name}: {result}")
                elif (
                    result is not None
                ):  # process_repo returns file count or None on initial skip
                    processed_repo_count += 1
                    total_files_processed += (
                        result  # Add count of files processed for this repo
                    )
                # Else: Repo was skipped intentionally (e.g., license), already logged.

    end_time = time.time()
    logger.info("Crawling Summary")
    logger.info(f"Crawling finished in {end_time - start_time:.2f} seconds.")
    logger.info(f"Attempted to process {len(repos_to_process)} repositories.")
    logger.info(
        f"Successfully processed details for {processed_repo_count} repositories."
    )
    logger.info(f"Total files written: {total_files_processed}")
    logger.info(f"Output data saved to {settings.OUTPUT_FILE}")


if __name__ == "__main__":
    asyncio.run(main())
