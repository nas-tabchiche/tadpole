import json
import time

from loguru import logger

import settings
from helpers import (
    get_blob_content,
    get_repo_tree,
    is_file_relevant,
    sanitize_content,
    search_repositories,
)

if __name__ == "__main__":
    if not settings.GITHUB_TOKEN:
        logger.error("GITHUB_TOKEN environment variable not set.")
        exit(1)

    headers = {
        "Authorization": f"token {settings.GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    }

    # Construct search query
    # TODO: Add license filter here too e.g., license:mit license:apache-2.0
    query = f"language:{settings.TARGET_LANGUAGE} stars:>{settings.MIN_STARS} pushed:>{settings.MIN_PUSH_DATE}"

    repositories = search_repositories(
        query, headers, per_page=settings.MAX_REPOS_TO_PROCESS * 2
    )  # Fetch more initially

    if not repositories:
        logger.info("No repositories found matching the criteria.")
        exit(0)

    logger.info(f"Found {len(repositories)} potential repositories.")
    logger.debug("Repositories:")
    for repo in repositories:
        logger.debug(f"{repo['html_url']} (Stars: {repo['stargazers_count']})")

    processed_repo_count = 0
    output_file = "crawled_data.jsonl"

    with open(output_file, "w", encoding="utf-8") as f:
        for repo in repositories:
            if processed_repo_count >= settings.MAX_REPOS_TO_PROCESS:
                break

            owner = repo["owner"]["login"]
            repo_name = repo["name"]
            repo_url = repo["html_url"]
            logger.info(f"Processing Repository: {owner}/{repo_name}")

            # Naive delay to be nice to the API
            time.sleep(1)

            tree, repo_license = get_repo_tree(owner, repo_name, headers)

            if tree is None:
                logger.warning(f"Skipping repo {owner}/{repo_name} due to tree error.")
                continue

            processed_repo_count += 1
            files_processed_in_repo = 0

            for file_info in tree:
                if files_processed_in_repo >= settings.MAX_FILES_PER_REPO:
                    break

                if is_file_relevant(file_info):
                    logger.info(
                        f"Relevant file: {file_info['path']} (Size: {file_info.get('size', 'N/A')})"
                    )

                    # Naive delay
                    time.sleep(0.5)

                    content = get_blob_content(
                        owner, repo_name, file_info["sha"], headers
                    )

                    if content:
                        findings = sanitize_content(content)

                        # Prepare output data
                        output_data = {
                            "repo_url": repo_url,
                            "path": file_info["path"],
                            "size": file_info.get("size"),
                            "license": repo_license,
                            "content_sha": file_info["sha"],
                            "findings": findings,
                            "content": content,
                        }

                        # Write as jsonl
                        f.write(json.dumps(output_data) + "\n")
                        files_processed_in_repo += 1
                    else:
                        logger.warning(
                            f"Could not fetch content for {file_info['path']}"
                        )

            logger.info(
                f"Finished processing {owner}/{repo_name} ({files_processed_in_repo} files)"
            )

    logger.info(f"Crawling finished. Processed {processed_repo_count} repositories.")
    logger.info(f"Output data saved to {output_file}")
