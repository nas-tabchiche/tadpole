import json
import hashlib
import os
from loguru import logger
import pyarrow as pa
import pyarrow.parquet as pq

import settings
from helpers import sanitize_content  # Import the existing sanitizer

logger.remove()
logger.add("pipeline_debug.log", rotation="10 MB", level="TRACE", encoding="utf-8")
logger.add(lambda msg: print(msg, end=""), level="DEBUG", format="{message}")

# --- Pipeline Stage Functions ---


def calculate_content_hash(content):
    """Calculates the SHA256 hash of the content."""
    return hashlib.sha256(content.encode("utf-8", errors="replace")).hexdigest()


def filter_and_sanitize(record, seen_hashes):
    """
    Applies filtering, sanitization, and exact deduplication.
    Returns the processed record if it passes, otherwise None.
    """
    content = record.get("content")
    if not content:
        return None  # Skip if no content

    # Basic Filtering (Example: line count)
    line_count = content.count("\n") + 1
    if line_count < settings.MIN_FILE_LINES:
        logger.trace(
            f"Skipping {record.get('path', 'N/A')} due to line count ({line_count} < {settings.MIN_FILE_LINES})"
        )
        return None

    # Calculate Hash for Deduplication
    content_hash = calculate_content_hash(content)

    # Deduplication Check
    if content_hash in seen_hashes:
        logger.trace(
            f"Skipping {record.get('path', 'N/A')} as duplicate content (hash: {content_hash[:8]})"
        )
        return None  # Skip duplicate

    # Add hash to seen set *only if* scope is file-level
    if settings.DEDUPLICATION_SCOPE == "file":
        seen_hashes.add(content_hash)

    # Sanitization (PII/Secrets)
    findings = sanitize_content(content)

    # Add hash and findings to the record
    record["processed_content_hash"] = content_hash
    record["sanitization_findings"] = findings

    record["line_count"] = line_count

    # Remove original git blob sha if not needed downstream
    # record.pop('content_sha', None)

    return record


def score_and_annotate(record):
    """
    Applies scoring heuristics and adds annotations.
    Returns the annotated record.
    """
    content = record.get("content", "")
    lines = content.splitlines()
    code_lines = [
        line for line in lines if line.strip() and not line.strip().startswith("#")
    ]
    comment_lines = [line for line in lines if line.strip().startswith("#")]

    score = 0.0
    annotations = {}

    total_lines = len(lines)
    if total_lines > 0:
        comment_ratio = len(comment_lines) / total_lines
        density = len(code_lines) / total_lines
        annotations["comment_ratio"] = round(comment_ratio, 3)
        annotations["code_density"] = round(density, 3)

        # Basic scoring based on heuristics
        score += density * 0.5  # Higher density is slightly good
        if 0.05 < comment_ratio < 0.3:  # Comments in a reasonable range
            score += 0.2

    # Check for test keywords (very basic)
    if any(
        kw in content.lower()
        for kw in ["import unittest", "import pytest", " test", " assert "]
    ):
        annotations["has_tests_keyword"] = True
        score += 0.3
    else:
        annotations["has_tests_keyword"] = False

    record["quality_score"] = round(
        max(0.0, min(1.0, score)), 3
    )  # Clamp score between 0 and 1
    record["annotations"] = annotations

    return record


# --- Main Pipeline Execution ---


def run_pipeline():
    """Runs the full data processing pipeline."""
    logger.info("Starting data processing pipeline...")

    if not os.path.exists(settings.RAW_OUTPUT_FILE):
        logger.error(f"Raw input file not found: {settings.RAW_OUTPUT_FILE}")
        return

    processed_records = []
    seen_content_hashes = set()  # For file-level deduplication
    records_read = 0
    records_filtered = 0
    records_deduplicated = 0

    logger.info(
        f"Stage 1: Reading '{settings.RAW_OUTPUT_FILE}', Filtering, Sanitizing, Deduplicating..."
    )

    # Process line by line to handle large files
    with open(settings.RAW_OUTPUT_FILE, "r", encoding="utf-8") as infile:
        for line in infile:
            records_read += 1
            try:
                raw_record = json.loads(line)
            except json.JSONDecodeError:
                logger.warning(f"Skipping invalid JSON line: {line.strip()}")
                continue

            # Apply Stage 1: Filter, Sanitize, Deduplicate
            processed_record = filter_and_sanitize(raw_record, seen_content_hashes)

            if processed_record:
                processed_records.append(processed_record)
            else:
                # Crude way to check if it was deduplicated vs other filters
                temp_hash = calculate_content_hash(raw_record.get("content", ""))
                if temp_hash in seen_content_hashes:
                    records_deduplicated += 1
                else:
                    records_filtered += (
                        1  # Filtered for other reasons (size, lines, etc.)
                    )

            if records_read % 1000 == 0:
                logger.debug(
                    f"Read: {records_read}, Filtered: {records_filtered}, Deduplicated: {records_deduplicated}, Kept: {len(processed_records)}"
                )

    logger.info(
        f"Finished Stage 1. Read: {records_read}, Filtered: {records_filtered}, Deduplicated: {records_deduplicated}, Kept: {len(processed_records)}"
    )

    if not processed_records:
        logger.warning(
            "No records remaining after filtering and deduplication. Exiting."
        )
        return

    logger.info("Stage 2: Scoring and Annotating...")
    scored_records = []
    for i, record in enumerate(processed_records):
        scored_record = score_and_annotate(record)
        scored_records.append(scored_record)
        if (i + 1) % 1000 == 0:
            logger.debug(f"Scored {i + 1}/{len(processed_records)} records.")

    logger.info(f"Finished Stage 2. Scored {len(scored_records)} records.")

    # Optional: Save intermediate scored data as jsonl for inspection
    # logger.info(f"Saving intermediate scored data to {settings.SCORED_OUTPUT_FILE}...")
    # with open(settings.SCORED_OUTPUT_FILE, 'w', encoding='utf-8') as outfile:
    #     for record in scored_records:
    #         outfile.write(json.dumps(record) + '\n')

    logger.info("Stage 3: Converting to Parquet format...")
    try:
        # Define the schema (optional but good practice)
        # Inferring schema can work but explicit is safer
        # NOTE: PyArrow might struggle with nested dicts ('annotations', 'findings') directly
        # We might need to flatten or serialize them first for robust Parquet writing.

        table = pa.Table.from_pylist(scored_records)

        # Write table to Parquet file
        pq.write_table(table, settings.FINAL_PARQUET_FILE, compression="snappy")
        logger.info(
            f"Finished Stage 3. Final dataset saved to '{settings.FINAL_PARQUET_FILE}'"
        )

    except Exception as e:
        logger.error(f"Failed to write Parquet file: {e}")
        logger.error(
            "Consider flattening nested structures (findings, annotations) or ensuring consistent keys if errors persist."
        )

    logger.info("Data processing pipeline finished.")


if __name__ == "__main__":
    run_pipeline()
