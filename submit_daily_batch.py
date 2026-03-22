"""Submit the daily budget of atomic QuickStatements via the QuickStatements API.

Only submits Phase 1 (P459 qualifiers) and Phase 1.5 (P1027→P459 replacement)
lines, which are atomic operations safe for unattended execution.

Phase 3 migration lines are non-atomic (remove old + add new) and require
manual review, so they are NOT submitted here.

Expects environment variables:
  QUICKSTATEMENTS_API_KEY  - API token from QuickStatements user page
  QUICKSTATEMENTS_USERNAME - Wikidata username associated with the token
"""

import io
import os
import random
import sys
import time
import requests

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

QS_API = "https://quickstatements.toolforge.org/api.php"
MAX_LINES_PER_BATCH = 200

ATOMIC_FILES = [
    "modern_shrine_ranking_qualifiers.txt",   # Phase 1: add P459 to existing P13723
    "replace_p1027_with_p459.txt",            # Phase 1.5: swap P1027→P459
]


def read_batch(filepath, max_lines=MAX_LINES_PER_BATCH):
    """Read up to max_lines from a file, return as list of non-empty lines."""
    if not os.path.exists(filepath):
        return []
    with open(filepath, "r", encoding="utf-8") as f:
        lines = []
        for i, line in enumerate(f):
            if i >= max_lines:
                break
            stripped = line.strip()
            if stripped:
                lines.append(stripped)
    return lines


def submit_batch(lines, token, username, batch_name):
    """Submit a batch of QuickStatements v1 lines to the API.

    Returns (success: bool, message: str).
    """
    if not lines:
        return True, "No lines to submit"

    data = "||".join(lines)

    r = requests.post(
        QS_API,
        data={
            "action": "import",
            "submit": "1",
            "format": "v1",
            "data": data,
            "username": username,
            "token": token,
            "batchname": batch_name,
            "compress": "1",
        },
        headers={"User-Agent": "ModernQuickstatements/1.0 (daily cron batch)"},
        timeout=120,
    )

    if r.status_code != 200:
        return False, f"HTTP {r.status_code}: {r.text[:500]}"

    result = r.json()
    if "batch_id" in result:
        return True, f"Batch created: #{result['batch_id']}"
    return False, f"API error: {result}"


def main():
    token = os.environ.get("QUICKSTATEMENTS_API_KEY", "")
    username = os.environ.get("QUICKSTATEMENTS_USERNAME", "")

    if not token or not username:
        print("ERROR: QUICKSTATEMENTS_API_KEY and QUICKSTATEMENTS_USERNAME must be set")
        sys.exit(1)

    # Random delay 1-3600 seconds to avoid predictable timing
    delay = random.randint(1, 3600)
    print(f"Waiting {delay}s before submitting ({delay // 60}m {delay % 60}s)...")
    time.sleep(delay)

    for filepath in ATOMIC_FILES:
        lines = read_batch(filepath)
        if not lines:
            print(f"{filepath}: nothing to submit")
            continue

        batch_name = f"auto: {os.path.splitext(filepath)[0]} ({len(lines)} lines)"
        print(f"{filepath}: submitting {len(lines)} lines as '{batch_name}'...")

        success, message = submit_batch(lines, token, username, batch_name)
        print(f"  → {message}")

        if not success:
            print("Submission failed, giving up.")
            sys.exit(1)

        # Small gap between batches
        time.sleep(5)

    print("Done.")


if __name__ == "__main__":
    main()
