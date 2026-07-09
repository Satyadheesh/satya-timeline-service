"""
export_snapshot.py — one-time Turso -> local SQLite snapshot for the sharded replay.

Reads all eligible articles (EXACT same filter as timeline_pipeline.py, imported
from there — single source of truth) in id-ordered batches, writes them into a
local snapshot.db, then computes N equal-article-count shard windows over
(scraped_at, id) order and writes shards.json.

Turso cost: roughly one full scan of the articles table, once.
"""
import os
import sys
import json
import time
import logging
import sqlite3
import argparse
from datetime import datetime

from timeline_pipeline import ELIGIBILITY_SQL  # noqa: E402 — single source of truth

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

SNAPSHOT_SCHEMA = """
CREATE TABLE IF NOT EXISTS articles (
    id INTEGER PRIMARY KEY,
    title TEXT,
    rephrased_title TEXT,
    rephrased_article BLOB,
    scraped_at INTEGER,
    party_mentioned TEXT,
    ministers_mentioned TEXT,
    states_mentioned TEXT,
    cities_mentioned TEXT,
    civic_flag INTEGER,
    category TEXT,
    status TEXT
);
CREATE INDEX IF NOT EXISTS idx_articles_sa_id ON articles(scraped_at, id);
"""


def turso_connect():
    db_url = os.environ.get('SATYA_DB_URL')
    db_token = os.environ.get('SATYA_DB_TOKEN')
    if not db_url or not (db_url.startswith('libsql://') or db_url.startswith('https://')):
        logging.critical("SATYA_DB_URL is not set (or not a remote URL). The export must run against Turso. Aborting.")
        sys.exit(1)
    import libsql
    logging.info(f"Connecting to remote Turso Database at: {db_url}")
    return libsql.connect(database=db_url, auth_token=db_token)


def compute_shards(ordered_rows, num_shards):
    """ordered_rows: list of (scraped_at, id) tuples sorted by (scraped_at, id).
    Returns contiguous, non-overlapping, gap-free windows of ~equal article
    count. Window membership: (from_ts, from_id) <= (sa, id) < (to_ts, to_id)."""
    total = len(ordered_rows)
    shards = []
    for k in range(num_shards):
        lo = k * total // num_shards
        hi = (k + 1) * total // num_shards
        if lo >= hi:
            continue  # more shards than articles — skip empty
        from_ts, from_id = ordered_rows[lo]
        if hi < total:
            to_ts, to_id = ordered_rows[hi]
        else:
            to_ts, to_id = ordered_rows[-1][0], ordered_rows[-1][1] + 1
        shards.append({
            "shard": k,
            "from_ts": int(from_ts), "from_id": int(from_id),
            "to_ts": int(to_ts), "to_id": int(to_id),
            "from_date": datetime.fromtimestamp(int(from_ts)).strftime("%Y-%m-%d") if from_ts else "epoch-0",
            "to_date": datetime.fromtimestamp(int(to_ts)).strftime("%Y-%m-%d") if to_ts else "epoch-0",
            "count": hi - lo,
        })
    return shards


def main():
    parser = argparse.ArgumentParser(description="Export Turso articles snapshot + shard windows for the parallel replay")
    parser.add_argument('--out', type=str, default='./snapshot.db', help="Output snapshot SQLite path")
    parser.add_argument('--shards', type=int, default=20, help="Number of shard windows")
    parser.add_argument('--config-out', type=str, default='./shards.json', help="Output shards config path")
    parser.add_argument('--batch', type=int, default=2000, help="Rows per remote fetch batch")
    args = parser.parse_args()

    if os.path.exists(args.out):
        logging.info(f"Removing existing {args.out} for a clean export.")
        os.remove(args.out)

    local = sqlite3.connect(args.out)
    local.executescript(SNAPSHOT_SCHEMA)
    local.commit()

    remote = turso_connect()

    last_id = 0
    total = 0
    t0 = time.time()
    while True:
        rows = None
        for attempt in range(2):
            try:
                cur = remote.cursor()
                cur.execute(f"""
                    SELECT a.id, a.title, a.rephrased_title, a.rephrased_article,
                           COALESCE(a.scraped_at, 0), a.party_mentioned, a.ministers_mentioned,
                           a.states_mentioned, a.cities_mentioned, COALESCE(a.civic_flag, 0),
                           a.category, a.status
                    FROM articles a
                    WHERE a.id > ?
                      AND {ELIGIBILITY_SQL}
                    ORDER BY a.id ASC
                    LIMIT ?
                """, (last_id, args.batch))
                rows = cur.fetchall()
                break
            except Exception as e:
                logging.error(f"Remote fetch failed (attempt {attempt + 1}): {e}")
                if attempt == 0:
                    logging.info("Reconnecting to Turso and retrying...")
                    try:
                        remote.close()
                    except Exception:
                        pass
                    remote = turso_connect()
                else:
                    raise

        if not rows:
            break

        local.executemany(
            "INSERT OR REPLACE INTO articles VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows
        )
        local.commit()
        last_id = int(rows[-1][0])
        total += len(rows)
        logging.info(f"Exported {total} articles so far (id cursor {last_id})...")

    try:
        remote.close()
    except Exception:
        pass

    if total == 0:
        logging.critical("No eligible articles exported — aborting before writing shards config.")
        sys.exit(1)

    logging.info(f"Export complete: {total} articles in {time.time() - t0:.0f}s. Computing shard windows...")

    cur = local.execute("SELECT scraped_at, id FROM articles ORDER BY scraped_at ASC, id ASC")
    ordered_rows = cur.fetchall()
    max_id = local.execute("SELECT MAX(id) FROM articles").fetchone()[0]

    shards = compute_shards(ordered_rows, args.shards)
    assert sum(s['count'] for s in shards) == total, "Shard counts must cover every article exactly once"

    config = {
        "num_shards": len(shards),
        "total_articles": total,
        "max_id": int(max_id),
        "generated_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "shards": shards,
    }
    with open(args.config_out, 'w') as f:
        json.dump(config, f, indent=2)

    local.close()
    print(json.dumps(config, indent=2))
    print(f"snapshot_articles={total}")
    print(f"snapshot_max_id={max_id}")


if __name__ == '__main__':
    main()
