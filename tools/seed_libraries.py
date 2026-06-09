#!/usr/bin/env python3
"""One-time seed of starter libraries into DynamoDB under placeholder owners.

Each film is written under owner key "seed:<name>" for ONE chat. Afterwards each
named person runs `/claim <name>` in that chat, which reassigns the library to
their real Telegram user_id (we map by user_id, never phone number — Telegram
never gives bots a phone number).

    python3 tools/seed_libraries.py --chat-id=-1001234567890 --dry-run
    python3 tools/seed_libraries.py --chat-id=-1001234567890 --file tools/seed_libraries.json

NOTE: group chat_ids are negative, so use the = form (--chat-id=-100...) or
argparse treats the value as a flag.

Re-runnable, but it APPENDS — running twice double-seeds. Use --dry-run first.
This tool is operator-only; it does not ship in the Lambda package.
"""
import argparse
import json
import os
import re
from datetime import datetime, timezone


def _slugify(s):
    s = re.sub(r"[^a-z0-9]+", "-", (s or "").lower()).strip("-")
    return s or "film"


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--chat-id", required=True, help="Telegram chat_id of the group")
    ap.add_argument("--file", default=os.path.join(os.path.dirname(__file__), "seed_libraries.json"))
    ap.add_argument("--table", default=os.environ.get("DDB_TABLE", "GraciaBotData"))
    ap.add_argument("--region", default=os.environ.get("AWS_REGION", "us-east-1"))
    ap.add_argument("--dry-run", action="store_true", help="print only, write nothing")
    args = ap.parse_args()

    with open(args.file, encoding="utf-8") as fh:
        libraries = json.load(fh)["libraries"]

    pk = f"movie#{args.chat_id}"
    now = datetime.now(timezone.utc).isoformat()
    table = None
    if not args.dry_run:
        import boto3
        table = boto3.resource("dynamodb", region_name=args.region).Table(args.table)

    total = 0
    for name, films in libraries.items():
        owner = f"seed:{name.strip().lower()}"
        for f in films:
            note = f.get("note", "")
            slug = _slugify(f["title"])
            item = {
                "PK": pk,
                "SK": f"lib#{owner}#{slug}",     # slug-keyed, like add_to_library
                "slug": slug,
                "owner_id": owner,               # placeholder until claimed
                "seed_name": name.strip(),
                "title": f["title"],
                "year": str(f.get("year", "")),
                "genres": [],                    # filled when looked up later
                "description": "",
                "rating": None,
                "rating_scale": None,
                "rating_source": None,
                "added_at": f.get("addedDate", now),
                "watched": False,                   # set only by an actual win
            }
            if note:
                item["note"] = note
            total += 1
            if not args.dry_run:
                table.put_item(Item=item)
        flag = "  (empty)" if not films else ""
        print(f"  {name}: {len(films)} films -> {owner}{flag}")

    head = "DRY RUN — would seed" if args.dry_run else "Seeded"
    print(f"\n{head} {total} films into {pk}.")
    print("Next: each person runs  /claim <name>  in the chat to link their library.")


if __name__ == "__main__":
    main()
