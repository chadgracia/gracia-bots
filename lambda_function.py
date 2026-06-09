"""
Gracia Telegram Bots — multi-mode bot platform on AWS Lambda.

One handler backs several BotFather identities. Each bot has its own token and
its own webhook, registered at the Lambda Function URL with a distinct path
suffix (.../movie, .../cleaning, .../salary) and a per-bot secret_token.

Routing:  rawPath -> mode  ->  verify X-Telegram-Bot-Api-Secret-Token  ->  mode handler.

Everything lives in this one file on purpose: the deploy step is
`zip -j function.zip lambda_function.py`, which packages ONLY this file. Adding
helper modules would silently drop them from the deployment package.
"""

import base64
import json
import logging
import os
import random
import re
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError

log = logging.getLogger()
log.setLevel(logging.INFO)

# --------------------------------------------------------------------------- #
# Config (env)
# --------------------------------------------------------------------------- #
REGION = os.environ.get("AWS_REGION", "us-east-1")
DDB_TABLE = os.environ.get("DDB_TABLE", "GraciaBotData")
# Same model the pipeline-agent Lambda uses. Overridable via env; the default is
# the confirmed working value. Set BEDROCK_MODEL_ID="" to disable AI replies
# (add/list/draw still work).
BEDROCK_MODEL_ID = os.environ.get("BEDROCK_MODEL_ID", "us.anthropic.claude-sonnet-4-6").strip()
AI_ENABLED = bool(BEDROCK_MODEL_ID)
# TMDB is OPTIONAL: it supplies runtime/genres/synopsis/similar titles and is the
# rating fallback. The headline rating comes from Letterboxd, which needs no key.
TMDB_API_KEY = os.environ.get("TMDB_API_KEY", "").strip()

_ddb = boto3.resource("dynamodb", region_name=REGION)
_table = _ddb.Table(DDB_TABLE)
_bedrock = boto3.client("bedrock-runtime", region_name=REGION)


# --------------------------------------------------------------------------- #
# MODES registry — adding a bot later = add token env, secret env, a row here,
# and a handler function.
# --------------------------------------------------------------------------- #
def _mode_config():
    return {
        "movie": {
            "token_env": "MOVIE_BOT_TOKEN",
            "secret_env": "MOVIE_WEBHOOK_SECRET",
            "handler": handle_movie,
        },
        "cleaning": {
            "token_env": "CLEANING_BOT_TOKEN",
            "secret_env": "CLEANING_WEBHOOK_SECRET",
            "handler": handle_cleaning,
        },
        "salary": {
            "token_env": "SALARY_BOT_TOKEN",
            "secret_env": "SALARY_WEBHOOK_SECRET",
            "handler": handle_salary,
        },
    }


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _decimals_to_native(obj):
    """DynamoDB returns numbers as Decimal; make them JSON-friendly."""
    if isinstance(obj, list):
        return [_decimals_to_native(v) for v in obj]
    if isinstance(obj, dict):
        return {k: _decimals_to_native(v) for k, v in obj.items()}
    if isinstance(obj, Decimal):
        return int(obj) if obj % 1 == 0 else float(obj)
    return obj


# --------------------------------------------------------------------------- #
# DynamoDB helpers (generic)
# --------------------------------------------------------------------------- #
def ddb_get(pk, sk):
    resp = _table.get_item(Key={"PK": pk, "SK": sk})
    return _decimals_to_native(resp.get("Item"))


def ddb_put(item):
    _table.put_item(Item=item)
    return item


def ddb_delete(pk, sk):
    _table.delete_item(Key={"PK": pk, "SK": sk})


def ddb_query(pk):
    """All items under one partition key, paginated."""
    items, kwargs = [], {
        "KeyConditionExpression": boto3.dynamodb.conditions.Key("PK").eq(pk)
    }
    while True:
        resp = _table.query(**kwargs)
        items.extend(resp.get("Items", []))
        if "LastEvaluatedKey" not in resp:
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    return _decimals_to_native(items)


# --------------------------------------------------------------------------- #
# Chat registry + supergroup migration
# --------------------------------------------------------------------------- #
# Chat id is NEVER hardcoded. We persist a registry item per (mode, chat) so
# proactive senders (e.g. a future morning-after poll cron) can resolve the
# current id, and so we can rewrite data when Telegram migrates a group to a
# supergroup (the failure that haunted the previous third-party setup).
def _pk(mode, chat_id):
    return f"{mode}#{chat_id}"


def remember_chat(mode, chat_id, title=None):
    item = {
        "PK": _pk(mode, chat_id),
        "SK": "chat",
        "mode": mode,
        "chat_id": int(chat_id),
        "last_seen": _now_iso(),
    }
    if title:
        item["title"] = title
    ddb_put(item)


def migrate_chat(mode, old_chat_id, new_chat_id):
    """Rewrite every item from the old chat partition to the new one."""
    if int(old_chat_id) == int(new_chat_id):
        return
    log.info("migrating %s chat %s -> %s", mode, old_chat_id, new_chat_id)
    old_pk, new_pk = _pk(mode, old_chat_id), _pk(mode, new_chat_id)
    for item in ddb_query(old_pk):
        item["PK"] = new_pk
        if item.get("SK") == "chat":
            item["chat_id"] = int(new_chat_id)
            item["last_seen"] = _now_iso()
        ddb_put(item)
        ddb_delete(old_pk, item["SK"])
    remember_chat(mode, new_chat_id)


def seen_update(mode, chat_id, update_id):
    """Idempotency: True if this update_id was already processed for this chat.

    Telegram retries webhook deliveries; a duplicate must never double-add a
    film, double-count a veto, or re-roll a selection. We claim the update_id
    with a conditional put — the first writer wins, retries see it and bail.
    """
    if update_id is None:
        return False
    try:
        _table.put_item(
            Item={
                "PK": _pk(mode, chat_id),
                "SK": f"dedupe#{update_id}",
                "seen_at": _now_iso(),
                # epoch TTL (1 day) — harmless if TTL isn't enabled on the table.
                "ttl": int(datetime.now(timezone.utc).timestamp()) + 86400,
            },
            ConditionExpression="attribute_not_exists(PK)",
        )
        return False
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            return True
        log.warning("dedupe check errored (proceeding): %s", e)
        return False


# --------------------------------------------------------------------------- #
# Telegram I/O
# --------------------------------------------------------------------------- #
def _tg_request(token, method, payload):
    url = f"https://api.telegram.org/bot{token}/{method}"
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/json"}, method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "replace")
        try:
            return json.loads(body)
        except ValueError:
            return {"ok": False, "error_code": e.code, "description": body}
    except Exception as e:  # network/timeout
        log.error("telegram %s failed: %s", method, e)
        return {"ok": False, "description": str(e)}


def _token_for(mode):
    cfg = _mode_config()[mode]
    token = os.environ.get(cfg["token_env"], "").strip()
    if not token:
        raise RuntimeError(f"missing env {cfg['token_env']} for mode {mode}")
    return token


def send_message(mode, chat_id, text, **kwargs):
    """Send text. On supergroup migration, update the stored id and retry once."""
    token = _token_for(mode)
    payload = {"chat_id": chat_id, "text": text, **kwargs}
    resp = _tg_request(token, "sendMessage", payload)
    if not resp.get("ok"):
        params = resp.get("parameters") or {}
        new_id = params.get("migrate_to_chat_id")
        desc = (resp.get("description") or "").lower()
        if new_id and "supergroup" in desc:
            migrate_chat(mode, chat_id, new_id)
            payload["chat_id"] = new_id
            resp = _tg_request(token, "sendMessage", payload)
        else:
            log.error("sendMessage to %s failed: %s", chat_id, resp.get("description"))
    return resp


def send_photo(mode, chat_id, photo_url, caption=None, **kwargs):
    token = _token_for(mode)
    payload = {"chat_id": chat_id, "photo": photo_url, **kwargs}
    if caption:
        payload["caption"] = caption
    resp = _tg_request(token, "sendPhoto", payload)
    if not resp.get("ok"):
        params = resp.get("parameters") or {}
        new_id = params.get("migrate_to_chat_id")
        if new_id and "supergroup" in (resp.get("description") or "").lower():
            migrate_chat(mode, chat_id, new_id)
            payload["chat_id"] = new_id
            resp = _tg_request(token, "sendPhoto", payload)
    return resp


def parse_update(update):
    """Flatten the bits we care about out of a Telegram update."""
    msg = (
        update.get("message")
        or update.get("edited_message")
        or update.get("channel_post")
        or {}
    )
    chat = msg.get("chat", {}) or {}
    frm = msg.get("from", {}) or {}
    reply_to = msg.get("reply_to_message") or {}
    return {
        "update_id": update.get("update_id"),
        "chat_id": chat.get("id"),
        "chat_title": chat.get("title") or chat.get("username"),
        "chat_type": chat.get("type"),
        "text": msg.get("text") or msg.get("caption") or "",
        "user_id": frm.get("id"),
        "user_name": (frm.get("first_name") or frm.get("username") or "someone"),
        "username": frm.get("username"),
        # message_id this update is a reply to (None if not a reply). This is the
        # backbone of confirmation/veto routing under privacy mode ON.
        "reply_to_message_id": reply_to.get("message_id"),
        "migrate_to_chat_id": msg.get("migrate_to_chat_id"),
        "migrate_from_chat_id": msg.get("migrate_from_chat_id"),
        "raw_message": msg,
    }


def parse_command(text):
    """'/movie@GraciaBot The Thing' -> ('movie', 'The Thing'). Else (None, text)."""
    if not text.startswith("/"):
        return None, text
    head, _, rest = text.partition(" ")
    cmd = head[1:].split("@", 1)[0].lower()
    return cmd, rest.strip()


# --------------------------------------------------------------------------- #
# Film data — Letterboxd is the rating source (0-5). TMDB supplies everything
# else (runtime/genres/synopsis/similar) and is the rating fallback only.
# All of this is isolated here so the source can be swapped later.
# --------------------------------------------------------------------------- #
_LB_BASE = "https://letterboxd.com"


def _http_get(url, timeout=12):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "replace")


def _letterboxd_rating(title):
    """Scrape the Letterboxd weighted average (0-5) for a title.

    Letterboxd has no public API, so this searches, opens the top film result,
    and reads aggregateRating out of that page's JSON-LD block. Returns a dict
    or None. It is HTML scraping: it can break if Letterboxd changes their
    markup, or be blocked from some server IPs — TMDB is the rating fallback.
    """
    try:
        search_html = _http_get(f"{_LB_BASE}/search/films/{urllib.parse.quote(title)}/")
    except Exception as e:
        log.warning("letterboxd search failed: %s", e)
        return None
    m = (re.search(r'data-target-link="(/film/[^"]+/)"', search_html)
         or re.search(r'href="(/film/[^"/]+/)"', search_html))
    if not m:
        return None
    slug = m.group(1)
    try:
        film_html = _http_get(f"{_LB_BASE}{slug}")
    except Exception as e:
        log.warning("letterboxd film page failed: %s", e)
        return None
    block = re.search(r'<script type="application/ld\+json">(.*?)</script>',
                      film_html, re.DOTALL)
    if not block:
        return None
    raw = block.group(1).replace("/* <![CDATA[ */", "").replace("/* ]]> */", "").strip()
    try:
        data = json.loads(raw)
    except ValueError:
        return None
    agg = data.get("aggregateRating") or {}
    if agg.get("ratingValue") is None:
        return None
    return {
        "rating_5": round(float(agg["ratingValue"]), 2),
        "votes": agg.get("ratingCount"),
        "title": data.get("name"),
        "url": f"{_LB_BASE}{slug}",
    }


def _tmdb_get(path, params):
    if not TMDB_API_KEY:
        raise RuntimeError("TMDB_API_KEY not set")
    qs = urllib.parse.urlencode({**params, "api_key": TMDB_API_KEY})
    return json.loads(_http_get(f"https://api.themoviedb.org/3{path}?{qs}", timeout=15))


def _tmdb_meta(title):
    """Runtime / genres / synopsis / similar + a fallback rating (0-10)."""
    search = _tmdb_get("/search/movie", {"query": title, "include_adult": "false"})
    results = search.get("results") or []
    if not results:
        return None
    movie_id = results[0]["id"]
    details = _tmdb_get(f"/movie/{movie_id}", {})
    recs = _tmdb_get(f"/movie/{movie_id}/recommendations", {})
    return {
        "title": details.get("title"),
        "year": (details.get("release_date") or "")[:4],
        "tmdb_rating_10": details.get("vote_average"),
        "runtime_min": details.get("runtime"),
        "genres": [g["name"] for g in (details.get("genres") or [])],
        "overview": details.get("overview"),
        "similar": [r["title"] for r in (recs.get("results") or [])[:5]],
    }


def lookup_film(title):
    """Rating (Letterboxd, 0-5) + metadata + similar titles.

    Letterboxd is the rating source. TMDB supplies runtime/genres/synopsis/
    similar, and is used for the rating ONLY when Letterboxd can't be reached.
    """
    lb = _letterboxd_rating(title)
    meta = None
    if TMDB_API_KEY:
        try:
            meta = _tmdb_meta(title)
        except Exception as e:
            log.warning("tmdb meta failed: %s", e)

    if not lb and not meta:
        return {"found": False, "query": title}

    out = {
        "found": True,
        "title": (lb or {}).get("title") or (meta or {}).get("title") or title,
        "year": (meta or {}).get("year") or "",
        "runtime_min": (meta or {}).get("runtime_min"),
        "genres": (meta or {}).get("genres") or [],
        "overview": (meta or {}).get("overview"),
        "similar": (meta or {}).get("similar") or [],
        "letterboxd_url": (lb or {}).get("url"),
    }
    if lb:
        out.update(rating=lb["rating_5"], rating_scale=5,
                   rating_source="Letterboxd", letterboxd_votes=lb.get("votes"))
    elif meta and meta.get("tmdb_rating_10") is not None:
        out.update(rating=meta["tmdb_rating_10"], rating_scale=10,
                   rating_source="TMDB (Letterboxd unavailable)")
    else:
        out.update(rating=None, rating_scale=None, rating_source=None)
    return out


# --------------------------------------------------------------------------- #
# Movie domain (DynamoDB-backed) — per-player libraries + stateful game.
#
# Key scheme under PK = "movie#{chat_id}":
#   member#{user_id}            -> {display_name, username, first_seen}
#   lib#{user_id}#{film_uuid}   -> {title, year, added_by, added_at,
#                                   watched(bool), vetoed_by:[user_id]}
#   game#current                -> the single active game session for the chat
#   history#{session_id}        -> a finished night's record
#   dedupe#{update_id}          -> idempotency marker
# --------------------------------------------------------------------------- #

# ---- Members -------------------------------------------------------------- #
def remember_member(chat_id, user_id, display_name, username=None):
    """Upsert a member so the bot can address/mention people later."""
    if user_id is None:
        return None
    sk = f"member#{user_id}"
    existing = ddb_get(_pk("movie", chat_id), sk)
    item = {
        "PK": _pk("movie", chat_id),
        "SK": sk,
        "user_id": int(user_id),
        "display_name": display_name,
        "username": username,
        "first_seen": (existing or {}).get("first_seen") or _now_iso(),
        "last_seen": _now_iso(),
    }
    ddb_put(item)
    return item


def get_member(chat_id, user_id):
    return ddb_get(_pk("movie", chat_id), f"member#{user_id}")


def mention_for(chat_id, user_id):
    """A Telegram-ready mention string for a user (prefers @username)."""
    m = get_member(chat_id, user_id) or {}
    if m.get("username"):
        return f"@{m['username']}"
    return m.get("display_name") or "someone"


# ---- Libraries ------------------------------------------------------------ #
def add_film(chat_id, user_id, title, year=None):
    """Add a film to one player's library within this chat."""
    film_id = str(uuid.uuid4())
    item = {
        "PK": _pk("movie", chat_id),
        "SK": f"lib#{user_id}#{film_id}",
        "film_id": film_id,
        "owner_id": int(user_id) if user_id is not None else None,
        "title": title,
        "year": str(year) if year else "",
        "added_at": _now_iso(),
        "watched": False,
        "vetoed_by": [],
    }
    ddb_put(item)
    return item


def get_library(chat_id, user_id):
    """All library items for one player, oldest first."""
    prefix = f"lib#{user_id}#"
    films = [
        i for i in ddb_query(_pk("movie", chat_id))
        if str(i.get("SK", "")).startswith(prefix)
    ]
    films.sort(key=lambda f: f.get("added_at", ""))
    return films


def get_film(chat_id, user_id, film_id):
    return ddb_get(_pk("movie", chat_id), f"lib#{user_id}#{film_id}")


def mark_watched(chat_id, user_id, film_id):
    f = get_film(chat_id, user_id, film_id)
    if f:
        f["watched"] = True
        ddb_put(f)


def record_veto(chat_id, owner_id, film_id, vetoer_id):
    """Append the vetoer to a library film's vetoed_by set."""
    f = get_film(chat_id, owner_id, film_id)
    if not f:
        return
    vb = f.get("vetoed_by") or []
    if int(vetoer_id) not in [int(x) for x in vb]:
        vb.append(int(vetoer_id))
    f["vetoed_by"] = vb
    ddb_put(f)


# ---- Seeding: link a named starter library to a real Telegram user_id ----- #
# Starter libraries are loaded under a placeholder owner "seed:<name>" (see
# tools/seed_libraries.py). The person then "claims" their name once, which
# rewrites those films to their real user_id. We map by user_id, never phone
# number — Telegram never gives bots a phone number.
def _seed_owner(name):
    return f"seed:{name.strip().lower()}"


def list_seed_names(chat_id):
    """Names that still have an unclaimed seeded library in this chat."""
    names = set()
    for i in ddb_query(_pk("movie", chat_id)):
        sk = str(i.get("SK", ""))
        if sk.startswith("lib#seed:"):
            names.add(sk.split("#", 2)[1].split(":", 1)[1])
    return sorted(names)


def claim_library(chat_id, name, user_id):
    """Reassign the seeded 'name' library to user_id. Idempotent; one claimer."""
    key = name.strip().lower()
    marker_sk = f"seedclaim#{key}"
    marker = ddb_get(_pk("movie", chat_id), marker_sk)
    if marker and str(marker.get("claimed_by")) != str(user_id):
        return {"status": "taken", "by": marker.get("claimed_by")}
    seed_prefix = f"lib#{_seed_owner(name)}#"
    seed_items = [i for i in ddb_query(_pk("movie", chat_id))
                  if str(i.get("SK", "")).startswith(seed_prefix)]
    if not seed_items and not marker:
        return {"status": "none"}
    moved = 0
    for it in seed_items:
        fid = it["film_id"]
        it["SK"] = f"lib#{user_id}#{fid}"
        it["owner_id"] = int(user_id)
        it.pop("seed_name", None)
        ddb_put(it)
        ddb_delete(_pk("movie", chat_id), f"{seed_prefix}{fid}")
        moved += 1
    ddb_put({"PK": _pk("movie", chat_id), "SK": marker_sk,
             "claimed_by": int(user_id), "seed_name": name.strip(),
             "claimed_at": _now_iso()})
    return {"status": "ok", "moved": moved}


# ---- Selection (all randomness in Python, never the LLM) ------------------ #
def eligible_films(library, participant_ids):
    """Films a player's library may contribute to selection.

    Always excludes watched (past winners). Then applies the veto-aware rule:
    drop films vetoed by anyone currently in the room — but only while doing so
    still leaves something. If excluding the participant-vetoes empties the list,
    the vetoed films become eligible again (better to re-offer than offer nothing).
    """
    pids = {int(p) for p in participant_ids}
    unwatched = [f for f in library if not f.get("watched")]

    def vetoed_by_present(f):
        return any(int(v) in pids for v in (f.get("vetoed_by") or []))

    non_vetoed = [f for f in unwatched if not vetoed_by_present(f)]
    return non_vetoed if non_vetoed else unwatched


def select_for_user(chat_id, user_id, participant_ids, exclude_ids=None, n=3):
    """random.sample of up to n eligible films for one player. Code, not LLM."""
    exclude = set(exclude_ids or [])
    pool = [
        f for f in eligible_films(get_library(chat_id, user_id), participant_ids)
        if f["film_id"] not in exclude
    ]
    k = min(n, len(pool))
    return random.sample(pool, k) if k else []


def draw_film(chat_id, user_id):
    """Quick personal 'surprise me' — random film from the sender's library."""
    lib = get_library(chat_id, user_id)
    return random.choice(lib) if lib else None


# ---- Game session --------------------------------------------------------- #
def get_game(chat_id):
    return ddb_get(_pk("movie", chat_id), "game#current")


def put_game(game):
    ddb_put(game)
    return game


def clear_game(chat_id):
    ddb_delete(_pk("movie", chat_id), "game#current")


def new_game(chat_id):
    return {
        "PK": _pk("movie", chat_id),
        "SK": "game#current",
        "session_id": str(uuid.uuid4()),
        "phase": "roster",
        "participants": [],
        "selections": {},        # {str(user_id): [film_uuid, ...]}
        "confirmed": [],         # [str(user_id)] who locked in their card
        "message_index": {},     # {str(message_id): {kind, user_id, film_uuids}}
        "pool": [],              # [film_uuid] candidates at lock time
        "film_owner": {},        # {film_uuid: str(owner_id)} for the whole game
        "vetoes_left": {},       # {str(user_id): int}
        "vetoed_pile": [],       # [film_uuid] removed by veto (fallback if pool empties)
        "picked": None,          # current film_uuid on the table
        "pick_token": None,      # guards which pick is live
        "status": "active",
        "created_at": _now_iso(),
    }


def index_message(game, message_id, kind, user_id=None, film_uuids=None):
    """Record one of the bot's own messages so replies can be routed back."""
    if message_id is None:
        return
    game["message_index"][str(message_id)] = {
        "kind": kind,
        "user_id": str(user_id) if user_id is not None else None,
        "film_uuids": film_uuids or [],
    }


def owner_of(game, film_uuid):
    o = game.get("film_owner", {}).get(film_uuid)
    return int(o) if o is not None else None


# --------------------------------------------------------------------------- #
# Bedrock Converse tool-use loop
# --------------------------------------------------------------------------- #
MOVIE_TOOLS = [
    {
        "toolSpec": {
            "name": "lookup_film",
            "description": "Look up a film's Letterboxd rating (0-5), plus runtime, genres, synopsis and similar titles. The result names which source the rating came from.",
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {"title": {"type": "string"}},
                    "required": ["title"],
                }
            },
        }
    },
    {
        "toolSpec": {
            "name": "add_film",
            "description": "Add a film to the sender's own library in this chat.",
            "inputSchema": {
                "json": {
                    "type": "object",
                    "properties": {
                        "title": {"type": "string"},
                        "year": {"type": "string"},
                    },
                    "required": ["title"],
                }
            },
        }
    },
    {
        "toolSpec": {
            "name": "list_films",
            "description": "List the films in the sender's own library in this chat.",
            "inputSchema": {"json": {"type": "object", "properties": {}}},
        }
    },
    {
        "toolSpec": {
            "name": "draw_film",
            "description": "Pick one film at random from the sender's own library.",
            "inputSchema": {"json": {"type": "object", "properties": {}}},
        }
    },
]


def _dispatch_tool(name, tool_input, ctx):
    """ctx carries chat_id / sender so the model can't pick the wrong group/library."""
    chat_id = ctx["chat_id"]
    user_id = ctx.get("user_id")
    if name == "lookup_film":
        return lookup_film(tool_input["title"])
    if name == "add_film":
        item = add_film(chat_id, user_id, tool_input["title"], tool_input.get("year"))
        return {"added": True, "title": item["title"], "added_by": ctx.get("user_name")}
    if name == "list_films":
        return {
            "films": [
                {"title": f["title"], "year": f.get("year")}
                for f in get_library(chat_id, user_id)
            ]
        }
    if name == "draw_film":
        chosen = draw_film(chat_id, user_id)  # random.choice in code
        return {"chosen": chosen["title"]} if chosen else {"chosen": None}
    return {"error": f"unknown tool {name}"}


def converse(system_prompt, user_text, ctx, tools=MOVIE_TOOLS, max_turns=6):
    """Run the tool-use loop and return the model's final text."""
    messages = [{"role": "user", "content": [{"text": user_text}]}]
    for _ in range(max_turns):
        resp = _bedrock.converse(
            modelId=BEDROCK_MODEL_ID,
            system=[{"text": system_prompt}],
            messages=messages,
            inferenceConfig={"maxTokens": 1000, "temperature": 0.7},
            toolConfig={"tools": tools},
        )
        out = resp["output"]["message"]
        messages.append(out)
        if resp.get("stopReason") != "tool_use":
            return "".join(b.get("text", "") for b in out["content"]).strip()
        tool_results = []
        for block in out["content"]:
            if "toolUse" not in block:
                continue
            tu = block["toolUse"]
            try:
                result = _dispatch_tool(tu["name"], tu.get("input", {}), ctx)
            except Exception as e:
                log.error("tool %s failed: %s", tu["name"], e)
                result = {"error": str(e)}
            tool_results.append(
                {
                    "toolResult": {
                        "toolUseId": tu["toolUseId"],
                        "content": [{"json": {"result": result}}],
                    }
                }
            )
        messages.append({"role": "user", "content": tool_results})
    return "I got a bit tangled up there — try again?"


# --------------------------------------------------------------------------- #
# Mode handler: MOVIE
# --------------------------------------------------------------------------- #
MOVIE_SYSTEM = (
    "You are Gracia's film-night helper in a Telegram group chat. Talk like a "
    "person, warm and brief, not like a system. The film named by the user has "
    "already been added to their personal library for this chat. Use lookup_film "
    "to get its rating and similar titles, then in one short reply: confirm the "
    "add, give the rating (the Letterboxd average out of 5 — state it plainly as "
    "'X/5 on Letterboxd'; if the tool says the rating came from TMDB instead, "
    "say it's the TMDB score out of 10 because Letterboxd was unavailable), name "
    "one related film fans also enjoy, and offer to add it. Use the tools rather "
    "than guessing ratings."
)

FREEFORM_SYSTEM = (
    "You are Gracia's film-night helper in a Telegram group chat. Be warm and "
    "concise. You can look up films, add them to the group's list, list what's on "
    "it, or draw one at random, using your tools. Ratings come from Letterboxd "
    "(out of 5); the tool tells you the source and scale — present them honestly "
    "and never invent ratings."
)


def _rating_phrase(info):
    """'Letterboxd 4.1/5' / 'TMDB 7.6/10' / '' — from a lookup_film result."""
    if not info.get("found") or info.get("rating") is None:
        return ""
    src = "Letterboxd" if str(info.get("rating_source", "")).startswith("Letterboxd") else "TMDB"
    return f"{src} {info['rating']}/{info.get('rating_scale', 5)}"


def _plain_add_reply(film_item):
    """Used when AI is off or Bedrock fails — still acknowledge the add."""
    line = f"Added \u201c{film_item['title']}\u201d to your library \U0001f3ac"
    try:
        info = lookup_film(film_item["title"])
    except Exception as e:
        log.warning("lookup for plain reply failed: %s", e)
        return line
    phrase = _rating_phrase(info)
    if phrase:
        yr = f" ({info['year']})" if info.get("year") else ""
        line = (
            f"Added \u201c{info['title']}\u201d{yr} to your library \U0001f3ac "
            f"\u2014 {phrase}."
        )
    return line


# --------------------------------------------------------------------------- #
# Game presentation + deterministic confirm/veto parsing.
# Selection, vetoes and the winner are decided in code (above); these helpers
# only render messages and parse player intent — never decide outcomes.
# --------------------------------------------------------------------------- #
_YES_TOKENS = {"👍", "✅", "👌", "y", "yes", "keep", "ok"}
_NO_TOKENS = {"👎", "❌", "n", "no", "drop", "nope"}


def _parse_confirm_tokens(text):
    """'👍 👎 👍' or 'y n y' -> ([True,False,True], keep_all).

    keep_all is True when the whole reply is a single affirmative ('keep all').
    Unknown tokens are ignored. Parsing is deterministic and done in code.
    """
    t = text.strip().lower()
    parts = t.split()
    if len(parts) <= 1 and t and not t.isascii():
        parts = list(t)  # a run of emoji with no spaces
    toks = []
    for p in parts:
        p = p.strip(".,!")
        if p in _YES_TOKENS:
            toks.append(True)
        elif p in _NO_TOKENS:
            toks.append(False)
    keep_all = len(toks) == 1 and toks[0] is True
    return toks, keep_all


def _is_veto(text):
    t = text.strip().lower()
    return t.startswith("/veto") or t in {"veto", "❌", "👎", "no"}


def _is_play(text):
    t = text.strip().lower()
    return (t.startswith(("/watch", "/play", "/go"))
            or t in {"go", "watch", "play", "✅", "👍", "yes"})


def _film_label(item):
    yr = f" ({item['year']})" if item.get("year") else ""
    return f"{item['title']}{yr}"


def _enriched_card(item):
    """Full info block for ONE film (pick / winner / draw). Network best-effort."""
    label = _film_label(item)
    try:
        info = lookup_film(item["title"])
    except Exception as e:
        log.warning("enrich failed: %s", e)
        return label
    bits = []
    phrase = _rating_phrase(info)
    if phrase:
        bits.append(phrase)
    if info.get("runtime_min"):
        bits.append(f"{info['runtime_min']} min")
    if info.get("genres"):
        bits.append(", ".join(info["genres"][:2]))
    if bits:
        label += "\n" + " · ".join(bits)
    if info.get("letterboxd_url"):
        label += f"\n{info['letterboxd_url']}"
    return label


def winner_note(title, year):
    """A short spoiler-free context note. LLM writes prose; it picks nothing."""
    if not AI_ENABLED:
        return ""
    try:
        ystr = f" ({year})" if year else ""
        resp = _bedrock.converse(
            modelId=BEDROCK_MODEL_ID,
            system=[{"text": (
                "You write a 2-3 sentence, SPOILER-FREE note about a film for a "
                "film-night group: production facts, trivia, legacy, why it's worth "
                "watching. NEVER reveal plot, twists, or the ending. Warm and concise."
            )}],
            messages=[{"role": "user", "content": [
                {"text": f'The film is "{title}"{ystr}. Write the note.'}]}],
            inferenceConfig={"maxTokens": 250, "temperature": 0.7},
        )
        return "".join(b.get("text", "") for b in resp["output"]["message"]["content"]).strip()
    except Exception as e:
        log.warning("winner note failed: %s", e)
        return ""


# ---- roster / participants ------------------------------------------------ #
def _add_participant(game, user_id):
    if user_id is None:
        return False
    uid = str(user_id)
    if uid not in [str(p) for p in game["participants"]]:
        game["participants"].append(int(user_id))
        game["vetoes_left"][uid] = 1
        return True
    return False


def _mark_confirmed(game, user_id):
    uid = str(user_id)
    if uid not in [str(c) for c in game["confirmed"]]:
        game["confirmed"].append(int(user_id))


# ---- selection + confirmation cards --------------------------------------- #
def _run_selection(mode, chat_id, game):
    pids = game["participants"]
    game["selections"] = {}
    for uid in pids:
        chosen = select_for_user(chat_id, uid, pids)  # random.sample, in code
        game["selections"][str(uid)] = [f["film_id"] for f in chosen]
    game["phase"] = "confirm"
    game["confirmed"] = []
    put_game(game)
    posted = False
    for uid in pids:
        if game["selections"].get(str(uid)):
            _post_card(mode, chat_id, game, uid)
            posted = True
    put_game(game)
    if not posted:
        send_message(mode, chat_id,
                     "Nobody has eligible films yet — add some with /movie, then /select again.")


def _post_card(mode, chat_id, game, user_id):
    sel = game["selections"].get(str(user_id), [])
    lines = [f"🎬 {mention_for(chat_id, user_id)} — your picks:"]
    for i, fid in enumerate(sel, 1):
        item = get_film(chat_id, user_id, fid)
        if item:
            lines.append(f"{i}. {_film_label(item)}")
    lines.append("")
    lines.append("Reply to this card: 👍 keep all, or 👍/👎 per film "
                 "(in order). /swap <n> to reroll one.")
    resp = send_message(mode, chat_id, "\n".join(lines))
    mid = (resp.get("result") or {}).get("message_id")
    index_message(game, mid, "card", user_id=user_id, film_uuids=sel)


def _backfill(chat_id, game, owner, keep, n=3):
    need = n - len(keep)
    if need <= 0:
        return keep
    extra = select_for_user(chat_id, owner, game["participants"],
                            exclude_ids=set(keep), n=need)
    return keep + [f["film_id"] for f in extra]


def _swap_one(mode, chat_id, game, owner, arg):
    sel = game["selections"].get(str(owner), [])
    try:
        n = int((arg or "").strip())
    except ValueError:
        send_message(mode, chat_id, "Usage: /swap <n> — e.g. /swap 2")
        return
    if not (1 <= n <= len(sel)):
        send_message(mode, chat_id, f"Pick a number 1–{len(sel)}.")
        return
    repl = select_for_user(chat_id, owner, game["participants"],
                           exclude_ids=set(sel), n=1)
    if not repl:
        send_message(mode, chat_id, "No other eligible films in your library to swap in.")
        return
    sel[n - 1] = repl[0]["film_id"]
    game["selections"][str(owner)] = sel
    _post_card(mode, chat_id, game, owner)


def _handle_card_reply(mode, chat_id, game, entry, upd, text):
    owner = entry.get("user_id")
    if owner is None or str(upd.get("user_id")) != str(owner):
        return  # only the card's owner edits it
    owner_id = int(owner)
    cmd, arg = parse_command(text)
    sel = game["selections"].get(owner, [])
    if cmd == "swap":
        _swap_one(mode, chat_id, game, owner_id, arg)
        put_game(game)
        return
    toks, keep_all = _parse_confirm_tokens(text)
    if cmd == "confirm" or keep_all:
        _mark_confirmed(game, owner_id)
        put_game(game)
        send_message(mode, chat_id, f"Locked in {mention_for(chat_id, owner_id)}'s picks ✅")
        return
    if not toks:
        send_message(mode, chat_id, "Reply 👍 to keep all, or one 👍/👎 per film.")
        return
    if len(toks) != len(sel):
        send_message(mode, chat_id,
                     f"I count {len(sel)} films — send {len(sel)} marks "
                     "(👍/👎), one per film.")
        return
    keep = [fid for fid, ok in zip(sel, toks) if ok]
    game["selections"][owner] = _backfill(chat_id, game, owner_id, keep, n=len(sel))
    _mark_confirmed(game, owner_id)
    put_game(game)
    _post_card(mode, chat_id, game, owner_id)  # show the updated card
    put_game(game)


# ---- lock / pick / veto / finalize ---------------------------------------- #
def _do_lock(mode, chat_id, game):
    pool, owner_map = [], {}
    for uid in game["participants"]:
        for fid in game["selections"].get(str(uid), []):
            pool.append(fid)
            owner_map[fid] = str(uid)
    if not pool:
        send_message(mode, chat_id, "No films selected yet — run /select first.")
        return
    game["pool"] = pool
    game["film_owner"] = owner_map
    game["phase"] = "locked"
    lines = ["🔒 Locked in! Tonight's candidates:"]
    for fid in pool:
        item = get_film(chat_id, int(owner_map[fid]), fid)
        if item:
            lines.append(f"• {_film_label(item)}")
    lines.append("")
    lines.append("Send /go (or reply 'go') to draw tonight's film.")
    resp = send_message(mode, chat_id, "\n".join(lines))
    index_message(game, (resp.get("result") or {}).get("message_id"), "lock")
    put_game(game)


def _do_pick(mode, chat_id, game):
    candidates = [f for f in game["pool"] if f not in game.get("vetoed_pile", [])]
    if not candidates:
        candidates = game.get("vetoed_pile", [])
        game["no_more_vetoes"] = True
    if not candidates:
        send_message(mode, chat_id, "Nothing left to pick.")
        return
    game["picked"] = random.choice(candidates)  # random.choice, in code
    game["pick_token"] = str(uuid.uuid4())
    game["phase"] = "picking"
    _post_pick(mode, chat_id, game)
    put_game(game)


def _post_pick(mode, chat_id, game):
    picked = game["picked"]
    owner = owner_of(game, picked)
    item = get_film(chat_id, owner, picked) if owner is not None else None
    label = _enriched_card(item) if item else "(film)"
    if game.get("no_more_vetoes"):
        nudge = "Last one standing — reply ✅ (or /watch) to start."
    else:
        nudge = ("⏳ You have 60 seconds — veto or press play!\n"
                 "Reply ❌ (or /veto) to veto · reply ✅ (or /watch) to start.")
    text = f"🎲 Tonight's pick:\n\n{label}\n\n{nudge}"
    resp = send_message(mode, chat_id, text)
    index_message(game, (resp.get("result") or {}).get("message_id"), "pick",
                  film_uuids=[picked])


def _do_veto(mode, chat_id, game, vetoer_id):
    if game.get("phase") != "picking":
        return
    uid = str(vetoer_id)
    if uid not in [str(p) for p in game["participants"]]:
        send_message(mode, chat_id, "Only tonight's players can veto.")
        return
    if game.get("no_more_vetoes"):
        send_message(mode, chat_id, "No vetoes left — this is the final pick.")
        return
    if game["vetoes_left"].get(uid, 0) <= 0:
        send_message(mode, chat_id,
                     f"{mention_for(chat_id, vetoer_id)}, you already played your veto 😶")
        return
    picked = game.get("picked")
    if not picked:
        return
    game["vetoes_left"][uid] = game["vetoes_left"].get(uid, 0) - 1
    owner = owner_of(game, picked)
    if owner is not None:
        record_veto(chat_id, owner, picked, vetoer_id)
    game.setdefault("vetoed_pile", [])
    if picked not in game["vetoed_pile"]:
        game["vetoed_pile"].append(picked)
    item = get_film(chat_id, owner, picked) if owner is not None else None
    label = _film_label(item) if item else "that one"
    send_message(mode, chat_id,
                 f"❌ {mention_for(chat_id, vetoer_id)} vetoed “{label}”. Re-drawing…")
    _do_pick(mode, chat_id, game)  # re-pick (handles pool exhaustion + persists)


def _do_finalize(mode, chat_id, game):
    if game.get("phase") != "picking":
        return
    picked = game.get("picked")
    if not picked:
        return
    owner = owner_of(game, picked)
    item = get_film(chat_id, owner, picked) if owner is not None else None
    title = item["title"] if item else "tonight's film"
    year = item.get("year") if item else ""
    if owner is not None and item:
        mark_watched(chat_id, owner, picked)
    session_id = game["session_id"]
    ddb_put({
        "PK": _pk("movie", chat_id),
        "SK": f"history#{session_id}",
        "session_id": session_id,
        "winner_title": title,
        "winner_year": year,
        "winner_film_id": picked,
        "winner_owner_id": owner,
        "watched_date": _now_iso(),
        "participants": [int(p) for p in game["participants"]],
        "ratings": {},
    })
    clear_game(chat_id)
    label = _enriched_card(item) if item else title
    text = f"🍿 Tonight we're watching:\n\n{label}"
    note = winner_note(title, year)
    if note:
        text += f"\n\n{note}"
    text += "\n\nEnjoy! 🎬"
    send_message(mode, chat_id, text)


def _handle_reply(mode, chat_id, upd, game, entry, text):
    """Route a reply to one of the bot's own messages (privacy-mode backbone)."""
    kind = entry.get("kind")
    if kind == "card":
        _handle_card_reply(mode, chat_id, game, entry, upd, text)
    elif kind == "lock":
        if _is_play(text):
            _do_pick(mode, chat_id, game)
    elif kind == "pick":
        if _is_veto(text):
            _do_veto(mode, chat_id, game, upd.get("user_id"))
        elif _is_play(text):
            _do_finalize(mode, chat_id, game)


def _resolve_username(chat_id, token):
    uname = token.lstrip("@").lower()
    for i in ddb_query(_pk("movie", chat_id)):
        if (str(i.get("SK", "")).startswith("member#")
                and (i.get("username") or "").lower() == uname):
            return i.get("user_id")
    return None


def _send_library(mode, chat_id, user_id, lib, mine):
    who = "Your" if mine else f"{mention_for(chat_id, user_id)}'s"
    head = f"🍿 {who} library ({len(lib)} films):"
    body = [f"• {_film_label(f)}{' ✓' if f.get('watched') else ''}" for f in lib]
    MAX = 40
    if len(body) > MAX:
        body = body[:MAX] + [f"…and {len(body) - MAX} more."]
    send_message(mode, chat_id, "\n".join([head] + body))


_HELP_TEXT = (
    "🎬 SirWatchalot — film night\n\n"
    "Library:\n"
    "/movie <title> — add a film to your library\n"
    "/library [@user] — show a library\n"
    "/draw — surprise me from your library\n"
    "/claim <name> — link a seeded starter library to you\n\n"
    "Game:\n"
    "/movienight — start a night\n"
    "/join — join the night\n"
    "/select — draw 3 from each player's library\n"
    "  (reply to your card: 👍 keep all, or 👍/👎 per film; /swap <n>)\n"
    "/lock — lock the candidates\n"
    "/go — draw tonight's film\n"
    "/veto — veto it (one each) · /watch — start it\n"
    "/cancel — scrap the night"
)


def handle_movie(mode, upd):
    chat_id = upd["chat_id"]
    user_id = upd.get("user_id")
    text = upd["text"].strip()
    cmd, arg = parse_command(text)
    remember_member(chat_id, user_id, upd["user_name"], upd.get("username"))
    game = get_game(chat_id)

    # ---- Reply routing: the backbone of confirm/veto under privacy mode ON ----
    rtid = upd.get("reply_to_message_id")
    if game and rtid is not None:
        entry = game.get("message_index", {}).get(str(rtid))
        if entry:
            _handle_reply(mode, chat_id, upd, game, entry, text)
            return

    # ---- Library commands (work with or without an active game) ----
    if cmd == "movie":
        if not arg:
            send_message(mode, chat_id, "Usage: /movie <title> — e.g. /movie Rear Window")
            return
        item = add_film(chat_id, user_id, arg)
        if game and game.get("phase") == "roster" and _add_participant(game, user_id):
            put_game(game)  # adding a film during the roster auto-joins you
        if AI_ENABLED:
            try:
                ctx = {"chat_id": chat_id, "user_name": upd["user_name"], "user_id": user_id}
                reply = converse(MOVIE_SYSTEM, f'I just added the film "{arg}". Respond now.', ctx)
                send_message(mode, chat_id, reply or _plain_add_reply(item))
                return
            except Exception as e:
                log.error("bedrock movie add failed: %s", e)
        send_message(mode, chat_id, _plain_add_reply(item))
        return

    if cmd in ("movies", "library"):
        target = user_id
        if arg and arg.startswith("@"):
            r = _resolve_username(chat_id, arg.split()[0])
            if r is None:
                send_message(mode, chat_id, "I don't know that person yet — they have to interact with me first.")
                return
            target = r
        lib = get_library(chat_id, target)
        mine = (str(target) == str(user_id))
        if not lib:
            who = "Your" if mine else f"{mention_for(chat_id, target)}'s"
            send_message(mode, chat_id, f"{who} library is empty — add films with /movie <title>.")
            return
        _send_library(mode, chat_id, target, lib, mine)
        return

    if cmd == "draw":
        chosen = draw_film(chat_id, user_id)  # random.choice in code
        if not chosen:
            send_message(mode, chat_id, "Your library is empty — add films with /movie first.")
            return
        send_message(mode, chat_id, f"🎲 From your library:\n\n{_enriched_card(chosen)}")
        return

    if cmd == "claim":
        if not arg:
            names = list_seed_names(chat_id)
            hint = f" Available: {', '.join(names)}." if names else ""
            send_message(mode, chat_id, f"Usage: /claim <name> — e.g. /claim Chad.{hint}")
            return
        res = claim_library(chat_id, arg.strip(), user_id)
        if res["status"] == "taken":
            send_message(mode, chat_id,
                         f"That library's already claimed by {mention_for(chat_id, res['by'])}.")
        elif res["status"] == "none":
            names = list_seed_names(chat_id)
            hint = f" Available: {', '.join(names)}." if names else ""
            send_message(mode, chat_id, f"No seeded library named “{arg.strip()}”.{hint}")
        elif res["moved"]:
            send_message(mode, chat_id,
                         f"✅ Linked {res['moved']} films to you, {mention_for(chat_id, user_id)}. "
                         "See them with /library.")
        else:
            send_message(mode, chat_id, "That library's already yours — /library to see it.")
        return

    # ---- Game lifecycle ----
    if cmd == "movienight":
        if game:
            send_message(mode, chat_id, "A movie night's already going. /join to get in, or /cancel to scrap it.")
            return
        game = new_game(chat_id)
        _add_participant(game, user_id)
        put_game(game)
        send_message(mode, chat_id,
                     f"🎬 Movie night! {mention_for(chat_id, user_id)} is in. "
                     "Others: /join. When everyone's in, /select to draw 3 from each library.")
        return

    if cmd == "join":
        if not game:
            send_message(mode, chat_id, "No movie night yet — start one with /movienight.")
            return
        if game.get("phase") != "roster":
            send_message(mode, chat_id, "Roster's closed — selection already started.")
            return
        if _add_participant(game, user_id):
            put_game(game)
            roster = ", ".join(mention_for(chat_id, p) for p in game["participants"])
            send_message(mode, chat_id, f"✅ {mention_for(chat_id, user_id)} joined. Playing tonight: {roster}")
        else:
            send_message(mode, chat_id, "You're already in 😊")
        return

    if cmd == "select":
        if not game or game.get("phase") != "roster":
            send_message(mode, chat_id, "Start with /movienight and gather players with /join first.")
            return
        if not game["participants"]:
            send_message(mode, chat_id, "Nobody's joined yet — /join first.")
            return
        send_message(mode, chat_id, "🎲 Drawing 3 from each library…")
        _run_selection(mode, chat_id, game)
        if game.get("phase") == "confirm":
            send_message(mode, chat_id, "Reply to your card to keep/swap, then the host runs /lock.")
        return

    if cmd == "confirm":
        if game and game.get("phase") == "confirm":
            _mark_confirmed(game, user_id)
            put_game(game)
            send_message(mode, chat_id, f"Locked in {mention_for(chat_id, user_id)}'s picks ✅")
        return

    if cmd == "swap":
        if game and game.get("phase") == "confirm":
            _swap_one(mode, chat_id, game, user_id, arg)
            put_game(game)
        return

    if cmd == "lock":
        if not game or game.get("phase") != "confirm":
            send_message(mode, chat_id, "Nothing to lock — run /select first.")
            return
        _do_lock(mode, chat_id, game)
        return

    if cmd == "go":
        if not game or game.get("phase") != "locked":
            send_message(mode, chat_id, "Run /lock first, then /go to draw.")
            return
        _do_pick(mode, chat_id, game)
        return

    if cmd == "veto":
        if game and game.get("phase") == "picking":
            _do_veto(mode, chat_id, game, user_id)
        return

    if cmd in ("watch", "play"):
        if game and game.get("phase") == "picking":
            _do_finalize(mode, chat_id, game)
        return

    if cmd == "cancel":
        if game:
            clear_game(chat_id)
            send_message(mode, chat_id, "Movie night cancelled.")
        return

    if cmd in ("start", "help"):
        send_message(mode, chat_id, _HELP_TEXT)
        return

    if cmd is not None:
        return  # unknown command — stay quiet

    # Free-form (privacy mode ON means we only see mentions/replies here).
    if not text:
        return
    if AI_ENABLED:
        try:
            ctx = {"chat_id": chat_id, "user_name": upd["user_name"], "user_id": user_id}
            reply = converse(FREEFORM_SYSTEM, text, ctx)
            if reply:
                send_message(mode, chat_id, reply)
            return
        except Exception as e:
            log.error("bedrock freeform failed: %s", e)
    send_message(mode, chat_id, "I can add films (/movie), show a library (/library) or run /movienight.")


# --------------------------------------------------------------------------- #
# Mode handler: CLEANING (stub)
# --------------------------------------------------------------------------- #
def handle_cleaning(mode, upd):
    # TODO(real cleaning logic): room-by-room checklist flow in Ukrainian,
    # daily completion tracking in DynamoDB, end-of-shift summary to Chad.
    send_message(mode, upd["chat_id"], "The cleaning bot isn't set up yet.")


# --------------------------------------------------------------------------- #
# Mode handler: SALARY (stub)
# --------------------------------------------------------------------------- #
def handle_salary(mode, upd):
    # TODO(real salary logic): the LLM ONLY parses messy time entries into
    # structured fields (start, end, breaks). ALL pay math — rates, overtime,
    # rounding — is done in plain Python and shown for confirmation. The model
    # must never compute money.
    send_message(mode, upd["chat_id"], "The salary bot isn't set up yet.")


# --------------------------------------------------------------------------- #
# Lambda entry point
# --------------------------------------------------------------------------- #
def _header(event, name):
    headers = event.get("headers") or {}
    name = name.lower()
    for k, v in headers.items():
        if k.lower() == name:
            return v
    return None


def _mode_from_path(event):
    path = event.get("rawPath") or event.get("path") or ""
    if not path:
        rc = event.get("requestContext", {}).get("http", {})
        path = rc.get("path", "")
    return path.rstrip("/").rsplit("/", 1)[-1].lower()


def _load_body(event):
    body = event.get("body") or "{}"
    if event.get("isBase64Encoded"):
        body = base64.b64decode(body).decode("utf-8")
    return json.loads(body)


def lambda_handler(event, context):
    mode = _mode_from_path(event)
    modes = _mode_config()
    if mode not in modes:
        log.warning("unknown mode path: %r", mode)
        return {"statusCode": 404, "body": "unknown mode"}

    # Verify the per-bot secret. Endpoint is public, so a mismatch is rejected.
    expected = os.environ.get(modes[mode]["secret_env"], "").strip()
    provided = _header(event, "X-Telegram-Bot-Api-Secret-Token")
    if not expected or provided != expected:
        log.warning("secret mismatch for mode %s", mode)
        return {"statusCode": 403, "body": "forbidden"}

    try:
        update = _load_body(event)
    except Exception as e:
        log.error("bad body: %s", e)
        return {"statusCode": 200, "body": "ok"}  # don't make Telegram retry

    try:
        upd = parse_update(update)
        if upd["chat_id"] is None:
            return {"statusCode": 200, "body": "ok"}

        # Persist / refresh the chat id (never hardcoded).
        remember_chat(mode, upd["chat_id"], upd.get("chat_title"))

        # Idempotency: drop Telegram's retried deliveries before any state change.
        if seen_update(mode, upd["chat_id"], upd.get("update_id")):
            log.info("duplicate update %s ignored", upd.get("update_id"))
            return {"statusCode": 200, "body": "ok"}

        # Handle migration service messages on the way in.
        if upd.get("migrate_to_chat_id"):
            migrate_chat(mode, upd["chat_id"], upd["migrate_to_chat_id"])
            return {"statusCode": 200, "body": "ok"}
        if upd.get("migrate_from_chat_id"):
            migrate_chat(mode, upd["migrate_from_chat_id"], upd["chat_id"])

        modes[mode]["handler"](mode, upd)
    except Exception as e:
        log.exception("handler error in mode %s: %s", mode, e)
        # Always 200 so Telegram doesn't hammer us with retries.

    return {"statusCode": 200, "body": "ok"}
