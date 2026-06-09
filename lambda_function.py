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
# Resolution/overview is source-agnostic and NOT gated on any one provider.
# TMDB (optional) supplies title/year/runtime/genres/synopsis/similar.
# Ratings are separate and non-blocking: Letterboxd average (scraped, no key) is
# primary; Rotten Tomatoes comes from OMDb's Ratings array (free key, OMDB_API_KEY).
TMDB_API_KEY = os.environ.get("TMDB_API_KEY", "").strip()
OMDB_API_KEY = os.environ.get("OMDB_API_KEY", "").strip()

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


def send_poll(mode, chat_id, question, options, **kwargs):
    """Send a native poll. Returns the Telegram response (result.poll.id, message_id)."""
    token = _token_for(mode)
    payload = {"chat_id": chat_id, "question": question,
               "options": [{"text": o} for o in options], **kwargs}
    resp = _tg_request(token, "sendPoll", payload)
    if not resp.get("ok"):
        params = resp.get("parameters") or {}
        new_id = params.get("migrate_to_chat_id")
        if new_id and "supergroup" in (resp.get("description") or "").lower():
            migrate_chat(mode, chat_id, new_id)
            payload["chat_id"] = new_id
            resp = _tg_request(token, "sendPoll", payload)
        else:
            log.error("sendPoll to %s failed: %s", chat_id, resp.get("description"))
    return resp


def stop_poll(mode, chat_id, message_id):
    return _tg_request(_token_for(mode), "stopPoll",
                       {"chat_id": chat_id, "message_id": message_id})


def answer_callback(mode, callback_query_id, text=None):
    payload = {"callback_query_id": callback_query_id}
    if text:
        payload["text"] = text
    return _tg_request(_token_for(mode), "answerCallbackQuery", payload)


def edit_message_text(mode, chat_id, message_id, text, **kwargs):
    return _tg_request(_token_for(mode), "editMessageText",
                       {"chat_id": chat_id, "message_id": message_id, "text": text, **kwargs})


def parse_update(update):
    """Normalize a Telegram update into a flat event dict with a 'kind'.

    Movie mode runs privacy OFF, so we handle ordinary messages plus the
    interactive update types the game needs: callback_query (Join/Start and the
    thumb fallback buttons), message_reaction (👍/👎 on selection cards), and
    poll / poll_answer (the veto poll). poll / poll_answer carry no chat_id —
    the caller resolves it from the stored poll map.
    """
    ev = {"update_id": update.get("update_id"), "kind": "other",
          "chat_id": None, "chat_title": None, "chat_type": None,
          "text": "", "user_id": None, "user_name": "someone", "username": None,
          "message_id": None, "reactions": [], "callback_data": None,
          "callback_query_id": None, "poll_id": None, "poll_is_closed": None,
          "poll_option_ids": [], "reply_to_message_id": None,
          "migrate_to_chat_id": None, "migrate_from_chat_id": None}

    if "callback_query" in update:
        cq = update["callback_query"]
        msg = cq.get("message") or {}
        chat = msg.get("chat") or {}
        frm = cq.get("from") or {}
        ev.update(kind="callback", chat_id=chat.get("id"),
                  chat_title=chat.get("title") or chat.get("username"),
                  chat_type=chat.get("type"), message_id=msg.get("message_id"),
                  callback_data=cq.get("data"), callback_query_id=cq.get("id"),
                  user_id=frm.get("id"),
                  user_name=frm.get("first_name") or frm.get("username") or "someone",
                  username=frm.get("username"))
        return ev

    if "message_reaction" in update:
        mr = update["message_reaction"]
        chat = mr.get("chat") or {}
        frm = mr.get("user") or {}
        emojis = [r.get("emoji") for r in (mr.get("new_reaction") or [])
                  if r.get("type") == "emoji" and r.get("emoji")]
        ev.update(kind="reaction", chat_id=chat.get("id"),
                  chat_title=chat.get("title") or chat.get("username"),
                  chat_type=chat.get("type"), message_id=mr.get("message_id"),
                  reactions=emojis, user_id=frm.get("id"),
                  user_name=frm.get("first_name") or frm.get("username") or "someone",
                  username=frm.get("username"))
        return ev

    if "poll_answer" in update:
        pa = update["poll_answer"]
        frm = pa.get("user") or {}
        ev.update(kind="poll_answer", poll_id=pa.get("poll_id"),
                  poll_option_ids=pa.get("option_ids") or [],
                  user_id=frm.get("id"),
                  user_name=frm.get("first_name") or frm.get("username") or "someone",
                  username=frm.get("username"))
        return ev

    if "poll" in update:
        p = update["poll"]
        ev.update(kind="poll", poll_id=p.get("id"), poll_is_closed=p.get("is_closed"))
        return ev

    msg = (update.get("message") or update.get("edited_message")
           or update.get("channel_post") or {})
    if msg:
        chat = msg.get("chat") or {}
        frm = msg.get("from") or {}
        reply_to = msg.get("reply_to_message") or {}
        ev.update(kind="message", chat_id=chat.get("id"),
                  chat_title=chat.get("title") or chat.get("username"),
                  chat_type=chat.get("type"),
                  text=msg.get("text") or msg.get("caption") or "",
                  user_id=frm.get("id"),
                  user_name=frm.get("first_name") or frm.get("username") or "someone",
                  username=frm.get("username"),
                  reply_to_message_id=reply_to.get("message_id"),
                  migrate_to_chat_id=msg.get("migrate_to_chat_id"),
                  migrate_from_chat_id=msg.get("migrate_from_chat_id"))
    return ev


def parse_command(text):
    """'/movie@GraciaBot The Thing' -> ('movie', 'The Thing'). Else (None, text)."""
    if not text.startswith("/"):
        return None, text
    head, _, rest = text.partition(" ")
    cmd = head[1:].split("@", 1)[0].lower()
    return cmd, rest.strip()


# --------------------------------------------------------------------------- #
# Film data — Letterboxd is the rating source. We scrape the public film page
# and read the rating + metadata from its embedded JSON-LD. The official API is
# application-gated and NOT used. TMDB (optional, TMDB_API_KEY) fills in
# runtime/genres/synopsis/similar. Isolated here so the source can be swapped;
# results are cached in DynamoDB (lookup_film_cached) to rate-limit politely.
# --------------------------------------------------------------------------- #
_LB_BASE = "https://letterboxd.com"


def _slugify(s):
    s = re.sub(r"[^a-z0-9]+", "-", (s or "").lower()).strip("-")
    return s or "film"


def _http_get(url, timeout=12):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "replace")


def _letterboxd(title):
    """Resolve a bare title via Letterboxd SEARCH (best/most-popular match),
    then scrape the film page for canonical name, year, rating (0-5), runtime,
    genres and synopsis. Returns a dict (any field may be None) or None only if
    the title can't be resolved to a film at all.
    """
    try:
        search_html = _http_get(f"{_LB_BASE}/search/films/{urllib.parse.quote(title.strip())}/")
    except Exception as e:
        log.warning("letterboxd search failed for %r: %s", title, e)
        return None
    # First film result = Letterboxd's best/most-popular match for the query.
    m = (re.search(r'data-film-slug="([^"/]+)"', search_html)
         or re.search(r'data-target-link="/film/([^"/]+)/"', search_html)
         or re.search(r'href="/film/([^"/]+)/"', search_html))
    if not m:
        log.warning("letterboxd: no film result for %r", title)
        return None
    slug = m.group(1)
    slug_path = f"/film/{slug}/"
    try:
        film_html = _http_get(f"{_LB_BASE}{slug_path}")
    except Exception as e:
        log.warning("letterboxd film page failed for %r: %s", slug, e)
        return None

    name = rating = year = runtime = desc = None
    genres = []
    block = re.search(r'<script type="application/ld\+json">(.*?)</script>',
                      film_html, re.DOTALL)
    if block:
        raw = block.group(1).replace("/* <![CDATA[ */", "").replace("/* ]]> */", "").strip()
        try:
            data = json.loads(raw)
            name = data.get("name")
            agg = data.get("aggregateRating") or {}
            if agg.get("ratingValue") is not None:
                rating = round(float(agg["ratingValue"]), 2)
            rel = data.get("releasedEvent") or []
            if rel and rel[0].get("startDate"):
                year = str(rel[0]["startDate"])[:4]
            g = data.get("genre")
            genres = [g] if isinstance(g, str) else (g or [])
        except (ValueError, TypeError, KeyError, IndexError):
            pass

    # Fallbacks straight from the page markup (so a JSON-LD miss isn't fatal).
    if not name:
        mt = re.search(r'<meta property="og:title" content="([^"]+)"', film_html)
        name = mt.group(1).strip() if mt else slug.replace("-", " ").title()
    ym = re.search(r"\((\d{4})\)\s*$", name or "")  # og:title often "Dune (2021)"
    if ym:
        name = name[: ym.start()].strip()
        year = year or ym.group(1)
    if year is None:
        my = re.search(r"/films/year/(\d{4})/", film_html)
        year = my.group(1) if my else None
    if not runtime:
        mr = re.search(r"(\d+)\s*mins", film_html)
        runtime = int(mr.group(1)) if mr else None
    if not genres:
        seen = dict.fromkeys(re.findall(r"/films/genre/([a-z0-9-]+)/", film_html))
        genres = [g.replace("-", " ").title() for g in list(seen)[:4]]
    md = (re.search(r'<meta name="description" content="([^"]*)"', film_html)
          or re.search(r'<meta property="og:description" content="([^"]*)"', film_html))
    if md:
        desc = md.group(1).strip() or None

    return {"title": name, "slug": slug, "url": f"{_LB_BASE}{slug_path}",
            "rating_5": rating, "year": year, "runtime_min": runtime,
            "genres": genres, "description": desc}


def _runtime_to_min(s):
    if not s or s == "N/A":
        return None
    m = re.search(r"(\d+)", s)
    return int(m.group(1)) if m else None


def _omdb(title, year=None):
    """OMDb overview + Rotten Tomatoes (from the Ratings array, NOT tomatoMeter
    fields which are often N/A). Returns a dict or None. Non-blocking."""
    if not OMDB_API_KEY:
        return None
    params = {"apikey": OMDB_API_KEY, "type": "movie", "t": title.strip()}
    if year:
        params["y"] = str(year)[:4]
    try:
        data = json.loads(_http_get(
            "https://www.omdbapi.com/?" + urllib.parse.urlencode(params), timeout=12))
    except Exception as e:
        log.warning("omdb request failed for %r: %s", title, e)
        return None
    if data.get("Response") != "True":
        return None

    def clean(x):
        return None if x in (None, "", "N/A") else x

    rt = None
    for r in data.get("Ratings") or []:           # the Ratings array, deliberately
        if r.get("Source") == "Rotten Tomatoes":
            rt = clean(r.get("Value"))             # e.g. "83%"
    genre = clean(data.get("Genre"))
    return {
        "title": clean(data.get("Title")),
        "year": (clean(data.get("Year")) or "")[:4],
        "runtime_min": _runtime_to_min(data.get("Runtime")),
        "genres": [g.strip() for g in genre.split(",")] if genre else [],
        "description": clean(data.get("Plot")),
        "rt_rating": rt,
    }


def _tmdb_get(path, params):
    if not TMDB_API_KEY:
        raise RuntimeError("TMDB_API_KEY not set")
    qs = urllib.parse.urlencode({**params, "api_key": TMDB_API_KEY})
    return json.loads(_http_get(f"https://api.themoviedb.org/3{path}?{qs}", timeout=15))


def _tmdb_meta(title):
    search = _tmdb_get("/search/movie", {"query": title, "include_adult": "false"})
    results = search.get("results") or []
    if not results:
        return None
    movie_id = results[0]["id"]
    details = _tmdb_get(f"/movie/{movie_id}", {})
    recs = _tmdb_get(f"/movie/{movie_id}/recommendations", {})
    overview = (details.get("overview") or "").replace("\n", " ").strip()
    return {
        "title": details.get("title"),
        "year": (details.get("release_date") or "")[:4],
        "tmdb_rating_10": details.get("vote_average"),
        "runtime_min": details.get("runtime"),
        "genres": [g["name"] for g in (details.get("genres") or [])],
        "description": overview,
        "similar": [r["title"] for r in (recs.get("results") or [])[:5]],
    }


def lookup_film(title):
    """Canonical title, year, runtime, genres, one-line description, Letterboxd
    rating, slug, and similar titles. Letterboxd is the rating source; TMDB is
    metadata + the rating fallback only. Returns {'found': False} when nothing.
    """
    # --- Resolution + overview: source-agnostic, never gated on one provider. ---
    lb = None
    try:
        lb = _letterboxd(title)        # popularity-ordered search; also the LB rating
    except Exception as e:
        log.warning("letterboxd lookup failed for %r: %s", title, e)
    tmdb = None
    if TMDB_API_KEY:
        try:
            tmdb = _tmdb_meta(title)
        except Exception as e:
            log.warning("tmdb meta failed: %s", e)
    # Canonical resolution comes from a popularity-ordered source if available.
    base = lb or tmdb
    year_hint = (base or {}).get("year")
    # --- Ratings: independent, non-blocking. RT is fetched for the SAME film
    #     (year_hint) so we never show RT for a different version. ---
    omdb = None
    try:
        omdb = _omdb(title, year_hint)
    except Exception as e:
        log.warning("omdb lookup failed for %r: %s", title, e)
    base = base or omdb

    if not base:
        # Nothing resolved at all — still return a usable record so the add and
        # the overview are never blocked; ratings are simply absent.
        log.info("lookup_film %r -> NOT FOUND (adding bare title)", title)
        return {"found": False, "title": title, "year": "", "slug": _slugify(title),
                "runtime_min": None, "genres": [], "description": "", "similar": [],
                "letterboxd_url": None, "lb_rating": None, "rt_rating": None}

    sources = [base, lb, omdb, tmdb]

    def pick(field):
        for s in sources:
            if s and s.get(field):
                return s[field]
        return None

    canonical = pick("title") or title
    year = pick("year") or ""
    out = {
        "found": True,
        "title": canonical,
        "year": year,
        "slug": (lb or {}).get("slug") or _slugify(f"{canonical} {year}".strip()),
        "runtime_min": pick("runtime_min"),
        "genres": pick("genres") or [],
        "description": pick("description") or "",
        "similar": (tmdb or {}).get("similar") or [],
        "letterboxd_url": (lb or {}).get("url"),
        "lb_rating": (lb or {}).get("rating_5"),     # 0-5, or None
        "rt_rating": (omdb or {}).get("rt_rating"),  # "83%", or None
    }
    log.info("lookup_film %r -> %s (%s) LB=%s RT=%s", title, out["title"],
             out["year"], out.get("lb_rating"), out.get("rt_rating"))
    return out


def lookup_film_cached(title):
    """lookup_film with a DynamoDB cache (stored as JSON to dodge float/Decimal)."""
    key = f"filmcache#{title.strip().lower()}"
    try:
        cached = ddb_get(key, "ref")
        if cached and cached.get("json"):
            return json.loads(cached["json"])
    except Exception as e:
        log.warning("film cache read failed: %s", e)
    info = lookup_film(title)
    if info.get("found"):
        try:
            ddb_put({"PK": key, "SK": "ref", "json": json.dumps(info),
                     "cached_at": _now_iso()})
        except Exception as e:
            log.warning("film cache write failed: %s", e)
    return info


# --------------------------------------------------------------------------- #
# Members — captured so the bot can address/mention people later.
# --------------------------------------------------------------------------- #
def remember_member(chat_id, user_id, display_name, username=None):
    if user_id is None:
        return None
    sk = f"member#{user_id}"
    existing = ddb_get(_pk("movie", chat_id), sk)
    item = {
        "PK": _pk("movie", chat_id), "SK": sk, "user_id": int(user_id),
        "display_name": display_name, "username": username,
        "first_seen": (existing or {}).get("first_seen") or _now_iso(),
        "last_seen": _now_iso(),
    }
    ddb_put(item)
    return item


def get_member(chat_id, user_id):
    return ddb_get(_pk("movie", chat_id), f"member#{user_id}")


def mention_for(chat_id, user_id):
    m = get_member(chat_id, user_id) or {}
    if m.get("username"):
        return f"@{m['username']}"
    return m.get("display_name") or "someone"


# --------------------------------------------------------------------------- #
# Library — per (chat, user), keyed by Letterboxd slug. Metadata is cached on
# the item so cards render without re-scraping. Floats are stored as strings
# (DynamoDB resource rejects float).
# --------------------------------------------------------------------------- #
def add_to_library(chat_id, user_id, title):
    info = lookup_film_cached(title)
    slug = info.get("slug") or _slugify(title)
    name = info.get("title") or title
    item = {
        "PK": _pk("movie", chat_id), "SK": f"lib#{user_id}#{slug}",
        "slug": slug, "owner_id": int(user_id), "title": name,
        "year": str(info.get("year") or ""),
        "runtime_min": info.get("runtime_min"),
        "genres": info.get("genres") or [],
        "description": info.get("description") or "",
        # ratings stored as strings (DDB resource rejects float); either may be None
        "lb_rating": (str(info["lb_rating"]) if info.get("lb_rating") is not None else None),
        "rt_rating": info.get("rt_rating"),
        "added_at": _now_iso(), "watched": False,
    }
    ddb_put(item)
    return item, info


def get_library(chat_id, user_id):
    prefix = f"lib#{user_id}#"
    films = [i for i in ddb_query(_pk("movie", chat_id))
             if str(i.get("SK", "")).startswith(prefix)]
    films.sort(key=lambda f: f.get("added_at", ""))
    return films


def get_film(chat_id, user_id, slug):
    return ddb_get(_pk("movie", chat_id), f"lib#{user_id}#{slug}")


def mark_watched(chat_id, user_id, slug):
    f = get_film(chat_id, user_id, slug)
    if f:
        f["watched"] = True
        ddb_put(f)


def remove_from_library(chat_id, user_id, title):
    """Best-effort remove by slug, then exact title, then substring."""
    info = lookup_film_cached(title)
    lib = get_library(chat_id, user_id)
    slug = info.get("slug")
    tl = title.strip().lower()
    target = None
    if slug:
        target = next((f for f in lib if f.get("slug") == slug), None)
    if not target:
        target = next((f for f in lib if f.get("title", "").strip().lower() == tl), None)
    if not target:
        target = next((f for f in lib if tl and tl in f.get("title", "").lower()), None)
    if not target:
        return None
    ddb_delete(_pk("movie", chat_id), f"lib#{user_id}#{target['slug']}")
    return target["title"]


# --------------------------------------------------------------------------- #
# Seeding — link a named starter library to a real Telegram user_id (never a
# phone number; bots can't see those). Starter films load under owner
# "seed:<name>" (tools/seed_libraries.py); /claim or NL "I'm <name>" reassigns.
# --------------------------------------------------------------------------- #
def _seed_owner(name):
    return f"seed:{name.strip().lower()}"


def list_seed_names(chat_id):
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
        seg = str(it["SK"]).split("#")[-1]
        it["SK"] = f"lib#{user_id}#{seg}"
        it["owner_id"] = int(user_id)
        it.pop("seed_name", None)
        ddb_put(it)
        ddb_delete(_pk("movie", chat_id), f"{seed_prefix}{seg}")
        moved += 1
    ddb_put({"PK": _pk("movie", chat_id), "SK": marker_sk,
             "claimed_by": int(user_id), "seed_name": name.strip(),
             "claimed_at": _now_iso()})
    return {"status": "ok", "moved": moved}


# --- Embedded starter libraries -------------------------------------------- #
# Bundled here (not the unshipped tools/seed_libraries.json) so the group can
# seed from inside the chat with no AWS CLI/chat_id. Load via the
# seed_starter_libraries tool; people then claim with "I'm <name>".
_STARTER_LIBRARIES = json.loads(
    '{\n"libraries": {\n"Chad": [\n{\n"title": "Red River",\n"year": 1948\n},\n{\n"title": "Barry Lyndon",\n"year": 1975\n},\n{\n"title": "Tokyo Story",\n"year": 1953,\n"note": "Previously vetoed (by Chad himself)"\n},\n{\n"title": "Scenes from a Marriage",\n"year": 1973\n},\n{\n"title": "Where Is the Friend\'s House?",\n"year": 1987\n},\n{\n"title": "All About Eve",\n"year": 1950\n},\n{\n"title": "Rear Window",\n"year": 1954\n},\n{\n"title": "Brief Encounter",\n"year": 1945\n},\n{\n"title": "Mirror",\n"year": 1975\n},\n{\n"title": "Fail Safe",\n"year": 1964\n},\n{\n"title": "Ace in the Hole",\n"year": 1951\n},\n{\n"title": "Winter Light",\n"year": 1963\n},\n{\n"title": "The Third Man",\n"year": 1949\n},\n{\n"title": "Moonlight",\n"year": 2016\n},\n{\n"title": "The Cook, the Thief, His Wife & Her Lover",\n"year": 1989\n},\n{\n"title": "All the President\'s Men",\n"year": 1976\n},\n{\n"title": "Touch of Evil",\n"year": 1958\n},\n{\n"title": "Synecdoche, New York",\n"year": 2008\n},\n{\n"title": "The Virgin Spring",\n"year": 1960\n},\n{\n"title": "Hiroshima Mon Amour",\n"year": 1959\n},\n{\n"title": "Brokeback Mountain",\n"year": 2005\n},\n{\n"title": "Midnight Cowboy",\n"year": 1969\n},\n{\n"title": "Icarus",\n"year": 2017\n},\n{\n"title": "7 Up",\n"year": 1964\n},\n{\n"title": "Brother\'s Keeper",\n"year": 1992\n},\n{\n"title": "Man on Wire",\n"year": 2008\n},\n{\n"title": "Salesman",\n"year": 1969\n},\n{\n"title": "The Overnighters",\n"year": 2014\n},\n{\n"title": "Won\'t You Be My Neighbor?",\n"year": 2018\n},\n{\n"title": "Cameraperson",\n"year": 2016\n},\n{\n"title": "Grizzly Man",\n"year": 2005\n},\n{\n"title": "When We Were Kings",\n"year": 1996\n},\n{\n"title": "Hoop Dreams",\n"year": 1994\n},\n{\n"title": "The Thin Blue Line",\n"year": 1988\n},\n{\n"title": "To Be and to Have",\n"year": 2002\n},\n{\n"title": "The King of Kong",\n"year": 2007\n},\n{\n"title": "Stop Making Sense",\n"year": 1984\n},\n{\n"title": "Koyaanisqatsi",\n"year": 1982\n},\n{\n"title": "Senna",\n"year": 2010\n},\n{\n"title": "L\'eclisse",\n"year": 1962\n}\n],\n"Alberto": [\n{\n"title": "The Shop Around the Corner",\n"year": 1940\n},\n{\n"title": "The Good, the Bad and the Ugly",\n"year": 1966\n},\n{\n"title": "Unforgiven",\n"year": 1992,\n"note": "Won session 1"\n},\n{\n"title": "Once Upon a Time in Hollywood",\n"year": 2019\n},\n{\n"title": "Spartacus",\n"year": 1960\n},\n{\n"title": "The War Wagon",\n"year": 1967\n},\n{\n"title": "Meet John Doe",\n"year": 1941\n},\n{\n"title": "Druk (Another Round)",\n"year": 2020\n}\n],\n"Asa": [\n{\n"title": "Buena Vista Social Club",\n"year": 1999\n},\n{\n"title": "The American Friend",\n"year": 1977,\n"note": "Previously vetoed by Asa"\n},\n{\n"title": "Kings of the Road",\n"year": 1976,\n"note": "Previously vetoed by Alberto"\n},\n{\n"title": "Woman in the Dunes",\n"year": 1964\n},\n{\n"title": "Red Sorghum",\n"year": 1988\n},\n{\n"title": "Poetry",\n"year": 2010\n}\n],\n"Anya": [\n{\n"title": "Perfect Days",\n"year": 2023\n},\n{\n"title": "Natural Born Killers",\n"year": 1994\n},\n{\n"title": "The Night Porter",\n"year": 1974,\n"note": "Italian: Il Portiere di Notte"\n}\n],\n"Khimka": [\n{\n"title": "Nowhere",\n"year": 1997\n},\n{\n"title": "Ritual",\n"year": 2000\n},\n{\n"title": "Lost Highway",\n"year": 1997\n}\n]\n}\n}'
)["libraries"]


def seed_starter_libraries(chat_id):
    """Write the embedded starter libraries into this chat under seed:<name>
    placeholders. Idempotent: skips a name already seeded or already claimed.
    Returns {name: films_written}."""
    written = {}
    existing_items = ddb_query(_pk("movie", chat_id))
    for name, films in _STARTER_LIBRARIES.items():
        if not films:
            continue
        key = name.strip().lower()
        if ddb_get(_pk("movie", chat_id), f"seedclaim#{key}"):
            continue  # already claimed by someone
        owner = _seed_owner(name)
        prefix = f"lib#{owner}#"
        if any(str(i.get("SK", "")).startswith(prefix) for i in existing_items):
            continue  # already seeded
        n = 0
        for f in films:
            slug = _slugify(f["title"])
            # Seeded films are never pre-marked watched — "watched" is only set
            # when a film actually wins a movie night in this chat.
            ddb_put({"PK": _pk("movie", chat_id), "SK": f"lib#{owner}#{slug}",
                     "slug": slug, "owner_id": owner, "seed_name": name.strip(),
                     "title": f["title"], "year": str(f.get("year") or ""),
                     "genres": [], "description": "", "lb_rating": None,
                     "rt_rating": None, "added_at": _now_iso(),
                     "watched": False})
            n += 1
        written[name] = n
    return written


# --------------------------------------------------------------------------- #
# Game state machine: IDLE -> JOINING -> SELECTING -> VETO -> DONE.
# All randomness (3-per-player draw, pool pick), thumb/lock tracking, veto
# counting and win resolution are plain Python. The LLM never does game math.
# --------------------------------------------------------------------------- #
def _now_epoch():
    return int(datetime.now(timezone.utc).timestamp())


def get_game(chat_id):
    return ddb_get(_pk("movie", chat_id), "game#current")


def put_game(game):
    ddb_put(game)
    return game


def clear_game(chat_id):
    ddb_delete(_pk("movie", chat_id), "game#current")


def new_game(chat_id, initiator_id):
    return {
        "PK": _pk("movie", chat_id), "SK": "game#current",
        "session_id": str(uuid.uuid4()), "phase": "JOINING",
        "players": [], "initiator": int(initiator_id) if initiator_id else None,
        "vetoes_remaining": {},
        "join_message_id": None,
        "selection": {},   # {uid: {slots:[{slug,title,state}], shown:[slug], locked}}
        "cards": {},       # {message_id: {uid, slot}}
        "pool": [],        # [{owner, slug, title}]
        "current": None,   # {film, poll_id, poll_message_id, presented_at, resolved}
        "created_at": _now_iso(),
    }


# ---- global poll map (poll/poll_answer updates carry no chat_id) ---------- #
def put_poll_map(poll_id, chat_id, film):
    ddb_put({"PK": f"pollmap#{poll_id}", "SK": "ref",
             "chat_id": int(chat_id), "film": film, "at": _now_iso()})


def get_poll_map(poll_id):
    return ddb_get(f"pollmap#{poll_id}", "ref")


def del_poll_map(poll_id):
    ddb_delete(f"pollmap#{poll_id}", "ref")


def _add_player(game, user_id):
    if user_id is None:
        return False
    uid = str(user_id)
    if uid not in [str(p) for p in game["players"]]:
        game["players"].append(int(user_id))
        game["vetoes_remaining"][uid] = 1
        return True
    return False


# ---- presentation --------------------------------------------------------- #
def _item_rating_phrase(item):
    """Whatever ratings we have, shown together; empty string if none."""
    parts = []
    lb = item.get("lb_rating")
    if lb not in (None, "", "None"):
        parts.append(f"{lb}/5 Letterboxd")
    rt = item.get("rt_rating")
    if rt not in (None, "", "N/A"):
        parts.append(f"{rt} RT")
    return " · ".join(parts)


def _film_card(item):
    """A selection / candidate card: title, runtime, genre, one-line description."""
    if not item:
        return "(film)"
    yr = f" ({item['year']})" if item.get("year") else ""
    lines = [f"🎬 {item['title']}{yr}"]
    meta = []
    rp = _item_rating_phrase(item)
    if rp:
        meta.append(rp)
    if item.get("runtime_min"):
        meta.append(f"{item['runtime_min']} min")
    if item.get("genres"):
        meta.append(", ".join(item["genres"][:2]))
    if meta:
        lines.append(" · ".join(meta))
    if item.get("description"):
        d = item["description"]
        lines.append(d if len(d) <= 200 else d[:197] + "…")
    return "\n".join(lines)


def _join_keyboard():
    return {"inline_keyboard": [
        [{"text": "🎬 Join", "callback_data": "join"}],
        [{"text": "▶️ Start", "callback_data": "start"}],
    ]}


def _thumb_keyboard():
    return {"inline_keyboard": [[
        {"text": "👍", "callback_data": "up"},
        {"text": "👎", "callback_data": "down"},
    ]]}


def _join_text(chat_id, game):
    if game["players"]:
        roster = ", ".join(mention_for(chat_id, p) for p in game["players"])
    else:
        roster = "(nobody yet)"
    return ("🎬 Movie night! Tap 🎬 Join to play.\n"
            f"In so far: {roster}\n\n"
            "When everyone's in, the host taps ▶️ Start.")


# ---- start / roster ------------------------------------------------------- #
def start_game(mode, chat_id, initiator_id):
    if get_game(chat_id):
        send_message(mode, chat_id, "A movie night's already going. Tap 🎬 Join on the card above.")
        return
    game = new_game(chat_id, initiator_id)
    _add_player(game, initiator_id)
    resp = send_message(mode, chat_id, _join_text(chat_id, game),
                        reply_markup=_join_keyboard())
    game["join_message_id"] = (resp.get("result") or {}).get("message_id")
    put_game(game)


def on_callback(mode, ev):
    chat_id, uid = ev["chat_id"], ev.get("user_id")
    data, cqid, mid = ev.get("callback_data"), ev.get("callback_query_id"), ev.get("message_id")
    answer_callback(mode, cqid)
    game = get_game(chat_id)
    if not game:
        return
    if _veto_backstop(mode, chat_id, game):
        return
    if data == "join" and game["phase"] == "JOINING":
        remember_member(chat_id, uid, ev["user_name"], ev.get("username"))
        if _add_player(game, uid):
            put_game(game)
            if game.get("join_message_id"):
                edit_message_text(mode, chat_id, game["join_message_id"],
                                  _join_text(chat_id, game), reply_markup=_join_keyboard())
        return
    if data == "start" and game["phase"] == "JOINING":
        if game.get("initiator") and uid != game["initiator"]:
            answer_callback(mode, cqid, "Only the host can start.")
            return
        if not game["players"]:
            answer_callback(mode, cqid, "Nobody has joined yet.")
            return
        _begin_selection(mode, chat_id, game)
        return
    if data in ("up", "down") and game["phase"] == "SELECTING":
        _handle_thumb(mode, chat_id, game, uid, mid, up=(data == "up"))
        return


# ---- selection ------------------------------------------------------------ #
def _begin_selection(mode, chat_id, game):
    game["phase"] = "SELECTING"
    game["selection"] = {}
    game["cards"] = {}
    put_game(game)
    send_message(mode, chat_id,
                 "🎲 Drawing 3 films from each player's library. "
                 "React 👍 to keep a card or 👎 to swap it. Three 👍 locks you in.")
    for uid in list(game["players"]):
        _start_player_selection(mode, chat_id, game, uid)
    put_game(game)
    _maybe_finish_selection(mode, chat_id, game)


def _start_player_selection(mode, chat_id, game, uid):
    lib = [f for f in get_library(chat_id, uid) if not f.get("watched")]
    sel = {"slots": [], "shown": [], "locked": False}
    game["selection"][str(uid)] = sel
    picks = random.sample(lib, min(3, len(lib))) if lib else []
    for f in picks:
        sel["shown"].append(f["slug"])
        slot = len(sel["slots"])
        sel["slots"].append({"slug": f["slug"], "title": f["title"], "state": "pending"})
        _post_selection_card(mode, chat_id, game, uid, slot)
    if not picks:
        sel["locked"] = True  # empty library contributes nothing


def _post_selection_card(mode, chat_id, game, uid, slot):
    sel = game["selection"][str(uid)]
    item = get_film(chat_id, uid, sel["slots"][slot]["slug"])
    text = f"{mention_for(chat_id, uid)} — pick {slot + 1}:\n\n{_film_card(item)}"
    resp = send_message(mode, chat_id, text, reply_markup=_thumb_keyboard())
    mid = (resp.get("result") or {}).get("message_id")
    if mid is not None:
        game["cards"][str(mid)] = {"uid": str(uid), "slot": slot}


def _handle_thumb(mode, chat_id, game, uid, message_id, up):
    card = game["cards"].get(str(message_id))
    if not card or str(uid) != str(card["uid"]):
        return  # only the card's owner controls it
    sel = game["selection"][card["uid"]]
    slot = card["slot"]
    if up:
        sel["slots"][slot]["state"] = "locked"
        if sel["slots"] and all(s["state"] == "locked" for s in sel["slots"]):
            sel["locked"] = True
        put_game(game)
        _maybe_finish_selection(mode, chat_id, game)
        return
    # thumbs-down: replace with another unshown random film from THIS library
    lib = [f for f in get_library(chat_id, int(card["uid"])) if not f.get("watched")]
    avail = [f for f in lib if f["slug"] not in sel["shown"]]
    if not avail:
        send_message(mode, chat_id,
                     f"{mention_for(chat_id, int(card['uid']))}, no more films to swap in — "
                     "react 👍 to keep this one.")
        return
    nf = random.choice(avail)
    sel["shown"].append(nf["slug"])
    sel["slots"][slot] = {"slug": nf["slug"], "title": nf["title"], "state": "pending"}
    del game["cards"][str(message_id)]
    _post_selection_card(mode, chat_id, game, int(card["uid"]), slot)
    put_game(game)


def _maybe_finish_selection(mode, chat_id, game):
    if not game["players"]:
        return
    if all(game["selection"].get(str(p), {}).get("locked") for p in game["players"]):
        _begin_veto(mode, chat_id, game)


# ---- veto ----------------------------------------------------------------- #
def _begin_veto(mode, chat_id, game):
    pool = []
    for uid in game["players"]:
        for s in game["selection"].get(str(uid), {}).get("slots", []):
            if s["state"] == "locked":
                pool.append({"owner": str(uid), "slug": s["slug"], "title": s["title"]})
    game["phase"] = "VETO"
    game["pool"] = pool
    game["current"] = None
    if not pool:
        send_message(mode, chat_id, "Nobody had any films to put forward — no winner tonight.")
        clear_game(chat_id)
        return
    send_message(mode, chat_id,
                 f"🗳 Veto round! {len(pool)} films in the pool, one veto each. "
                 "Vote 🚫 Veto within 90s to knock a pick out.")
    _present_candidate(mode, chat_id, game)
    put_game(game)


def _present_candidate(mode, chat_id, game):
    pool = game["pool"]
    if not pool:
        send_message(mode, chat_id, "Pool's empty — no winner.")
        clear_game(chat_id)
        return
    cand = random.choice(pool)        # random pick, in code
    pool.remove(cand)
    item = get_film(chat_id, int(cand["owner"]), cand["slug"])
    send_message(mode, chat_id, f"🎲 Candidate:\n\n{_film_card(item) if item else cand['title']}")
    resp = send_poll(mode, chat_id, "Veto this pick?", ["🚫 Veto", "👍 Fine by me"],
                     is_anonymous=False, open_period=90)
    result = resp.get("result") or {}
    poll_id = (result.get("poll") or {}).get("id")
    game["current"] = {
        "film": cand, "poll_id": poll_id,
        "poll_message_id": result.get("message_id"),
        "presented_at": _now_epoch(), "resolved": False,
    }
    if poll_id:
        put_poll_map(poll_id, chat_id, cand)


def on_poll_answer(mode, ev):
    chat_id = ev["chat_id"]
    game = get_game(chat_id)
    if not game or game.get("phase") != "VETO":
        return
    if _veto_backstop(mode, chat_id, game):
        return
    cur = game.get("current")
    if not cur or cur.get("poll_id") != ev.get("poll_id") or cur.get("resolved"):
        return  # stale or already resolved poll
    if 0 not in (ev.get("poll_option_ids") or []):
        return  # only the Veto option (index 0) matters
    uid = str(ev.get("user_id"))
    if game["vetoes_remaining"].get(uid, 0) <= 0:
        return  # no veto left — ignore
    game["vetoes_remaining"][uid] -= 1
    cur["resolved"] = True
    if cur.get("poll_message_id"):
        stop_poll(mode, chat_id, cur["poll_message_id"])
    if cur.get("poll_id"):
        del_poll_map(cur["poll_id"])
    send_message(mode, chat_id,
                 f"🚫 {mention_for(chat_id, ev.get('user_id'))} vetoed "
                 f"“{cur['film']['title']}”. Next pick…")
    _present_candidate(mode, chat_id, game)
    put_game(game)


def on_poll(mode, ev):
    if not ev.get("poll_is_closed"):
        return
    chat_id = ev["chat_id"]
    game = get_game(chat_id)
    if not game or game.get("phase") != "VETO":
        return
    cur = game.get("current")
    if not cur or cur.get("poll_id") != ev.get("poll_id") or cur.get("resolved"):
        return  # only the un-vetoed CURRENT poll auto-closing declares a winner
    _declare_winner(mode, chat_id, game, cur["film"])


def _veto_backstop(mode, chat_id, game):
    """No scheduler: if an un-vetoed candidate is past its 90s and any update
    arrives, resolve it as the winner now. Returns True if it fired."""
    if not game or game.get("phase") != "VETO":
        return False
    cur = game.get("current")
    if cur and not cur.get("resolved") and _now_epoch() - cur.get("presented_at", 0) >= 90:
        _declare_winner(mode, chat_id, game, cur["film"])
        return True
    return False


def _declare_winner(mode, chat_id, game, film):
    cur = game.get("current") or {}
    cur["resolved"] = True
    owner, slug = int(film["owner"]), film["slug"]
    item = get_film(chat_id, owner, slug)
    title = (item or {}).get("title") or film["title"]
    year = (item or {}).get("year") or ""
    mark_watched(chat_id, owner, slug)
    ddb_put({
        "PK": _pk("movie", chat_id), "SK": f"history#{game['session_id']}",
        "session_id": game["session_id"], "winner_title": title,
        "winner_slug": slug, "winner_owner_id": owner,
        "watched_date": _now_iso(),
        "participants": [int(p) for p in game["players"]], "ratings": {},
    })
    if cur.get("poll_id"):
        del_poll_map(cur["poll_id"])
    note = winner_note(title, year)
    card = _film_card(item) if item else title
    text = f"🏆 Tonight's winner:\n\n{card}"
    if note:
        text += f"\n\n{note}"
    text += "\n\nEnjoy! 🎬"
    send_message(mode, chat_id, text)
    clear_game(chat_id)


def winner_note(title, year):
    """Short, spoiler-free context for the winner. LLM writes prose; picks nothing."""
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


# --------------------------------------------------------------------------- #
# Natural-language intent (Bedrock). The model parses what people say and calls
# tools; code performs the action. Privacy is OFF, so EVERY message arrives —
# the model must stay silent on anything that isn't a film/library/game intent.
# --------------------------------------------------------------------------- #
MOVIE_TOOLS = [
    {"toolSpec": {"name": "lookup_film",
                  "description": "Look up a film: year, runtime, genres, one-line synopsis, plus ratings (lb_rating 0-5 from Letterboxd, rt_rating like '83%' from Rotten Tomatoes). Any rating may be null.",
                  "inputSchema": {"json": {"type": "object",
                                           "properties": {"title": {"type": "string"}},
                                           "required": ["title"]}}}},
    {"toolSpec": {"name": "add_to_library",
                  "description": "Add a film to the SENDER's personal library for this chat (also for 'I want to see X').",
                  "inputSchema": {"json": {"type": "object",
                                           "properties": {"title": {"type": "string"}},
                                           "required": ["title"]}}}},
    {"toolSpec": {"name": "remove_from_library",
                  "description": "Remove a film from the SENDER's library.",
                  "inputSchema": {"json": {"type": "object",
                                           "properties": {"title": {"type": "string"}},
                                           "required": ["title"]}}}},
    {"toolSpec": {"name": "list_library",
                  "description": "List the films in the SENDER's library.",
                  "inputSchema": {"json": {"type": "object", "properties": {}}}}},
    {"toolSpec": {"name": "claim_library",
                  "description": "Link a seeded starter library (by person's name, e.g. 'Chad') to the sender.",
                  "inputSchema": {"json": {"type": "object",
                                           "properties": {"name": {"type": "string"}},
                                           "required": ["name"]}}}},
    {"toolSpec": {"name": "start_movie_night",
                  "description": "Start a movie-night game in this chat (posts the Join/Start card).",
                  "inputSchema": {"json": {"type": "object", "properties": {}}}}},
    {"toolSpec": {"name": "seed_starter_libraries",
                  "description": "Load the bundled starter libraries (Chad, Alberto, Asa, Anya, …) into this chat so people can claim them. Use when asked to 'load/seed the starter libraries'.",
                  "inputSchema": {"json": {"type": "object", "properties": {}}}}},
]

MOVIE_SYSTEM = (
    "You are SirWatchalot, a film-night helper in a Telegram group. Privacy is OFF so "
    "you see every message, but ONLY act on clear film intent: add/remove/list a "
    "personal library, look up a film, claim a seeded library ('I'm Chad'), load the "
    "starter libraries, or start movie night. For anything else, reply with exactly "
    "'(silent)' and call no tools.\n"
    "SEED ('load/seed the starter libraries'): call seed_starter_libraries, report the "
    "per-name counts, and tell people to claim theirs by saying 'I'm <name>'.\n"
    "ADD ('add X to my library', 'I want to see X'): immediately call add_to_library(X). "
    "The tool resolves the title to a SINGLE film via Letterboxd search (the most popular "
    "match). If a title has several versions (e.g. Dune 1984 vs 2021) it has ALREADY "
    "picked the prominent/recent one — do NOT ask which; just state the one you saved so "
    "it's correctable, e.g. 'Added Dune (2021) — say \"the 1984 one\" if you meant that.' "
    "Then give a one-line synopsis, the runtime and genre, and whatever ratings came back: "
    "Letterboxd as 'lb_rating/5 on Letterboxd' and/or Rotten Tomatoes as 'rt_rating on "
    "Rotten Tomatoes'. Confirm it's saved. The overview and the add do NOT depend on "
    "ratings — if lb_rating and/or rt_rating are null, simply omit them and proceed.\n"
    "LOOK UP ('what's X rated', 'tell me about X'): call lookup_film and answer with a "
    "one-line synopsis + whatever ratings are present; offer to add it.\n"
    "NEVER mention a 'database' or internal storage, and NEVER invent ratings or details "
    "— use only the tool's fields and omit any that are missing. Ask a clarifying "
    "question ONLY when the tool returns resolved/found = false (no reasonable match at "
    "all). Keep replies to 1-3 sentences."
)


def _dispatch_tool(name, tool_input, ctx):
    chat_id, uid, mode = ctx["chat_id"], ctx.get("user_id"), ctx["mode"]
    if name == "lookup_film":
        return lookup_film_cached(tool_input["title"])
    if name == "add_to_library":
        item, info = add_to_library(chat_id, uid, tool_input["title"])
        return {"added": True, "resolved": info.get("found", False),
                "title": item["title"], "year": item.get("year"),
                "runtime_min": item.get("runtime_min"), "genres": item.get("genres"),
                "description": info.get("description"),
                "lb_rating": info.get("lb_rating"), "rt_rating": info.get("rt_rating"),
                "similar": info.get("similar")}
    if name == "remove_from_library":
        removed = remove_from_library(chat_id, uid, tool_input["title"])
        return {"removed": removed}
    if name == "list_library":
        return {"films": [{"title": f["title"], "year": f.get("year")}
                          for f in get_library(chat_id, uid)]}
    if name == "claim_library":
        res = claim_library(chat_id, tool_input["name"], uid)
        if res.get("status") == "ok":
            remember_member(chat_id, uid, ctx.get("user_name"), ctx.get("username"))
        return res
    if name == "start_movie_night":
        start_game(mode, chat_id, uid)
        return {"started": True}
    if name == "seed_starter_libraries":
        written = seed_starter_libraries(chat_id)
        return {"seeded": written, "names": list(_STARTER_LIBRARIES.keys())}
    return {"error": f"unknown tool {name}"}


def converse(system_prompt, user_text, ctx, tools=MOVIE_TOOLS, max_turns=6):
    """Run the Bedrock tool-use loop; return the model's final text."""
    messages = [{"role": "user", "content": [{"text": user_text}]}]
    for _ in range(max_turns):
        resp = _bedrock.converse(
            modelId=BEDROCK_MODEL_ID, system=[{"text": system_prompt}],
            messages=messages, inferenceConfig={"maxTokens": 1000, "temperature": 0.5},
            toolConfig={"tools": tools},
        )
        out = resp["output"]["message"]
        messages.append(out)
        if resp.get("stopReason") != "tool_use":
            return "".join(b.get("text", "") for b in out["content"]).strip()
        results = []
        for block in out["content"]:
            if "toolUse" not in block:
                continue
            tu = block["toolUse"]
            try:
                result = _dispatch_tool(tu["name"], tu.get("input", {}), ctx)
            except Exception as e:
                log.error("tool %s failed: %s", tu["name"], e)
                result = {"error": str(e)}
            results.append({"toolResult": {"toolUseId": tu["toolUseId"],
                                           "content": [{"json": {"result": result}}]}})
        messages.append({"role": "user", "content": results})
    return ""


def on_message(mode, ev):
    chat_id, uid = ev["chat_id"], ev.get("user_id")
    text = (ev.get("text") or "").strip()
    remember_member(chat_id, uid, ev["user_name"], ev.get("username"))
    game = get_game(chat_id)
    if _veto_backstop(mode, chat_id, game):
        return
    if not text or not AI_ENABLED:
        return
    ctx = {"chat_id": chat_id, "user_id": uid, "user_name": ev["user_name"],
           "username": ev.get("username"), "mode": mode}
    try:
        reply = converse(MOVIE_SYSTEM, text, ctx)
    except Exception as e:
        log.error("bedrock movie failed: %s", e)
        return
    reply = (reply or "").strip()
    if reply and reply.lower() != "(silent)":
        send_message(mode, chat_id, reply)


# --------------------------------------------------------------------------- #
# Mode handler: MOVIE — dispatch by update kind.
# --------------------------------------------------------------------------- #
def handle_movie(mode, ev):
    kind = ev.get("kind")
    if kind == "message":
        on_message(mode, ev)
    elif kind == "callback":
        on_callback(mode, ev)
    elif kind == "reaction":
        _on_reaction(mode, ev)
    elif kind == "poll_answer":
        on_poll_answer(mode, ev)
    elif kind == "poll":
        on_poll(mode, ev)


def _on_reaction(mode, ev):
    chat_id, uid = ev["chat_id"], ev.get("user_id")
    game = get_game(chat_id)
    if not game:
        return
    if _veto_backstop(mode, chat_id, game):
        return
    if game.get("phase") != "SELECTING":
        return
    emojis = ev.get("reactions") or []
    if "👎" in emojis:
        _handle_thumb(mode, chat_id, game, uid, ev.get("message_id"), up=False)
    elif "👍" in emojis:
        _handle_thumb(mode, chat_id, game, uid, ev.get("message_id"), up=True)


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
        ev = parse_update(update)
        chat_id = ev.get("chat_id")

        # poll / poll_answer updates carry no chat — resolve via the global poll map.
        if chat_id is None and ev.get("poll_id"):
            ref = get_poll_map(ev["poll_id"]) if mode == "movie" else None
            if ref:
                chat_id = ref.get("chat_id")
        if chat_id is None:
            return {"statusCode": 200, "body": "ok"}
        ev["chat_id"] = chat_id

        # Persist / refresh the chat id (never hardcoded).
        remember_chat(mode, chat_id, ev.get("chat_title"))

        # Idempotency: drop Telegram's retried deliveries before any state change.
        if seen_update(mode, chat_id, ev.get("update_id")):
            log.info("duplicate update %s ignored", ev.get("update_id"))
            return {"statusCode": 200, "body": "ok"}

        # Handle migration service messages on the way in.
        if ev.get("migrate_to_chat_id"):
            migrate_chat(mode, chat_id, ev["migrate_to_chat_id"])
            return {"statusCode": 200, "body": "ok"}
        if ev.get("migrate_from_chat_id"):
            migrate_chat(mode, ev["migrate_from_chat_id"], chat_id)

        modes[mode]["handler"](mode, ev)
    except Exception as e:
        log.exception("handler error in mode %s: %s", mode, e)
        # Always 200 so Telegram doesn't hammer us with retries.

    return {"statusCode": 200, "body": "ok"}
