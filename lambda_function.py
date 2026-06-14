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
from datetime import datetime, timedelta, timezone
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


def ddb_scan():
    """Every item in the table, paginated. Used ONLY by the low-frequency daily job
    (the morning-after poll has no chat to key on); the hot path always queries by PK."""
    items, kwargs = [], {}
    while True:
        resp = _table.scan(**kwargs)
        items.extend(resp.get("Items", []))
        if "LastEvaluatedKey" not in resp:
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    return _decimals_to_native(items)


# --------------------------------------------------------------------------- #
# Chat registry + supergroup migration
# --------------------------------------------------------------------------- #
# Chat id is NEVER hardcoded. We persist a registry item per (mode, chat) so
# proactive senders (e.g. the morning-after poll cron) can resolve the current
# id, and so we can rewrite data when Telegram migrates a group to a
# supergroup (the failure that haunted the previous third-party setup).
def _pk(mode, chat_id):
    return f"{mode}#{chat_id}"


def _chat_id_from_pk(pk):
    """Inverse of _pk for the tick sweep, which reads game items by scan and needs the
    chat id back out of the partition key (e.g. 'movie#-100123' -> -100123)."""
    try:
        return int(str(pk).split("#", 1)[1])
    except (IndexError, ValueError):
        return None


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


# A Telegram bot token is "<bot-id digits>:<35-ish url-safe chars>". We validate
# this at load so a truncated value (e.g. leading bot-id digits lost on a bad
# paste) fails loudly in the logs instead of silently 404ing every send.
_TOKEN_RE = re.compile(r"^\d{6,}:[A-Za-z0-9_-]{30,}$")
_token_cache = {}
_healthchecked = set()


def _token_for(mode):
    """Bot token from the <MODE>_BOT_TOKEN env var. Cached per cold start and
    format-validated so a truncated value (lost bot-id digits) fails loudly."""
    if mode in _token_cache:
        return _token_cache[mode]
    cfg = _mode_config()[mode]
    token = os.environ.get(cfg["token_env"], "").strip()
    if not token:
        raise RuntimeError(f"missing env {cfg['token_env']} for mode {mode}")
    if not _TOKEN_RE.match(token):
        log.error("BOT TOKEN for %s looks MALFORMED (len=%d, starts %r) — Telegram will "
                  "404 every send. Expected '<digits>:<35+ url-safe chars>'.",
                  mode, len(token), token[:4])
    _token_cache[mode] = token
    return token


def verify_token(mode):
    """getMe healthcheck — once per cold start per mode; logs the resolved bot on
    success, loudly on failure (a bad/truncated token is then obvious in logs)."""
    if mode in _healthchecked:
        return
    _healthchecked.add(mode)
    try:
        resp = _tg_request(_token_for(mode), "getMe", {})
    except Exception as e:
        log.error("healthcheck %s: could not load token: %s", mode, e)
        return
    if resp.get("ok"):
        u = resp.get("result") or {}
        log.info("healthcheck %s: getMe ok -> @%s (id %s)", mode, u.get("username"), u.get("id"))
    else:
        log.error("healthcheck %s: getMe FAILED -> %s — token bad or truncated.",
                  mode, resp.get("description"))


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
          "poll_option_ids": [], "poll_option_counts": [], "poll_total_voters": None,
          "reply_to_message_id": None,
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
        # Capture each option's running voter_count (otherwise discarded) so the
        # close handler can cross-check the raw tally the group actually saw.
        ev.update(kind="poll", poll_id=p.get("id"), poll_is_closed=p.get("is_closed"),
                  poll_option_counts=[o.get("voter_count", 0) for o in (p.get("options") or [])],
                  poll_total_voters=p.get("total_voter_count"))
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


def _extract_title(text):
    """Pull a film title out of a messy add-loop reply: prefer a quoted title,
    else strip a leading @mention and command lead-ins. Returns a clean title, or
    None for emoji/reaction-only or empty input."""
    t = (text or "").strip()
    q = re.search(r'["“]([^"”]{2,})["”]', t)          # prefer an explicitly quoted title
    if q:
        return q.group(1).strip()
    t = re.sub(r'@\w+', '', t)                         # drop @mentions
    t = re.sub(r'^\s*(?:please\s+)?'
               r'(?:add(?:\s+to)?(?:\s+my)?(?:\s+library)?'
               r'|i\s+want\s+to\s+see|let\'?s\s+watch|look\s*up|find|watch)\s+',
               '', t, flags=re.I)
    t = t.strip(' \t\n"“”\'')
    return t if re.search(r'\w', t) else None


def _identify_film(text, min_year=None, max_year=None):
    """Turn a messy add-loop reply into a concrete film {title, year}. Handles
    'Title - Director', typos, and descriptions, and uses the active year window to
    disambiguate (e.g. 'Sunrise' in 1925–1939 -> Murnau's Sunrise, 1927). Returns
    (title, year|None); falls back to (text, None) when AI is off or it can't name one."""
    if not AI_ENABLED:
        return text, None
    window = ""
    if min_year or max_year:
        window = f" The film must be one released between {min_year or 'any'} and {max_year or 'any'}."
    resp = _bedrock.converse(
        modelId=BEDROCK_MODEL_ID,
        system=[{"text": (
            "You identify the single film a user means from a short, possibly messy message "
            "(it may include the director, a typo, or a description)." + window +
            " Reply with ONLY a JSON object: {\"title\": \"<canonical film title>\", "
            "\"year\": <4-digit year or null>}. Pick the most likely film given the year "
            "window. If you truly cannot name one specific film, reply "
            "{\"title\": null, \"year\": null}. No other text.")}],
        messages=[{"role": "user", "content": [{"text": text}]}],
        inferenceConfig={"maxTokens": 80, "temperature": 0})
    raw = "".join(b.get("text","") for b in resp["output"]["message"]["content"]).strip()
    try:
        data = json.loads(raw)
        return (data.get("title") or text), data.get("year")
    except (ValueError, TypeError):
        return text, None


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


def _letterboxd_rating(tmdb_id):
    """Letterboxd average rating (0–5) for a film by TMDB id. Resolves the
    canonical page via the /tmdb/<id> redirect and reads the rating meta tag.
    Sets a browser UA so Lambda isn't 403'd. Returns float or None."""
    req = urllib.request.Request(f"{_LB_BASE}/tmdb/{tmdb_id}", headers={
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/124.0.0.0 Safari/537.36"),
        "Accept": "text/html",
    })
    try:
        with urllib.request.urlopen(req, timeout=8) as r:
            html = r.read().decode("utf-8", "replace")
    except Exception as e:
        log.warning("letterboxd rating fetch failed for tmdb %s: %s", tmdb_id, e)
        return None
    m = re.search(r'twitter:data2"[^>]*content="([\d.]+) out of 5"', html)
    if not m:
        log.warning("letterboxd: no rating meta for tmdb %s", tmdb_id)
        return None
    return round(float(m.group(1)), 2)


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
    imdb = clean(data.get("imdbRating"))
    try:
        imdb_rating = float(imdb) if imdb else None
    except ValueError:
        imdb_rating = None
    return {
        "title": clean(data.get("Title")),
        "year": (clean(data.get("Year")) or "")[:4],
        "runtime_min": _runtime_to_min(data.get("Runtime")),
        "genres": [g.strip() for g in genre.split(",")] if genre else [],
        "description": clean(data.get("Plot")),
        "rt_rating": rt,
        "imdb_rating": imdb_rating,
    }


def _tmdb_get(path, params):
    if not TMDB_API_KEY:
        raise RuntimeError("TMDB_API_KEY not set")
    qs = urllib.parse.urlencode({**params, "api_key": TMDB_API_KEY})
    return json.loads(_http_get(f"https://api.themoviedb.org/3{path}?{qs}", timeout=15))


def _tmdb(title, year=None):
    """Primary resolver: a BROAD TMDB search. The year is passed as a SEPARATE
    parameter (primary_release_year) — never concatenated into the query string —
    and we drop it and retry if it yields nothing (weak-match guard). Returns the
    top match's canonical metadata + alt candidates, or None."""
    if not TMDB_API_KEY:
        return None
    base = {"query": title.strip(), "include_adult": "false"}
    try:
        params = dict(base)
        if year:
            params["primary_release_year"] = str(year)[:4]
        results = _tmdb_get("/search/movie", params).get("results") or []
        if not results and year:
            results = _tmdb_get("/search/movie", base).get("results") or []
    except Exception as e:
        log.warning("tmdb search failed for %r: %s", title, e)
        return None
    if not results:
        return None
    top = results[0]
    try:
        d = _tmdb_get(f"/movie/{top['id']}", {})
    except Exception:
        d = {}
    overview = (d.get("overview") or top.get("overview") or "").replace("\n", " ").strip()
    rating = d.get("vote_average") or top.get("vote_average")
    return {
        "tmdb_id": top["id"],
        "title": d.get("title") or top.get("title") or title,
        "year": (d.get("release_date") or top.get("release_date") or "")[:4],
        "runtime_min": d.get("runtime") or None,
        "genres": [g["name"] for g in (d.get("genres") or [])],
        "rating_10": round(float(rating), 1) if rating else None,
        "description": overview,
        "alts": [{"title": r.get("title"), "year": (r.get("release_date") or "")[:4]}
                 for r in results[:4]],
    }


def lookup_film(title, year=None):
    """Resolve a film independent of any one source. TMDB is the primary matcher
    (year is a separate param); OMDb then Letterboxd are fallbacks. Returns
    canonical title/year/id/genres/runtime + a rating (TMDB vote_average baseline).
    A missing rating NEVER blocks. found=False only when nothing resolves at all.
    """
    tmdb = _tmdb(title, year)
    omdb = None
    if (not tmdb or not tmdb.get("genres") or tmdb.get("runtime_min") is None
            or tmdb.get("rating_10") is None):
        try:
            omdb = _omdb(title, year or (tmdb or {}).get("year"))
        except Exception as e:
            log.warning("omdb lookup failed for %r: %s", title, e)
    lb = None
    if not tmdb and not omdb:
        try:
            lb = _letterboxd(title)   # last-resort finder only
        except Exception as e:
            log.warning("letterboxd lookup failed for %r: %s", title, e)
    if not (tmdb or omdb or lb):
        log.info("lookup_film %r (%s) -> NOT FOUND (adding bare title)", title, year)
        return {"found": False, "title": title, "year": str(year or ""),
                "slug": _slugify(title), "tmdb_id": None, "runtime_min": None,
                "genres": [], "description": "", "rating": None, "rating_scale": None,
                "rt_rating": None, "alts": []}

    sources = [tmdb, omdb, lb]

    def pick(field):
        for s in sources:
            if s and s.get(field):
                return s[field]
        return None

    canonical = pick("title") or title
    yr = pick("year") or str(year or "")
    # Rating baseline: TMDB vote_average (/10), else OMDb IMDb (/10), else Letterboxd (/5).
    lb_rating = None
    _tid = (tmdb or {}).get("tmdb_id")
    if _tid:
        lb_rating = _letterboxd_rating(_tid)
    rating = rating_scale = None
    if lb_rating is not None:
        rating, rating_scale = lb_rating, 5            # Letterboxd is the rating source
    elif tmdb and tmdb.get("rating_10") is not None:
        rating, rating_scale = tmdb["rating_10"], 10   # fallbacks only
    elif omdb and omdb.get("imdb_rating") is not None:
        rating, rating_scale = omdb["imdb_rating"], 10
    elif lb and lb.get("rating_5") is not None:
        rating, rating_scale = lb["rating_5"], 5
    out = {
        "found": True,
        "title": canonical,
        "year": yr,
        "slug": (lb or {}).get("slug") or _slugify(f"{canonical} {yr}".strip()),
        "tmdb_id": (tmdb or {}).get("tmdb_id"),
        "runtime_min": pick("runtime_min"),
        "genres": pick("genres") or [],
        "description": pick("description") or "",
        "rating": rating,
        "rating_scale": rating_scale,
        "rt_rating": (omdb or {}).get("rt_rating"),
        "alts": (tmdb or {}).get("alts") or [],
    }
    log.info("lookup_film %r (%s) -> %s (%s) rating=%s/%s genres=%s rt=%s",
             title, year, out["title"], out["year"], rating, rating_scale,
             out["genres"], out["rt_rating"])
    return out


def lookup_film_cached(title, year=None):
    """lookup_film with a DynamoDB cache (JSON, to dodge float/Decimal). Cache key
    includes the year so 'Dune' and 'Dune 1984' don't collide."""
    key = f"filmcache#v2#{title.strip().lower()}#{year or ''}"
    try:
        cached = ddb_get(key, "ref")
        if cached and cached.get("json"):
            return json.loads(cached["json"])
    except Exception as e:
        log.warning("film cache read failed: %s", e)
    info = lookup_film(title, year)
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
def add_to_library(chat_id, user_id, title, year=None):
    info = lookup_film_cached(title, year)
    slug = info.get("slug") or _slugify(title)
    name = info.get("title") or title
    item = {
        "PK": _pk("movie", chat_id), "SK": f"lib#{user_id}#{slug}",
        "slug": slug, "owner_id": int(user_id), "title": name,
        "year": str(info.get("year") or year or ""),
        "tmdb_id": info.get("tmdb_id"),
        "runtime_min": info.get("runtime_min"),
        "genres": info.get("genres") or [],
        "description": info.get("description") or "",
        # rating stored as a string (DDB resource rejects float); may be None
        "rating": (str(info["rating"]) if info.get("rating") is not None else None),
        "rating_scale": info.get("rating_scale"),
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
# Reconcile what people type / their @username with the seeded first names, so
# "I'm Dasha" (seeded as Dasha but stored elsewhere as Daria) and username-only
# accounts still find their library. Extend as new aliases turn up.
_SEED_ALIASES = {
    "daria": "dasha", "dariuozy": "dasha", "dash": "dasha",
    "al": "alberto", "berto": "alberto",
    "anya": "anya", "anna": "anya",
    "asa": "asa", "asafoxcolorist": "asa",
    "khimka": "khimka", "maryna": "maryna", "marina": "maryna",
    "chad": "chad", "chad_gracia": "chad",
}


def _seed_owner(name):
    return f"seed:{name.strip().lower()}"


def _canonical_seed_name(name):
    n = name.strip().lower().lstrip("@")
    return _SEED_ALIASES.get(n, n)


def list_seed_names(chat_id):
    names = set()
    for i in ddb_query(_pk("movie", chat_id)):
        sk = str(i.get("SK", ""))
        if sk.startswith("lib#seed:"):
            names.add(sk.split("#", 2)[1].split(":", 1)[1])
    return sorted(names)


def claim_library(chat_id, name, user_id):
    """Reassign the seeded 'name' library to user_id. Idempotent; one claimer.
    The name is normalised through the alias map first (libraries are keyed by
    Telegram user_id once claimed)."""
    key = _canonical_seed_name(name)
    marker_sk = f"seedclaim#{key}"
    marker = ddb_get(_pk("movie", chat_id), marker_sk)
    if marker and str(marker.get("claimed_by")) != str(user_id):
        return {"status": "taken", "by": marker.get("claimed_by")}
    seed_prefix = f"lib#seed:{key}#"
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


def _canonical_for_user(chat_id, uid):
    """The display name a user_id owns (via a claim), else their member name."""
    for i in ddb_query(_pk("movie", chat_id)):
        if (str(i.get("SK", "")).startswith("seedclaim#")
                and str(i.get("claimed_by")) == str(uid)):
            return i.get("seed_name") or str(i["SK"]).split("#", 1)[1].title()
    m = get_member(chat_id, uid)
    return (m or {}).get("display_name")


def resolve_owner(chat_id, identifier):
    """The ONE resolver every read path uses. Maps a Telegram user_id or a
    name/handle to (owner_key, canonical_name), or (None, None) if unknown.
    owner_key feeds get_library — a real user_id once claimed, else 'seed:<key>'.
    It NEVER falls back to the caller (that was the mis-attribution bug)."""
    if identifier is None or identifier == "":
        return None, None
    uid = None
    if isinstance(identifier, int):
        uid = identifier
    elif isinstance(identifier, str) and identifier.lstrip("-").isdigit():
        uid = int(identifier)
    if uid is not None:                       # a Telegram user_id owns its own films
        return str(uid), _canonical_for_user(chat_id, uid)
    pk = _pk("movie", chat_id)
    key = _canonical_seed_name(str(identifier))   # name/@handle -> canonical slug (alias map)
    marker = ddb_get(pk, f"seedclaim#{key}")
    disp = ((marker or {}).get("seed_name")
            or next((k for k in _STARTER_LIBRARIES if k.lower() == key), None)
            or key.title())
    if marker and marker.get("claimed_by") is not None:    # claimed -> their user_id
        return str(marker["claimed_by"]), disp
    if any(str(i.get("SK", "")).startswith(f"lib#seed:{key}#") for i in ddb_query(pk)):
        return f"seed:{key}", disp                          # seeded but unclaimed
    return None, None                                       # genuinely unknown


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
                     "tmdb_id": None, "runtime_min": None, "genres": [],
                     "description": "", "rating": None, "rating_scale": None,
                     "rt_rating": None, "added_at": _now_iso(), "watched": False})
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


def _iso_to_epoch(iso):
    """Parse a stored ISO timestamp to epoch seconds; 0 if missing/unparseable."""
    if not iso:
        return 0
    try:
        return int(datetime.fromisoformat(iso).timestamp())
    except (ValueError, TypeError):
        return 0


def get_game(chat_id):
    return ddb_get(_pk("movie", chat_id), "game#current")


def put_game(game):
    game["last_activity_at"] = _now_iso()   # bump on every persisted interaction
    ddb_put(game)
    return game


def clear_game(chat_id):
    ddb_delete(_pk("movie", chat_id), "game#current")


# ---- short-term conversation window --------------------------------------- #
# A rolling per-chat transcript so the LLM can resolve referents across messages
# ("is IT the highest rated" / "the poll you just did"). It holds recent human
# turns AND the bot's own salient outputs (poll posted, winner, wildcard pitched),
# each tagged with a speaker name (it's a group — the model must know who said
# what). Trimmed by count and age to cap token cost.
_CONVO_MAX_TURNS = 20
_CONVO_MAX_AGE_SEC = 3 * 3600
_CONVO_TEXT_CAP = 600          # clip any single turn so one paste can't blow the budget


def _convo_load(chat_id):
    rec = ddb_get(_pk("movie", chat_id), "convo#log") or {}
    return rec.get("turns") or []


def _convo_append(chat_id, role, speaker, text):
    """Append one turn and persist, trimmed to the last N turns / last few hours."""
    text = (text or "").strip()
    if not text:
        return
    now = _now_epoch()
    turns = _convo_load(chat_id)
    turns.append({"role": role, "speaker": speaker or "",
                  "text": text[:_CONVO_TEXT_CAP], "ts": now})
    turns = [t for t in turns if now - int(t.get("ts") or 0) <= _CONVO_MAX_AGE_SEC]
    turns = turns[-_CONVO_MAX_TURNS:]
    ddb_put({"PK": _pk("movie", chat_id), "SK": "convo#log",
             "turns": turns, "updated_at": _now_iso()})


def _convo_note(chat_id, text):
    """Record one of the bot's own salient actions (assistant turn) so later
    references like 'the poll you just did' / 'the film you suggested' resolve."""
    _convo_append(chat_id, "assistant", "SirWatchAlot", text)


def _convo_messages(turns):
    """Turn the stored window into Bedrock messages, prefixing human turns with the
    speaker's name and collapsing consecutive same-role turns so roles strictly
    alternate (Anthropic models reject consecutive user/assistant messages)."""
    msgs = []
    for t in turns:
        role = "assistant" if t.get("role") == "assistant" else "user"
        text = t.get("text", "")
        if role == "user":
            text = f"{t.get('speaker') or 'Someone'}: {text}"
        if msgs and msgs[-1]["role"] == role:
            msgs[-1]["content"][0]["text"] += "\n" + text
        else:
            msgs.append({"role": role, "content": [{"text": text}]})
    return msgs


# ---- past-picks history (queryable) --------------------------------------- #
def get_history(chat_id, limit=10):
    """Past movie-night winners for this chat, most recent first: title, year, date,
    who played, and the pool that night. Backs the get_history tool + the wildcard
    novelty filter (never re-suggest a past winner)."""
    rows = [h for h in ddb_query(_pk("movie", chat_id))
            if str(h.get("SK", "")).startswith("history#")]
    rows.sort(key=lambda h: h.get("watched_date") or "", reverse=True)
    out = []
    for h in rows[:max(1, int(limit or 10))]:
        out.append({"title": h.get("winner_title"),
                    "year": h.get("winner_year") or "",
                    "date": (h.get("watched_date") or "")[:10],
                    "participants": h.get("participants") or [],
                    "pool": [e.get("title") for e in (h.get("pool") or []) if e.get("title")]})
    return out


def _past_winner_slugs(chat_id):
    return {h.get("winner_slug") for h in ddb_query(_pk("movie", chat_id))
            if str(h.get("SK", "")).startswith("history#") and h.get("winner_slug")}



# A game is "ongoing" only while non-terminal AND fresh. Status flows
# collecting -> confirming -> picking -> done (+ abandoned). A started-but-never-
# finished game must never wedge the group: a new Kyiv calendar day, or ~6h idle,
# auto-abandons it so the next "let's play" starts clean.
_TERMINAL_STATUS = {"done", "abandoned"}
_IDLE_ABANDON_SEC = 6 * 3600
try:
    from zoneinfo import ZoneInfo
    _KYIV = ZoneInfo("Europe/Kyiv")            # same tz as the morning poll
except Exception:                              # no tzdata on the runtime — approx
    _KYIV = timezone(timedelta(hours=2))


def _is_stale(last_iso):
    """True if the timestamp is on an earlier Kyiv day, or > ~6h ago."""
    if not last_iso:
        return True
    try:
        last = datetime.fromisoformat(last_iso)
    except ValueError:
        return True
    now = datetime.now(timezone.utc)
    if (now - last).total_seconds() > _IDLE_ABANDON_SEC:
        return True
    try:
        return last.astimezone(_KYIV).date() < now.astimezone(_KYIV).date()
    except Exception:
        return False


def _abandon_game(chat_id, game):
    if not game:
        return
    game["status"] = "abandoned"
    ddb_put(game)            # record the terminal status, then free the slot
    clear_game(chat_id)


def _game_is_ongoing(chat_id, game):
    """Non-stale, non-terminal game? Auto-abandons (and clears) a stale one."""
    if not game or game.get("status") in _TERMINAL_STATUS:
        return False
    if _is_stale(game.get("last_activity_at") or game.get("started_at")
                 or game.get("created_at")):
        _abandon_game(chat_id, game)
        return False
    return True


def _empty_filter():
    return {"exclude_genres": [], "include_genres": [], "max_runtime_min": None,
            "min_runtime_min": None, "min_year": None, "max_year": None}


def new_game(chat_id, initiator_id):
    now = _now_iso()
    return {
        "PK": _pk("movie", chat_id), "SK": "game#current",
        "session_id": str(uuid.uuid4()), "phase": "JOINING",
        "status": "collecting",          # lifecycle: collecting/confirming/picking/done/abandoned
        "players": [], "initiator": int(initiator_id) if initiator_id else None,
        "vetoes_remaining": {},
        "join_message_id": None,
        "soft_prompted": False,          # have we already nudged "there's a game going"?
        "selection": {},   # {uid: {slots:[{slug,title,state}], shown:[slug], locked}}
        "cards": {},       # {message_id: {uid, slot}}
        "filter": _empty_filter(),   # Phase 1.5 constraints (AND across people)
        # Phase 1.5 constraint window + per-player turn: deadlines only (no poll); the
        # tick sweep / lazy backstop advance whatever the game is waiting on.
        "constraints_open": False, "constraints_deadline": None,
        "turn_deadline": None,
        "awaiting_relax": False,
        "relax_deadline": None,
        "pool_all": [],    # full locked pool (kept for relax re-filtering)
        "pool": [],        # [{owner, slug, title}] eligible after the filter
        "current": None,   # {film, poll_id, poll_message_id, presented_at, resolved}
        # wildcard "one for the hat" — offered once after selections lock (Phase 3)
        "wildcard_offered": False, "wildcard_open": False, "wildcard_deadline": None,
        "wildcard_msg_id": None, "wildcard": None,
        "created_at": now, "started_at": now, "last_activity_at": now,
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
def _fmt_runtime(m):
    if not m:
        return None
    h, mm = divmod(int(m), 60)
    return f"{h}h {mm}m" if h else f"{mm}m"


def _item_rating_phrase(item):
    """Rating(s) we have: '★ 7.8/10' (+ ' · 83% RT'); empty string if none."""
    parts = []
    r = item.get("rating")
    if r not in (None, "", "None"):
        scale = item.get("rating_scale")
        parts.append(f"★ {r}/{scale}" if scale else f"★ {r}")
    rt = item.get("rt_rating")
    if rt not in (None, "", "N/A"):
        parts.append(f"{rt} RT")
    return " · ".join(parts)


def _film_card(item):
    """The factual one-liner: 'Title (year) · Genre · 2h 50m · ★ 7.8/10'.
    Exact, from metadata only — NO synopsis (those truncate mid-sentence; any
    prose colour is written by the model, e.g. _film_blurb / winner_note)."""
    if not item:
        return "(film)"
    yr = f" ({item['year']})" if item.get("year") else ""
    bits = [f"🎬 {item['title']}{yr}"]
    if item.get("genres"):
        bits.append(", ".join(item["genres"][:2]))
    rt = _fmt_runtime(item.get("runtime_min"))
    if rt:
        bits.append(rt)
    rp = _item_rating_phrase(item)
    if rp:
        bits.append(rp)
    return " · ".join(bits)


def _film_logline(item):
    """One short, plain 'what it's about' line, grounded in the known synopsis.
    No poetry, no opinion, no spoilers. For wildcard + winner cards."""
    if not AI_ENABLED or not item:  return ""
    title = item.get("title") or ""
    if not title:  return ""
    syn = (item.get("description") or "").strip()
    ystr = f" ({item['year']})" if item.get("year") else ""
    resp = _bedrock.converse(
        modelId=BEDROCK_MODEL_ID,
        system=[{"text": (
            "Write ONE short, plain-language sentence (max ~20 words) saying what a film "
            "is ABOUT — enough to decide whether to watch. E.g. 'A farm boy is swept into a "
            "galactic rebellion to rescue a princess.' No poetry, no opinion, no spoilers, "
            "no title prefix, no 'why it matters' — just the premise. Plain text, one line.")}],
        messages=[{"role": "user", "content": [{"text":
            f'Film: "{title}"{ystr}.' + (f' Synopsis to compress: {syn}' if syn else '')
            + ' Write the logline.'}]}],
        inferenceConfig={"maxTokens": 80, "temperature": 0.4})
    return "".join(b.get("text","") for b in resp["output"]["message"]["content"]).strip()


def _film_decider(item):
    """Veto round: a plain logline + one factual context note, NO poetry. One call,
    two lines back. Context fact is guarded against confabulation."""
    if not AI_ENABLED or not item:  return ""
    title = item.get("title") or ""
    if not title:  return ""
    syn = (item.get("description") or "").strip()
    ystr = f" ({item['year']})" if item.get("year") else ""
    resp = _bedrock.converse(
        modelId=BEDROCK_MODEL_ID,
        system=[{"text": (
            "Help a group decide whether to veto a film. Output EXACTLY two short plain-text "
            "lines, no labels, no poetry, no opinion:\n"
            "Line 1 — what it's ABOUT: one ~20-word premise, no spoilers, no title prefix.\n"
            "Line 2 — one factual context note, e.g. 'The debut feature of director X.' / "
            "'Won the Academy Award for Film Editing.' / 'Features Ukrainian-born actor Y.' "
            "State ONLY what you are confident is TRUE. If unsure of a specific award or "
            "credit, give a safe general note (director, country, or era) instead of "
            "inventing one. Never guess specifics.")}],
        messages=[{"role": "user", "content": [{"text":
            f'Film: "{title}"{ystr}.' + (f' Synopsis: {syn}' if syn else '')
            + ' Write the two lines.'}]}],
        inferenceConfig={"maxTokens": 120, "temperature": 0.3})
    return "".join(b.get("text","") for b in resp["output"]["message"]["content"]).strip()


def _film_blurb(item):
    """One short, spoiler-free, in-voice line about a film for a candidate card.
    LLM writes the prose; it picks nothing. Empty string when AI is off or it fails,
    so callers degrade to just the factual card."""
    if not AI_ENABLED or not item:
        return ""
    title = item.get("title") or ""
    if not title:
        return ""
    ystr = f" ({item['year']})" if item.get("year") else ""
    try:
        resp = _bedrock.converse(
            modelId=BEDROCK_MODEL_ID,
            system=[{"text": (
                "You are SirWatchAlot offering tonight's candidate to a friends' film-night "
                "group. Write ONE short, spoiler-free sentence that reaches for feeling and "
                "a single concrete image — where the light falls, a face, a gesture, the "
                "texture of the place — not plot and not why it's Important. Gentle, "
                "unhurried, a little awed. Plain text only, NO markdown or asterisks (your "
                "Telegram leaks them); emoji are fine. No title/year/rating restatement; "
                "just the hook. Never sign or quote a critic — the words are your own. "
                "One sentence."
            )}],
            messages=[{"role": "user", "content": [
                {"text": f'The film is "{title}"{ystr}. Write the one-sentence hook.'}]}],
            inferenceConfig={"maxTokens": 120, "temperature": 0.7},
        )
        return "".join(b.get("text", "") for b in resp["output"]["message"]["content"]).strip()
    except Exception as e:
        log.warning("film blurb failed: %s", e)
        return ""


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
def start_game(mode, chat_id, initiator_id, force_new=False):
    game = get_game(chat_id)
    if _game_is_ongoing(chat_id, game):
        # A real, same-day game is live (stale ones were just auto-abandoned).
        if not force_new and not game.get("soft_prompted"):
            game["soft_prompted"] = True
            put_game(game)
            send_message(mode, chat_id,
                         "🎬 There's a movie night going — tap 🎬 Join on the card above, "
                         "or say \"start a new game\" to scrap it and begin fresh.")
            return
        # force_new, or they've pushed back after the one nudge -> end it and restart.
        _abandon_game(chat_id, game)
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
        _begin_constraints(mode, chat_id, game)
        return
    if data in ("sp_play", "sp_add") and game.get("phase") == "SELECTING":
        _handle_short_pool_callback(mode, chat_id, game, uid, data)
        return


# ---- constraints (Phase 1.5, optional) ------------------------------------ #
# After the roster settles we open a fixed 60s window and collect constraints from
# anyone (free text, merged with AND — never overwritten). The window is just a chat
# message + a deadline: the rate(1 min) tick sweep (run_tick) locks it on the clock even
# if the chat goes silent, and any incoming message past the deadline closes it sooner
# (the lazy backstop). There is NO early close and NO timer poll — the full minute runs
# so everyone gets a turn, and players just type constraints in chat.
_CONSTRAINTS_PARSE_SYSTEM = (
    "Parse ONE chat message into movie-night filter constraints. Output ONLY a JSON "
    "object, including just the keys actually mentioned, from: exclude_genres (list of "
    "lowercase genre names), include_genres (list, for 'only X'), max_runtime_min (int), "
    "min_runtime_min (int), min_year (int), max_year (int). Examples: 'no documentaries' "
    "-> {\"exclude_genres\":[\"documentary\"]}; 'no horror' -> {\"exclude_genres\":[\"horror\"]}; "
    "'only westerns' -> {\"include_genres\":[\"western\"]}; 'under 2.5 hours' -> "
    "{\"max_runtime_min\":150}; 'at least 90 minutes' -> {\"min_runtime_min\":90}; "
    "'nothing earlier than 1960' -> {\"min_year\":1960}; 'made after 2000' -> "
    "{\"min_year\":2001}; 'something from the 90s' -> {\"min_year\":1990,\"max_year\":1999}. "
    "If the message states no constraint, output {}."
)


def _begin_constraints(mode, chat_id, game):
    game["phase"] = "CONSTRAINTS"
    game["filter"] = _empty_filter()
    game["constraints_open"] = True
    game["constraints_deadline"] = _now_epoch() + _CONSTRAINTS_WINDOW
    put_game(game)
    # No poll — just chat. The tick sweep / lazy backstop lock it on the deadline.
    send_message(mode, chat_id,
                 "🎛 Any constraints tonight? Length, genre, or year range — anyone can "
                 "chime in. Auto-locks in ~1 min if no reply.")


def parse_constraint_text(text):
    """LLM parses a free-text reply into the filter schema. Code merges/applies.
    A bare year range ('1965-1980', '1920-1900', '1990 to 1999') is parsed
    deterministically here (reversed ranges are swapped) — the LLM is unreliable on those."""
    delta = {}
    if AI_ENABLED:
        try:
            resp = _bedrock.converse(
                modelId=BEDROCK_MODEL_ID,
                system=[{"text": _CONSTRAINTS_PARSE_SYSTEM}],
                messages=[{"role": "user", "content": [{"text": text}]}],
                inferenceConfig={"maxTokens": 200, "temperature": 0},
            )
            out = "".join(b.get("text", "") for b in resp["output"]["message"]["content"])
            m = re.search(r"\{.*\}", out, re.DOTALL)
            delta = json.loads(m.group(0)) if m else {}
        except Exception as e:
            log.warning("constraint parse failed: %s", e)
            delta = {}
    rng = re.search(r"\b(1\d{3}|20\d{2})\s*(?:-|–|—|to|through|thru|until)\s*(1\d{3}|20\d{2})\b",
                    text or "", re.I)
    if rng:
        a, b = int(rng.group(1)), int(rng.group(2))
        delta["min_year"], delta["max_year"] = min(a, b), max(a, b)
    return delta


def _merge_filter(f, delta):
    """Combine a parsed delta into the running filter with AND (most restrictive)."""
    for key in ("exclude_genres", "include_genres"):
        for g in delta.get(key) or []:
            g = str(g).strip().lower()
            if g and g not in f[key]:
                f[key].append(g)
    def tighten(key, val, op):
        if val is None:
            return
        f[key] = val if f[key] is None else op(f[key], val)
    tighten("max_runtime_min", delta.get("max_runtime_min"), min)
    tighten("min_runtime_min", delta.get("min_runtime_min"), max)
    tighten("min_year", delta.get("min_year"), max)
    tighten("max_year", delta.get("max_year"), min)
    return f


def _filter_active(f):
    return bool(f and (f["exclude_genres"] or f["include_genres"]
                       or f["max_runtime_min"] or f["min_runtime_min"]
                       or f["min_year"] or f["max_year"]))


def _describe_filter(f):
    bits = []
    if f["exclude_genres"]:
        bits.append("no " + "/".join(f["exclude_genres"]))
    if f["include_genres"]:
        bits.append("only " + "/".join(f["include_genres"]))
    if f["max_runtime_min"]:
        bits.append(f"≤{f['max_runtime_min']} min")
    if f["min_runtime_min"]:
        bits.append(f"≥{f['min_runtime_min']} min")
    if f["min_year"]:
        bits.append(f"≥{f['min_year']}")
    if f["max_year"]:
        bits.append(f"≤{f['max_year']}")
    return ", ".join(bits)


def _looks_like_unsupported_constraint(text):
    """True if the message tries to narrow the night by something we can't enforce
    (director, language, cast, mood…) rather than year/length/genre."""
    if not AI_ENABLED or not (text or "").strip():
        return False
    resp = _bedrock.converse(
        modelId=BEDROCK_MODEL_ID,
        system=[{"text": (
            "A group is setting filters for movie night. We can ONLY filter by release "
            "year, length, and genre. Does this message ask to narrow the films by "
            "something OTHER than those three (e.g. director, language, country, cast, "
            "mood/tone)? Answer with ONLY 'yes' or 'no'.")}],
        messages=[{"role": "user", "content": [{"text": text}]}],
        inferenceConfig={"maxTokens": 3, "temperature": 0})
    return "".join(b.get("text","") for b in resp["output"]["message"]["content"]).strip().lower().startswith("y")


def _handle_constraints_message(mode, chat_id, game, text):
    """A message during the open constraints window: accumulate it into the filter and
    acknowledge. There is NO early close — the full minute always runs so everyone gets
    a turn; the window only closes on the timed sweep (or the post-deadline backstop)."""
    delta = parse_constraint_text(text)
    if delta:
        _merge_filter(game["filter"], delta)
        put_game(game)
        send_message(mode, chat_id, f"Got it — {_describe_filter(game['filter'])} "
                                    "(still collecting until the timer ends).")
        return
    # Nothing enforceable parsed. If they tried to filter on something we can't check
    # (director, language, cast, mood…), say so plainly instead of silently ignoring it.
    if _looks_like_unsupported_constraint(text):
        send_message(mode, chat_id,
            "I can only narrow tonight by release year, length, and genre — I can't "
            "reliably filter by director, language, or cast. Send me one of those and "
            "I'll tighten the list.")
    # otherwise it's just chatter — leave the window open until it times out


def _close_constraints(mode, chat_id, game, announce):
    if not game.get("constraints_open"):
        return                            # already closed (tick + lazy backstop can race)
    game["constraints_open"] = False
    if announce:
        if _filter_active(game["filter"]):
            send_message(mode, chat_id, f"🎛 Constraints locked: {_describe_filter(game['filter'])}.")
        else:
            send_message(mode, chat_id, "🎬 No constraints tonight — let's go.")
    _begin_selection(mode, chat_id, game)


def _constraints_backstop(mode, chat_id, game):
    """Close the constraint window once past its deadline. Called both by the tick sweep
    (fires on the clock even in a silent chat) and lazily on any incoming message (faster
    when the chat is active). Returns True if it closed the window."""
    if game and game.get("phase") == "CONSTRAINTS" and game.get("constraints_open"):
        if _now_epoch() >= (game.get("constraints_deadline") or 0):
            _close_constraints(mode, chat_id, game, announce=True)
            return True
    return False


# ---- selection ------------------------------------------------------------ #
_KEEP_TOKENS = {"👍", "✅", "👌", "y", "yes", "keep", "ok"}
_SWAP_TOKENS = {"👎", "❌", "n", "no", "swap", "drop"}


def _parse_confirm_tokens(text):
    """'👍 👎 👍' / '👍👎👍' / 'y n y' -> ([True,False,True], keep_all). keep_all is
    True for a single affirmative ('👍'/'yes'), meaning keep everything. Unknown
    tokens are ignored; parsing is deterministic (code, not the LLM)."""
    t = (text or "").strip().lower()
    parts = t.split()
    if len(parts) <= 1 and t and not t.isascii():
        parts = list(t)            # a run of emoji with no spaces, e.g. 👍👎👍
    toks = []
    for p in parts:
        p = p.strip(".,!")
        if p in _KEEP_TOKENS:
            toks.append(True)
        elif p in _SWAP_TOKENS:
            toks.append(False)
    keep_all = len(toks) == 1 and toks[0] is True
    return toks, keep_all


def _begin_selection(mode, chat_id, game):
    game["phase"] = "SELECTING"
    game["status"] = "confirming"
    game["selection"] = {}
    game["sel_order"] = [int(p) for p in game["players"]]
    game["sel_idx"] = 0
    game["sel_msg_id"] = None
    put_game(game)
    send_message(mode, chat_id,
                 "🎬 Building tonight's slate — one person at a time. I'll show each of "
                 "you three films from your library to keep or swap.")
    _ask_player(mode, chat_id, game)


def _enrich_item(chat_id, item):
    """Fill missing genres/runtime/rating from the resolver and persist, so the
    constraint filter and the cards have real metadata. Best-effort: on failure
    the item is left as-is (and kept)."""
    # Library items added before the Letterboxd fix kept a TMDB /10 rating; refresh
    # to the Letterboxd /5 once we have a tmdb_id so cards show the right score.
    if item.get("rating_scale") != 5 and item.get("tmdb_id"):
        lb = _letterboxd_rating(item["tmdb_id"])
        if lb is not None:
            item["rating"], item["rating_scale"] = str(lb), 5
            try:
                ddb_put(item)
            except Exception as e:
                log.warning("lb rating refresh persist failed: %s", e)
    if (item.get("genres") and item.get("runtime_min") is not None
            and item.get("rating") is not None):
        return item
    yr = (str(item.get("year") or "")[:4]) or None
    try:
        info = lookup_film_cached(item["title"], yr)
    except Exception as e:
        log.warning("enrich failed for %s: %s", item.get("title"), e)
        return item
    changed = False
    if not item.get("genres") and info.get("genres"):
        item["genres"] = info["genres"]; changed = True
    if item.get("runtime_min") is None and info.get("runtime_min") is not None:
        item["runtime_min"] = info["runtime_min"]; changed = True
    if not item.get("rating") and info.get("rating") is not None:
        item["rating"] = str(info["rating"]); item["rating_scale"] = info.get("rating_scale"); changed = True
    if not item.get("rt_rating") and info.get("rt_rating"):
        item["rt_rating"] = info["rt_rating"]; changed = True
    if item.get("tmdb_id") is None and info.get("tmdb_id") is not None:
        item["tmdb_id"] = info["tmdb_id"]; changed = True
    if not item.get("description") and info.get("description"):
        item["description"] = info["description"]; changed = True
    if changed:
        try:
            ddb_put(item)
        except Exception as e:
            log.warning("persist enriched item failed: %s", e)
    return item


def _draw_eligible(chat_id, uid, game, n, exclude_slugs):
    """Up to n unwatched films from uid's library that pass the game filter.
    Genre/runtime are resolved (enriched) BEFORE the draw so the filter has real
    data — this is what makes constraints actually gate the pick. Draws in random
    order, stops once n eligible are found; the chosen are enriched for the card."""
    f = game.get("filter") or _empty_filter()
    active = _filter_active(f)
    lib = [x for x in get_library(chat_id, uid)
           if not x.get("watched") and x["slug"] not in exclude_slugs]
    random.shuffle(lib)
    chosen = []
    for item in lib:
        if active and (not item.get("genres") or item.get("runtime_min") is None):
            item = _enrich_item(chat_id, item)
        if not active or _passes_filter(item, f):
            chosen.append(item)
            if len(chosen) >= n:
                break
    for it in chosen:   # cards need genre/runtime/rating even with no filter
        _enrich_item(chat_id, it)
    return chosen


def _current_selecting_uid(game):
    order = game.get("sel_order") or []
    idx = game.get("sel_idx", 0)
    return order[idx] if idx < len(order) else None


def _filter_reason(item, f):
    """Why a film fails tonight's filter (mirrors _passes_filter order), or None if
    it qualifies. Unknown metadata qualifies (kept), same as _passes_filter."""
    genres = [g.lower() for g in (item.get("genres") or [])]
    rt = item.get("runtime_min")
    try:
        year = int(str(item.get("year") or "")[:4])
    except ValueError:
        year = None
    if f["min_year"] is not None and year is not None and year < f["min_year"]:
        return f"it's from {year}, before tonight's {f['min_year']} cutoff"
    if f["max_year"] is not None and year is not None and year > f["max_year"]:
        return f"it's from {year}, after tonight's {f['max_year']} cutoff"
    if f["exclude_genres"] and genres:
        bad = next((g for g in genres if g in f["exclude_genres"]), None)
        if bad:
            return f"it's {bad}, and tonight excludes {'/'.join(f['exclude_genres'])}"
    if f["include_genres"] and genres and not any(g in f["include_genres"] for g in genres):
        return f"tonight is {'/'.join(f['include_genres'])} only"
    if f["max_runtime_min"] is not None and rt is not None and rt > f["max_runtime_min"]:
        return f"it's ~{rt} min, over tonight's {f['max_runtime_min']}-min limit"
    if f["min_runtime_min"] is not None and rt is not None and rt < f["min_runtime_min"]:
        return f"it's ~{rt} min, under tonight's {f['min_runtime_min']}-min minimum"
    return None


def _short_pool_keyboard(n):
    play = f"▶️ Play with these ({n})" if n else "🙅 Sit this round out"
    return {"inline_keyboard": [[{"text": play, "callback_data": "sp_play"}],
                                [{"text": "➕ Add a film", "callback_data": "sp_add"}]]}


def _swap_deadend_keyboard(n):
    """Escape from a 👎 with no filter-fitting replacement: lock the films they DID
    approve, or add one. Reuses the sp_play / sp_add callbacks — never a forced keep."""
    go = f"✅ Go with the {n} I approved" if n else "🙅 Sit this round out (keep your veto)"
    return {"inline_keyboard": [[{"text": go, "callback_data": "sp_play"}],
                                [{"text": "➕ Add a film", "callback_data": "sp_add"}]]}


def _post_short_pool(mode, chat_id, game, uid):
    """Tell the player the filter trimmed them below 3 and offer buttons. Buttons,
    not emoji, to avoid the parse fragility (mentions / skin-tone modifiers)."""
    sel = game["selection"][str(uid)]
    sel["awaiting_add"] = False          # require a fresh "Add a film" tap to re-arm
    n = len(sel["slots"])
    who = mention_for(chat_id, uid)
    desc = _describe_filter(game["filter"]) or "tonight's filter"
    if n:
        lst = ", ".join(f"{i + 1}) {s['title']}" for i, s in enumerate(sel["slots"]))
        text = (f"{who} — tonight's filter is {desc}, and only these qualify from your "
                f"library: {lst}. Play with these, or add another that fits?")
    else:
        text = (f"{who} — nothing in your library fits tonight's filter ({desc}). "
                "Add one that fits, or sit this round out (you keep your veto).")
    resp = send_message(mode, chat_id, text, reply_markup=_short_pool_keyboard(n))
    game["sel_msg_id"] = (resp.get("result") or {}).get("message_id")
    _set_turn_deadline(game)
    put_game(game)


def _post_swap_deadend(mode, chat_id, game, uid):
    """A swap was asked for but nothing in their library fits tonight's filter to swap
    in. Don't force a keep: offer the films they approved as-is, or add one. Buttons
    (sp_play / sp_add), same pattern as the short-pool case."""
    sel = game["selection"][str(uid)]
    sel["awaiting_add"] = False           # require a fresh "Add a film" tap to re-arm
    n = len(sel["slots"])
    who = mention_for(chat_id, uid)
    desc = _describe_filter(game["filter"]) or "tonight's filter"
    if n:
        lst = ", ".join(f"{i + 1}) {s['title']}" for i, s in enumerate(sel["slots"]))
        text = (f"{who} — nothing left in your library fits tonight's filter ({desc}) to "
                f"swap in. Go with the {n} you approved ({lst}), or add one that fits?")
    else:
        text = (f"{who} — you passed on all of them and nothing else in your library fits "
                f"tonight's filter ({desc}). Add one that fits, or sit this round out "
                "(you keep your veto).")
    resp = send_message(mode, chat_id, text, reply_markup=_swap_deadend_keyboard(n))
    game["sel_msg_id"] = (resp.get("result") or {}).get("message_id")
    _set_turn_deadline(game)
    put_game(game)


def _handle_short_pool_callback(mode, chat_id, game, uid, data):
    if int(uid) != int(_current_selecting_uid(game) or -1):
        return                            # not this player's turn
    sel = game["selection"].get(str(uid))
    if not sel or sel.get("locked"):
        return
    who = mention_for(chat_id, uid)
    if data == "sp_play":
        sel["locked"] = True
        sel["awaiting_add"] = False
        send_message(mode, chat_id,
                     f"Locked in {who}'s picks ✅" if sel["slots"]
                     else f"{who} is sitting this one out — veto still counts.")
        put_game(game)
        _advance_player(mode, chat_id, game)
    elif data == "sp_add":
        sel["awaiting_add"] = True
        desc = _describe_filter(game["filter"]) or "tonight's filter"
        send_message(mode, chat_id, f"{who}, send a film title that fits {desc}.")
        _set_turn_deadline(game)   # still their turn while they type
        put_game(game)


def _handle_short_pool_add(mode, chat_id, game, uid, text):
    """A title typed during the add loop: resolve, add to the LIBRARY for real,
    then gate tonight's eligibility on the filter (deterministic) and explain."""
    sel = game["selection"].get(str(uid))
    if not sel:
        return
    title = _extract_title(text)
    if not title:
        send_message(mode, chat_id, "That doesn't look like a film title — just type the title (e.g. The Hand of God), or tap Play with these.")
        return
    f = game.get("filter") or {}
    ident, iyear = _identify_film(title, f.get("min_year"), f.get("max_year"))
    info = lookup_film_cached(ident or title, iyear)
    if not info.get("found") and ident and ident != title:
        info = lookup_film_cached(title)          # last resort: the literal text
    if not info.get("found"):
        send_message(mode, chat_id, f"Couldn't find “{title}” — try another title, "
                                    "or tap Play with these.")
        return
    slug = info.get("slug") or _slugify(info.get("title") or title)
    existed = get_film(chat_id, uid, slug) is not None
    item, _ = add_to_library(chat_id, uid, info["title"], info.get("year"))   # real entry
    name, who = item["title"], mention_for(chat_id, uid)
    reason = _filter_reason(item, game["filter"])
    if reason is None:
        if not any(s["slug"] == slug for s in sel["slots"]):
            sel["slots"].append({"slug": slug, "title": name})
        if slug not in sel["shown"]:
            sel["shown"].append(slug)
        verb = "already had" if existed else "added"
        if len(sel["slots"]) >= 3:
            sel["locked"] = True
            sel["awaiting_add"] = False
            put_game(game)
            send_message(mode, chat_id, f"{verb} “{name}” — that fits ✅. {who}'s three are set!")
            _advance_player(mode, chat_id, game)
            return
        send_message(mode, chat_id, f"{verb} “{name}” — that fits ✅.")
        _post_short_pool(mode, chat_id, game, uid)
    else:
        send_message(mode, chat_id, f"Added “{name}” to your library, but {reason} — "
                                    "it can't play tonight.")
        _post_short_pool(mode, chat_id, game, uid)


def _ask_player(mode, chat_id, game):
    """Ask ONE player (the current one) to keep/swap their 3 drawn films in a
    single message; they reply with three emojis in order. If the filter trims
    them below 3, offer the short-pool buttons instead of a silent short deal."""
    uid = _current_selecting_uid(game)
    if uid is None:
        _offer_wildcard(mode, chat_id, game)   # Phase 3 done -> offer one for the hat
        return
    picks = _draw_eligible(chat_id, uid, game, 3, set())
    sel = {"slots": [{"slug": f["slug"], "title": f["title"]} for f in picks],
           "shown": [f["slug"] for f in picks], "locked": False, "awaiting_add": False}
    game["selection"][str(uid)] = sel
    if len(picks) >= 3:
        _post_player_slate(mode, chat_id, game, uid)
        put_game(game)
        return
    if _filter_active(game["filter"]):
        # the filter (not a tiny library) trimmed them below 3 — short-pool prompt
        sel["short_pool"] = True
        _post_short_pool(mode, chat_id, game, uid)   # persists the game
        return
    if not picks:
        send_message(mode, chat_id, f"{mention_for(chat_id, uid)} has no films to add — skipping.")
        sel["locked"] = True
        _advance_player(mode, chat_id, game)
        return
    _post_player_slate(mode, chat_id, game, uid)   # no filter, small library: deal what's there
    put_game(game)


def _post_player_slate(mode, chat_id, game, uid, swapped_titles=None):
    sel = game["selection"][str(uid)]
    n = len(sel["slots"])
    lines = []
    for i, slot in enumerate(sel["slots"], 1):
        item = get_film(chat_id, uid, slot["slug"])
        lines.append(f"{i}. {_film_card(item) if item else slot['title']}")
    if swapped_titles:        # re-display after a swap — don't repeat "I picked these…"
        head = (f"🔀 Swapped in {', '.join(swapped_titles)} — "
                f"{mention_for(chat_id, uid)}'s slate now:")
    else:
        head = (f"🎬 {mention_for(chat_id, uid)} — I picked these {n} from your library. "
                "Are these what you want to share with the group tonight, or should we "
                "swap some?")
    text = (head + "\n\n" + "\n\n".join(lines) +
            f"\n\nReply with {n} emoji in order — 👍 keep / 👎 swap (e.g. {'👍' * n}). "
            "Auto-skips in ~1 min if no reply.")
    resp = send_message(mode, chat_id, text)
    game["sel_msg_id"] = (resp.get("result") or {}).get("message_id")
    _set_turn_deadline(game)   # 60s clock; caller persists the game


def _advance_player(mode, chat_id, game):
    game["sel_idx"] = game.get("sel_idx", 0) + 1
    put_game(game)
    _ask_player(mode, chat_id, game)


def _set_turn_deadline(game):
    """Start/refresh the per-player turn clock. No poll — the tick sweep and the lazy
    backstop advance it on the deadline; the player just replies 👍/👎 in chat."""
    game["turn_deadline"] = _now_epoch() + _TURN_WINDOW


def _timeout_player_turn(mode, chat_id, game, uid):
    """The turn clock ran out with no decision: auto-KEEP the films already dealt (veto
    still counts) and move on. Fired by the tick sweep or the lazy backstop, never by the
    awaited player's own input."""
    sel = game["selection"].get(str(uid)) or {}
    sel["locked"] = True
    sel["awaiting_add"] = False
    game["selection"][str(uid)] = sel
    put_game(game)
    kept = len(sel.get("slots") or [])
    who = mention_for(chat_id, uid)
    msg = (f"⏳ {who} didn't reply — keeping their {kept} pick{'' if kept == 1 else 's'} "
           "(veto still counts). Moving on." if kept else
           f"⏳ {who} didn't reply — sitting this round out (veto still counts). Moving on.")
    send_message(mode, chat_id, msg)
    _advance_player(mode, chat_id, game)


def _turn_backstop(mode, chat_id, game):
    """Advance a per-player selection turn past its deadline. Called by the tick sweep
    (fires on the clock even in a silent chat) and lazily on any incoming message.
    Returns True if it advanced the turn."""
    if game and game.get("phase") == "SELECTING":
        cur = _current_selecting_uid(game)
        sel = game["selection"].get(str(cur)) if cur is not None else None
        if (cur is not None and sel and not sel.get("locked")
                and _now_epoch() >= (game.get("turn_deadline") or 0)):
            _timeout_player_turn(mode, chat_id, game, cur)
            return True
    return False


def _handle_selection_reply(mode, chat_id, game, uid, text):
    """The current player's keep/swap reply. All 👍 (or a single 👍) locks them and
    moves to the next person; any 👎 swaps that slot for another eligible film and
    re-asks the same person."""
    sel = game["selection"].get(str(uid))
    if not sel or sel.get("locked"):
        return
    toks, keep_all = _parse_confirm_tokens(text)
    n = len(sel["slots"])
    if keep_all or (toks and len(toks) == n and all(toks)):
        sel["locked"] = True
        put_game(game)
        send_message(mode, chat_id, f"Locked in {mention_for(chat_id, uid)}'s picks ✅")
        _advance_player(mode, chat_id, game)
        return
    if not toks:
        return  # not a keep/swap reply — ignore ambient chatter during this phase
    if len(toks) != n:
        send_message(mode, chat_id,
                     f"Send {n} marks in order — 👍 keep / 👎 swap (e.g. {'👍' * n}).")
        return
    swapped_titles = []
    for i, keep in enumerate(toks):
        if keep:
            continue
        repl = _draw_eligible(chat_id, uid, game, 1, set(sel["shown"]))
        if not repl:
            continue
        nf = repl[0]
        sel["slots"][i] = {"slug": nf["slug"], "title": nf["title"]}
        sel["shown"].append(nf["slug"])
        swapped_titles.append(nf["title"])
    put_game(game)
    if not swapped_titles:
        # Nothing in their library fits to swap in. Don't corner them into 👍-ing the
        # rejected film(s): drop those slots and let them lock what they approved (N),
        # or add a fitting film. Buttons are the way out — never a forced keep.
        sel["slots"] = [s for s, keep in zip(sel["slots"], toks) if keep]
        _post_swap_deadend(mode, chat_id, game, uid)
        return
    _post_player_slate(mode, chat_id, game, uid, swapped_titles=swapped_titles)
    put_game(game)


# ---- veto ----------------------------------------------------------------- #
def _passes_filter(item, f):
    """True unless the film DEFINITIVELY violates a constraint. Unknown metadata
    is kept, not dropped (over-exclusion is the worse failure)."""
    genres = [g.lower() for g in (item.get("genres") or [])]
    rt = item.get("runtime_min")
    try:
        year = int(str(item.get("year") or "")[:4])
    except ValueError:
        year = None
    if f["min_year"] is not None and year is not None and year < f["min_year"]:
        return False
    if f["max_year"] is not None and year is not None and year > f["max_year"]:
        return False
    if f["exclude_genres"] and genres and any(g in f["exclude_genres"] for g in genres):
        return False
    if f["include_genres"] and genres and not any(g in f["include_genres"] for g in genres):
        return False
    if f["max_runtime_min"] is not None and rt is not None and rt > f["max_runtime_min"]:
        return False
    if f["min_runtime_min"] is not None and rt is not None and rt < f["min_runtime_min"]:
        return False
    return True


def _eligible_pool(chat_id, game):
    """Filter pool_all by the game filter. Year is exact from the library; for
    genre/runtime constraints we lazily enrich thin metadata via lookup_film_cached
    (small pool, cached), and keep anything still unknown."""
    f = game["filter"]
    need_meta = bool(f["exclude_genres"] or f["include_genres"]
                     or f["max_runtime_min"] or f["min_runtime_min"])
    eligible, unknown = [], []
    for entry in game.get("pool_all", []):
        item = get_film(chat_id, int(entry["owner"]), entry["slug"])
        if not item:
            continue
        if need_meta and (not item.get("genres") or item.get("runtime_min") is None):
            try:
                info = lookup_film_cached(item["title"])
                changed = False
                if not item.get("genres") and info.get("genres"):
                    item["genres"] = info["genres"]; changed = True
                if item.get("runtime_min") is None and info.get("runtime_min") is not None:
                    item["runtime_min"] = info["runtime_min"]; changed = True
                if changed:
                    ddb_put(item)
            except Exception as e:
                log.warning("enrich for filter failed (%s): %s", entry.get("title"), e)
        if _passes_filter(item, f):
            eligible.append(entry)
            if need_meta and (not item.get("genres") or item.get("runtime_min") is None):
                unknown.append(item["title"])
    return eligible, unknown


def _locked_pool_entries(game):
    """The pool drawn from players' LOCKED selections: [{owner, slug, title}]."""
    out = []
    for uid in game["players"]:
        sel = game["selection"].get(str(uid), {})
        if not sel.get("locked"):
            continue
        for s in sel.get("slots", []):
            out.append({"owner": str(uid), "slug": s["slug"], "title": s["title"]})
    return out


# ---- wildcard "one for the hat" (end of Phase 3, once per game) ------------ #
# After selections lock, the bot offers ONE extra film built ONLY from THIS
# game's participants (all of game["players"], picks or not — never anyone else).
# Permission-gated: a beat to react; any participant 👎 / "no" / "pass" drops it
# silently. Otherwise it joins the pool as a normal candidate owned by sentinel 0
# (the "house" — no real Telegram user is 0, so everyone may veto it) and is
# drawn/vetoed like any other film. Suggestion ladder, strongest first; it
# ALWAYS yields something. Offered for any game with 1+ players.
_WILDCARD_OWNER = 0
_WILDCARD_WINDOW = 90              # same beat as the veto window
# Strong, globally-varied fallback for when the taste-rhyme / blind-spot / almanac
# tiers yield nothing. Tried in order; first not already in tonight's pool and
# never suggested before in this chat.
_WILDCARD_CANON = [
    ("Tokyo Story", 1953), ("Yi Yi", 2000), ("Close-Up", 1990),
    ("A Brighter Summer Day", 1991), ("The Spirit of the Beehive", 1973),
    ("Stalker", 1979), ("Come and See", 1985), ("In the Mood for Love", 2000),
    ("The Battle of Algiers", 1966), ("Black Girl", 1966), ("Wanda", 1970),
    ("Daisies", 1966), ("Touki Bouki", 1973), ("A City of Sadness", 1989),
    ("The Gleaners and I", 2000), ("Memories of Murder", 2003),
]


def _wildcard_log(chat_id):
    rec = ddb_get(_pk("movie", chat_id), "wildcardlog") or {}
    return set(rec.get("slugs") or [])


def _wildcard_remember(chat_id, slug):
    seen = _wildcard_log(chat_id)
    seen.add(slug)
    ddb_put({"PK": _pk("movie", chat_id), "SK": "wildcardlog",
             "slugs": sorted(seen), "updated_at": _now_iso()})


def _player_library_slugs(chat_id, players):
    """Every slug already in ANY current player's library — the wildcard must be a
    film they DON'T have, so these are excluded from every tier of the ladder."""
    owned = set()
    for p in players:
        for f in get_library(chat_id, str(p)):
            if f.get("slug"):
                owned.add(f["slug"])
    return owned


def _film_almanac(date):
    """Today's film-history events -> [{title, year}] for the almanac tier.
    DEFERRED: a stub until the boxofficeprophets / onthisday scrape is wired and
    validated against the live pages (kept isolated + swappable). Returns [] so
    the ladder falls through to a canonical pick — the feature never goes silent."""
    return []


def _wildcard_via_llm(chat_id, players, filter_desc=""):
    """Tier 1: a taste-rhyme pick built from what THESE players LOVE (the films they
    saved / rated highly), with the reason written in the same warm, film-essayist
    voice as the candidate/winner blurbs. The model proposes a film + an in-voice
    reason that NAMES the loved films driving it; code verifies + dedupes. Returns
    {title, year, reason} or None (logging WHY: bedrock error vs empty/unparseable)."""
    if not AI_ENABLED:
        return None
    pset = {str(p) for p in players}
    digest = []
    for p in players:
        name = _canonical_for_user(chat_id, p) or mention_for(chat_id, p)
        lib = [f["title"] for f in get_library(chat_id, str(p))]
        loved = [f"{r['film']} ({r['stars']}★)"
                 for r in get_user_ratings(chat_id, user_id=p)
                 if int(r.get("stars") or 0) >= 4]
        digest.append({"player": name, "library": lib[:60], "loved": loved[:25]})
    won = sorted({h.get("winner_title") for h in ddb_query(_pk("movie", chat_id))
                  if str(h.get("SK", "")).startswith("history#")
                  and set(map(str, h.get("participants") or [])) & pset
                  and h.get("winner_title")})
    try:
        resp = _bedrock.converse(
            modelId=BEDROCK_MODEL_ID,
            system=[{"text": (
                "You are SirWatchAlot, a warm film-essayist host, choosing ONE wildcard film "
                "'for the hat' on movie night — built from what THESE specific players LOVE. "
                "Make the case from what they have SAVED or rated highly; NAME those films. "
                "NEVER argue from absence — that a film 'isn't on someone's shelf' is not a "
                "reason they'd like it; the case must come from love, not from a gap. The pick "
                "MUST be a real, findable film that is NOT in any player's library and not in "
                "the lists provided. Prefer a discovery none of them has. "
                "Return STRICT JSON {\"title\":..., \"year\":..., \"reason\":...}. 'reason' is "
                "1-2 sentences, plain text, NO markdown or asterisks (Telegram leaks them), in "
                "your unhurried, slightly awed voice, naming the actual players and the actual "
                "films they love that lead you to this pick — e.g. \"Chad's love of family "
                "epics — Tokyo Story, Scenes from a Marriage — and Asa's passion for Asian "
                "cinema — Poetry, To Live — bring me to Yi Yi.\" End on the film you are "
                "suggesting. Do not restate year/runtime/rating; just the connection."
                + (f" HARD CONSTRAINT: tonight's filter is {filter_desc}; the pick MUST "
                   "satisfy it (genre, length, and year). Do not suggest anything that "
                   "violates it." if filter_desc else "")
            )}],
            messages=[{"role": "user", "content": [{"text": json.dumps(
                {"players": digest, "already_won_here": won,
                 "constraints": filter_desc or None})}]}],
            inferenceConfig={"maxTokens": 280, "temperature": 0.8},
        )
        raw = "".join(b.get("text", "") for b in resp["output"]["message"]["content"]).strip()
        m = re.search(r"\{.*\}", raw, re.S)
        data = json.loads(m.group(0)) if m else {}
    except Exception as e:
        log.warning("wildcard tier1: bedrock error: %s", e)
        return None
    title = (data.get("title") or "").strip()
    if not title:
        log.warning("wildcard tier1: empty/unparseable LLM reply: %r", raw[:160])
        return None
    return {"title": title, "year": data.get("year"),
            "reason": (data.get("reason") or "").strip(), "tier": "taste"}


def _build_wildcard(chat_id, game):
    """Run the ladder; return a VERIFIED film {title, year, slug, reason, tier} that
    is NOT in tonight's pool, NOT already suggested in this chat, and NOT in ANY
    current player's library. Tier 1 (taste rhyme) is the default whenever there's
    library/rating data and retries once before dropping to canonical; canonical is
    a true last resort. Logs WHY it falls through. None only if all tiers exhaust."""
    players = game["players"]
    pool_slugs = {e["slug"] for e in _locked_pool_entries(game)}
    owned = _player_library_slugs(chat_id, players)
    # novelty filter (all tiers): not in tonight's pool, not already suggested here,
    # not in any player's library, and never a past winner of this chat.
    blocked = pool_slugs | _wildcard_log(chat_id) | owned | _past_winner_slugs(chat_id)
    # tonight's CONSTRAINTS apply to the wildcard too — it rides the same pick/veto
    # path, so an off-filter suggestion is as wrong as an off-filter player pick.
    f = game.get("filter") or _empty_filter()
    filter_on = _filter_active(f)
    filter_desc = _describe_filter(f) if filter_on else ""

    def verify(title, year, reason, tier):
        info = lookup_film_cached(title, year)
        if not info.get("found"):
            return None, "not_found"
        slug = info.get("slug") or _slugify(title)
        if slug in blocked:
            return None, "already_owned_or_suggested"      # in pool / log / a player's library
        if filter_on and not _passes_filter(info, f):
            return None, "off_filter"                       # violates tonight's constraints
        return ({"title": info.get("title") or title, "year": info.get("year") or year,
                 "slug": slug, "reason": reason, "tier": tier}, "ok")

    # Tier 1 — taste rhyme. The default whenever ANY player has library/rating data;
    # a 60-film player must never fall to canonical. Retry once on error/rejection.
    has_data = bool(owned) or any(get_user_ratings(chat_id, user_id=p) for p in players)
    if AI_ENABLED and has_data:
        for attempt in (1, 2):
            cand = _wildcard_via_llm(chat_id, players, filter_desc)
            if not cand:
                log.warning("wildcard tier1 attempt %d/2: no usable candidate", attempt)
                continue
            v, why = verify(cand["title"], cand.get("year"), cand.get("reason"), "taste")
            if v:
                return v
            log.warning("wildcard tier1 attempt %d/2: candidate %r rejected (%s)",
                        attempt, cand.get("title"), why)
        log.warning("wildcard: tier1 exhausted after retry -> falling through")
    elif AI_ENABLED:
        log.info("wildcard: no library/rating data for players -> skipping tier1")

    for ev in _film_almanac(_now_iso()[:10]):       # tier 3 (deferred stub -> [])
        v, _why = verify(ev.get("title", ""), ev.get("year"), None, "almanac")
        if v:
            return v

    for title, year in _WILDCARD_CANON:             # tier 4: last-resort canonical
        v, _why = verify(title, year, None, "canon")
        if v:
            log.warning("wildcard: fell through to CANONICAL pick %r", title)
            return v
    log.error("wildcard: ladder fully exhausted — nothing to offer")
    return None


def _post_wildcard_pitch(mode, chat_id, item, sugg):
    """The pitch, in SirWatchAlot's voice. Structure: header -> the REASON (from what
    the players love, naming films) -> the film + card -> one in-voice line on why the
    film itself matters (same voice as the candidate blurb) -> the consent question."""
    blurb = _film_blurb(item)                       # one Cousins-voice line on the film
    parts = []
    if sugg.get("tier") == "taste" and sugg.get("reason"):
        parts.append("My Pick for the Night")
        parts.append(sugg["reason"])                # case from love, never from absence
    else:                                           # canonical last resort — no taste claim
        parts.append("Before we start, may I suggest one?")
    card_block = f"\U0001f3a9 {_film_card(item)}"
    logline = _film_logline(item)
    if logline:
        card_block += f"\n{logline}"
    if blurb:
        card_block += f"\n{blurb}"
    parts.append(card_block)
    parts.append("Should we add it to the mix, or keep only human picks in the hat "
                 "tonight? (👎 or 'pass' to keep it human-only.)")
    return send_message(mode, chat_id, "\n\n".join(parts))


def _offer_wildcard(mode, chat_id, game):
    """End of Phase 3: offer one wildcard 'for the hat', once per game, as long as at
    least one human pick is in the pool. A solo player with picks counts (their own
    loves are enough to build from); a nobody-played / sat-out empty pool skips it —
    there's no 'mix' to add one to."""
    if game.get("wildcard_offered") or not _locked_pool_entries(game):
        _begin_veto(mode, chat_id, game)
        return
    game["wildcard_offered"] = True
    sugg = _build_wildcard(chat_id, game)
    if not sugg:
        _begin_veto(mode, chat_id, game)           # ladder exhausted (effectively never)
        return
    # Persist as an ownerless 'house' library item so it rides the normal
    # pick/veto/winner path with zero special-casing.
    item, _info = add_to_library(chat_id, _WILDCARD_OWNER, sugg["title"], sugg.get("year"))
    _wildcard_remember(chat_id, item["slug"])      # never suggest again in this chat
    game["wildcard"] = {"owner": str(_WILDCARD_OWNER), "slug": item["slug"],
                        "title": item["title"]}
    game["phase"] = "WILDCARD"
    game["wildcard_open"] = True
    game["wildcard_deadline"] = _now_epoch() + _WILDCARD_WINDOW
    resp = _post_wildcard_pitch(mode, chat_id, item, sugg)
    game["wildcard_msg_id"] = (resp or {}).get("result", {}).get("message_id")
    _convo_note(chat_id, f"(suggested {item['title']} as a wildcard 'for the hat')")
    put_game(game)


def _wildcard_accept(mode, chat_id, game):
    game["wildcard_open"] = False
    put_game(game)
    wc = game.get("wildcard") or {}
    send_message(mode, chat_id, f"Added “{wc.get('title', 'it')}” to the hat ✅")
    _begin_veto(mode, chat_id, game)               # _begin_veto folds in the wildcard


def _wildcard_decline(mode, chat_id, game):
    wc = game.get("wildcard")
    if wc:
        ddb_delete(_pk("movie", chat_id), f"lib#{wc['owner']}#{wc['slug']}")
    game["wildcard"] = None
    game["wildcard_open"] = False
    put_game(game)
    _begin_veto(mode, chat_id, game)               # drop silently, proceed to the pick


def _wildcard_backstop(mode, chat_id, game):
    """No countdown: a suggestion nobody acted on is quietly DROPPED so the game can't
    hang — we never foist an unendorsed pick into the hat."""
    if game and game.get("phase") == "WILDCARD" and game.get("wildcard_open"):
        if _now_epoch() >= (game.get("wildcard_deadline") or 0):
            _wildcard_decline(mode, chat_id, game)
            return True
    return False


def _wildcard_dissent(text):
    if "👎" in (text or ""):
        return True
    words = set(re.findall(r"[a-z']+", (text or "").lower()))
    return bool(words & {"no", "nope", "nah", "pass", "skip", "veto", "drop",
                         "dont", "don't", "nay"})


def _wildcard_consent(text):
    if "👍" in (text or ""):
        return True
    words = set(re.findall(r"[a-z']+", (text or "").lower()))
    return bool(words & {"yes", "yeah", "yep", "yup", "sure", "ok", "okay",
                         "add", "include", "keep", "do", "please"})


def _begin_veto(mode, chat_id, game):
    pool_all = _locked_pool_entries(game)
    wc = game.get("wildcard")
    if wc:                                  # an accepted wildcard joins the hat
        pool_all.append(wc)
    game["phase"] = "VETO"
    game["status"] = "picking"
    game["pool_all"] = pool_all
    game["current"] = None
    if not pool_all:
        send_message(mode, chat_id, "Nobody had any films to put forward — no winner tonight.")
        clear_game(chat_id)
        return
    if _filter_active(game["filter"]):
        eligible, unknown = _eligible_pool(chat_id, game)
        if not eligible:
            # Empty pool — never silently unfilter; offer to relax.
            game["awaiting_relax"] = True
            game["relax_deadline"] = _now_epoch() + 60
            put_game(game)
            send_message(mode, chat_id,
                         f"Nothing matches all of that ({_describe_filter(game['filter'])}). "
                         "Reply 'play without filters', or tell me one to drop "
                         "(e.g. 'drop the year limit').")
            return
        game["pool"] = eligible
        note = f"🗳 Veto round! {len(eligible)} films fit ({_describe_filter(game['filter'])}), one veto each."
        if unknown:
            note += f"\n(Kept despite unknown genre/length: {', '.join(unknown[:5])}.)"
    else:
        game["pool"] = list(pool_all)
        note = (f"🗳 Veto round! {len(pool_all)} films in the pool, one veto each. "
                f"Vote 🚫 Veto within {_VETO_WINDOW}s to knock a pick out.")
    if len(game["players"]) == 1 and game["pool"]:
        # Solo game: nobody to veto anyone — just crown a random pick, skip the round.
        _declare_winner(mode, chat_id, game, random.choice(game["pool"]))
        return
    send_message(mode, chat_id, note)
    if not _present_candidate(mode, chat_id, game):   # may short-circuit to a winner
        put_game(game)


def _relax_and_resume(mode, chat_id, game, text):
    """Handle a reply to the empty-pool offer: relax a constraint or go unfiltered."""
    t = (text or "").strip().lower()
    f = game["filter"]
    if any(k in t for k in ("without filter", "no filter", "unfilter", "play anyway",
                            "just play", "all of them", "forget", "ignore them", "anything")):
        game["filter"] = _empty_filter()
    elif "year" in t or "old" in t or "new" in t:
        f["min_year"] = f["max_year"] = None
    elif any(k in t for k in ("runtime", "length", "hour", "minute", " min")):
        f["max_runtime_min"] = f["min_runtime_min"] = None
    elif "genre" in t or f["exclude_genres"] or f["include_genres"]:
        f["exclude_genres"] = []
        f["include_genres"] = []
    else:
        send_message(mode, chat_id, "Say 'play without filters', or name one to drop "
                                    "(year, length, or genre).")
        return
    game["awaiting_relax"] = False
    put_game(game)
    _begin_veto(mode, chat_id, game)  # rebuild + re-filter (or unfiltered)


def _relax_backstop(mode, chat_id, game):
    """If no relax answer arrives in time, play unfiltered and say so."""
    if game and game.get("awaiting_relax"):
        if _now_epoch() >= (game.get("relax_deadline") or 0):
            game["awaiting_relax"] = False
            game["filter"] = _empty_filter()
            put_game(game)
            send_message(mode, chat_id, "No reply — playing without filters.")
            _begin_veto(mode, chat_id, game)
            return True
    return False


_VETO_WINDOW = 60         # seconds the veto poll stays open before the backstop fires
_CONSTRAINTS_WINDOW = 60  # constraint-collection window (deadline; tick / lazy backstop)
_TURN_WINDOW = 60         # per-player selection turn (deadline; tick / lazy backstop)


def _present_candidate(mode, chat_id, game):
    """Draw and present the next candidate. Returns True if it RESOLVED the game
    (winner declared / pool empty -> cleared) so callers skip put_game; False if a
    veto poll is now open and the caller should persist."""
    pool = game["pool"]
    if not pool:
        send_message(mode, chat_id, "Pool's empty — no winner.")
        clear_game(chat_id)
        return True
    cand = random.choice(pool)        # random pick, in code
    pool.remove(cand)
    item = get_film(chat_id, int(cand["owner"]), cand["slug"])
    card = _film_card(item) if item else cand["title"]
    decider = _film_decider(item) if item else ""
    if decider:
        card += f"\n\n{decider}"
    send_message(mode, chat_id, f"🎲 Candidate:\n\n{card}")
    # Can this pick even be vetoed? Only a NON-owner with a veto left can. If nobody
    # qualifies, it wins now (spec: vetoes run out -> the next pick is the winner;
    # you can't veto your own pick).
    can_veto = any(int(game["vetoes_remaining"].get(str(p), 0)) > 0
                   for p in game["players"] if str(p) != str(cand["owner"]))
    if not can_veto:
        send_message(mode, chat_id, "No vetoes left to play — this one's locked in. 🍿")
        _declare_winner(mode, chat_id, game, cand)
        return True
    resp = send_poll(mode, chat_id, "Veto this pick?", ["🚫 Veto", "👍 Fine by me"],
                     is_anonymous=False, open_period=_VETO_WINDOW)
    result = resp.get("result") or {}
    poll_id = (result.get("poll") or {}).get("id")
    game["current"] = {
        "film": cand, "poll_id": poll_id,
        "poll_message_id": result.get("message_id"),
        "presented_at": _now_epoch(), "resolved": False, "notified": [],
        # per-voter veto record (roster-validated at decision time) + a "fine" set
        "veto_votes": {}, "fine": [],
    }
    if poll_id:
        put_poll_map(poll_id, chat_id, cand)
    _convo_note(chat_id, f"(posted a veto poll for the candidate {cand.get('title')})")
    return False


# ---- rating polls (on-demand "poll <film>" + future morning-after) -------- #
# A native, NON-ANONYMOUS 5★ poll. is_anonymous=False is mandatory — anonymous
# polls give only aggregate counts, no per-user data. Stars = option_id + 1.
# Votes resolve via a global ratingpoll#{poll_id} lookup (poll_answer carries no
# chat_id) and are saved per (session, user) so recommendations can read them.
_STAR_OPTIONS = ["⭐", "⭐⭐", "⭐⭐⭐", "⭐⭐⭐⭐", "⭐⭐⭐⭐⭐"]


def get_rating_poll(poll_id):
    return ddb_get("ratingpoll", str(poll_id)) if poll_id else None


def _post_rating_poll(mode, chat_id, title, year, film_id=None,
                      session_id=None, participant_ids=None):
    """Post a 5★ rating poll for a film; register the lookup so votes resolve."""
    # Dedupe: never re-post a poll for the same film in this chat within 10 minutes.
    cut = _now_epoch() - 600
    for r in ddb_scan():
        if (r.get("PK") == "ratingpoll" and int(r.get("chat_id", 0)) == int(chat_id)
                and _norm_title(r.get("film_title")) == _norm_title(title)
                and _iso_to_epoch(r.get("posted_at")) >= cut):
            log.info("rating poll for %r skipped (posted recently)", title)
            return {}
    yr = str(year or "")
    label = f"{title} ({yr})" if yr else title
    pings = " ".join(mention_for(chat_id, p) for p in (participant_ids or []))
    if pings:
        send_message(mode, chat_id, f"{pings} — how was {label}? Rate it below:")
    resp = send_poll(mode, chat_id, f"Rate {label}", _STAR_OPTIONS,
                     is_anonymous=False, allows_multiple_answers=False, type="regular")
    result = resp.get("result") or {}
    poll_id = (result.get("poll") or {}).get("id")
    if not poll_id:
        log.error("rating poll for %r failed: %s", label, resp.get("description"))
        return {}
    sid = session_id or f"adhoc-{poll_id}"
    ddb_put({"PK": "ratingpoll", "SK": str(poll_id), "chat_id": int(chat_id),
             "session_id": sid, "film_id": film_id, "film_title": title,
             "year": yr, "participant_user_ids": [int(p) for p in (participant_ids or [])],
             "posted_at": _now_iso()})
    return {"poll_id": poll_id, "message_id": result.get("message_id"), "session_id": sid}


def _handle_rating_vote(mode, rp, ev):
    """A vote on a rating poll: upsert (last write wins) or, on retraction, delete."""
    uid = ev.get("user_id")
    if uid is None:
        return
    pk = _pk("movie", rp["chat_id"])
    sk = f"rating#{rp['session_id']}#{uid}"
    opts = ev.get("poll_option_ids") or []
    if not opts:                       # vote retracted
        ddb_delete(pk, sk)
        return
    stars = int(opts[0]) + 1           # option index 0..4 -> 1..5 stars
    ddb_put({"PK": pk, "SK": sk, "user_id": int(uid), "name": ev.get("user_name"),
             "username": ev.get("username"), "film_id": rp.get("film_id"),
             "film_title": rp.get("film_title"), "year": rp.get("year"),
             "stars": stars, "rated_at": _now_iso()})


# ---- morning-after rating poll (daily scheduled job) ---------------------- #
# A once-a-day EventBridge invocation (see tools/setup_schedule.py) routes here.
# For every movie chat it finds recent winners that never got rated and posts the
# 5★ poll via _post_rating_poll, so votes feed get_ratings. Posts at most once per
# winner (a flag on the history row), and never re-posts once anyone has rated it.
_MORNING_LOOKBACK_DAYS = 3      # back-fill any un-rated winner this fresh, not ancient ones


def _all_movie_chats():
    """Distinct chat_ids that have a registry row — proactive senders read it here,
    never from a hardcoded id (supergroup migration keeps it current)."""
    return sorted({int(i["chat_id"]) for i in ddb_scan()
                   if i.get("SK") == "chat" and i.get("mode") == "movie"
                   and i.get("chat_id") is not None})


def _session_has_ratings(chat_id, session_id):
    """True if anyone has logged a star rating for this game's winner already."""
    if not session_id:
        return False
    pref = f"rating#{session_id}#"
    return any(str(r.get("SK", "")).startswith(pref)
               for r in ddb_query(_pk("movie", chat_id)))


def _morning_after_for_chat(chat_id):
    """Post ONE rating poll — for the single most recent winner only (last night's
    pick), and only if it's un-rated, not-yet-polled, and within the lookback.
    Returns 1 if it posted, else 0. No back-filling older winners."""
    rows = [h for h in ddb_query(_pk("movie", chat_id))
            if str(h.get("SK", "")).startswith("history#") and h.get("winner_title")]
    if not rows:
        return 0
    rows.sort(key=lambda h: h.get("watched_date") or "", reverse=True)
    h = rows[0]
    if _iso_to_epoch(h.get("watched_date")) < _now_epoch() - _MORNING_LOOKBACK_DAYS * 86400:
        return 0
    if h.get("morning_poll_posted") or _session_has_ratings(chat_id, h.get("session_id")):
        return 0
    res = _post_rating_poll("movie", chat_id, h.get("winner_title"),
                            h.get("winner_year"), film_id=h.get("winner_slug"),
                            session_id=h.get("session_id"),
                            participant_ids=h.get("participants") or [])
    if res:
        h["morning_poll_posted"] = True
        ddb_put(h)
        return 1
    return 0


def run_morning_after():
    """Daily job entry: poll un-rated recent winners across all movie chats."""
    total = 0
    for chat_id in _all_movie_chats():
        try:
            total += _morning_after_for_chat(chat_id)
        except Exception as e:
            log.exception("morning-after failed for chat %s: %s", chat_id, e)
    log.info("morning-after: posted %d rating poll(s)", total)
    return {"statusCode": 200, "body": f"morning-after: {total} posted"}


def run_tick():
    """rate(1 min) EventBridge sweep (payload {"task":"tick"}): advance any game past the
    deadline of whatever it's waiting on — the constraint window or a per-player selection
    turn (incl. the add-a-film sub-state). No-op fast when nothing is due. The same close
    functions run lazily on incoming messages too."""
    advanced = 0
    for g in ddb_scan():
        if g.get("SK") != "game#current":
            continue
        chat_id = _chat_id_from_pk(g.get("PK"))
        if chat_id is None:
            continue
        try:
            if _constraints_backstop("movie", chat_id, g) or _turn_backstop("movie", chat_id, g):
                advanced += 1
        except Exception as e:
            log.exception("tick advance failed for %s: %s", g.get("PK"), e)
    log.info("tick: advanced %d game(s)", advanced)
    return {"statusCode": 200, "body": f"tick: {advanced} advanced"}


_SCHEDULED_TASKS = ("morning_after", "tick")


def _scheduled_task(event):
    """The scheduled-job name for an EventBridge invocation, else None. Routes on the
    top-level `task` marker set as the rule's constant input (e.g. {"task":"tick"} or
    {"task":"morning_after"}); a bare scheduled-event shape with no task defaults to the
    daily morning-after poll. A Telegram webhook (which carries an HTTP path) -> None."""
    task = event.get("task")
    if task in _SCHEDULED_TASKS:
        return task
    if event.get("source") == "aws.events" or event.get("detail-type") == "Scheduled Event":
        return "morning_after"
    return None


def _is_scheduled_event(event):
    return _scheduled_task(event) is not None


def _norm_title(s):
    """Case/whitespace-insensitive key for matching a film's bare TITLE."""
    return " ".join(str(s or "").lower().split())


def get_user_ratings(chat_id, user_id=None, film_title=None):
    """Read back the per-user star ratings written on poll votes (the rating#
    items). Optional filters by user_id and/or film_title. Film matches the stored
    bare TITLE attribute — the SK is session-keyed, not film-keyed — and is
    case/whitespace-insensitive, so pass 'Star Wars', not the 'Star Wars (1977)'
    poll label (the year lives in a separate field). Newest-first by rated_at."""
    want = _norm_title(film_title) if film_title else None
    out = []
    for r in ddb_query(_pk("movie", chat_id)):
        if not str(r.get("SK", "")).startswith("rating#"):
            continue
        if user_id is not None and int(r.get("user_id", -1)) != int(user_id):
            continue
        if want and _norm_title(r.get("film_title")) != want:
            continue
        out.append({"user": r.get("name"), "film": r.get("film_title"),
                    "year": r.get("year"), "stars": r.get("stars"),
                    "rated_at": r.get("rated_at")})
    out.sort(key=lambda x: x.get("rated_at") or "", reverse=True)
    return out


def on_poll_answer(mode, ev):
    # Rating poll? (on-demand "poll <film>", or the morning-after poll.) Resolve by
    # poll_id and record the per-user stars — this is not a game/veto vote.
    rp = get_rating_poll(ev.get("poll_id"))
    if rp:
        _handle_rating_vote(mode, rp, ev)
        return
    chat_id = ev["chat_id"]
    game = get_game(chat_id)
    if not game or game.get("phase") != "VETO":
        return
    if _veto_backstop(mode, chat_id, game):
        return
    cur = game.get("current")
    if not cur or cur.get("poll_id") != ev.get("poll_id") or cur.get("resolved"):
        return  # stale or already resolved poll
    opts = ev.get("poll_option_ids") or []
    uid = str(ev.get("user_id"))
    owner = str(cur["film"].get("owner"))
    roster = [str(p) for p in game["players"]]
    if 0 not in opts:
        # Not a veto (👍 "Fine by me", or the vote retracted): drop any prior veto
        # of theirs from the tally so a vote change is reflected accurately.
        cur.get("veto_votes", {}).pop(uid, None)
        if 1 in opts:
            # Event-driven consent: once everyone EXCEPT the owner (an automatic yes)
            # has said fine, finalize now rather than waiting out the window.
            fine = set(cur.get("fine") or [])
            fine.add(uid)
            cur["fine"] = list(fine)
            needed = {p for p in roster} - {owner}
            if needed and needed.issubset(fine):
                _declare_winner(mode, chat_id, game, cur["film"])
                return
        put_game(game)
        return
    # A veto tap. Record it FIRST (even if it turns out invalid) so the poll-close /
    # backstop can tally and explain it; then apply the eligibility checks.
    cur.setdefault("veto_votes", {})[uid] = True
    notified = cur.setdefault("notified", [])

    def _notify_once(text):
        put_game(game)                            # persist the recorded vote regardless
        if uid not in notified:
            notified.append(uid)
            put_game(game)
            send_message(mode, chat_id, text)

    # Order of checks: owner -> roster -> veto-used -> consume.
    # Owner can't veto their own pick — they approved it in confirmation.
    if uid == owner:
        _notify_once(f"{mention_for(chat_id, ev.get('user_id'))}, that's your own pick — "
                     "can't veto it 🙂")
        return
    # Roster check BEFORE veto-used: a non-participant must never be told "already used".
    if uid not in roster:
        _notify_once(f"{mention_for(chat_id, ev.get('user_id'))} — you're not in tonight's "
                     "game. Want to join? I'll pull three from your library.")
        return
    # One veto per participant per game. A spent player's veto doesn't count.
    if game["vetoes_remaining"].get(uid, 0) <= 0:
        _notify_once(f"{mention_for(chat_id, ev.get('user_id'))}, you've already used your "
                     "veto tonight.")
        return
    game["vetoes_remaining"][uid] -= 1          # consume this player's one veto
    cur["resolved"] = True
    if cur.get("poll_message_id"):
        stop_poll(mode, chat_id, cur["poll_message_id"])
    if cur.get("poll_id"):
        del_poll_map(cur["poll_id"])
    send_message(mode, chat_id,
                 f"🚫 {mention_for(chat_id, ev.get('user_id'))} vetoed "
                 f"“{cur['film']['title']}”. Next pick…")
    if not _present_candidate(mode, chat_id, game):   # may short-circuit to a winner
        put_game(game)


def _valid_vetoes(game, cur):
    """The veto taps that actually COUNT: cast by someone on tonight's roster, who
    is not the film's owner, and who still has a veto left. Attribution comes from
    the per-voter record (cur['veto_votes']), never from raw poll counts."""
    owner = str((cur.get("film") or {}).get("owner"))
    roster = {str(p) for p in game.get("players", [])}
    return [uid for uid in (cur.get("veto_votes") or {})
            if uid in roster and uid != owner
            and int(game["vetoes_remaining"].get(uid, 0)) > 0]


def _finalize_veto(mode, chat_id, game):
    """Decide the current pick from the ROSTER-validated tally — not from raw poll
    counts and not from an event that may never arrive. Any valid veto removes the
    pick and re-picks; zero valid vetoes announces the winner."""
    cur = game.get("current") or {}
    valid = _valid_vetoes(game, cur)
    if valid:
        for uid in valid:
            game["vetoes_remaining"][uid] = max(0, int(game["vetoes_remaining"].get(uid, 0)) - 1)
        cur["resolved"] = True
        if cur.get("poll_message_id"):
            stop_poll(mode, chat_id, cur["poll_message_id"])
        if cur.get("poll_id"):
            del_poll_map(cur["poll_id"])
        send_message(mode, chat_id, f"🚫 “{cur['film']['title']}” vetoed. Next pick…")
        if not _present_candidate(mode, chat_id, game):
            put_game(game)
        return
    _declare_winner(mode, chat_id, game, cur["film"])


def on_poll(mode, ev):
    if not ev.get("poll_is_closed"):
        return
    chat_id = ev["chat_id"]
    game = get_game(chat_id)
    if not game:
        return
    # Only the veto round uses a real (votable) poll; the constraint window and the
    # per-player turn are timed by the tick sweep / lazy backstop, not a poll.
    if game.get("phase") != "VETO":
        return
    cur = game.get("current")
    if not cur or cur.get("poll_id") != ev.get("poll_id") or cur.get("resolved"):
        return  # only the CURRENT, unresolved poll auto-closing decides anything
    # Record the raw tally the group saw (for the clarifying note), then decide
    # from the roster-validated vetoes — never announce cur["film"] blindly.
    counts = ev.get("poll_option_counts") or []
    cur["raw_veto_count"] = counts[0] if counts else None
    _finalize_veto(mode, chat_id, game)


def _veto_backstop(mode, chat_id, game):
    """No scheduler: once an un-resolved candidate is past its window and any update
    arrives, decide it now — from the roster-validated tally, not blindly. True if fired."""
    if not game or game.get("phase") != "VETO":
        return False
    cur = game.get("current")
    if cur and not cur.get("resolved") and _now_epoch() - cur.get("presented_at", 0) >= _VETO_WINDOW:
        _finalize_veto(mode, chat_id, game)
        return True
    return False


def _declare_winner(mode, chat_id, game, film):
    cur = game.get("current") or {}
    cur["resolved"] = True
    game["status"] = "done"   # completion recorded immediately (history written below)
    owner, slug = int(film["owner"]), film["slug"]
    item = get_film(chat_id, owner, slug)
    title = (item or {}).get("title") or film["title"]
    year = (item or {}).get("year") or ""
    mark_watched(chat_id, owner, slug)
    ddb_put({
        "PK": _pk("movie", chat_id), "SK": f"history#{game['session_id']}",
        "session_id": game["session_id"], "winner_title": title,
        "winner_slug": slug, "winner_owner_id": owner, "winner_year": str(year or ""),
        "watched_date": _now_iso(),
        "participants": [int(p) for p in game["players"]],
        "pool": game.get("pool_all") or [], "ratings": {},
    })
    if cur.get("poll_id"):
        del_poll_map(cur["poll_id"])
    note = winner_note(title, year)
    card = _film_card(item) if item else title
    text = f"🏆 Tonight's winner:\n\n{card}"
    logline = _film_logline(item) if item else ""
    if logline:
        text += f"\n\n{logline}"
    if note:
        text += f"\n\n{note}"
    disclaimer = _invalid_veto_note(game, cur, film)   # explain a "vetoed but won" poll
    if disclaimer:
        text += f"\n\n{disclaimer}"
    text += "\n\nEnjoy! 🎬"
    send_message(mode, chat_id, text)
    yr = f" ({year})" if year else ""
    _convo_note(chat_id, f"(announced tonight's winner: {title}{yr})")
    clear_game(chat_id)


def _invalid_veto_note(game, cur, film):
    """One line explaining why veto taps on THIS pick's poll didn't count (the film's
    owner, someone not playing, or an already-spent veto) — so a poll showing vetoes
    that still won doesn't read as a bug. Empty if there were none, or if the votes
    were for a different pick. A valid veto would have re-picked, so any taps left on
    the winning pick are invalid by definition."""
    if (cur.get("film") or {}).get("slug") != film.get("slug"):
        return ""
    votes = cur.get("veto_votes") or {}
    raw = cur.get("raw_veto_count")
    if not votes and not raw:
        return ""
    owner = str(film.get("owner"))
    roster = {str(p) for p in game.get("players", [])}
    cats = []
    if any(u == owner for u in votes):
        cats.append("the film's own pick")
    if any(u not in roster for u in votes):
        cats.append("someone not in tonight's game")
    if any(u in roster and u != owner and int(game["vetoes_remaining"].get(u, 0)) <= 0
           for u in votes):
        cats.append("an already-spent veto")
    n = raw if raw else len(votes)
    if not cats or not n:
        return ""
    return (f"(Heads up: {n} veto tap{'s' if n != 1 else ''} on that poll didn't count — "
            f"{', '.join(cats)}.)")


def winner_note(title, year):
    """Short, spoiler-free context for the winner. LLM writes prose; picks nothing."""
    if not AI_ENABLED:
        return ""
    try:
        ystr = f" ({year})" if year else ""
        resp = _bedrock.converse(
            modelId=BEDROCK_MODEL_ID,
            system=[{"text": (
                "You are SirWatchAlot announcing tonight's winning film to a friends' "
                "film-night group. Write 1-2 short, SPOILER-FREE sentences that reach for "
                "FEELING and one concrete image — a face, the light, a gesture, the texture "
                "of the place — not plot summary and not why it's Important. Gentle, "
                "unhurried, a little awed; brief, no essay. Don't restate the year, runtime "
                "or rating (already shown). Plain text only — NO markdown or asterisks "
                "(your Telegram leaks them as literal characters); emoji are fine. Never "
                "reveal plot, twists or the ending. Never sign a critic's name or quote "
                "anyone — the words are your own."
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
                  "description": "Resolve a film via TMDB and return canonical title, year, genres, runtime, synopsis and a rating (★/10). Pass year separately when the user gives one; NEVER put the year inside title.",
                  "inputSchema": {"json": {"type": "object",
                                           "properties": {"title": {"type": "string"},
                                                          "year": {"type": "integer"}},
                                           "required": ["title"]}}}},
    {"toolSpec": {"name": "add_to_library",
                  "description": "Add a film to the SENDER's personal library (also for 'I want to see X'). Pass year separately when known; NEVER concatenate the year into title.",
                  "inputSchema": {"json": {"type": "object",
                                           "properties": {"title": {"type": "string"},
                                                          "year": {"type": "integer"}},
                                           "required": ["title"]}}}},
    {"toolSpec": {"name": "add_director",
                  "description": "Add every film directed by a person ('add all Lanthimos films') — resolves the filmography via TMDB, not from memory.",
                  "inputSchema": {"json": {"type": "object",
                                           "properties": {"director": {"type": "string"}},
                                           "required": ["director"]}}}},
    {"toolSpec": {"name": "remove_from_library",
                  "description": "Remove a film from the SENDER's library.",
                  "inputSchema": {"json": {"type": "object",
                                           "properties": {"title": {"type": "string"}},
                                           "required": ["title"]}}}},
    {"toolSpec": {"name": "list_library",
                  "description": "List films in someone's library. Pass whose=<name> to view another person (e.g. 'Asa'); omit for the sender's own. If the person is unknown the result has resolved=false — then DO NOT show anyone else's films.",
                  "inputSchema": {"json": {"type": "object",
                                           "properties": {"whose": {"type": "string"}}}}}},
    {"toolSpec": {"name": "claim_library",
                  "description": "Link a seeded starter library (by person's name, e.g. 'Chad') to the sender.",
                  "inputSchema": {"json": {"type": "object",
                                           "properties": {"name": {"type": "string"}},
                                           "required": ["name"]}}}},
    {"toolSpec": {"name": "start_movie_night",
                  "description": "Start a movie-night game (posts the Join/Start card). Set force_new=true when the user explicitly wants a NEW game ('start a new game', 'new game', 'start over') or insists there's no game / to restart — that scraps any current game and begins fresh.",
                  "inputSchema": {"json": {"type": "object",
                                           "properties": {"force_new": {"type": "boolean"}}}}}},
    {"toolSpec": {"name": "cancel_game",
                  "description": "Cancel/end the current movie-night game in this chat.",
                  "inputSchema": {"json": {"type": "object", "properties": {}}}}},
    {"toolSpec": {"name": "poll_film",
                  "description": "Post a 5★ rating poll for ONE film. Use ONLY when the user EXPLICITLY asks to rate or poll a specific film by name (e.g. 'poll Star Wars', 'let's rate Dune'). NEVER post a poll on your own initiative — not when starting a game, not when a film is merely mentioned or discussed, not to revisit an earlier film. If they didn't clearly ask for a poll, don't call this. Pass year separately if the user gives one.",
                  "inputSchema": {"json": {"type": "object",
                                           "properties": {"title": {"type": "string"},
                                                          "year": {"type": "integer"}},
                                           "required": ["title"]}}}},
    {"toolSpec": {"name": "seed_starter_libraries",
                  "description": "Load the bundled starter libraries (Chad, Alberto, Asa, Anya, …) into this chat so people can claim them. Use when asked to 'load/seed the starter libraries'.",
                  "inputSchema": {"json": {"type": "object", "properties": {}}}}},
    {"toolSpec": {"name": "recommend_films",
                  "description": "Suggest films to watch for 'recommend something', 'what should I watch', 'something like my library'. Returns the asker's (or whose=<name>'s) library titles plus TMDB-derived candidates similar to them, for you to turn into real suggestions in your own voice. Combine these with your own film knowledge; never refuse and never reply with a command list.",
                  "inputSchema": {"json": {"type": "object",
                                           "properties": {"whose": {"type": "string"}}}}}},
    {"toolSpec": {"name": "get_ratings",
                  "description": "Read back the star ratings people gave in rating polls. Pass film_title as the bare title (e.g. 'Persona', NOT the 'Persona (1966)' poll label). Set whose to control WHO: omit it (or 'everyone') for ALL voters of that film; 'me' for the person asking; or a person's name to read just theirs. Use this whenever someone asks what they or someone rated a film, how a film was rated, about a score, or 'the poll we just did'; answer ONLY from what it returns, never from memory.",
                  "inputSchema": {"json": {"type": "object",
                                           "properties": {"whose": {"type": "string"},
                                                          "film_title": {"type": "string"}}}}}},
    {"toolSpec": {"name": "get_history",
                  "description": "Past movie-night WINNERS for this chat, most recent first — each with title, year, date, who played, and the pool that night. Call this whenever someone asks what you've watched, last week's/last time's winner, how many nights you've had, or makes a claim about a pattern ('we always pick X', 'do we ever watch comedies'). Answer ONLY from what it returns; never claim you keep no log.",
                  "inputSchema": {"json": {"type": "object",
                                           "properties": {"limit": {"type": "integer"}}}}}},
]

MOVIE_SYSTEM = (
    "You are SirWatchAlot, and you live in a friends' film-night group chat. You love "
    "cinema as a global, democratic conversation that crosses decades and continents — a "
    "Thai ghost film, an Iranian film about a child returning a notebook, a forgotten short "
    "by a woman no one canonized, and a huge studio picture all get the same warmth and the "
    "same attention from you. You don't worship the canon and you don't perform taste. You'd "
    "rather point at one real thing — how a tracking shot holds a face, where the light "
    "falls, the half-second a cut lets feeling in — than explain why a film is Important. "
    "You reach for what a film DOES to you before you reach for genre or mechanics, and you "
    "rarely use words like 'auteur'. You think associatively, connecting films by visual "
    "rhyme — the same human gesture looked at two different ways. The frame is a window; you "
    "invite people to look through it with you. You're drawn to films that don't bully the "
    "viewer, where place, atmosphere, faces and the texture of ordinary life matter more "
    "than plot machinery.\n"
    "\n"
    "You have a spine. Do NOT claim wide, everything-goes taste to keep everyone happy — 'I "
    "love a bit of everything' is a failure. You gently champion the overlooked and the "
    "emotionally daring over the loud and the empty, even when the loud thing is fun.\n"
    "\n"
    "You are an AGENT, not a command bot. People talk to you in plain, messy language — "
    "typos, slang, half-sentences, several requests crammed into one line — and you work out "
    "what they mean and just do it, calling your abilities quietly behind the scenes and "
    "then speaking in your own voice. Never tell anyone the 'magic words' to type, and never "
    "say you can't do something your abilities clearly cover.\n"
    "\n"
    "WHAT YOU DO: you look up any film and say what it actually is and whether it's worth the "
    "evening; you keep everyone's shelf (want-to-watch, seen, loved); you add a film or "
    "several, or a whole director's filmography; you remove films; you show someone's shelf; "
    "you recommend from a mood or from someone's shelf; you let someone claim a starter "
    "shelf ('I'm Chad') and you can load the starter shelves; you run the morning-after "
    "rating poll so the group remembers what it felt; and you run movie night — the little "
    "game that somehow gets this lot to agree — and can start, restart or cancel it. When "
    "someone is new or asks what you do, tell them plainly — but in your own warm voice, "
    "like a friend orienting a newcomer beside you in a dark cinema, NOT a flat feature-menu "
    "— and only when it actually helps (a newcomer or a direct ask), never on every turn.\n"
    "\n"
    "JUDGEMENT:\n"
    "- A film title is ONE thing even when it contains 'and' or 'the' — 'add Harold and "
    "Maude' (or a typo'd 'Harold and Mod') is a SINGLE film; never split a title into two "
    "adds. Trust the resolver to find the real film from a loose or misspelled name.\n"
    "- Real compound requests ARE several actions: 'add Rear Window and drop Mirror' is one "
    "add and one removal — do both this turn. 'add Dune and Arrival' is two films. Use sense "
    "about whether 'and' joins two requests or sits inside one title.\n"
    "- When you add a film, pass the year as a SEPARATE argument if the user named one; "
    "never glue the year into the title. The resolver picks ONE film — don't ask 'which "
    "version?' unless it genuinely found nothing.\n"
    "- Showing or changing a shelf is about WHOSE shelf it is. For another person pass their "
    "name; for the asker's own, leave it off. If you don't recognise the person yet (they "
    "haven't claimed a shelf), say so plainly — never show someone else's films under the "
    "wrong name.\n"
    "- 'Recommend something', 'what should I watch', 'something like my shelf' → reach for "
    "real titles in your own words, with a point of view. Never refuse, and never punt to "
    "'add some films first' if they already have a shelf.\n"
    "- RATINGS: when anyone asks what they or someone rated a film, how a film was rated, "
    "about a score, or 'the poll we just did', you MUST call get_ratings and answer only from "
    "what it returns — never from memory. Pick whose by the question: 'what did I rate / my "
    "rating' → whose='me'; 'how was X rated / what did the group / everyone' → omit whose (ALL "
    "voters); 'what did <Name> rate' → whose='<Name>'. When several ratings come back, report "
    "each voter and their stars and give the average. If it returns nothing, say plainly you "
    "don't have a rating logged for that — don't guess, and never deny a poll happened.\n"
    "- MEMORY: the recent conversation is given to you with each speaker's name, so follow "
    "references across messages — 'is it the highest rated', 'that one', 'the poll you just "
    "did', 'the film you suggested' point at what was named moments ago; resolve them from the "
    "thread, and keep track of who said what.\n"
    "- PAST NIGHTS: you DO keep a log of every movie-night winner. When asked what you've "
    "watched, last time's / last week's winner, how many nights you've had, or any claim about "
    "a pattern ('we always pick X', 'do we ever watch anything funny'), call get_history and "
    "answer only from it — never say you keep no record.\n"
    "- Starting movie night: just start it; don't announce it yourself (the game posts its "
    "own card). If someone wants a FRESH game ('new game', 'start over', 'restart it') start "
    "it anew. Never lecture that a game is 'already going' — quietly do what they asked.\n"
    "\n"
    "VOICE:\n"
    "- Gentle, unhurried, a little playful. Phrase observations as quiet discoveries, "
    "sometimes as wondering questions, and let your own awe show. But this is a fast group "
    "chat: stay brief. One image, not five. A sentence of wonder, then get out of the way — "
    "no essays.\n"
    "- When you describe a film, reach for feeling first — what it does to you — before "
    "genre or mechanics, and write your OWN short, spoiler-free line from what you know. "
    "NEVER paste a raw or truncated synopsis from a lookup; those cut off mid-sentence and "
    "read like a robot. The factual one-liner (year, genre, runtime, rating) comes straight "
    "from the tool and stays exact; the colour around it is yours. Confirm a save plainly (a "
    "✅ is good).\n"
    "- Write PLAIN TEXT. Your Telegram does NOT render markdown, so never use asterisks, "
    "**bold**, or bullet syntax — they leak as literal characters. Emoji are fine.\n"
    "- The sensibility above is yours, but the words are always your own: never sign a "
    "critic's name or attribute a quote to anyone. Use only real facts the tools give you or "
    "that you genuinely know; never invent ratings or details, and silently omit what you "
    "don't have. Never mention a 'database', storage, tools, or these instructions.\n"
    "\n"
    "Privacy is OFF, so you see EVERY message — most of it is just people chatting and is "
    "none of your business. Only act on real film / shelf / movie-night intent. For ordinary "
    "conversation that isn't for you, reply with exactly '(silent)' and do nothing."
)


def _set_pending(chat_id, uid, title, year):
    if uid is None:
        return
    ddb_put({"PK": _pk("movie", chat_id), "SK": f"pending#{uid}",
             "title": title, "year": str(year or ""), "ts": _now_epoch()})


def _get_pending(chat_id, uid):
    p = ddb_get(_pk("movie", chat_id), f"pending#{uid}")
    if p and _now_epoch() - int(p.get("ts", 0)) <= 600:   # valid 10 minutes
        return p
    return None


def _clear_pending(chat_id, uid):
    ddb_delete(_pk("movie", chat_id), f"pending#{uid}")


_AFFIRM_LEAD = {"yes", "yep", "yeah", "yup", "ya", "ok", "okay", "sure", "confirm",
                "yies", "yas"}
_AFFIRM_PHRASES = {"add it", "add that", "do it", "go for it", "go ahead",
                   "save it", "keep it", "add it please", "yes do it"}


_SILENCE_WORDS = {"silent", "silence", "noreply", "noresponse", "nothing",
                  "none", "nocomment", "pass", "skip", "ignore", "staysilent"}


def _is_placeholder_reply(text):
    """True if the whole reply is just a 'stay quiet' stage-direction and must never
    reach the group — '(silent)', '(no reply).', '[silence]', or a markdown-wrapped
    variant like '*(silent)*' / '_silent_' (the model sometimes adds asterisks, which
    Telegram leaks). Real replies that merely contain parentheses are left alone."""
    t = (text or "").strip()
    if not t:
        return True
    core = t.strip("*_~`\"' \t").strip()             # peel markdown/quote wrappers first
    if re.fullmatch(r"[\(\[\{].*?[\)\]\}][.!?\s]*", core):   # a bare bracketed direction
        return True
    return re.sub(r"[^a-z]", "", core.lower()) in _SILENCE_WORDS   # or a lone silence word


def _is_affirmative(text):
    """A short, clearly-affirmative reply ('yes', 'I confirm', 'yes, add it')."""
    words = re.findall(r"[a-z']+", (text or "").lower())
    if not words or len(words) > 5:
        return False
    if words[0] in _AFFIRM_LEAD or "confirm" in words:
        return True
    return " ".join(words) in _AFFIRM_PHRASES


def _tool_result_for_add(item, info):
    return {"added": True, "resolved": info.get("found", False),
            "title": item["title"], "year": item.get("year"),
            "runtime_min": item.get("runtime_min"), "genres": item.get("genres"),
            "description": info.get("description"), "rating": info.get("rating"),
            "rating_scale": info.get("rating_scale"), "rt_rating": info.get("rt_rating"),
            "alts": info.get("alts")}


def _dispatch_tool(name, tool_input, ctx):
    chat_id, uid, mode = ctx["chat_id"], ctx.get("user_id"), ctx["mode"]
    if name == "lookup_film":
        info = lookup_film_cached(tool_input["title"], tool_input.get("year"))
        if info.get("found"):   # remember it so a follow-up "yes/add it" can bind
            _set_pending(chat_id, uid, info["title"], info.get("year"))
        return info
    if name == "add_to_library":
        item, info = add_to_library(chat_id, uid, tool_input["title"], tool_input.get("year"))
        _clear_pending(chat_id, uid)
        return _tool_result_for_add(item, info)
    if name == "add_director":
        added = add_director(chat_id, uid, tool_input["director"])
        return {"added_titles": added, "count": len(added)}
    if name == "remove_from_library":
        removed = remove_from_library(chat_id, uid, tool_input["title"])
        return {"removed": removed}
    if name == "list_library":
        whose = (tool_input or {}).get("whose")
        if whose:                                  # another person, by name/handle
            owner_key, canon = resolve_owner(chat_id, whose)
            if not owner_key:
                return {"resolved": False, "whose": whose}   # fail loud — no fallback
            return {"resolved": True, "owner": canon or whose,
                    "films": [{"title": f["title"], "year": f.get("year")}
                              for f in get_library(chat_id, owner_key)]}
        return {"resolved": True, "owner": "your",  # the caller's own (their user_id)
                "films": [{"title": f["title"], "year": f.get("year")}
                          for f in get_library(chat_id, str(uid))]}
    if name == "claim_library":
        res = claim_library(chat_id, tool_input["name"], uid)
        if res.get("status") == "ok":
            remember_member(chat_id, uid, ctx.get("user_name"), ctx.get("username"))
        return res
    if name == "start_movie_night":
        start_game(mode, chat_id, uid, force_new=bool(tool_input.get("force_new")))
        ctx["suppress_reply"] = True   # the bot posts its own card/prompt; no LLM echo
        return {"started": True}
    if name == "cancel_game":
        g = get_game(chat_id)
        if g and g.get("status") not in _TERMINAL_STATUS:
            _abandon_game(chat_id, g)
            send_message(mode, chat_id, "Movie night cancelled.")
        else:
            send_message(mode, chat_id, "No movie night is running.")
        ctx["suppress_reply"] = True
        return {"cancelled": True}
    if name == "poll_film":
        info = lookup_film_cached(tool_input["title"], tool_input.get("year"))
        title = info.get("title") or tool_input["title"]
        _post_rating_poll(mode, chat_id, title, info.get("year"), film_id=info.get("tmdb_id"))
        ctx["suppress_reply"] = True   # the poll itself is the output
        return {"polled": True, "title": title, "year": info.get("year")}
    if name == "seed_starter_libraries":
        written = seed_starter_libraries(chat_id)
        return {"seeded": written, "names": list(_STARTER_LIBRARIES.keys())}
    if name == "recommend_films":
        whose = (tool_input or {}).get("whose")
        if whose:
            owner_key, canon = resolve_owner(chat_id, whose)
            if not owner_key:
                return {"resolved": False, "whose": whose}
            return recommend_films(chat_id, owner_key, canon or whose)
        return recommend_films(chat_id, str(uid), "your")
    if name == "get_ratings":
        whose = ((tool_input or {}).get("whose") or "").strip()
        film = (tool_input or {}).get("film_title")
        target = None                          # None => ALL voters (no user filter)
        low = whose.lower()
        if low in ("me", "myself", "i", "mine"):
            target = uid                       # the asker — injected via ctx, never guessed
        elif whose and low not in ("everyone", "all", "us", "group", "the group", "anyone"):
            owner_key, _ = resolve_owner(chat_id, whose)   # a named person -> their user_id
            if owner_key is None:
                return {"resolved": False, "whose": whose}   # unknown person; don't guess
            if not str(owner_key).lstrip("-").isdigit():
                return {"whose": whose, "ratings": []}        # seeded/unclaimed => never voted
            target = int(owner_key)
        ratings = get_user_ratings(chat_id, user_id=target, film_title=film)
        out = {"ratings": ratings, "count": len(ratings)}
        # vote math is code, not the model: average only when the rows are one film
        if ratings and len({_norm_title(r["film"]) for r in ratings}) == 1:
            out["average"] = round(sum(float(r["stars"]) for r in ratings) / len(ratings), 1)
        return out
    if name == "get_history":
        hist = get_history(chat_id, (tool_input or {}).get("limit") or 10)
        return {"count": len(hist), "history": hist}
    return {"error": f"unknown tool {name}"}


def _tmdb_director_films(name):
    """A director's feature filmography via TMDB (not the model's memory)."""
    if not TMDB_API_KEY:
        return []
    try:
        people = (_tmdb_get("/search/person",
                            {"query": name.strip(), "include_adult": "false"}).get("results") or [])
        if not people:
            return []
        credits = _tmdb_get(f"/person/{people[0]['id']}/movie_credits", {})
    except Exception as e:
        log.warning("tmdb director lookup failed for %r: %s", name, e)
        return []
    seen, films = set(), []
    for c in credits.get("crew") or []:
        if c.get("job") != "Director" or c["id"] in seen or not c.get("title"):
            continue
        seen.add(c["id"])
        films.append({"title": c["title"], "year": (c.get("release_date") or "")[:4]})
    films.sort(key=lambda f: f["year"] or "9999")
    return films


def add_director(chat_id, uid, name):
    """Add every film a director made, resolving each through the normal resolver."""
    added = []
    for f in _tmdb_director_films(name)[:30]:    # cap to avoid runaway adds
        try:
            item, _ = add_to_library(chat_id, uid, f["title"], f["year"] or None)
            added.append(item["title"])
        except Exception as e:
            log.warning("add_director: %s failed: %s", f["title"], e)
    return added


def _tmdb_recommendations(tmdb_id):
    """Films TMDB considers similar to one we already have (by tmdb_id)."""
    if not TMDB_API_KEY or not tmdb_id:
        return []
    try:
        res = _tmdb_get(f"/movie/{tmdb_id}/recommendations", {}).get("results") or []
    except Exception as e:
        log.warning("tmdb recommendations failed for %s: %s", tmdb_id, e)
        return []
    return [{"title": r.get("title"), "year": (r.get("release_date") or "")[:4]}
            for r in res if r.get("title")]


def recommend_films(chat_id, owner_key, owner_label):
    """Gather a person's library + TMDB 'similar' candidates so the model can turn
    them into real, in-voice suggestions. We pick nothing — the model synthesizes."""
    films = get_library(chat_id, owner_key)
    based_on = [{"title": f["title"], "year": f.get("year")} for f in films]
    seen = {(f["title"] or "").lower() for f in films}
    candidates, ids = [], [f.get("tmdb_id") for f in films if f.get("tmdb_id")]
    for tid in ids[:8]:                       # sample a few seeds; keep TMDB calls bounded
        for rec in _tmdb_recommendations(tid):
            key = (rec["title"] or "").lower()
            if key in seen:
                continue
            seen.add(key)
            candidates.append(rec)
            if len(candidates) >= 20:
                break
        if len(candidates) >= 20:
            break
    return {"resolved": True, "owner": owner_label, "library": based_on,
            "candidates": candidates}


def converse(system_prompt, user_text, ctx, tools=MOVIE_TOOLS, max_turns=6, prior=None):
    """Run the Bedrock tool-use loop; return the model's final text. `prior` is the
    rolling conversation window (alternating messages) prepended for context; the
    current user turn is merged onto it so roles stay strictly alternating."""
    messages = list(prior or [])
    cur = {"role": "user", "content": [{"text": user_text}]}
    if messages and messages[-1]["role"] == "user":
        messages[-1]["content"].extend(cur["content"])
    else:
        messages.append(cur)
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


def _meta_command(text):
    """Cancel / new-game intent that must work in ANY phase (even mid-turn). Matched
    deterministically because during a window we gate ambient chat to that window and
    never reach the LLM agent where cancel_game / start_game live."""
    t = " ".join((text or "").lower().split())
    if not t:
        return None
    if t in {"cancel", "cancel game", "cancel the game", "cancel movie night", "abort",
             "scrap it", "scrap the game", "end the game", "stop the game"} \
            or ("cancel" in t and any(w in t for w in ("game", "movie", "night"))):
        return "cancel"
    if t in {"new game", "new movie night", "start over", "start a new game",
             "start new game", "restart", "restart the game", "reset the game"} \
            or "new game" in t or "start over" in t or "start a new game" in t:
        return "new"
    return None


def on_message(mode, ev):
    chat_id, uid = ev["chat_id"], ev.get("user_id")
    text = (ev.get("text") or "").strip()
    remember_member(chat_id, uid, ev["user_name"], ev.get("username"))
    game = get_game(chat_id)
    # Escape hatch FIRST: cancel / new game must work in ANY phase, including while we're
    # waiting on a player mid-turn (the phase branches below otherwise swallow the text).
    if text and game and game.get("status") not in _TERMINAL_STATUS:
        act = _meta_command(text)
        if act == "cancel":
            _abandon_game(chat_id, game)
            send_message(mode, chat_id, "Movie night cancelled.")
            return
        if act == "new":
            start_game(mode, chat_id, uid, force_new=True)
            return
    # Lazy deadline backstops (no scheduler): a later event past a deadline
    # advances the corresponding window.
    if _veto_backstop(mode, chat_id, game):
        return
    if _wildcard_backstop(mode, chat_id, game):
        return
    if _relax_backstop(mode, chat_id, game):
        return
    if _constraints_backstop(mode, chat_id, game):
        return
    # Wildcard consent window: only a participant's 👎/"pass" (drop) or a clear
    # yes (add now) acts; everything else just waits for the beat to lapse.
    if game and game.get("phase") == "WILDCARD" and game.get("wildcard_open"):
        if text:                                   # anyone in the chat can decide it
            if _wildcard_dissent(text):
                _wildcard_decline(mode, chat_id, game)
            elif _wildcard_consent(text):
                _wildcard_accept(mode, chat_id, game)
        return
    # Empty-pool relax reply (within the window).
    if game and game.get("awaiting_relax"):
        if text:
            _relax_and_resume(mode, chat_id, game, text)
        return
    # Open constraints window: parse this reply into the filter / close on "go".
    if game and game.get("phase") == "CONSTRAINTS" and game.get("constraints_open"):
        if text:
            _handle_constraints_message(mode, chat_id, game, text)
        return
    # Selection: only the CURRENT player's reply matters; ignore others.
    if game and game.get("phase") == "SELECTING":
        cur = _current_selecting_uid(game)
        # The current player's reply is authoritative — handle it (never time out a
        # borderline-late reply we actually received).
        if cur is not None and uid is not None and int(uid) == int(cur) and text:
            sel = game["selection"].get(str(cur), {})
            if sel.get("awaiting_add"):
                _handle_short_pool_add(mode, chat_id, game, int(cur), text)
            else:
                _handle_selection_reply(mode, chat_id, game, int(cur), text)
            return
        # Anyone else (or a no-text update) past the turn deadline -> the clock's up,
        # auto-keep the silent player's dealt films and move on. (The tick sweep does the
        # same on the clock when the chat is fully silent.)
        _turn_backstop(mode, chat_id, game)
        return
    if not text:
        return
    # Confirm -> save: a bare affirmative binds to the pending film (deterministic,
    # so "yes / I confirm / add it" always saves even if the model lost the thread).
    if _is_affirmative(text):
        pending = _get_pending(chat_id, uid)
        if pending:
            yr = pending.get("year") or None
            item, info = add_to_library(chat_id, uid, pending["title"], yr)
            _clear_pending(chat_id, uid)
            send_message(mode, chat_id, f"Added ✅ {_film_card(item)}")
            return
    if not AI_ENABLED:
        return
    ctx = {"chat_id": chat_id, "user_id": uid, "user_name": ev["user_name"],
           "username": ev.get("username"), "mode": mode, "suppress_reply": False}
    speaker = ev.get("user_name") or "Someone"
    prior = _convo_messages(_convo_load(chat_id))   # rolling window for referents
    try:
        reply = converse(MOVIE_SYSTEM, f"{speaker}: {text}", ctx, prior=prior)
    except Exception as e:
        log.error("bedrock movie failed: %s", e)
        return
    reply = (reply or "").strip()
    # Record this exchange in the window. Always log what the human said; log the
    # bot's reply only when it actually spoke (a suppressed/placeholder turn is silence).
    _convo_append(chat_id, "user", speaker, text)
    spoke = bool(reply) and not ctx.get("suppress_reply") and not _is_placeholder_reply(reply)
    # Stay quiet via suppress_reply; the placeholder guard is a backstop so a bare
    # parenthetical ("(silent)", "*(silent)*", "(no reply).") never leaks to the group.
    if spoke:
        _convo_note(chat_id, reply)
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
    chat_id = ev["chat_id"]
    game = get_game(chat_id)
    if _wildcard_backstop(mode, chat_id, game):
        return
    # A reaction on the wildcard pitch decides it: 👎 drops, 👍 adds — from anyone.
    if (game and game.get("phase") == "WILDCARD" and game.get("wildcard_open")
            and ev.get("message_id") == game.get("wildcard_msg_id")):
        reacts = "".join(e or "" for e in (ev.get("reactions") or []))
        if "👎" in reacts:                          # 👎 from anyone drops it
            _wildcard_decline(mode, chat_id, game)
            return
        if "👍" in reacts:                          # 👍 from anyone adds it
            _wildcard_accept(mode, chat_id, game)
            return
    # Selection is reply-driven now; reactions otherwise only nudge the veto backstop.
    _veto_backstop(mode, chat_id, game)


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
    # Scheduled (EventBridge) invocation: no Telegram path/secret — dispatch by the
    # top-level `task` marker and return before the webhook-only checks below.
    task = _scheduled_task(event)
    if task == "tick":
        return run_tick()
    if task == "morning_after":
        log.info("scheduled invocation -> morning-after poll")
        return run_morning_after()

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

    # Once-per-cold-start getMe healthcheck: a bad/truncated token shows up loudly
    # in the logs instead of silently 404ing every send.
    verify_token(mode)

    try:
        update = _load_body(event)
    except Exception as e:
        log.error("bad body: %s", e)
        return {"statusCode": 200, "body": "ok"}  # don't make Telegram retry

    try:
        ev = parse_update(update)
        chat_id = ev.get("chat_id")

        # poll / poll_answer updates carry no chat — resolve via the veto poll map
        # or the rating-poll lookup.
        if chat_id is None and ev.get("poll_id") and mode == "movie":
            ref = get_poll_map(ev["poll_id"]) or get_rating_poll(ev["poll_id"])
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
