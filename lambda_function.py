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
    return {
        "chat_id": chat.get("id"),
        "chat_title": chat.get("title") or chat.get("username"),
        "chat_type": chat.get("type"),
        "text": msg.get("text") or msg.get("caption") or "",
        "user_id": frm.get("id"),
        "user_name": (frm.get("first_name") or frm.get("username") or "someone"),
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
# Movie domain (DynamoDB-backed)
# --------------------------------------------------------------------------- #
def add_film(chat_id, title, added_by, added_by_id=None, year=None):
    film_id = str(uuid.uuid4())
    item = {
        "PK": _pk("movie", chat_id),
        "SK": f"film#{film_id}",
        "film_id": film_id,
        "title": title,
        "year": str(year) if year else "",
        "added_by": added_by,
        "added_by_id": str(added_by_id) if added_by_id else "",
        "added_at": _now_iso(),
    }
    ddb_put(item)
    return item


def list_films(chat_id):
    items = ddb_query(_pk("movie", chat_id))
    films = [i for i in items if str(i.get("SK", "")).startswith("film#")]
    films.sort(key=lambda f: f.get("added_at", ""))
    return films


def draw_film(chat_id):
    """Pick ONE film at random — in Python, never via the LLM."""
    films = list_films(chat_id)
    if not films:
        return None
    return random.choice(films)


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
            "description": "Add a film to this group's list, attributed to the given person.",
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
            "description": "List the films currently on this group's list with who added each.",
            "inputSchema": {"json": {"type": "object", "properties": {}}},
        }
    },
    {
        "toolSpec": {
            "name": "draw_film",
            "description": "Pick one film at random from this group's list.",
            "inputSchema": {"json": {"type": "object", "properties": {}}},
        }
    },
]


def _dispatch_tool(name, tool_input, ctx):
    """ctx carries chat_id / sender so the model can't pick the wrong group."""
    chat_id = ctx["chat_id"]
    if name == "lookup_film":
        return lookup_film(tool_input["title"])
    if name == "add_film":
        item = add_film(
            chat_id,
            tool_input["title"],
            ctx["user_name"],
            ctx.get("user_id"),
            tool_input.get("year"),
        )
        return {"added": True, "title": item["title"], "added_by": item["added_by"]}
    if name == "list_films":
        return {
            "films": [
                {"title": f["title"], "year": f.get("year"), "added_by": f.get("added_by")}
                for f in list_films(chat_id)
            ]
        }
    if name == "draw_film":
        chosen = draw_film(chat_id)  # random.choice in code
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
    "already been added to the group's list (attributed to them). Use lookup_film "
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
    line = f"Added \u201c{film_item['title']}\u201d for {film_item['added_by']} \U0001f3ac"
    try:
        info = lookup_film(film_item["title"])
    except Exception as e:
        log.warning("lookup for plain reply failed: %s", e)
        return line
    phrase = _rating_phrase(info)
    if phrase:
        yr = f" ({info['year']})" if info.get("year") else ""
        line = (
            f"Added \u201c{info['title']}\u201d{yr} for {film_item['added_by']} \U0001f3ac "
            f"\u2014 {phrase}."
        )
    return line


def handle_movie(mode, upd):
    chat_id = upd["chat_id"]
    text = upd["text"].strip()
    cmd, arg = parse_command(text)

    if cmd == "movie":
        if not arg:
            send_message(mode, chat_id, "Usage: /movie <title> — e.g. /movie Rear Window")
            return
        item = add_film(chat_id, arg, upd["user_name"], upd.get("user_id"))
        if AI_ENABLED:
            try:
                ctx = {"chat_id": chat_id, "user_name": upd["user_name"], "user_id": upd.get("user_id")}
                reply = converse(
                    MOVIE_SYSTEM,
                    f'I just added the film "{arg}". Respond now.',
                    ctx,
                )
                send_message(mode, chat_id, reply or _plain_add_reply(item))
                return
            except Exception as e:
                log.error("bedrock movie add failed: %s", e)
        send_message(mode, chat_id, _plain_add_reply(item))
        return

    if cmd == "movies":
        films = list_films(chat_id)
        if not films:
            send_message(mode, chat_id, "No films on the list yet. Add one with /movie <title>.")
            return
        lines = ["\U0001f37f This group's film list:"]
        for f in films:
            yr = f" ({f['year']})" if f.get("year") else ""
            lines.append(f"\u2022 {f['title']}{yr} — added by {f.get('added_by', '?')}")
        send_message(mode, chat_id, "\n".join(lines))
        return

    if cmd == "draw":
        chosen = draw_film(chat_id)  # random.choice in code
        if not chosen:
            send_message(mode, chat_id, "Nothing to draw from — add films with /movie first.")
            return
        yr = f" ({chosen['year']})" if chosen.get("year") else ""
        line = f"\U0001f3b2 Tonight's pick: {chosen['title']}{yr} (added by {chosen.get('added_by', '?')})"
        try:
            info = lookup_film(chosen["title"])
            phrase = _rating_phrase(info)
            if phrase:
                line += f"\n{phrase}"
                if info.get("runtime_min"):
                    line += f" \u00b7 {info['runtime_min']} min"
                if info.get("genres"):
                    line += f" \u00b7 {', '.join(info['genres'][:2])}"
        except Exception as e:
            log.warning("draw lookup failed: %s", e)
        send_message(mode, chat_id, line)
        return

    if cmd in ("start", "help"):
        send_message(
            mode, chat_id,
            "\U0001f3ac Film-night helper. Commands:\n"
            "/movie <title> — add a film\n"
            "/movies — show the list\n"
            "/draw — pick one at random\n"
            "Or just talk to me about films.",
        )
        return

    if cmd is not None:
        return  # unknown command — stay quiet

    # Free-form (privacy mode ON means we only see mentions/replies here).
    if not text:
        return
    if AI_ENABLED:
        try:
            ctx = {"chat_id": chat_id, "user_name": upd["user_name"], "user_id": upd.get("user_id")}
            reply = converse(FREEFORM_SYSTEM, text, ctx)
            if reply:
                send_message(mode, chat_id, reply)
            return
        except Exception as e:
            log.error("bedrock freeform failed: %s", e)
    send_message(mode, chat_id, "I can add films (/movie), list them (/movies) or draw one (/draw).")


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
