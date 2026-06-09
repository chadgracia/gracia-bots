#!/usr/bin/env python3
"""
Telegram webhook manager for the Gracia bots — a LOCAL admin tool.

This is NOT part of the Lambda package (the deploy step zips only
lambda_function.py). Run it from your laptop once per environment change to
point each BotFather identity at the Lambda Function URL, then to verify
Telegram can actually reach it.

It mirrors the Lambda's routing exactly: each mode is registered at
  <FUNCTION_URL>/<mode>
with a per-bot secret_token that the Lambda checks against <MODE>_WEBHOOK_SECRET
(header X-Telegram-Bot-Api-Secret-Token). Get any of these wrong and the Lambda
answers 403 and the bot stays silent — `info` will show that.

Config comes from the SAME env vars the Lambda uses, plus FUNCTION_URL:

    export FUNCTION_URL="https://<id>.lambda-url.us-east-1.on.aws"
    export MOVIE_BOT_TOKEN="123:ABC"            # from BotFather
    export MOVIE_WEBHOOK_SECRET="a-long-random-string"
    # cleaning/salary are optional until those bots exist:
    # export CLEANING_BOT_TOKEN=...   CLEANING_WEBHOOK_SECRET=...
    # export SALARY_BOT_TOKEN=...     SALARY_WEBHOOK_SECRET=...

Usage:
    python tools/setup_webhooks.py set      # register webhooks for every configured mode
    python tools/setup_webhooks.py info     # getWebhookInfo for each (url, pending, last error)
    python tools/setup_webhooks.py delete   # remove webhooks
    python tools/setup_webhooks.py smoke     # POST a synthetic /movie update straight at the URL

`smoke` exercises the whole Lambda path (routing -> secret check -> handler ->
DynamoDB/Bedrock/Letterboxd) WITHOUT needing a Telegram group. It posts as a
real chat_id if you set SMOKE_CHAT_ID (so you actually receive the reply);
otherwise it uses a dummy id and the outbound sendMessage will fail
server-side, which is fine — you're testing that the Lambda accepts and
processes the update (look for 200 + a new row in DynamoDB + Bedrock logs).
"""
import json
import os
import sys
import urllib.error
import urllib.request

# Same modes, same env var names, same path suffixes as lambda_function.py.
MODES = {
    "movie": ("MOVIE_BOT_TOKEN", "MOVIE_WEBHOOK_SECRET"),
    "cleaning": ("CLEANING_BOT_TOKEN", "CLEANING_WEBHOOK_SECRET"),
    "salary": ("SALARY_BOT_TOKEN", "SALARY_WEBHOOK_SECRET"),
}


def _base_url():
    url = os.environ.get("FUNCTION_URL", "").strip().rstrip("/")
    if not url:
        sys.exit("FUNCTION_URL is not set (e.g. https://<id>.lambda-url.us-east-1.on.aws)")
    return url


def _configured():
    """Yield (mode, token, secret) only for modes whose env vars are present."""
    found = False
    for mode, (token_env, secret_env) in MODES.items():
        token = os.environ.get(token_env, "").strip()
        secret = os.environ.get(secret_env, "").strip()
        if token and secret:
            found = True
            yield mode, token, secret
        elif token or secret:
            print(f"  ! {mode}: only one of {token_env}/{secret_env} set — skipping")
    if not found:
        sys.exit("No bot configured. Set at least MOVIE_BOT_TOKEN and MOVIE_WEBHOOK_SECRET.")


def _post(url, payload, headers=None):
    data = json.dumps(payload).encode("utf-8")
    hdrs = {"Content-Type": "application/json", **(headers or {})}
    req = urllib.request.Request(url, data=data, headers=hdrs, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return r.status, r.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "replace")
    except Exception as e:  # noqa: BLE001 — surface network/timeout plainly
        return None, str(e)


def _tg(token, method, payload):
    status, body = _post(f"https://api.telegram.org/bot{token}/{method}", payload)
    try:
        return json.loads(body)
    except (ValueError, TypeError):
        return {"ok": False, "status": status, "description": body}


def cmd_set():
    base = _base_url()
    for mode, token, secret in _configured():
        target = f"{base}/{mode}"
        resp = _tg(token, "setWebhook", {
            "url": target,
            "secret_token": secret,
            "allowed_updates": ["message", "edited_message", "channel_post"],
            "drop_pending_updates": True,
        })
        ok = "OK " if resp.get("ok") else "ERR"
        print(f"[{ok}] {mode} -> {target}: {resp.get('description', resp)}")


def cmd_info():
    for mode, token, _ in _configured():
        resp = _tg(token, "getWebhookInfo", {})
        r = resp.get("result", resp)
        print(f"--- {mode} ---")
        print(f"  url:               {r.get('url')}")
        print(f"  pending_updates:   {r.get('pending_update_count')}")
        print(f"  last_error_date:   {r.get('last_error_date')}")
        print(f"  last_error_message:{r.get('last_error_message')}")


def cmd_delete():
    for mode, token, _ in _configured():
        resp = _tg(token, "deleteWebhook", {"drop_pending_updates": False})
        print(f"[{mode}] deleteWebhook: {resp.get('description', resp)}")


def cmd_smoke():
    """POST a synthetic '/movie Rear Window' update directly at the Function URL."""
    base = _base_url()
    secret = os.environ.get("MOVIE_WEBHOOK_SECRET", "").strip()
    if not secret:
        sys.exit("smoke needs MOVIE_WEBHOOK_SECRET (must match the Lambda env var)")
    chat_id = int(os.environ.get("SMOKE_CHAT_ID", "-1000000000000"))
    real = "SMOKE_CHAT_ID" in os.environ
    update = {
        "update_id": 1,
        "message": {
            "message_id": 1,
            "date": 0,
            "chat": {"id": chat_id, "type": "group", "title": "smoke-test"},
            "from": {"id": 1, "first_name": "Smoke", "is_bot": False},
            "text": "/movie Rear Window",
        },
    }
    status, body = _post(
        f"{base}/movie", update,
        headers={"X-Telegram-Bot-Api-Secret-Token": secret},
    )
    print(f"POST {base}/movie -> HTTP {status}: {body}")
    if status == 200:
        print("  Lambda accepted the update. Check: a new film row in DynamoDB,")
        print("  Bedrock/Letterboxd calls in CloudWatch logs" +
              (", and the reply in your chat." if real
               else ". (Set SMOKE_CHAT_ID to a real chat to receive the reply.)"))
    elif status == 403:
        print("  403 = secret mismatch. MOVIE_WEBHOOK_SECRET here must equal the Lambda's env var.")
    elif status == 404:
        print("  404 = the /movie path didn't route. Check FUNCTION_URL has no trailing path.")


COMMANDS = {"set": cmd_set, "info": cmd_info, "delete": cmd_delete, "smoke": cmd_smoke}


def main():
    if len(sys.argv) != 2 or sys.argv[1] not in COMMANDS:
        sys.exit(f"usage: {sys.argv[0]} {{{'|'.join(COMMANDS)}}}")
    COMMANDS[sys.argv[1]]()


if __name__ == "__main__":
    main()
