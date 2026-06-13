# Going live — Gracia bots

Deploy is automated (push to `main` → GitHub Actions → `aws lambda update-function-code`).
But the Lambda is **inert** until the runtime plumbing below exists. Do these once
per AWS account; after that, code changes ship on every push to `main`.

Order matters — each step depends on the one above it.

## 0. The bot in Telegram (BotFather)
The `movie` bot already exists: **Sir Watchalot** — `@SirWatchalot_bot`
(t.me/SirWatchalot_bot). Its token is the `MOVIE_BOT_TOKEN` env var in step 3.
For future bots (cleaning/salary), repeat `/newbot` in @BotFather.

Two BotFather settings on Sir Watchalot:
- `/setprivacy` → **Enable** — in groups the bot only sees commands and
  replies/mentions, which is what the free-form handler assumes.
- `/setcommands` → paste:
  ```
  movie - Add a film to the list
  movies - Show the group's film list
  draw - Pick one at random
  ```

> The token never goes in this repo — only in the Lambda env var (step 3).

## 1. DynamoDB table
Create the table the handler reads/writes (`DDB_TABLE`, default `GraciaBotData`):

- **Table name:** `GraciaBotData`
- **Partition key:** `PK` — type **String**
- **Sort key:** `SK` — type **String**
- **Capacity:** On-demand (pay-per-request) is plenty

The code stores everything as `PK = "<mode>#<chat_id>"`, `SK = "chat"` or `"film#<uuid>"`.
No secondary indexes needed.

## 2. IAM permissions on the Lambda role
Role: `gracia-bots-role-dsuzb4hf`. It needs three things:

- **CloudWatch Logs** — comes with the default Lambda execution role.
- **DynamoDB** — ✅ already added. Scoped to the `GraciaBotData` table:
  `GetItem`, `PutItem`, `DeleteItem`, `Query`.
- **Bedrock** — ⚠️ **still needed for AI replies.** Add `bedrock:InvokeModel`
  (and `bedrock:InvokeModelWithResponseStream`) for the model in `BEDROCK_MODEL_ID`
  (`us.anthropic.claude-sonnet-4-6`). Because that's an inference profile, allow
  both the profile ARN and the underlying foundation model ARN.
  Also enable model access for it in the **Bedrock console → Model access** (one-time).

> Without Bedrock, the bot still works: `/movie`, `/movies`, `/draw` and the plain
> add-reply all run. Only free-form chat and the conversational add go quiet.
> (`BEDROCK_MODEL_ID=""` disables AI cleanly.)

## 3. Lambda environment variables
Configuration → Environment variables:

| Variable | Required | Notes |
|---|---|---|
| `MOVIE_BOT_TOKEN` | ✅ | From BotFather |
| `MOVIE_WEBHOOK_SECRET` | ✅ | Any long random string; must match step 5 |
| `DDB_TABLE` | optional | Defaults to `GraciaBotData` |
| `TMDB_API_KEY` | optional | Adds runtime/genres/synopsis/similar + rating fallback |
| `BEDROCK_MODEL_ID` | optional | Defaults to `us.anthropic.claude-sonnet-4-6`; set `""` to disable AI |
| `CLEANING_BOT_TOKEN` / `CLEANING_WEBHOOK_SECRET` | later | Only when the cleaning bot is built |
| `SALARY_BOT_TOKEN` / `SALARY_WEBHOOK_SECRET` | later | Only when the salary bot is built |

`AWS_REGION` is set by Lambda automatically — don't add it.

## 4. Lambda Function URL
Configuration → Function URL → Create:

- **Auth type:** `NONE` (the endpoint is public — the per-bot `secret_token` is what
  authenticates Telegram; the handler rejects any request without the matching header).
- Copy the URL, e.g. `https://abc123.lambda-url.us-east-1.on.aws`.

The handler routes on the **last path segment**, so Telegram must hit
`<function-url>/movie` (and `/cleaning`, `/salary` later).

## 5. Register the Telegram webhooks
From your laptop (needs only Python 3, no deps):

```bash
export FUNCTION_URL="https://abc123.lambda-url.us-east-1.on.aws"
export MOVIE_BOT_TOKEN="...from BotFather..."
export MOVIE_WEBHOOK_SECRET="...same string as the Lambda env var..."

python tools/setup_webhooks.py set     # register
python tools/setup_webhooks.py info    # verify Telegram can reach the URL
```

`info` is the truth-teller: `url` should show `<function-url>/movie`, and
`last_error_message` should be empty. A `403`/`Wrong response` there means the
secret doesn't match between BotFather-side and the Lambda env var.

In BotFather, also set the bot's **group privacy** as you intend: privacy ON
(default) means the bot only sees commands and replies/mentions in groups — which
is what the free-form handler comment assumes.

## 5b. Schedule the morning-after rating poll (EventBridge)
The deploy pipeline only updates Lambda **code**, so the daily trigger is created
once by hand — the same way the webhook is. It invokes the Lambda each morning with
`{"task": "morning_after"}`; the handler (`_is_scheduled_event` → `run_morning_after`)
scans the chat registry, finds recent un-rated winners, and posts the 5★ rating poll
so votes feed `get_ratings`.

```
python tools/setup_schedule.py set       # create the rule + target + invoke permission
python tools/setup_schedule.py info       # show the rule and its target
python tools/setup_schedule.py test       # invoke once now with the cron payload
python tools/setup_schedule.py console    # print exact Console/CLI steps to do it by hand
```

Rule `gracia-bots-morning-after`, `cron(0 6 * * ? *)` = 06:00 UTC ≈ 09:00 Kyiv
(summer). Classic rules fire on fixed UTC, so local time drifts ~1h across DST — for
exact 09:00 Kyiv year-round use EventBridge **Scheduler** with
`ScheduleExpressionTimezone="Europe/Kyiv"` (see `setup_schedule.py console`). It posts
at most once per winner (a `morning_poll_posted` flag on the history row) and never
re-posts once anyone has rated it.

## 6. Verify end-to-end
Two ways:

- **Without a group:** `python tools/setup_webhooks.py smoke` posts a synthetic
  `/movie Rear Window` straight at the Function URL. Expect `HTTP 200`, a new
  `film#…` row in DynamoDB, and Bedrock/Letterboxd calls in CloudWatch. Set
  `SMOKE_CHAT_ID` to a real chat id to also receive the reply.
- **For real:** add the bot to a group and send `/movie Rear Window`. You should
  get a warm reply with the Letterboxd rating and a similar-film suggestion.

## Watch items (not blockers)
- **Latency vs. Telegram retries:** the handler replies `200` only after the
  Bedrock loop + Letterboxd scrape finish. If that runs long, Telegram may resend
  the update. The function timeout is 123s; if you see duplicate replies, that's
  the cause — revisit by acking fast and doing work async.
- **Letterboxd scraping** can break if their markup changes or the Lambda IP is
  blocked; TMDB is the rating fallback (needs `TMDB_API_KEY`).
