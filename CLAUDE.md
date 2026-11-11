# gracia-bots — project memory

Multi-mode Telegram bot platform on a single AWS Lambda. One handler
(`lambda_function.py`) backs several BotFather identities, routed by the path
suffix of the Lambda Function URL (`.../movie`, `.../cleaning`, `.../salary`).

## Modes
- **movie** — bot **@SirWatchedALot_bot** (was @SirWatchalot_bot; that account died with Chad's old Telegram account). Film-night game (per-player libraries,
  stateful multi-phase game). Full spec: `docs/MOVIE_NIGHT_BRIEF.md`.
- **cleaning** — stub. Intended: Ukrainian room-by-room checklist + daily tracking.
- **salary** — stub. Intended: LLM parses time entries, **Python does all pay math**.

## Deploy (do not break this)
- GitHub Actions → Lambda. Push to `main` runs `.github/workflows/deploy.yml`,
  which assumes the shared OIDC role `github-actions-deploy` and runs
  `aws lambda update-function-code --function-name gracia-bots`.
- The deploy zips **only `lambda_function.py`** (`zip -j function.zip lambda_function.py`).
  **Everything that must ship has to live in that one file** — adding helper
  modules silently drops them from the package. Keep the handler single-file.
- Account `271378210266`, region `us-east-1`. Full deploy reference + failure
  modes (esp. wrong `--function-name` clobbering other repos): see the
  `lambda-deploy` skill / `docs/DEPLOY.md` if present.

## Platform invariants (apply to every mode)
- **Telegram privacy mode is per-bot.** **movie** runs privacy **OFF** (or bot = admin):
  it reads every group message and routes them to the LLM for intent — the
  natural-language design needs this. Modes that keep privacy **ON** (the default)
  receive only slash-commands, @mentions, replies to the bot's own messages, and
  service messages, and must **never** write logic that depends on ambient chat.
  Changing the setting only takes effect after the bot is removed and re-added to the
  group. (In private 1:1 DMs the bot always sees all messages regardless.)
- **Never hardcode chat_id.** Persist a chat registry in DynamoDB and resolve it
  from there (proactive senders + supergroup migration depend on this).
- **Handle supergroup migration** in the send helper: on the 400 "upgraded to
  supergroup", read `parameters.migrate_to_chat_id`, rewrite stored state, retry once.
  Also handle `migrate_to_chat_id`/`migrate_from_chat_id` on incoming updates.
- **Idempotency:** Telegram retries deliveries. Dedupe on `update_id` so retries
  never double-add / double-count / re-roll.
- **No in-process waiting.** Each update is a fresh invocation; all "waiting" is
  state persisted in DynamoDB + an explicit trigger (or an EventBridge schedule).
- **Verify the per-mode secret token** (`X-Telegram-Bot-Api-Secret-Token`) before
  doing any work — the Function URL is public.
- **LLM vs. code split:** the model parses/communicates; **code decides.** All
  randomness, money math, vote/veto counting, and state transitions are plain
  Python. The model never picks winners, computes pay, or counts.
- **No secrets in the repo.** Tokens, webhook secrets, API keys live in Lambda
  env vars only.

## Data
- Single DynamoDB table `GraciaBotData`, keys `PK` (string) / `SK` (string).
- Movie mode key scheme: `PK = "movie#{chat_id}"`, item type via `SK`
  (`member#…`, `lib#…`, `game#current`, `history#…`). See the brief.

## Infra config (set up in AWS/Telegram, not in repo) — see `SETUP.md`
DynamoDB table, Bedrock invoke permission on the execution role, Lambda env vars,
Function URL (auth NONE), and the Telegram webhook (`tools/setup_webhooks.py`).

## SECURITY RULES — MANDATORY (read before any change)

This repo is PUBLIC, and so are its GitHub Actions logs. Secrets have leaked from Chad's repos 5-6 times. Never again.

1. No secret value ever goes in code, tests, docs, comments, commit messages, PR text or logs. That includes API keys, admin keys, HMAC/signing secrets, tokens, passwords, and any URL containing `key=`, `token=` or `sso=`. Secrets come from `os.environ[...]` (Lambda environment variables) or `s3://pipeline-token/` only. Tests use obvious fakes like `"test-key"`.
2. Every `aws lambda ...` command in a workflow must end with `> /dev/null`. Lambda API responses include every environment variable, and the logs are public. Never print the environment (`printenv`, `env`, `set -x`, or `print(os.environ)`).
3. Never log or print a secret, even partially, or a request URL that carries one.
4. `.github/scripts/secret_guard.py` runs automatically before every `git commit` and `git push` (via `.claude/settings.json`), and again in CI. If it blocks you, remove the secret and move the value to an environment variable. NEVER bypass, weaken, delete or skip the guard, and never use `--no-verify` or `secret-guard: allow` to get past it without Chad's explicit approval.
5. A new secret needs Chad to add a Lambda environment variable first. Tell him the exact variable name. Code that reads it ships after that.
6. Before finishing, run `python3 .github/scripts/secret_guard.py --tree` and report the result.
