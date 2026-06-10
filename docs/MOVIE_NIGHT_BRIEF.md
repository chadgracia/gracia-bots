# Brief for Claude Code — Movie Night game management in `movie` mode (bot: SirWatchalot_bot)

> **⚠️ SUPERSEDED (2026-06-09).** The shipped build now follows the **"Movie mode —
> full build spec"**: **natural language, no slash commands, privacy OFF (bot is a
> group admin)**, and a reaction/poll-driven state machine
> `IDLE → JOINING → SELECTING → VETO → DONE` (inline Join/Start buttons, 👍/👎
> reactions on selection cards, a 90s non-anonymous veto **poll** per candidate with
> a no-scheduler backstop). See `lambda_function.py` (`on_message`, `on_callback`,
> `_on_reaction`, `on_poll_answer`, `on_poll`, `_begin_selection`, `_begin_veto`).
> The command-based, privacy-ON design described below is **historical** — kept for
> the rationale and the Telegram/idempotency hard-requirements, which still apply.

This extends the existing `movie` mode in the telegram-bots-deploy Lambda. It does **not**
replace the platform conventions in the project overview — those still hold (mode routing,
shared core, DynamoDB key scheme, Bedrock via env `BEDROCK_MODEL_ID`, no hardcoded chat_id,
no secrets in repo). Read this against the project overview, not instead of it.

---

## 0. The decision that shapes everything: privacy mode ON

The original `MOVIE_NIGHT__Game_Rules` doc was written for a bot that reads **every** message
in the group. This platform runs with **Telegram privacy mode ON, no admin**. With privacy ON,
a bot in a group receives only:

1. messages that start with a slash command (`/...`),
2. messages that **@mention the bot** by username,
3. **replies to one of the bot's own messages**,
4. service messages (member joined/left, group→supergroup migration).

It does **not** receive ambient chat. So the rules' free-text detection — watching for "veto",
"nope", "let's go", scanning history for posted movies — is not implementable as written. Every
player action in the game must be a command, an @mention, or a reply to a bot message. The whole
flow below is built on that. Do not introduce any step that depends on reading ambient messages.

---

## 1. What changes vs. the current `movie` mode

The current spec stores a single group list. The new requirement is **per-player libraries**, and
the game selects **3 films from each participating player**. Concrete changes:

- `/movie <title>` now adds to the **sender's own library** within this chat (attributed to the
  sender), not a shared group list.
- Add `/library [@user]` (or `/movies`) to show the sender's library, or another player's if named.
- A library can be large; never dump it all into one message — paginate or summarize. Selection of
  3 happens at game time, in code.
- The game is a new, stateful, multi-message flow (sections 3–5).

Keep `/draw` semantics (random pick **in code**, never the LLM) — the game's selection and pick
reuse that principle.

---

## 2. Data model additions (one DynamoDB table, existing key scheme)

`PK = "movie#{chat_id}"`. Item types via `SK`:

- **Member**: `SK = "member#{user_id}"` → `{ display_name, username (nullable), first_seen }`.
  Capture this every time a user interacts, so the bot can address/mention people later.
- **Library item**: `SK = "lib#{user_id}#{film_uuid}"` → `{ title, year, added_by=user_id,
  added_at, watched (bool), times_vetoed (int) }`.
- **Game session**: `SK = "game#current"` (single active game per chat) →
  `{ session_id, phase, participants:[user_id], selections:{user_id:[film_uuid]},
  message_index:{ message_id: {kind, user_id, film_uuids} }, pool:[film_uuid],
  vetoes_left:{user_id:int}, picked:film_uuid, status }`.
- **History**: `SK = "history#{session_id}"` → `{ winner, watched_date, participants,
  ratings:{}, poll_message_id }`. Ratings filled later by the morning poll.

`message_index` is the routing table: when an incoming update is a reply, look up
`reply_to_message.message_id` here to know which player/film(s)/phase the reply belongs to. This is
how confirmation/veto work under privacy mode.

**Decision needed — library scope.** Under this key, libraries are per `(chat, user)`. If you want a
player's library to follow them across groups, that needs a different key (`PK = "user#{user_id}"`)
and a join at game time. The seed file lists people (Chad, Alberto, …) not tied to a group, so
confirm which you want. I recommend per-`(chat, user)` to match the existing scheme; flag if not.

**Decision needed — seeding existing libraries.** The current `Library_of_movies_to_start_with`
JSON keys films by display name. The bot can't know "Chad" = which Telegram `user_id` until that
person interacts or you map it by hand. Plan a one-time seed step that maps names → user_ids; don't
let the bot silently attribute films to the wrong account.

---

## 3. Game lifecycle (command + reply driven)

Command names below are proposals — rename freely. The mechanics matter, not the spelling.

### Phase 1 — Start & roster
- `/movienight` (or `@SirWatchalot_bot let's play`) creates `game#current` for this chat, `phase=roster`.
- Participants join via `/join`. Auto-add anyone who runs `/movie` or `/join` during the night.
- Each participant gets **one veto for the night** (`vetoes_left[user_id]=1`), whether or not their
  library contributes films (mirrors the old "non-submitters still get a veto" rule).
- Bot announces who's in and that selection is about to happen. There is no "scan history" step —
  films come from libraries, not from the chat.
- **Decision needed:** is the roster (a) explicit `/join` only, or (b) auto = every known member with
  a library? I recommend explicit `/join` so veto counts stay bounded and predictable.

### Phase 1.5: Constraints (optional)
Once the player list is settled, ask once: "Any constraints tonight? Length, genre, or
year range — or just say go." Wait up to 60 seconds.

* No replies in 60s → proceed with no filter, exactly as normal.
* Replies → parse each into a filter ("no documentaries", "under 2.5 hours", "nothing
  before 1960") and combine everyone's together.
* Apply the combined filter as the eligibility gate for the Pick: only films that fit can
  be drawn. Year is exact from the library; genre/runtime use looked-up metadata; when a
  film's metadata is unknown, keep it rather than drop it.
* If nothing fits, say so and offer to relax one constraint or play unfiltered — never
  silently ignore the filter. An explicit "go" / "let's pick" closes the window early.

Implementation notes (this repo): the live flow is JOINING → **CONSTRAINTS** → SELECTING
→ VETO → DONE; the filter gates the VETO candidate pool (the `random.choice` Pick). The
60s window uses the **lazy backstop** — it closes on the next inbound event past the
deadline (no scheduler/EventBridge), matching the veto round. Downside: if the group goes
silent the window doesn't advance until someone speaks; the host can always say "go".

### Phase 2 — Selection ("3 from each")
- For each participant with a non-empty library, code does `random.sample(library, min(3, len))`.
  **In Python, not the LLM.** Store the chosen `film_uuid`s in `selections` so re-invocations are
  stable (Lambda is stateless; never re-roll on a retry).
- **Decision needed:** exclude films already `watched` (in history)? Deprioritize `times_vetoed > 0`?
  The seed data has "previously vetoed" notes, so this is a real choice. I'd exclude watched and
  allow (not exclude) previously vetoed. Confirm.

### Phase 3 — Confirmation (sequential, one person at a time — CURRENT design)
The implemented flow (supersedes the per-card/"post all cards" sketch below):
- The bot confirms **one player at a time**, in roster order (`sel_order` / `sel_idx` on the
  session). It does **not** post everyone's cards at once.
- For the current player it sends **a single message** listing their drawn films (each with the
  Phase-2 metadata — title, year, genre, runtime, ★ rating) and the framing line: *"I picked these
  three from your library — are these what you want to share with the group tonight, or should we
  swap some?"*
- The player replies with **emojis in order, one per film** — `👍` keep / `👎` swap — parsed **in
  code** (`_parse_confirm_tokens`, deterministic). A single `👍` (or `yes`) keeps all.
  - Any `👎` → that slot is swapped for another **eligible** film (`_draw_eligible`, filter-aware)
    and the updated slate is re-posted to the same player.
  - All `👍` → that player is **locked** and the bot moves to the next person.
  - Wrong number of marks → ask them to resend N marks.
- When every player is locked → Phase 4/5 (veto). Mentioning prefers `@username`.

*(Historical sketch — no longer accurate: the bot posts one card per film with 👍/👎 reactions and
"posts all cards before waiting." Replaced by the sequential single-message flow above.)*

### Phase 4 — Lock
- `/lock` (host) or once everyone has confirmed: drop all `👎` films, optionally backfill from
  libraries to keep ~3 each, build `pool`, post the final list, ask for `go`.
- "go" must be a command or a reply to the lock message (`/go` or reply `go`/`👍`), not ambient text.

### Phase 5 — The pick + veto
- Code does `random.choice(pool)` (**not** the LLM). Announce the pick with a full info card.
- Veto must be explicit: `/veto`, or a **reply** to the pick message with `veto`/`❌`. On veto:
  confirm it, decrement `vetoes_left` for that user, mark "out of vetoes," remove the film, re-pick.
  Reject a second veto from the same user ("you already played your veto").
- **No 60-second wait.** Lambda has no long-running process. Two options:
  - (recommended) the pick stands until someone vetoes or someone confirms with `/watch` (or reply
    `✅`). Clean, no scheduler, and explicit in a group.
  - (alternative) schedule a one-off EventBridge callback ~60s out that re-invokes the Lambda to
    finalize if no veto landed. More moving parts; only do this if you specifically want the timer.
- Pool exhausted: `random.choice` from the vetoed pile, no more vetoes, finalize.

### Phase 6 — Winner
- Announce winner with a full info card **plus a spoiler-free context note** (production facts,
  trivia, legacy — never plot/endings). The LLM may write this note; it must not pick the winner.
- Write `history#{session_id}` (winner, date, participants, empty ratings). Clear `game#current`.

---

## 4. Telegram / Lambda hard requirements (this is where the old setup failed)

These map directly to the failures catalogued in the third-party doc. Treat them as acceptance
criteria, not nice-to-haves.

- **Never hardcode chat_id.** Read/write it from DynamoDB. Every "chat not found" / "went silent"
  failure in the old logs traced to a stale or wrong chat_id.
- **Supergroup migration** must be handled in the shared send helper (already in the overview):
  catch the 400 "group chat was upgraded to a supergroup chat", read `parameters.migrate_to_chat_id`,
  update the stored chat_id, retry once. Also handle `migrate_to_chat_id` / `migrate_from_chat_id`
  on **incoming** service updates and rewrite stored state to the new id. Both the movie group and
  the cleaning group migrated in the old logs — assume it will happen.
- **Idempotency.** Telegram retries webhook deliveries. Dedupe on `update_id` (store last processed,
  or a short-TTL per-update marker) so a retry never double-adds a film, double-counts a veto, or
  re-rolls a selection. The old system double-processed messages against stale sessions; don't
  reproduce that.
- **No in-process state, no wait loops.** Each update is a fresh invocation. All "waiting" is just
  state persisted in `game#current` between invocations. Anything in the rules phrased as "wait N
  seconds" or "watch the room" must become persisted state + an explicit trigger (or an EventBridge
  schedule).
- **Reply routing.** On every incoming update, if it's a reply, resolve
  `reply_to_message.message_id` against `message_index` in `game#current`. If it matches, route to the
  right phase/player. If not, ignore. This is the backbone of confirmation and veto under privacy ON.
- **Verify the per-mode secret token** (`X-Telegram-Bot-Api-Secret-Token`) before doing anything —
  the Function URL is public.

---

## 5. LLM (Bedrock) boundaries

Same philosophy as salary mode (model parses/communicates; code decides):

- **LLM does:** the conversational confirmation/nudge wording, the spoiler-free winner context note,
  free-form "film-night helper" replies when @mentioned.
- **LLM does not:** select the 3-per-player, count vetoes, or pick the winner. All randomness and all
  game-state transitions are plain Python. Emoji/yes-no confirmation parsing is code, not LLM, so it's
  deterministic.
- `lookup_film` stays isolated: Letterboxd for the rating, TMDB (`TMDB_API_KEY`) for everything else,
  swappable later.

---

## 6. Attribution (simplified by going command-based)

The old rule ("a movie belongs to whoever sent the message; handle quoted/replied text") was a
message-scanning artifact. With commands, ownership is simply the **command sender**: `/movie <title>`
→ `added_by = sender user_id`. No inference across messages. Drop the quote/reply attribution logic.

---

## 7. Open decisions to confirm before building

1. Library scope: per-`(chat, user)` (recommended) vs. global per user.
2. Roster: explicit `/join` (recommended) vs. auto-include every member with a library.
3. Selection filters: exclude `watched`? include/exclude previously vetoed?
4. Veto finalization: explicit confirm (recommended) vs. EventBridge 60s callback.
5. How to seed the existing named libraries → real Telegram `user_id`s.

---

## 7a. Decisions confirmed (2026-06-09)

1. **Library scope:** per-`(chat, user)`. Keep `PK = "movie#{chat_id}"`.
2. **Roster:** explicit `/join` only (anyone who runs `/movie` is auto-joined too).
   Veto counts are bounded to joined participants.
3. **Selection eligibility (per participant library):**
   - Always **exclude `watched`** films (i.e. past winners).
   - **Veto-aware exclusion:** a film vetoed by user X is excluded from selection
     **only if** X is among the current participants **and** the library still has
     other eligible options. If excluding it would leave nothing, the vetoed film
     becomes eligible again. → Requires tracking **`vetoed_by: [user_id]`** on each
     library item (a set of who vetoed it), not just a `times_vetoed` count.
4. **Finalization: explicit, with flavor text (no real timer).** After the pick, the
   bot posts the card plus a static nudge — **"⏳ You have 60 seconds — veto or press
   play!"** — but there is **no actual clock and no scheduler** (keeps us on zero new
   infra). The pick simply stands until: a `/veto` (or reply `veto`/❌ to the pick)
   removes the film, decrements that user's veto, and re-picks; or an explicit
   "press play" (`/watch`, or reply `✅`/`go`) finalizes the winner. A second veto from
   the same user is rejected. Pool exhausted → pick from the vetoed pile, no more vetoes,
   finalize.

## 8. Test checklist (run before declaring it works — the old setup "passed" while broken)

- [ ] `/movie` in a group with privacy ON is received and attributed to the sender.
- [ ] A reply to a bot card is received and routed to the correct player/phase.
- [ ] An @mention with no slash command is received.
- [ ] An ambient (non-command, non-mention, non-reply) message is correctly **not** received — confirm
      the flow doesn't depend on it.
- [ ] Selection produces exactly `min(3, library_size)` per participant, stable across a retried update.
- [ ] Duplicate `update_id` does not double-add / double-veto / re-roll.
- [ ] Force a supergroup migration (or simulate the 400 + `migrate_to_chat_id`): send retries to the
      new id and stored chat_id is updated.
- [ ] Second veto from the same user is rejected; veto by a user with one left removes the film and
      re-picks.
- [ ] Winner is written to history and `game#current` is cleared.
- [ ] Morning poll / weekly nudge run as scheduled (EventBridge) invocations that read chat_id from
      DynamoDB — not as webhook events, and not against a hardcoded id.
