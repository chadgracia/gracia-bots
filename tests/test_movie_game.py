"""Local tests for movie-mode (natural-language + reaction/poll game engine).

These do NOT ship (deploy zips only lambda_function.py). They drive the engine
with in-memory fakes for DynamoDB + Telegram + Letterboxd/Bedrock, so the
deterministic parts — library, seeding/claim, selection, veto, win resolution,
and the no-scheduler backstop — run without AWS or network.
Run: python3 tests/test_movie_game.py
"""
import copy
import json
import os
import sys
import types

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Stub boto3 / botocore so the module imports without the AWS SDK installed.
_boto3 = types.ModuleType("boto3")
_boto3.resource = lambda *a, **k: types.SimpleNamespace(Table=lambda *a, **k: object())
_boto3.client = lambda *a, **k: object()
_cond = types.ModuleType("boto3.dynamodb.conditions")
_cond.Key = lambda *a, **k: None
_ddbmod = types.ModuleType("boto3.dynamodb")
_ddbmod.conditions = _cond
_boto3.dynamodb = _ddbmod
sys.modules.update({"boto3": _boto3, "boto3.dynamodb": _ddbmod,
                    "boto3.dynamodb.conditions": _cond})
_botocore = types.ModuleType("botocore")
_bexc = types.ModuleType("botocore.exceptions")


class _ClientError(Exception):
    pass


_bexc.ClientError = _ClientError
_botocore.exceptions = _bexc
sys.modules.update({"botocore": _botocore, "botocore.exceptions": _bexc})

import lambda_function as L  # noqa: E402

CHAT = -1001234567890
MODE = "movie"

STORE = {}
SENT = []
POLLS_STOPPED = []
_mid = [1000]
_pid = [0]
NOW = [1_000_000]


def _reset():
    STORE.clear()
    SENT.clear()
    POLLS_STOPPED.clear()
    _mid[0] = 1000
    _pid[0] = 0
    NOW[0] = 1_000_000


# ---- in-memory fakes ------------------------------------------------------ #
def fake_get(pk, sk):
    return copy.deepcopy(STORE.get((pk, sk)))


def fake_put(item):
    STORE[(item["PK"], item["SK"])] = copy.deepcopy(item)
    return item


def fake_delete(pk, sk):
    STORE.pop((pk, sk), None)


def fake_query(pk):
    return [copy.deepcopy(v) for (p, _s), v in STORE.items() if p == pk]


def fake_scan():
    return [copy.deepcopy(v) for v in STORE.values()]


def fake_seen(mode, chat_id, update_id):
    if update_id is None:
        return False
    key = (L._pk(mode, chat_id), f"dedupe#{update_id}")
    if key in STORE:
        return True
    STORE[key] = {"PK": key[0], "SK": key[1]}
    return False


def fake_send(mode, chat_id, text, **kwargs):
    _mid[0] += 1
    SENT.append((text, _mid[0]))
    return {"ok": True, "result": {"message_id": _mid[0]}}


def fake_send_poll(mode, chat_id, question, options, **kwargs):
    _mid[0] += 1
    _pid[0] += 1
    return {"ok": True, "result": {"message_id": _mid[0], "poll": {"id": f"poll{_pid[0]}"}}}


def fake_lookup(title, year=None):
    return {"found": True, "title": title, "slug": L._slugify(title),
            "year": str(year or "1950"), "runtime_min": 120, "genres": ["Drama"],
            "description": "A film.", "rating": 4.0, "rating_scale": 10,
            "rt_rating": "88%", "tmdb_id": 1, "alts": []}


L.ddb_get = fake_get
L.ddb_put = fake_put
L.ddb_delete = fake_delete
L.ddb_query = fake_query
L.ddb_scan = fake_scan
L.seen_update = fake_seen
L.send_message = fake_send
L.send_poll = fake_send_poll
L.stop_poll = lambda mode, chat_id, mid: POLLS_STOPPED.append(mid)
L.answer_callback = lambda *a, **k: {"ok": True}
L.edit_message_text = lambda *a, **k: {"ok": True}
L.lookup_film_cached = fake_lookup
L.lookup_film = fake_lookup
L.winner_note = lambda *a, **k: ""
L._now_epoch = lambda: NOW[0]
L.AI_ENABLED = False


# ---- event builders ------------------------------------------------------- #
def cb(user_id, data, message_id=None):
    return {"kind": "callback", "chat_id": CHAT, "user_id": user_id,
            "user_name": f"U{user_id}", "username": None, "callback_data": data,
            "callback_query_id": "cq", "message_id": message_id}


def reaction(user_id, message_id, emoji):
    return {"kind": "reaction", "chat_id": CHAT, "user_id": user_id,
            "user_name": f"U{user_id}", "message_id": message_id, "reactions": [emoji]}


def poll_answer(user_id, poll_id, option_ids):
    return {"kind": "poll_answer", "chat_id": CHAT, "user_id": user_id,
            "user_name": f"U{user_id}", "poll_id": poll_id, "poll_option_ids": option_ids}


def poll_closed(poll_id):
    return {"kind": "poll", "chat_id": CHAT, "poll_id": poll_id, "poll_is_closed": True}


def message(user_id, text):
    return {"kind": "message", "chat_id": CHAT, "user_id": user_id,
            "user_name": f"U{user_id}", "username": None, "text": text}


def cards_for(game, user_id):
    return [int(mid) for mid, e in game["cards"].items() if str(e["uid"]) == str(user_id)]


def _skip_constraints():
    """Constraints run the full ~60s (no early 'go') and are closed by the tick sweep.
    To reach SELECTING in tests, fast-forward past the deadline and fire a tick — the
    real clock-driven close path, even on total silence."""
    NOW[0] += L._CONSTRAINTS_WINDOW + 1
    L.run_tick()


# ---- tests ---------------------------------------------------------------- #
def test_add_to_library_is_slug_keyed_with_metadata():
    _reset()
    item, info = L.add_to_library(CHAT, 1, "Rear Window")
    assert item["slug"] == "rear-window"
    assert item["rating"] == "4.0"          # stored as string (DDB has no float)
    assert item["rating_scale"] == 10
    assert item["rt_rating"] == "88%"
    assert item["owner_id"] == 1
    lib = L.get_library(CHAT, 1)
    assert len(lib) == 1 and lib[0]["title"] == "Rear Window"
    assert L.get_library(CHAT, 2) == []
    # remove
    assert L.remove_from_library(CHAT, 1, "Rear Window") == "Rear Window"
    assert L.get_library(CHAT, 1) == []


def test_claim_links_seed_library():
    _reset()
    pk = L._pk(MODE, CHAT)
    for t in ["Red River", "Barry Lyndon"]:
        slug = L._slugify(t)
        STORE[(pk, f"lib#seed:chad#{slug}")] = {
            "PK": pk, "SK": f"lib#seed:chad#{slug}", "slug": slug,
            "owner_id": "seed:chad", "seed_name": "Chad", "title": t,
            "year": "1948", "watched": False, "added_at": "x"}
    assert L.list_seed_names(CHAT) == ["chad"]
    assert L.claim_library(CHAT, "Chad", 1) == {"status": "ok", "moved": 2}
    assert {f["title"] for f in L.get_library(CHAT, 1)} == {"Red River", "Barry Lyndon"}
    assert L.list_seed_names(CHAT) == []
    assert L.claim_library(CHAT, "Chad", 2)["status"] == "taken"
    assert L.claim_library(CHAT, "Chad", 1)["moved"] == 0
    assert L.claim_library(CHAT, "Nobody", 3)["status"] == "none"


def _two_player_game_to_veto():
    """Set up 2 players (1 film each), run join/start/selection -> VETO."""
    _reset()
    L.add_to_library(CHAT, 1, "Film A")
    L.add_to_library(CHAT, 2, "Film B")
    L.start_game(MODE, CHAT, 1)             # initiator 1 auto-joins
    L.handle_movie(MODE, cb(2, "join"))     # player 2 joins
    L.handle_movie(MODE, cb(1, "start"))    # -> CONSTRAINTS window
    _skip_constraints()                     # window times out -> SELECTING (asks player 1)
    assert L.get_game(CHAT)["phase"] == "SELECTING"
    L.handle_movie(MODE, message(1, "👍"))  # player 1 keeps (1 film) -> next player
    L.handle_movie(MODE, message(2, "👍"))  # player 2 keeps -> wildcard offered (2 players)
    assert L.get_game(CHAT)["phase"] == "WILDCARD"
    L.handle_movie(MODE, message(1, "pass"))  # decline the wildcard -> VETO (2-film pool)
    return L.get_game(CHAT)


def _veto_setup(players, pool):
    """Construct a game straight in the VETO phase. players: [uid] (each gets 1
    veto). pool: [(owner_uid, slug, title)]. Presents the first candidate."""
    _reset()
    game = L.new_game(CHAT, players[0])
    for p in players:
        L._add_player(game, p)
    game["phase"] = "VETO"
    game["status"] = "picking"
    entries = [{"owner": str(o), "slug": s, "title": t} for o, s, t in pool]
    game["pool_all"] = list(entries)
    game["pool"] = list(entries)
    L.put_game(game)
    if not L._present_candidate(MODE, CHAT, game):   # persist unless it short-circuited
        L.put_game(game)
    return L.get_game(CHAT)


def test_owner_blocked_then_nonowner_veto_consumes_and_repicks():
    game = _veto_setup([1, 2, 3], [(1, "a", "A"), (2, "b", "B"), (3, "c", "C")])
    cur = game["current"]
    owner, poll, slug = int(cur["film"]["owner"]), cur["poll_id"], cur["film"]["slug"]
    # Fix 2: the owner voting Veto is ignored, veto not consumed, pick stands
    L.handle_movie(MODE, poll_answer(owner, poll, [0]))
    g = L.get_game(CHAT)
    assert g["current"]["poll_id"] == poll and g["vetoes_remaining"][str(owner)] == 1
    # Fix 1: a non-owner with a veto -> pick vetoed, their veto consumed, re-pick
    voter = next(p for p in (1, 2, 3) if p != owner)
    L.handle_movie(MODE, poll_answer(voter, poll, [0]))
    g = L.get_game(CHAT)
    assert g["vetoes_remaining"][str(voter)] == 0
    assert g["current"]["poll_id"] != poll and g["current"]["film"]["slug"] != slug
    # that voter is now spent -> a second veto from them is ignored
    poll2 = g["current"]["poll_id"]
    L.handle_movie(MODE, poll_answer(voter, poll2, [0]))
    g2 = L.get_game(CHAT)
    assert g2.get("current") and g2["current"]["poll_id"] == poll2
    assert g2["vetoes_remaining"][str(voter)] == 0


# ---- Phase 1 fixes: roster-validated veto resolution + clarifying note ----- #
def test_owner_and_nonparticipant_vetoes_dont_block_win_with_note():
    # Scenario 1: owner vetoes own pick + a non-participant vetoes + everyone else
    # says fine -> the film WINS, with one line explaining why those taps didn't count.
    game = _veto_setup([1, 2, 3], [(1, "a", "A"), (2, "b", "B"), (3, "c", "C")])
    cur = game["current"]
    pid, owner = cur["poll_id"], int(cur["film"]["owner"])
    others = [p for p in (1, 2, 3) if p != owner]
    L.handle_movie(MODE, poll_answer(owner, pid, [0]))   # owner vetoes own pick -> ignored
    L.handle_movie(MODE, poll_answer(99, pid, [0]))      # a non-participant vetoes -> ignored
    assert any("not in tonight's game" in t for t, _ in SENT)
    L.handle_movie(MODE, poll_answer(others[0], pid, [1]))   # fine
    L.handle_movie(MODE, poll_answer(others[1], pid, [1]))   # all non-owners fine -> win
    assert L.get_game(CHAT) is None                          # WON despite 2 veto taps
    win = [t for t, _ in SENT if "winner" in t.lower()][-1]
    assert "didn't count" in win and "own pick" in win and "not in tonight's game" in win


def test_participant_veto_removes_pick_and_repicks():
    # Scenario 2: a real participant's single veto removes the pick and re-picks.
    game = _veto_setup([1, 2, 3], [(1, "a", "A"), (2, "b", "B"), (3, "c", "C")])
    cur = game["current"]
    pid, slug, owner = cur["poll_id"], cur["film"]["slug"], int(cur["film"]["owner"])
    voter = next(p for p in (1, 2, 3) if p != owner)
    L.handle_movie(MODE, poll_answer(voter, pid, [0]))
    g = L.get_game(CHAT)
    assert g is not None and g["vetoes_remaining"][str(voter)] == 0
    assert g["current"]["film"]["slug"] != slug and g["current"]["poll_id"] != pid


def test_nonparticipant_veto_gets_join_invite_not_already_used():
    # Scenario 3: a non-participant tapping veto is invited to join, never told
    # "already used", and removes nothing.
    game = _veto_setup([1, 2], [(1, "a", "A"), (2, "b", "B")])
    pid = game["current"]["poll_id"]
    L.handle_movie(MODE, poll_answer(77, pid, [0]))      # 77 never joined tonight
    msgs = " ".join(t for t, _ in SENT)
    assert "not in tonight's game" in msgs and "already used" not in msgs
    g = L.get_game(CHAT)
    assert g is not None and "77" not in g["vetoes_remaining"]   # nothing removed/created


def test_poll_close_decides_from_roster_not_raw_counts():
    # An invalid-only veto tally arriving via poll-close still declares the winner
    # (raw counts are a cross-check, the per-voter record is the decision).
    game = _veto_setup([1, 2], [(1, "a", "A"), (2, "b", "B")])
    cur = game["current"]
    pid, owner = cur["poll_id"], int(cur["film"]["owner"])
    L.handle_movie(MODE, poll_answer(owner, pid, [0]))   # only the owner vetoed (invalid)
    ev = poll_closed(pid); ev["poll_option_counts"] = [1, 0]   # poll shows 1 veto
    L.handle_movie(MODE, ev)
    assert L.get_game(CHAT) is None                          # winner, not blocked
    win = [t for t, _ in SENT if "winner" in t.lower()][-1]
    assert "didn't count" in win and "own pick" in win


def test_placeholder_reply_guard_suppresses_parentheticals():
    # bare / punctuated / markdown-wrapped / bracketed / lone-word silence -> suppressed
    for s in ["(silent)", "(silent).", "(no reply)", "(...)", "*(silent)*", "_silent_",
              "[silence]", "  silent  ", "Silent.", "(stay silent)"]:
        assert L._is_placeholder_reply(s), s
    # real replies (even ones that merely contain parentheses) -> sent
    for s in ["Add it? (yes/no)", "Sure, adding Dune (1984).", "Tokyo Story it is."]:
        assert not L._is_placeholder_reply(s), s


# ---- Task 2A: short-term conversation window ------------------------------ #
def test_convo_window_append_load_and_speaker_messages():
    _reset()
    L._convo_append(CHAT, "user", "Chad", "Let's watch Tokyo Story")
    L._convo_note(CHAT, "(posted a veto poll for the candidate Tokyo Story)")
    L._convo_append(CHAT, "user", "Asa", "is it the highest rated?")
    turns = L._convo_load(CHAT)
    assert [t["speaker"] for t in turns] == ["Chad", "SirWatchAlot", "Asa"]
    msgs = L._convo_messages(turns)
    assert [m["role"] for m in msgs] == ["user", "assistant", "user"]   # strict alternation
    assert "Chad: Let's watch Tokyo Story" in msgs[0]["content"][0]["text"]
    assert "Asa: is it the highest rated?" in msgs[2]["content"][0]["text"]


def test_convo_window_trims_to_max_turns():
    _reset()
    for i in range(L._CONVO_MAX_TURNS + 8):
        L._convo_append(CHAT, "user", f"U{i}", f"msg {i}")
    turns = L._convo_load(CHAT)
    assert len(turns) == L._CONVO_MAX_TURNS
    assert turns[-1]["text"] == f"msg {L._CONVO_MAX_TURNS + 7}"        # newest kept


def test_convo_window_drops_stale_turns_by_age():
    _reset()
    L._convo_append(CHAT, "user", "Old", "ancient")
    NOW[0] += L._CONVO_MAX_AGE_SEC + 10
    L._convo_append(CHAT, "user", "New", "fresh")
    assert [t["speaker"] for t in L._convo_load(CHAT)] == ["New"]


def test_convo_messages_collapses_consecutive_same_role():
    msgs = L._convo_messages([
        {"role": "user", "speaker": "Chad", "text": "one"},
        {"role": "user", "speaker": "Asa", "text": "two"},
        {"role": "assistant", "speaker": "SirWatchAlot", "text": "ok"},
    ])
    assert [m["role"] for m in msgs] == ["user", "assistant"]
    assert "Chad: one" in msgs[0]["content"][0]["text"]
    assert "Asa: two" in msgs[0]["content"][0]["text"]


# ---- Task 2B: past-picks history ------------------------------------------ #
def test_history_written_with_year_pool_and_queryable_tool():
    game = _two_player_game_to_veto()                 # 2-film pool, in VETO
    NOW[0] += L._VETO_WINDOW + 1                       # window lapses
    L.handle_movie(MODE, message(2, "go on then"))     # backstop -> winner
    assert L.get_game(CHAT) is None
    hist = L.get_history(CHAT, 10)
    assert len(hist) == 1
    h = hist[0]
    assert h["title"] in ("Film A", "Film B")
    assert h["year"] == "1950"                         # fake_lookup default year
    assert set(h["participants"]) == {1, 2}
    assert "Film A" in h["pool"] and "Film B" in h["pool"]
    out = L._dispatch_tool("get_history", {"limit": 5}, _ctx(1))   # the tool surfaces it
    assert out["count"] == 1 and out["history"][0]["title"] == h["title"]
    assert any("winner" in t["text"].lower() for t in L._convo_load(CHAT))  # salient note


def test_wildcard_excludes_past_winner():
    _reset()
    L.ddb_put({"PK": L._pk(MODE, CHAT), "SK": "history#prev",
               "winner_title": "Tokyo Story", "winner_slug": L._slugify("Tokyo Story"),
               "watched_date": "2026-01-01T00:00:00+00:00"})
    game = L.new_game(CHAT, 1)
    L._add_player(game, 1)
    L._add_player(game, 2)
    game["selection"] = {
        "1": {"locked": True, "slots": [{"slug": "film-a", "title": "Film A"}]},
        "2": {"locked": True, "slots": [{"slug": "film-b", "title": "Film B"}]},
    }
    sugg = L._build_wildcard(CHAT, game)
    assert sugg["slug"] != L._slugify("Tokyo Story")   # past winner excluded
    assert sugg["slug"] == L._slugify("Yi Yi")


def _wildcard_game_with_filter(**filter_overrides):
    game = L.new_game(CHAT, 1)
    L._add_player(game, 1)
    L._add_player(game, 2)
    game["selection"] = {
        "1": {"locked": True, "slots": [{"slug": "film-a", "title": "Film A"}]},
        "2": {"locked": True, "slots": [{"slug": "film-b", "title": "Film B"}]},
    }
    game["filter"].update(filter_overrides)
    return game


def test_wildcard_skipped_when_nothing_fits_the_filter():
    # Every fake-lookup film is Drama/120m/1950; a filter that excludes all of those
    # must yield NO wildcard rather than an off-filter suggestion.
    _reset()
    game = _wildcard_game_with_filter(exclude_genres=["drama"])
    assert L._build_wildcard(CHAT, game) is None
    _reset()
    game = _wildcard_game_with_filter(include_genres=["documentary"], min_runtime_min=201)
    assert L._build_wildcard(CHAT, game) is None


def test_wildcard_respects_filter_and_only_returns_a_fitting_film():
    _reset()
    game = _wildcard_game_with_filter(exclude_genres=["horror"])   # Drama passes this
    sugg = L._build_wildcard(CHAT, game)
    assert sugg is not None
    info = L.lookup_film_cached(sugg["title"], sugg.get("year"))
    assert L._passes_filter(info, game["filter"])                  # the pick truly fits


def test_no_eligible_vetoer_wins_immediately():
    # solo player can't veto their own pick -> nobody can veto -> instant winner
    _veto_setup([1], [(1, "a", "A")])
    assert L.get_game(CHAT) is None
    assert [v for (_p, s), v in STORE.items() if s.startswith("history#")]


def test_full_game_to_winner_via_poll_close():
    game = _two_player_game_to_veto()
    assert game["phase"] == "VETO"
    assert len(game["pool"]) == 1           # 2 locked films, one already presented
    cur = game["current"]
    assert cur and cur["poll_id"]
    # no veto -> poll auto-closes -> that film wins
    L.handle_movie(MODE, poll_closed(cur["poll_id"]))
    assert L.get_game(CHAT) is None         # game cleared
    hist = [v for (_p, s), v in STORE.items() if s.startswith("history#")]
    assert len(hist) == 1
    # winner is marked watched in its owner's library
    w = hist[0]
    assert L.get_film(CHAT, w["winner_owner_id"], w["winner_slug"])["watched"] is True


def test_veto_consumes_and_repicks():
    # 3 films so a re-pick still has a live poll (covered deeper in
    # test_owner_blocked_then_nonowner_veto_consumes_and_repicks)
    game = _veto_setup([1, 2, 3], [(1, "a", "A"), (2, "b", "B"), (3, "c", "C")])
    cur = game["current"]
    owner, first_poll, first_film = int(cur["film"]["owner"]), cur["poll_id"], cur["film"]["slug"]
    voter = next(p for p in (1, 2, 3) if p != owner)
    L.handle_movie(MODE, poll_answer(voter, first_poll, [0]))
    g = L.get_game(CHAT)
    assert g["vetoes_remaining"][str(voter)] == 0
    assert g["current"]["poll_id"] != first_poll and g["current"]["film"]["slug"] != first_film


def test_stale_poll_close_is_ignored():
    game = _veto_setup([1, 2, 3], [(1, "a", "A"), (2, "b", "B"), (3, "c", "C")])
    cur = game["current"]
    owner, first_poll = int(cur["film"]["owner"]), cur["poll_id"]
    voter = next(p for p in (1, 2, 3) if p != owner)
    L.handle_movie(MODE, poll_answer(voter, first_poll, [0]))   # valid veto -> re-pick
    assert L.get_game(CHAT)["current"]["poll_id"] != first_poll
    # the vetoed poll's own auto-close arrives late: must NOT declare a winner
    L.handle_movie(MODE, poll_closed(first_poll))
    g = L.get_game(CHAT)
    assert g is not None and g["phase"] == "VETO"


def test_backstop_resolves_after_window_on_any_update():
    game = _two_player_game_to_veto()
    cur = game["current"]
    NOW[0] += L._VETO_WINDOW + 1              # candidate is now stale
    L.handle_movie(MODE, message(2, "anything at all"))   # any update fires backstop
    assert L.get_game(CHAT) is None           # no valid veto -> winner declared, game cleared


def test_thumbs_down_swaps_a_slot_and_reasks():
    _reset()
    for t in ["A", "B", "C", "D"]:
        L.add_to_library(CHAT, 1, t)
    L.start_game(MODE, CHAT, 1)
    L.handle_movie(MODE, cb(1, "start"))
    _skip_constraints()                         # -> SELECTING, player 1 asked
    sel = L.get_game(CHAT)["selection"]["1"]
    assert len(sel["slots"]) == 3 and len(sel["shown"]) == 3
    slot0_before = sel["slots"][0]["slug"]
    L.handle_movie(MODE, message(1, "👎👍👍"))  # swap slot 1, keep the rest
    sel = L.get_game(CHAT)["selection"]["1"]
    assert len(sel["shown"]) == 4               # a 4th film was drawn in
    assert sel["slots"][0]["slug"] != slot0_before
    assert L.get_game(CHAT)["phase"] == "SELECTING"   # re-asked, not locked yet
    L.handle_movie(MODE, message(1, "👍👍👍"))  # keep all -> locks -> wildcard offered
    assert L.get_game(CHAT)["phase"] == "WILDCARD"
    L.handle_movie(MODE, message(1, "pass"))    # decline -> solo veto -> winner
    assert L.get_game(CHAT) is None


def test_selection_is_sequential_one_player_at_a_time():
    _reset()
    for t in ["A1", "A2", "A3", "A4"]:
        L.add_to_library(CHAT, 1, t)
    for t in ["B1", "B2", "B3", "B4"]:
        L.add_to_library(CHAT, 2, t)
    L.start_game(MODE, CHAT, 1)
    L.handle_movie(MODE, cb(2, "join"))
    L.handle_movie(MODE, cb(1, "start"))
    _skip_constraints()                              # -> SELECTING, player 1 first
    g = L.get_game(CHAT)
    assert g["sel_idx"] == 0 and L._current_selecting_uid(g) == 1
    # a reply from player 2 (not their turn yet) is ignored
    L.handle_movie(MODE, message(2, "👍👍👍"))
    g = L.get_game(CHAT)
    assert g["sel_idx"] == 0 and not g["selection"].get("2", {}).get("locked")
    # player 1 keeps -> advance to player 2
    L.handle_movie(MODE, message(1, "👍👍👍"))
    g = L.get_game(CHAT)
    assert g["selection"]["1"]["locked"] and L._current_selecting_uid(g) == 2
    # player 2 keeps -> both locked -> wildcard offered (2 players)
    L.handle_movie(MODE, message(2, "👍👍👍"))
    assert L.get_game(CHAT)["phase"] == "WILDCARD"
    L.handle_movie(MODE, message(1, "pass"))   # decline -> VETO
    assert L.get_game(CHAT)["phase"] == "VETO"


# ---- per-player turn timer + cancel/new-game escape hatch ----------------- #
def _two_player_to_selection():
    """Two players (3 films each), driven to SELECTING with player 1 on the clock."""
    _reset()
    for t in ["A1", "A2", "A3"]:
        L.add_to_library(CHAT, 1, t)
    for t in ["B1", "B2", "B3"]:
        L.add_to_library(CHAT, 2, t)
    L.start_game(MODE, CHAT, 1)
    L.handle_movie(MODE, cb(2, "join"))
    L.handle_movie(MODE, cb(1, "start"))
    _skip_constraints()
    g = L.get_game(CHAT)
    assert g["phase"] == "SELECTING" and L._current_selecting_uid(g) == 1
    return g


def test_turn_sets_a_deadline_not_a_poll():
    g = _two_player_to_selection()
    assert g["turn_deadline"] and not g.get("turn_poll_id")    # clock, no fake poll
    assert not any("i'm choosing" in (t or "").lower() for t, _ in SENT)


def test_turn_tick_keeps_dealt_films_for_silent_player():
    g = _two_player_to_selection()
    dealt = [s["slug"] for s in g["selection"]["1"]["slots"]]
    NOW[0] += L._TURN_WINDOW + 1
    L.run_tick()                                              # clock-driven, no chat activity
    g = L.get_game(CHAT)
    assert g["selection"]["1"]["locked"]                      # auto-kept, not stuck
    assert [s["slug"] for s in g["selection"]["1"]["slots"]] == dealt   # films preserved
    assert L._current_selecting_uid(g) == 2                   # advanced to next player
    assert any("didn't reply" in t.lower() for t, _ in SENT)


def test_turn_timer_backstop_fires_on_other_activity_past_deadline():
    _two_player_to_selection()
    NOW[0] += L._TURN_WINDOW + 1
    L.handle_movie(MODE, message(2, "anyone there?"))          # other player, past deadline
    g = L.get_game(CHAT)
    assert g["selection"]["1"]["locked"] and L._current_selecting_uid(g) == 2


def test_current_player_late_reply_still_counts():
    # A reply we actually received from the current player is authoritative — never
    # sacrificed to the timer, even if it lands a hair late.
    _two_player_to_selection()
    NOW[0] += L._TURN_WINDOW + 5
    L.handle_movie(MODE, message(1, "👍👍👍"))                 # late, but it's THEIR pick
    g = L.get_game(CHAT)
    assert g["selection"]["1"]["locked"] and g["selection"]["1"]["slots"]   # kept, not sat out
    assert L._current_selecting_uid(g) == 2


def test_cancel_works_mid_turn():
    _two_player_to_selection()
    L.handle_movie(MODE, message(1, "cancel the game"))
    assert L.get_game(CHAT) is None
    assert any("cancelled" in t.lower() for t, _ in SENT)


def test_new_game_works_mid_turn_from_any_player():
    _two_player_to_selection()
    L.handle_movie(MODE, message(2, "new game"))              # not even the current player
    g = L.get_game(CHAT)
    assert g is not None and g["phase"] == "JOINING"          # scrapped + fresh game


def test_veto_window_is_sixty_seconds():
    assert L._VETO_WINDOW == 60


def test_tick_task_routes_through_handler_and_is_noop_when_idle():
    _reset()
    assert L._scheduled_task({"task": "tick"}) == "tick"
    assert L._scheduled_task({"task": "morning_after"}) == "morning_after"
    assert L._scheduled_task({"source": "aws.events"}) == "morning_after"   # bare -> daily
    assert L._scheduled_task({"rawPath": "/movie"}) is None                 # webhook
    out = L.lambda_handler({"task": "tick"}, None)
    assert out["statusCode"] == 200 and out["body"].endswith("0 advanced")


def test_add_a_film_state_times_out_via_tick_keeping_films():
    _short_pool_setup(["drama", "drama", "horror"])          # 2 qualify -> short pool
    L.handle_movie(MODE, cb(1, "sp_add"))                     # awaiting a typed title
    assert L.get_game(CHAT)["selection"]["1"]["awaiting_add"] is True
    NOW[0] += L._TURN_WINDOW + 1
    L.run_tick()                                             # silent -> auto-keep + advance
    g = L.get_game(CHAT)
    assert g["selection"]["1"]["locked"] and len(g["selection"]["1"]["slots"]) == 2
    assert g["phase"] == "WILDCARD"                          # solo -> wildcard after lock


def test_swap_redisplay_says_swapped_in_not_picked_these():
    _reset()
    for t in ["A", "B", "C", "D"]:
        L.add_to_library(CHAT, 1, t)
    L.start_game(MODE, CHAT, 1)
    L.handle_movie(MODE, cb(1, "start"))
    _skip_constraints()                                      # -> SELECTING, 3 of 4 dealt
    SENT.clear()
    L.handle_movie(MODE, message(1, "👎👍👍"))               # swap slot 1
    assert any("swapped in" in t.lower() for t, _ in SENT)
    assert not any("i picked these" in t.lower() for t, _ in SENT)   # preamble not repeated


# ---- wildcard "one for the hat" ------------------------------------------- #
def _two_player_to_wildcard():
    """Two players (one film each) driven through selection to the wildcard offer."""
    _reset()
    L.add_to_library(CHAT, 1, "Film A")
    L.add_to_library(CHAT, 2, "Film B")
    L.start_game(MODE, CHAT, 1)
    L.handle_movie(MODE, cb(2, "join"))
    L.handle_movie(MODE, cb(1, "start"))
    _skip_constraints()
    L.handle_movie(MODE, message(1, "👍"))
    L.handle_movie(MODE, message(2, "👍"))
    g = L.get_game(CHAT)
    assert g["phase"] == "WILDCARD" and g["wildcard"]
    return g


def test_wildcard_offered_after_lock_builds_canonical():
    g = _two_player_to_wildcard()
    wc = g["wildcard"]
    assert wc["owner"] == "0"                          # ownerless 'house' candidate
    assert wc["slug"] not in {"film-a", "film-b"}      # not in tonight's pool
    assert wc["slug"] == L._slugify("Tokyo Story")     # first canonical (AI off)
    assert wc["slug"] in L._wildcard_log(CHAT)         # logged so it's never repeated
    assert L.get_film(CHAT, 0, wc["slug"]) is not None  # stored as a house lib item
    pitch = next(t for t, _ in SENT if "🎩" in t)       # the permission pitch
    assert "may I suggest one" in pitch                 # canonical lead (no fake taste claim)
    assert "keep only human picks" in pitch             # consent question
    assert g["wildcard_offered"] is True


def test_wildcard_novelty_excludes_a_players_library():
    # A canonical pick already in a player's library (but NOT in tonight's pool) must
    # be skipped on novelty grounds — the whole point is a film they don't have.
    _reset()
    L.add_to_library(CHAT, 1, "Tokyo Story")            # player 1 already owns the 1st canonical
    game = L.new_game(CHAT, 1)
    L._add_player(game, 1)
    L._add_player(game, 2)
    game["selection"] = {
        "1": {"locked": True, "slots": [{"slug": "film-a", "title": "Film A"}]},
        "2": {"locked": True, "slots": [{"slug": "film-b", "title": "Film B"}]},
    }
    sugg = L._build_wildcard(CHAT, game)
    assert sugg is not None
    assert sugg["slug"] != L._slugify("Tokyo Story")    # owned -> excluded
    assert sugg["slug"] == L._slugify("Yi Yi")          # next canonical instead


def test_wildcard_dropped_when_ignored_via_backstop():
    g = _two_player_to_wildcard()
    wc_slug = g["wildcard"]["slug"]
    NOW[0] += L._WILDCARD_WINDOW + 1
    L.handle_movie(MODE, message(2, "so what's the pick"))   # ignored past the beat
    g = L.get_game(CHAT)
    assert g is not None and g["phase"] == "VETO"
    slugs = {e["slug"] for e in g["pool_all"]}
    assert wc_slug not in slugs and {"film-a", "film-b"} <= slugs   # dropped, not foisted


def test_wildcard_declined_via_text_drops_silently():
    g = _two_player_to_wildcard()
    wc_slug = g["wildcard"]["slug"]
    L.handle_movie(MODE, message(1, "nah pass"))       # a participant declines
    g = L.get_game(CHAT)
    assert g["phase"] == "VETO" and g["wildcard"] is None
    assert wc_slug not in {e["slug"] for e in g["pool_all"]}   # not in the hat
    assert L.get_film(CHAT, 0, wc_slug) is None         # the house item is cleaned up
    assert g["wildcard_offered"] is True                # still won't re-offer


def test_wildcard_declined_via_thumbsdown_reaction():
    g = _two_player_to_wildcard()
    wc_slug, msg_id = g["wildcard"]["slug"], g["wildcard_msg_id"]
    L.handle_movie(MODE, reaction(2, msg_id, "👎"))    # 👎 from a participant on the pitch
    g = L.get_game(CHAT)
    assert g["phase"] == "VETO" and g["wildcard"] is None
    assert wc_slug not in {e["slug"] for e in g["pool_all"]}


def test_wildcard_accepted_via_text_joins_pool():
    g = _two_player_to_wildcard()
    wc_slug = g["wildcard"]["slug"]
    SENT.clear()
    L.handle_movie(MODE, message(2, "yes add it"))     # consent from anyone, in chat
    g = L.get_game(CHAT)
    assert g is not None and g["phase"] == "VETO"
    slugs = {e["slug"] for e in g["pool_all"]}
    assert wc_slug in slugs and {"film-a", "film-b"} <= slugs   # joined the hat
    assert any(e["owner"] == "0" for e in g["pool_all"])        # ownerless house item
    assert any("hat" in t.lower() and "✅" in t for t, _ in SENT)   # acknowledged the add


def test_wildcard_accepted_via_thumbsup_reaction():
    g = _two_player_to_wildcard()
    wc_slug, msg_id = g["wildcard"]["slug"], g["wildcard_msg_id"]
    L.handle_movie(MODE, reaction(2, msg_id, "👍"))    # 👍 from anyone on the pitch adds it
    g = L.get_game(CHAT)
    assert g is not None and g["phase"] == "VETO"
    assert wc_slug in {e["slug"] for e in g["pool_all"]}


def test_solo_game_skips_veto_round():
    _reset()
    L.add_to_library(CHAT, 1, "Solo Film")
    L.start_game(MODE, CHAT, 1)
    L.handle_movie(MODE, cb(1, "start"))
    _skip_constraints()
    L.handle_movie(MODE, message(1, "👍"))             # lock -> wildcard offered
    SENT.clear()
    L.handle_movie(MODE, message(1, "pass"))           # decline -> solo: straight to winner
    assert L.get_game(CHAT) is None                    # game ended (winner crowned)
    assert not any("veto round" in t.lower() for t, _ in SENT)   # no veto round shown
    assert any("winner" in t.lower() for t, _ in SENT)


def test_year_range_reversed_is_swapped():
    assert L.parse_constraint_text("1920-1900") == {"min_year": 1900, "max_year": 1920}
    d = L.parse_constraint_text("films 1965 to 1980 please")
    assert d["min_year"] == 1965 and d["max_year"] == 1980


def test_rating_poll_deduped_within_window():
    _reset()
    assert L._post_rating_poll(MODE, CHAT, "Dune", "2021", session_id="s1")
    assert L._post_rating_poll(MODE, CHAT, "dune", "2021", session_id="s2") == {}   # same film, skipped


def test_wildcard_offered_in_solo_game():
    # A solo player's own loves are enough — the wildcard fires for a one-player game too.
    _reset()
    L.add_to_library(CHAT, 1, "Solo Film")
    L.start_game(MODE, CHAT, 1)
    L.handle_movie(MODE, cb(1, "start"))
    _skip_constraints()
    L.handle_movie(MODE, message(1, "👍"))             # solo lock -> wildcard offered
    assert L.get_game(CHAT)["phase"] == "WILDCARD"
    assert any("🎩" in t for t, _ in SENT)             # the pitch went out
    L.handle_movie(MODE, message(1, "pass"))           # decline -> solo veto -> winner
    assert L.get_game(CHAT) is None


def test_wildcard_never_repeats_across_games():
    _reset()
    L.add_to_library(CHAT, 1, "Film A")
    L.add_to_library(CHAT, 2, "Film B")

    def play_to_wildcard():
        L.start_game(MODE, CHAT, 1, force_new=True)
        L.handle_movie(MODE, cb(2, "join"))
        L.handle_movie(MODE, cb(1, "start"))
        _skip_constraints()
        L.handle_movie(MODE, message(1, "👍"))
        L.handle_movie(MODE, message(2, "👍"))
        return L.get_game(CHAT)["wildcard"]["slug"]

    first = play_to_wildcard()
    L.handle_movie(MODE, message(1, "pass"))           # finish offering game 1
    second = play_to_wildcard()                         # force_new wipes it, starts game 2
    assert first != second                              # never the same film twice
    assert {first, second} <= L._wildcard_log(CHAT)


def test_is_stale():
    assert L._is_stale("2000-01-01T00:00:00+00:00") is True   # earlier day / long ago
    assert L._is_stale(None) is True
    assert L._is_stale(L._now_iso()) is False


def test_new_day_expiry_starts_clean():
    _reset()
    L.add_to_library(CHAT, 1, "Film A")
    L.start_game(MODE, CHAT, 1)
    g = L.get_game(CHAT)
    s1 = g["session_id"]
    g["last_activity_at"] = g["created_at"] = "2000-01-01T00:00:00+00:00"  # yesterday-ish
    fake_put(g)                                   # write WITHOUT bumping last_activity
    assert L._game_is_ongoing(CHAT, L.get_game(CHAT)) is False  # auto-abandons + clears
    assert L.get_game(CHAT) is None
    L.start_game(MODE, CHAT, 1)                   # new day -> clean start
    assert L.get_game(CHAT)["session_id"] != s1


def test_force_new_supersedes_active_game():
    _reset()
    L.add_to_library(CHAT, 1, "Film A")
    L.start_game(MODE, CHAT, 1)
    s1 = L.get_game(CHAT)["session_id"]
    assert L.get_game(CHAT)["status"] == "collecting"
    SENT.clear()
    L.start_game(MODE, CHAT, 1, force_new=True)   # explicit new game -> supersede, no argument
    assert L.get_game(CHAT)["session_id"] != s1
    assert not any("already going" in t.lower() for t, _ in SENT)


def test_soft_prompt_once_then_restarts():
    _reset()
    L.add_to_library(CHAT, 1, "Film A")
    L.start_game(MODE, CHAT, 1)
    s1 = L.get_game(CHAT)["session_id"]
    SENT.clear()
    L.start_game(MODE, CHAT, 1)                   # ambiguous nudge while live -> prompt once
    assert L.get_game(CHAT)["session_id"] == s1
    assert L.get_game(CHAT)["soft_prompted"] is True
    assert sum("Join" in t for t, _ in SENT) == 1
    L.start_game(MODE, CHAT, 1)                   # pushed again -> just restart, no repeat line
    assert L.get_game(CHAT)["session_id"] != s1


def test_rating_poll_post_vote_change_retract():
    _reset()
    res = L._post_rating_poll(MODE, CHAT, "Star Wars", "1977", film_id=11)
    pid, sid = res["poll_id"], res["session_id"]
    rp = L.get_rating_poll(pid)
    assert rp and rp["chat_id"] == CHAT and rp["film_title"] == "Star Wars" and rp["year"] == "1977"
    pk = L._pk(MODE, CHAT)
    # user 1 taps ⭐⭐⭐⭐ (option index 3 -> 4 stars)
    L.on_poll_answer(MODE, poll_answer(1, pid, [3]))
    r = STORE[(pk, f"rating#{sid}#1")]
    assert r["stars"] == 4 and r["film_title"] == "Star Wars" and r["user_id"] == 1
    # change vote to ⭐⭐ (index 1 -> 2 stars) — last write wins
    L.on_poll_answer(MODE, poll_answer(1, pid, [1]))
    assert STORE[(pk, f"rating#{sid}#1")]["stars"] == 2
    # retract -> rating removed
    L.on_poll_answer(MODE, poll_answer(1, pid, []))
    assert (pk, f"rating#{sid}#1") not in STORE


def _seed_rating(session, uid, name, film_title, year, stars, rated_at):
    pk = L._pk(MODE, CHAT)
    STORE[(pk, f"rating#{session}#{uid}")] = {
        "PK": pk, "SK": f"rating#{session}#{uid}", "user_id": int(uid), "name": name,
        "film_id": None, "film_title": film_title, "year": year, "stars": stars,
        "rated_at": rated_at}


def test_get_ratings_reads_back_asas_star_wars_vote():
    _reset()
    # Asa (uid 9) rated Star Wars 3★ in the poll we just did; another film/user too.
    _seed_rating("adhoc-p1", 9, "Asa", "Star Wars", "1977", 3, "2026-06-11T01:00:00Z")
    _seed_rating("adhoc-p2", 1, "Chad", "Mirror", "1975", 5, "2026-06-11T02:00:00Z")
    # bare-title, case/space-insensitive match (NOT the "Star Wars (1977)" label)
    rows = L.get_user_ratings(CHAT, user_id=9, film_title="  star   WARS ")
    assert len(rows) == 1
    r = rows[0]
    assert r["user"] == "Asa" and r["film"] == "Star Wars" and r["year"] == "1977"
    assert r["stars"] == 3
    # filtering by film alone finds it; the year-label form must NOT match
    assert len(L.get_user_ratings(CHAT, film_title="Star Wars")) == 1
    assert L.get_user_ratings(CHAT, film_title="Star Wars (1977)") == []


def test_get_ratings_newest_first_and_user_filter():
    _reset()
    _seed_rating("adhoc-p1", 9, "Asa", "Dune", "2021", 4, "2026-06-10T10:00:00Z")
    _seed_rating("adhoc-p2", 9, "Asa", "Dune", "2021", 2, "2026-06-11T10:00:00Z")
    rows = L.get_user_ratings(CHAT, user_id=9)
    assert [r["rated_at"] for r in rows] == ["2026-06-11T10:00:00Z", "2026-06-10T10:00:00Z"]
    assert L.get_user_ratings(CHAT, user_id=1) == []   # nobody else rated


def _persona_two_voters():
    """One Persona poll with TWO real votes via the live path: Chad(1)=5★, Asa(9)=4★."""
    _reset()
    res = L._post_rating_poll(MODE, CHAT, "Persona", "1966", film_id=490)
    pid, sid = res["poll_id"], res["session_id"]
    L.on_poll_answer(MODE, poll_answer(1, pid, [4]))   # option 4 -> 5 stars
    L.on_poll_answer(MODE, poll_answer(9, pid, [3]))   # option 3 -> 4 stars
    pk = L._pk(MODE, CHAT)
    # give the votes display names (handler stored "U1"/"U9")
    for u, nm in ((1, "Chad"), (9, "Asa")):
        row = STORE[(pk, f"rating#{sid}#{u}")]; row["name"] = nm; L.ddb_put(row)
    return sid


def test_get_ratings_everyone_returns_both_with_average():
    _persona_two_voters()
    # "how was Persona rated" -> omit whose -> ALL voters + the average (math in code)
    out = L._dispatch_tool("get_ratings", {"film_title": "Persona"}, _ctx(1))
    assert out["count"] == 2
    assert {(r["user"], r["stars"]) for r in out["ratings"]} == {("Chad", 5), ("Asa", 4)}
    assert out["average"] == 4.5
    # explicit "everyone" behaves the same as omitting whose
    out2 = L._dispatch_tool("get_ratings", {"whose": "everyone", "film_title": "Persona"}, _ctx(1))
    assert out2["count"] == 2 and out2["average"] == 4.5


def test_get_ratings_me_returns_only_asker():
    _persona_two_voters()
    # asker is Asa (uid 9): "what was my rating for Persona" -> whose='me' -> just Asa's 4★
    out = L._dispatch_tool("get_ratings", {"whose": "me", "film_title": "Persona"}, _ctx(9))
    assert out["count"] == 1 and out["ratings"][0]["user"] == "Asa"
    assert out["ratings"][0]["stars"] == 4
    # and for Chad as asker -> just his 5★
    out2 = L._dispatch_tool("get_ratings", {"whose": "me", "film_title": "Persona"}, _ctx(1))
    assert out2["count"] == 1 and out2["ratings"][0]["stars"] == 5


def test_get_ratings_named_person_resolves_to_their_row():
    sid = _persona_two_voters()
    # claim the "Asa" starter name onto uid 9 so resolve_owner maps the name -> id
    _seed_named("Asa", ["A1"])
    assert L.claim_library(CHAT, "Asa", 9)["status"] == "ok"
    # "what did Asa rate Persona" (asked by Chad) -> Asa's 4★ only
    out = L._dispatch_tool("get_ratings", {"whose": "Asa", "film_title": "Persona"}, _ctx(1))
    assert out["count"] == 1 and out["ratings"][0]["user"] == "Asa"
    assert out["ratings"][0]["stars"] == 4
    # an unknown person fails loud (model must not guess / leak someone else's)
    out2 = L._dispatch_tool("get_ratings", {"whose": "Ghost", "film_title": "Persona"}, _ctx(1))
    assert out2["resolved"] is False and "ratings" not in out2


def test_get_ratings_no_asker_default_anymore():
    # a user who never voted asking "how was Persona rated" still sees BOTH votes,
    # not an empty/own-only result (the old asker-default bug).
    _persona_two_voters()
    out = L._dispatch_tool("get_ratings", {"film_title": "Persona"}, _ctx(42))
    assert out["count"] == 2 and out["average"] == 4.5


def test_poll_film_tool_resolves_and_registers():
    _reset()
    ctx = {"chat_id": CHAT, "user_id": 1, "user_name": "U1", "mode": MODE, "suppress_reply": False}
    out = L._dispatch_tool("poll_film", {"title": "Star Wars"}, ctx)
    assert out["polled"] and out["title"] == "Star Wars"
    polls = [v for (p, _s), v in STORE.items() if p == "ratingpoll"]
    assert len(polls) == 1 and polls[0]["film_title"] == "Star Wars"


def test_rating_vote_does_not_disturb_veto_poll():
    # a veto-phase poll vote routes to the veto logic (not the rating handler)
    game = _veto_setup([1, 2, 3], [(1, "a", "A"), (2, "b", "B"), (3, "c", "C")])
    pid = game["current"]["poll_id"]
    assert L.get_rating_poll(pid) is None
    owner = int(game["current"]["film"]["owner"])
    voter = next(p for p in (1, 2, 3) if p != owner)
    L.handle_movie(MODE, poll_answer(voter, pid, [0]))
    assert L.get_game(CHAT)["vetoes_remaining"][str(voter)] == 0


def _short_pool_setup(genres):
    """Player 1, filter 'no horror', a library of films with the given genres
    (1980, 100min). Returns the game after _ask_player runs."""
    _reset()
    pk = L._pk(MODE, CHAT)
    for i, g in enumerate(genres):
        slug = f"f{i}"
        STORE[(pk, f"lib#1#{slug}")] = {"PK": pk, "SK": f"lib#1#{slug}", "slug": slug,
            "owner_id": 1, "title": f"Film{i}", "year": "1980", "genres": [g],
            "runtime_min": 100, "watched": False}
    game = L.new_game(CHAT, 1)
    L._add_player(game, 1)
    game["phase"] = "SELECTING"
    game["status"] = "confirming"
    game["sel_order"] = [1]
    game["sel_idx"] = 0
    game["selection"] = {}
    game["filter"]["exclude_genres"] = ["horror"]
    L.put_game(game)
    L._ask_player(MODE, CHAT, game)
    return L.get_game(CHAT)


def test_short_pool_prompts_with_buttons_and_play_advances():
    g = _short_pool_setup(["drama", "drama", "horror"])   # 2 qualify, horror trimmed
    sel = g["selection"]["1"]
    assert sel.get("short_pool") and not sel["locked"] and len(sel["slots"]) == 2
    assert any("qualify" in t.lower() and "no horror" in t.lower() for t, _ in SENT)
    L.handle_movie(MODE, cb(1, "sp_play"))                # accept the 2 -> wildcard offered
    assert L.get_game(CHAT)["phase"] == "WILDCARD"
    L.handle_movie(MODE, message(1, "pass"))              # decline -> solo veto -> winner
    assert L.get_game(CHAT) is None


def test_short_pool_add_qualifying_reaches_three():
    _short_pool_setup(["drama", "drama", "horror"])
    L.handle_movie(MODE, cb(1, "sp_add"))
    assert L.get_game(CHAT)["selection"]["1"]["awaiting_add"] is True
    L.handle_movie(MODE, message(1, "New Drama"))         # fake_lookup -> Drama, fits
    assert L.get_game(CHAT)["phase"] == "WILDCARD"        # reached 3 -> locked -> wildcard
    L.handle_movie(MODE, message(1, "pass"))              # decline -> solo veto -> winner
    assert L.get_game(CHAT) is None
    assert any(f["title"] == "New Drama" for f in L.get_library(CHAT, 1))   # persisted


def test_short_pool_add_nonqualifying_persists_and_flags():
    _short_pool_setup(["drama", "drama", "horror"])
    L.handle_movie(MODE, cb(1, "sp_add"))
    orig = L.lookup_film_cached
    L.lookup_film_cached = lambda t, y=None: {
        "found": True, "title": t, "slug": L._slugify(t), "year": "1980",
        "genres": ["Horror"], "runtime_min": 100, "rating": 4.0, "rating_scale": 10,
        "rt_rating": None, "tmdb_id": 2, "alts": []}
    try:
        L.handle_movie(MODE, message(1, "Scary Movie"))
    finally:
        L.lookup_film_cached = orig
    g = L.get_game(CHAT)
    sel = g["selection"]["1"]
    assert g["phase"] == "SELECTING"                      # still this player
    assert len(sel["slots"]) == 2 and sel["awaiting_add"] is False
    assert any(f["title"] == "Scary Movie" for f in L.get_library(CHAT, 1))  # kept in library
    assert any("horror" in t.lower() and "can't play" in t.lower() for t, _ in SENT)


def test_short_pool_zero_eligible_can_sit_out():
    g = _short_pool_setup(["horror", "horror"])           # nothing qualifies
    sel = g["selection"]["1"]
    assert sel.get("short_pool") and len(sel["slots"]) == 0
    assert any("sit this round out" in t.lower() for t, _ in SENT)
    L.handle_movie(MODE, cb(1, "sp_play"))                # sit out -> empty pool -> no winner
    assert L.get_game(CHAT) is None


# ---- swap dead-end: 👎 with nothing left in the library that fits ---------- #
def test_swap_deadend_offers_buttons_instead_of_forcing_keep():
    # Meri's bug: 👎 a film when nothing else in the library fits tonight's filter must
    # NOT corner her into 👍👍👍. Drop the rejected film, offer a button way out.
    g = _short_pool_setup(["drama", "drama", "drama"])    # all fit -> full 3-slot slate
    assert len(g["selection"]["1"]["slots"]) == 3 and not g["selection"]["1"].get("short_pool")
    SENT.clear()
    L.handle_movie(MODE, message(1, "👎👍👍"))            # drop 1, nothing to swap in
    g = L.get_game(CHAT)
    sel = g["selection"]["1"]
    assert g["phase"] == "SELECTING" and not sel["locked"]   # NOT forced to lock
    assert len(sel["slots"]) == 2                            # the rejected film is dropped
    assert not any("keep these" in t.lower() for t, _ in SENT)  # no forced-keep demand
    assert any("go with the 2" in t.lower() for t, _ in SENT)
    kb = L._swap_deadend_keyboard(2)["inline_keyboard"]
    assert kb[0][0] == {"text": "✅ Go with the 2 I approved", "callback_data": "sp_play"}
    assert kb[1][0] == {"text": "➕ Add a film", "callback_data": "sp_add"}


def test_swap_deadend_go_with_approved_locks_the_subset():
    _short_pool_setup(["drama", "drama", "drama"])
    L.handle_movie(MODE, message(1, "👎👍👍"))
    assert len(L.get_game(CHAT)["selection"]["1"]["slots"]) == 2
    L.handle_movie(MODE, cb(1, "sp_play"))                # "Go with the 2" -> solo wildcard
    g = L.get_game(CHAT)
    assert g["phase"] == "WILDCARD" and g["selection"]["1"]["locked"]
    assert len(g["selection"]["1"]["slots"]) == 2
    L.handle_movie(MODE, message(1, "pass"))              # decline -> solo veto -> winner
    assert L.get_game(CHAT) is None


def test_swap_deadend_add_film_uses_filter_validation():
    _short_pool_setup(["drama", "drama", "drama"])
    L.handle_movie(MODE, message(1, "👎👍👍"))            # 2 approved, dead-ended
    L.handle_movie(MODE, cb(1, "sp_add"))
    assert L.get_game(CHAT)["selection"]["1"]["awaiting_add"] is True
    L.handle_movie(MODE, message(1, "New Drama"))         # fake_lookup -> Drama, fits filter
    g = L.get_game(CHAT)
    assert g["phase"] == "WILDCARD"                       # 2 + 1 added = 3 -> locks -> wildcard
    assert any(f["title"] == "New Drama" for f in L.get_library(CHAT, 1))


def test_swap_deadend_add_nonqualifying_is_rejected_and_reprompts():
    _short_pool_setup(["drama", "drama", "drama"])
    L.handle_movie(MODE, message(1, "👎👍👍"))
    L.handle_movie(MODE, cb(1, "sp_add"))
    orig = L.lookup_film_cached
    L.lookup_film_cached = lambda t, y=None: {
        "found": True, "title": t, "slug": L._slugify(t), "year": "1980",
        "genres": ["Horror"], "runtime_min": 100, "rating": 4.0, "rating_scale": 10,
        "rt_rating": None, "tmdb_id": 2, "alts": []}
    try:
        L.handle_movie(MODE, message(1, "Scary Movie"))   # horror -> excluded tonight
    finally:
        L.lookup_film_cached = orig
    g = L.get_game(CHAT)
    sel = g["selection"]["1"]
    assert g["phase"] == "SELECTING" and not sel["locked"]   # rejected, still choosing
    assert len(sel["slots"]) == 2                            # not added to tonight's slate
    assert any("can't play" in t.lower() for t, _ in SENT)


def test_swap_deadend_all_thumbs_down_drops_go_with_and_offers_sit_out():
    g = _short_pool_setup(["drama", "drama", "drama"])
    SENT.clear()
    L.handle_movie(MODE, message(1, "👎👎👎"))            # reject all, nothing to swap in
    g = L.get_game(CHAT)
    sel = g["selection"]["1"]
    assert g["phase"] == "SELECTING" and not sel["locked"] and len(sel["slots"]) == 0
    assert any("sit this round out" in t.lower() for t, _ in SENT)
    kb = L._swap_deadend_keyboard(0)["inline_keyboard"]
    assert kb[0][0] == {"text": "🙅 Sit this round out (keep your veto)", "callback_data": "sp_play"}
    L.handle_movie(MODE, cb(1, "sp_play"))                # sit out -> empty pool -> no winner
    assert L.get_game(CHAT) is None


def _seed_named(name, titles):
    pk = L._pk(MODE, CHAT)
    okey = f"seed:{name.lower()}"
    for t in titles:
        slug = L._slugify(t)
        STORE[(pk, f"lib#{okey}#{slug}")] = {"PK": pk, "SK": f"lib#{okey}#{slug}",
            "slug": slug, "owner_id": okey, "seed_name": name, "title": t,
            "year": "1980", "watched": False}


def _ctx(uid=1):
    return {"chat_id": CHAT, "user_id": uid, "user_name": f"U{uid}", "mode": MODE,
            "suppress_reply": False}


def test_show_other_library_resolves_and_unknown_fails():
    _reset()
    _seed_named("Asa", ["A1", "A2", "A3", "A4", "A5", "A6"])
    ok, canon = L.resolve_owner(CHAT, "Asa")
    assert ok == "seed:asa" and canon == "Asa" and len(L.get_library(CHAT, ok)) == 6
    out = L._dispatch_tool("list_library", {"whose": "Asa"}, _ctx())
    assert out["resolved"] and out["owner"] == "Asa" and len(out["films"]) == 6
    out2 = L._dispatch_tool("list_library", {"whose": "Ghost"}, _ctx())
    assert out2["resolved"] is False and "films" not in out2


def test_show_library_never_falls_back_to_caller():
    _reset()
    L.add_to_library(CHAT, 1, "Chad's Own Film")     # caller has a library
    out = L._dispatch_tool("list_library", {"whose": "Asa"}, _ctx(1))   # Asa unknown
    assert out["resolved"] is False and "films" not in out   # NOT the caller's library


def test_dasha_daria_resolve_to_same_owner():
    _reset()
    _seed_named("Dasha", ["D1", "D2"])
    a, _ = L.resolve_owner(CHAT, "Dasha")
    b, _ = L.resolve_owner(CHAT, "Daria")
    assert a == b == "seed:dasha"


def test_claim_binds_uid_draw_and_show_agree():
    _reset()
    _seed_named("Asa", ["A1", "A2", "A3", "A4", "A5", "A6"])
    assert L.claim_library(CHAT, "Asa", 7) == {"status": "ok", "moved": 6}
    # name and user_id both resolve to the same owner
    by_name, canon = L.resolve_owner(CHAT, "Asa")
    by_uid, _ = L.resolve_owner(CHAT, 7)
    assert by_name == "7" and by_uid == "7" and canon == "Asa"
    # the draw's fetch (get_library by user_id) and the show fetch are identical
    draw = {f["slug"] for f in L.get_library(CHAT, 7)}
    show = {f["slug"] for f in L.get_library(CHAT, by_name)}
    assert draw == show and len(draw) == 6
    # a second user can't silently steal the name
    assert L.claim_library(CHAT, "Asa", 8)["status"] == "taken"


def test_parse_update_kinds():
    assert L.parse_update({"message": {"chat": {"id": 5}, "from": {"id": 9},
                                       "text": "hi"}})["kind"] == "message"
    cbev = L.parse_update({"callback_query": {"id": "x", "data": "join",
                                              "from": {"id": 9},
                                              "message": {"message_id": 7, "chat": {"id": 5}}}})
    assert cbev["kind"] == "callback" and cbev["callback_data"] == "join" and cbev["message_id"] == 7
    rx = L.parse_update({"message_reaction": {"chat": {"id": 5}, "message_id": 7,
                                              "user": {"id": 9},
                                              "new_reaction": [{"type": "emoji", "emoji": "👍"}]}})
    assert rx["kind"] == "reaction" and rx["reactions"] == ["👍"]
    pa = L.parse_update({"poll_answer": {"poll_id": "p1", "user": {"id": 9}, "option_ids": [0]}})
    assert pa["kind"] == "poll_answer" and pa["poll_id"] == "p1" and pa["chat_id"] is None
    pc = L.parse_update({"poll": {"id": "p1", "is_closed": True}})
    assert pc["kind"] == "poll" and pc["poll_is_closed"] is True


def test_seed_starter_libraries_then_claim():
    _reset()
    written = L.seed_starter_libraries(CHAT)
    assert written.get("Chad") == 40 and written.get("Asa") == 6
    assert "chad" in L.list_seed_names(CHAT)
    assert L.seed_starter_libraries(CHAT) == {}      # idempotent — nothing re-written
    res = L.claim_library(CHAT, "Chad", 1)           # link to a real user
    assert res["status"] == "ok" and res["moved"] == 40
    assert len(L.get_library(CHAT, 1)) == 40
    assert "chad" not in L.list_seed_names(CHAT)      # claimed, no longer a seed
    L.seed_starter_libraries(CHAT)                    # re-seed skips the claimed name
    assert "chad" not in L.list_seed_names(CHAT)


def test_filter_merge_and_passes():
    f = L._empty_filter()
    L._merge_filter(f, {"exclude_genres": ["Documentary"]})
    L._merge_filter(f, {"max_runtime_min": 150})
    L._merge_filter(f, {"min_year": 1960})
    L._merge_filter(f, {"max_runtime_min": 120})   # tighter, from another person -> AND
    assert f["exclude_genres"] == ["documentary"]
    assert f["max_runtime_min"] == 120 and f["min_year"] == 1960
    P = L._passes_filter
    assert P({"genres": ["Documentary"], "year": "1970", "runtime_min": 90}, f) is False
    assert P({"genres": ["Drama"], "year": "1955", "runtime_min": 100}, f) is False   # year
    assert P({"genres": ["Drama"], "year": "1980", "runtime_min": 130}, f) is False   # runtime
    assert P({"genres": ["Drama"], "year": "1980", "runtime_min": 100}, f) is True
    # unknown metadata is kept rather than dropped
    assert P({"genres": [], "year": "1980", "runtime_min": None}, f) is True
    assert P({"genres": ["Drama"], "year": "", "runtime_min": 100}, f) is True


def test_eligible_pool_excludes_definitive_violations():
    _reset()
    pk = L._pk(MODE, CHAT)
    STORE[(pk, "lib#1#doc")] = {"PK": pk, "SK": "lib#1#doc", "slug": "doc", "owner_id": 1,
        "title": "A Doc", "year": "1970", "genres": ["documentary"], "runtime_min": 90}
    STORE[(pk, "lib#1#dra")] = {"PK": pk, "SK": "lib#1#dra", "slug": "dra", "owner_id": 1,
        "title": "A Drama", "year": "1980", "genres": ["drama"], "runtime_min": 100}
    game = L.new_game(CHAT, 1)
    game["pool_all"] = [{"owner": "1", "slug": "doc", "title": "A Doc"},
                        {"owner": "1", "slug": "dra", "title": "A Drama"}]
    game["filter"]["exclude_genres"] = ["documentary"]
    elig, unknown = L._eligible_pool(CHAT, game)
    assert {e["slug"] for e in elig} == {"dra"} and unknown == []


def test_over_constrained_offers_relax_then_unfilter():
    _reset()
    pk = L._pk(MODE, CHAT)
    STORE[(pk, "lib#1#dra")] = {"PK": pk, "SK": "lib#1#dra", "slug": "dra", "owner_id": 1,
        "title": "A Drama", "year": "1980", "genres": ["drama"], "runtime_min": 100}
    game = L.new_game(CHAT, 1)
    L._add_player(game, 1)
    game["phase"] = "SELECTING"
    game["selection"] = {"1": {"slots": [{"slug": "dra", "title": "A Drama", "state": "locked"}],
                               "shown": [], "locked": True}}
    game["filter"]["exclude_genres"] = ["drama"]      # excludes the only film
    L.put_game(game)
    L._begin_veto(MODE, CHAT, game)
    g = L.get_game(CHAT)
    assert g["awaiting_relax"] is True and g.get("current") is None   # no silent unfilter
    L.handle_movie(MODE, message(1, "play without filters"))
    # unfiltered -> dra eligible; solo player can't veto own pick -> it wins
    assert L.get_game(CHAT) is None
    hist = [v for (_p, s), v in STORE.items() if s.startswith("history#")]
    assert hist and hist[0]["winner_slug"] == "dra"


def test_constraints_window_parses_three_people_then_times_out():
    _reset()
    pk = L._pk(MODE, CHAT)   # a film that survives the filter below (1980, drama, 100m)
    STORE[(pk, "lib#1#film-a")] = {"PK": pk, "SK": "lib#1#film-a", "slug": "film-a",
        "owner_id": 1, "title": "Film A", "year": "1980", "genres": ["drama"],
        "runtime_min": 100, "watched": False}
    L.start_game(MODE, CHAT, 1)
    L.handle_movie(MODE, cb(1, "start"))              # -> CONSTRAINTS, deadline set
    assert L.get_game(CHAT)["phase"] == "CONSTRAINTS"
    assert L.get_game(CHAT).get("constraints_deadline")
    mapping = {"no documentaries": {"exclude_genres": ["documentary"]},
               "under 2.5 hours": {"max_runtime_min": 150},
               "nothing earlier than 1960": {"min_year": 1960}}
    orig = L.parse_constraint_text
    L.parse_constraint_text = lambda t: mapping.get(t.strip().lower(), {})
    try:
        L.handle_movie(MODE, message(1, "no documentaries"))
        L.handle_movie(MODE, message(2, "under 2.5 hours"))
        L.handle_movie(MODE, message(3, "nothing earlier than 1960"))
        f = L.get_game(CHAT)["filter"]
        assert f["exclude_genres"] == ["documentary"]
        assert f["max_runtime_min"] == 150 and f["min_year"] == 1960
        # No early 'go' — saying go does NOT close; the full window must run.
        L.handle_movie(MODE, message(1, "go"))
        assert L.get_game(CHAT)["phase"] == "CONSTRAINTS"
        _skip_constraints()                           # timed sweep closes it
        assert L.get_game(CHAT)["phase"] == "SELECTING"
    finally:
        L.parse_constraint_text = orig


def test_constraints_backstop_closes_after_deadline_on_next_update():
    # Belt-and-suspenders path: when the chat IS active, the first update past the
    # deadline closes the window immediately (faster than waiting for the sweep).
    _reset()
    L.add_to_library(CHAT, 1, "Film A")
    L.start_game(MODE, CHAT, 1)
    L.handle_movie(MODE, cb(1, "start"))
    assert L.get_game(CHAT)["phase"] == "CONSTRAINTS"
    NOW[0] += 61
    L.handle_movie(MODE, message(2, "anything"))      # next event past deadline closes it
    assert L.get_game(CHAT)["phase"] == "SELECTING"


def test_constraints_window_uses_a_deadline_not_a_poll():
    # The window is just a chat message + a deadline — no fake timer poll.
    _reset()
    L.add_to_library(CHAT, 1, "Film A")
    seen = []
    orig = L.send_poll
    L.send_poll = lambda *a, **k: seen.append(a) or orig(*a, **k)
    try:
        L.start_game(MODE, CHAT, 1)
        L.handle_movie(MODE, cb(1, "start"))
    finally:
        L.send_poll = orig
    assert seen == []                                 # no poll posted for the window
    assert L.get_game(CHAT).get("constraints_deadline")


def test_constraint_window_closes_on_tick_even_when_silent():
    # The whole point: the tick sweep closes the window with no message in the chat.
    _reset()
    L.add_to_library(CHAT, 1, "Film A")
    L.start_game(MODE, CHAT, 1)
    L.handle_movie(MODE, cb(1, "start"))
    assert L.get_game(CHAT)["phase"] == "CONSTRAINTS"
    NOW[0] += L._CONSTRAINTS_WINDOW + 1
    out = L.run_tick()                                # clock-driven, no chat activity
    assert out["body"].endswith("1 advanced")
    assert L.get_game(CHAT)["phase"] == "SELECTING"


def test_no_early_go_keeps_window_open_for_everyone():
    # Saying 'go' must NOT short-circuit the window — everyone still gets their turn.
    _reset()
    L.add_to_library(CHAT, 1, "Film A")
    L.start_game(MODE, CHAT, 1)
    L.handle_movie(MODE, cb(1, "start"))
    L.handle_movie(MODE, message(1, "go"))
    L.handle_movie(MODE, message(1, "lets go"))
    assert L.get_game(CHAT)["phase"] == "CONSTRAINTS"   # still open


def test_resolver_passes_year_as_separate_param():
    # The year must go in primary_release_year, never concatenated into the query.
    calls = []

    def fake_get(path, params):
        calls.append((path, dict(params)))
        if path == "/search/movie":
            return {"results": [{"id": 7, "title": "Funny Games",
                                 "release_date": "1997-03-11", "vote_average": 7.6}]}
        return {"id": 7, "title": "Funny Games", "release_date": "1997-03-11",
                "runtime": 108, "genres": [{"name": "Thriller"}], "vote_average": 7.6,
                "overview": "A family is taken hostage."}

    orig_get, orig_key = L._tmdb_get, L.TMDB_API_KEY
    L._tmdb_get, L.TMDB_API_KEY = fake_get, "testkey"
    try:
        r = L._tmdb("Funny Games", 1997)
    finally:
        L._tmdb_get, L.TMDB_API_KEY = orig_get, orig_key
    search = next(p for path, p in calls if path == "/search/movie")
    assert search["query"] == "Funny Games"                  # no year in the query
    assert str(search.get("primary_release_year")) == "1997"  # year is separate
    assert r["title"] == "Funny Games" and r["year"] == "1997"
    assert r["runtime_min"] == 108 and r["genres"] == ["Thriller"]
    assert r["rating_10"] == 7.6


def test_film_card_has_no_synopsis_line():
    # The card is the exact factual one-liner only — no pasted/truncated synopsis.
    item = {"title": "Rear Window", "year": "1954", "genres": ["Mystery", "Thriller"],
            "runtime_min": 112, "rating": "8.5", "rating_scale": 10,
            "description": "A long synopsis that should never appear on the card."}
    card = L._film_card(item)
    assert "\n" not in card                          # single factual line
    assert "synopsis" not in card.lower()
    assert "Rear Window (1954)" in card and "★ 8.5/10" in card
    assert "1h 52m" in card and "Mystery, Thriller" in card


def test_film_blurb_silent_when_ai_off():
    # AI gated off in tests -> no Bedrock call, empty string, card degrades cleanly.
    assert L._film_blurb({"title": "Mirror", "year": "1975"}) == ""


def test_recommend_films_returns_library_and_candidates():
    _reset()
    L.add_to_library(CHAT, 1, "Rear Window")     # fake_lookup gives tmdb_id=1
    L.add_to_library(CHAT, 1, "Vertigo")

    def fake_get(path, params):
        if path.endswith("/recommendations"):
            return {"results": [
                {"title": "Notorious", "release_date": "1946-08-15"},
                {"title": "Rear Window", "release_date": "1954-08-04"},  # dup, filtered
            ]}
        return {}

    orig_get, orig_key = L._tmdb_get, L.TMDB_API_KEY
    L._tmdb_get, L.TMDB_API_KEY = fake_get, "testkey"
    try:
        out = L._dispatch_tool("recommend_films", {}, _ctx(1))
    finally:
        L._tmdb_get, L.TMDB_API_KEY = orig_get, orig_key
    assert out["resolved"] and out["owner"] == "your"
    assert {f["title"] for f in out["library"]} == {"Rear Window", "Vertigo"}
    titles = {c["title"] for c in out["candidates"]}
    assert "Notorious" in titles and "Rear Window" not in titles   # no dup of owned film


def test_recommend_films_unknown_person_fails_loud():
    _reset()
    out = L._dispatch_tool("recommend_films", {"whose": "Ghost"}, _ctx(1))
    assert out["resolved"] is False and "candidates" not in out


def test_confirm_saves_pending_film():
    _reset()
    L._set_pending(CHAT, 1, "One from the Heart", "1981")
    L.handle_movie(MODE, message(1, "yes, add it"))     # affirmative binds to pending
    lib = L.get_library(CHAT, 1)
    assert [f["title"] for f in lib] == ["One from the Heart"]
    assert L._get_pending(CHAT, 1) is None               # cleared after add
    assert any("Added" in t for t, _ in SENT)


def test_unanimous_fine_finalizes_without_clock():
    # The owner is an automatic yes; finalize the instant every NON-owner says "fine".
    game = _veto_setup([1, 2, 3], [(1, "a", "A"), (2, "b", "B"), (3, "c", "C")])
    cur = game["current"]
    pid, owner = cur["poll_id"], int(cur["film"]["owner"])
    others = [p for p in (1, 2, 3) if p != owner]
    L.handle_movie(MODE, poll_answer(others[0], pid, [1]))   # one non-owner: fine
    assert L.get_game(CHAT) is not None                      # not yet — one still pending
    L.handle_movie(MODE, poll_answer(others[1], pid, [1]))   # all non-owners fine
    assert L.get_game(CHAT) is None                          # finalized immediately
    hist = [v for (_p, s), v in STORE.items() if s.startswith("history#")]
    assert len(hist) == 1


def test_token_regex_catches_truncation():
    assert L._TOKEN_RE.match("8846476802:AAEGfNsuU8hTTcM31ZiNJSmygDBmoaBVzTk")
    assert not L._TOKEN_RE.match(":AAEGfNsuU8hTTcM31ZiNJSmygDBmoaBVzTk")   # lost bot-id
    assert not L._TOKEN_RE.match("8846476802AAEGfNsuU8hTTcM31ZiNJSmygDB")  # lost colon
    assert not L._TOKEN_RE.match("123:short")                              # too short


def test_omdb_reads_rt_from_ratings_array():
    # RT must come from the Ratings array, not tomatoMeter (often N/A).
    payload = json.dumps({
        "Response": "True", "Title": "Dune", "Year": "2021",
        "Runtime": "155 min", "Genre": "Action, Adventure, Drama",
        "Plot": "Paul Atreides leads a rebellion.",
        "Ratings": [{"Source": "Internet Movie Database", "Value": "8.0/10"},
                    {"Source": "Rotten Tomatoes", "Value": "83%"},
                    {"Source": "Metacritic", "Value": "74/100"}],
        "tomatoMeter": "N/A",
    })
    orig, L.OMDB_API_KEY = L._http_get, "testkey"
    L._http_get = lambda url, timeout=12: payload
    try:
        r = L._omdb("Dune", 2021)
    finally:
        L._http_get = orig
        L.OMDB_API_KEY = ""
    assert r["rt_rating"] == "83%"
    assert r["runtime_min"] == 155
    assert r["genres"][0] == "Action"
    assert r["description"].startswith("Paul")


def test_letterboxd_resolves_via_search_and_scrapes():
    # 'Dune' must resolve to the best search match and scrape full metadata,
    # even without a JSON-LD-only path. Fixtures stand in for the network.
    search_html = (
        '<ul class="results">'
        '<li><div class="film-poster" data-film-slug="dune-2021" '
        'data-target-link="/film/dune-2021/" data-film-name="Dune"></div></li>'
        '<li><div class="film-poster" data-film-slug="dune-1984" '
        'data-target-link="/film/dune-1984/"></div></li>'
        '</ul>'
    )
    film_html = (
        '<html><head>'
        '<meta property="og:title" content="Dune (2021)">'
        '<meta name="description" content="Paul Atreides leads a desert rebellion.">'
        '<script type="application/ld+json">/* <![CDATA[ */ '
        '{"name":"Dune","releasedEvent":[{"startDate":"2021"}],'
        '"aggregateRating":{"ratingValue":4.24,"ratingCount":900000}} '
        '/* ]]> */</script></head><body>'
        '<a href="/films/genre/science-fiction/">Science Fiction</a>'
        '<a href="/films/genre/adventure/">Adventure</a>'
        '<p class="text-footer"><span>155 mins</span></p>'
        '</body></html>'
    )
    orig = L._http_get
    L._http_get = lambda url, timeout=12: search_html if "/search/" in url else film_html
    try:
        r = L._letterboxd("Dune")
    finally:
        L._http_get = orig
    assert r is not None
    assert r["slug"] == "dune-2021"          # best (top) match, not the 1984 one
    assert r["title"] == "Dune"              # trailing "(2021)" stripped off og:title
    assert r["year"] == "2021"
    assert r["rating_5"] == 4.24
    assert r["runtime_min"] == 155
    assert "Science Fiction" in r["genres"]
    assert r["description"].startswith("Paul Atreides")


# ---- morning-after rating poll (daily scheduled job) ---------------------- #
import datetime as _dt


def _iso_at(epoch):
    return _dt.datetime.fromtimestamp(epoch, _dt.timezone.utc).isoformat()


def _seed_winner(session="s1", title="The Overnighters", year="2014",
                 watched=None, participants=(1, 2)):
    pk = L._pk(MODE, CHAT)
    STORE[(pk, "chat")] = {"PK": pk, "SK": "chat", "mode": "movie", "chat_id": CHAT}
    STORE[(pk, f"history#{session}")] = {
        "PK": pk, "SK": f"history#{session}", "session_id": session,
        "winner_title": title, "winner_slug": L._slugify(title), "winner_year": year,
        "watched_date": watched or _iso_at(NOW[0]),
        "participants": list(participants), "pool": [], "ratings": {}}


def _rating_polls():
    return [v for (p, _s), v in STORE.items() if p == "ratingpoll"]


def test_morning_after_posts_poll_for_unrated_winner():
    _reset()
    _seed_winner()
    res = L.run_morning_after()
    assert res["body"].endswith("1 posted")
    rp = _rating_polls()
    assert len(rp) == 1 and rp[0]["session_id"] == "s1"
    assert rp[0]["film_title"] == "The Overnighters"
    # the winner row is flagged so tomorrow's run won't double-post
    assert STORE[(L._pk(MODE, CHAT), "history#s1")]["morning_poll_posted"] is True


def test_morning_after_idempotent_no_double_post():
    _reset()
    _seed_winner()
    L.run_morning_after()
    n = len(_rating_polls())
    assert L.run_morning_after()["body"].endswith("0 posted")   # already flagged
    assert len(_rating_polls()) == n


def test_morning_after_skips_already_rated_winner():
    _reset()
    _seed_winner(session="s2")
    pk = L._pk(MODE, CHAT)
    STORE[(pk, "rating#s2#1")] = {"PK": pk, "SK": "rating#s2#1", "stars": 5}
    assert L.run_morning_after()["body"].endswith("0 posted")
    assert _rating_polls() == []


def test_morning_after_skips_winner_outside_lookback():
    _reset()
    _seed_winner(session="old", watched=_iso_at(NOW[0] - 10 * 86400))   # ancient
    assert L.run_morning_after()["body"].endswith("0 posted")
    assert _rating_polls() == []


def test_morning_after_no_winners_is_clean_noop():
    _reset()
    pk = L._pk(MODE, CHAT)
    STORE[(pk, "chat")] = {"PK": pk, "SK": "chat", "mode": "movie", "chat_id": CHAT}
    assert L.run_morning_after()["body"].endswith("0 posted")


def test_scheduled_event_routes_to_morning_after():
    _reset()
    _seed_winner()
    out = L.lambda_handler({"task": "morning_after"}, None)   # cron event, no path/secret
    assert out["statusCode"] == 200 and "morning-after" in out["body"]
    assert len(_rating_polls()) == 1
    # the native EventBridge shape is recognised too
    assert L._is_scheduled_event({"source": "aws.events", "detail-type": "Scheduled Event"})
    assert not L._is_scheduled_event({"rawPath": "/movie"})


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    failed = 0
    for t in tests:
        try:
            t()
            print(f"PASS {t.__name__}")
        except AssertionError as e:
            failed += 1
            print(f"FAIL {t.__name__}: {e}")
        except Exception as e:  # noqa: BLE001
            import traceback
            failed += 1
            print(f"ERROR {t.__name__}: {type(e).__name__}: {e}")
            traceback.print_exc()
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    sys.exit(1 if failed else 0)
