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
    # owner voting Veto is ignored, veto not consumed, pick stands
    L.handle_movie(MODE, poll_answer(owner, poll, [0]))
    g = L.get_game(CHAT)
    assert g["current"]["poll_id"] == poll and g["vetoes_remaining"][str(owner)] == 2
    # non-owner with a veto -> pick vetoed, first veto consumed (2->1), re-pick
    voter = next(p for p in (1, 2, 3) if p != owner)
    L.handle_movie(MODE, poll_answer(voter, poll, [0]))
    g = L.get_game(CHAT)
    assert g["vetoes_remaining"][str(voter)] == 1
    assert g["current"]["poll_id"] != poll and g["current"]["film"]["slug"] != slug


def test_voter_exhausts_both_vetoes_then_blocked():
    # All films owned by player 1 so voter=2 never owns the candidate and can
    # always attempt. Verifies both vetoes are consumed then the third is blocked.
    game = _veto_setup([1, 2, 3], [(1, "a", "A"), (1, "b", "B"), (1, "c", "C")])
    voter = 2
    g = L.get_game(CHAT)
    poll1 = g["current"]["poll_id"]
    L.handle_movie(MODE, poll_answer(voter, poll1, [0]))   # first veto (2->1)
    g = L.get_game(CHAT)
    assert g["vetoes_remaining"][str(voter)] == 1
    poll2 = g["current"]["poll_id"]
    L.handle_movie(MODE, poll_answer(voter, poll2, [0]))   # second veto (1->0)
    g = L.get_game(CHAT)
    assert g["vetoes_remaining"][str(voter)] == 0
    poll3 = g["current"]["poll_id"]
    L.handle_movie(MODE, poll_answer(voter, poll3, [0]))   # blocked — no vetoes left
    g = L.get_game(CHAT)
    assert g.get("current") and g["current"]["poll_id"] == poll3
    assert g["vetoes_remaining"][str(voter)] == 0


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
    assert g is not None and g["vetoes_remaining"][str(voter)] == 1
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
    assert g["vetoes_remaining"][str(voter)] == 1
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


def test_soft_prompt_once_then_stays_quiet():
    # Without force_new, start_game nudges exactly once then stays silent.
    # Restarting requires an explicit force_new=True (via _meta_command "new game").
    _reset()
    L.add_to_library(CHAT, 1, "Film A")
    L.start_game(MODE, CHAT, 1)
    s1 = L.get_game(CHAT)["session_id"]
    SENT.clear()
    L.start_game(MODE, CHAT, 1)                   # first implicit call -> nudge once
    assert L.get_game(CHAT)["session_id"] == s1
    assert L.get_game(CHAT)["soft_prompted"] is True
    assert sum("Join" in t for t, _ in SENT) == 1
    SENT.clear()
    L.start_game(MODE, CHAT, 1)                   # second implicit call -> silent, game unchanged
    assert L.get_game(CHAT)["session_id"] == s1
    assert not SENT                                # no repeat nudge, no restart
    L.start_game(MODE, CHAT, 1, force_new=True)   # explicit new game -> restarts
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
    assert L.get_game(CHAT)["vetoes_remaining"][str(voter)] == 1


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


def test_short_pool_add_multiple_films_in_one_message():
    _short_pool_setup(["horror", "horror"])               # 0 qualify -> add-loop, empty slate
    L.handle_movie(MODE, cb(1, "sp_add"))
    orig = L._identify_films
    L._identify_films = lambda text, miny=None, maxy=None: [("Drama One", 1980), ("Drama Two", 1981)]
    try:
        L.handle_movie(MODE, message(1, "Drama One and Drama Two"))   # two films, one message
    finally:
        L._identify_films = orig
    sel = L.get_game(CHAT)["selection"]["1"]
    titles = {s["title"] for s in sel["slots"]}
    assert {"Drama One", "Drama Two"} <= titles            # both resolved + added at once
    assert any("those fit" in t.lower() for t, _ in SENT)  # grouped acknowledgement


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
    assert L._TOKEN_RE.match("1234567890:AAFakeTokenForTestsOnly000000000000")
    assert not L._TOKEN_RE.match(":AAFakeTokenForTestsOnly000000000000")   # lost bot-id
    assert not L._TOKEN_RE.match("1234567890AAFakeTokenForTestsOnly000")  # lost colon
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


def test_morning_after_asks_open_question_no_assumption():
    _reset()
    _seed_winner()
    res = L.run_morning_after()
    assert res["body"].endswith("1 posted")
    # it asks openly, without naming/assuming the winner, and no poll yet
    assert _rating_polls() == []
    assert any("What did you end up watching" in t for t, _m in SENT)
    assert not any("The Overnighters" in t for t, _m in SENT)
    assert STORE[(L._pk(MODE, CHAT), "history#s1")]["watch_confirm_posted"] is True


def test_morning_after_naming_the_winner_posts_its_poll():
    _reset()
    _seed_winner()
    L.run_morning_after()
    L.handle_movie(MODE, message(1, "The Overnighters"))     # they DID watch the winner
    h = L.ddb_get(L._pk(MODE, CHAT), "history#s1")
    assert h["winner_title"] == "The Overnighters"           # unchanged
    rp = _rating_polls()
    assert len(rp) == 1 and rp[0]["film_title"] == "The Overnighters"


def test_morning_after_naming_a_different_film_corrects_and_polls():
    _reset()
    _seed_winner()
    L.run_morning_after()
    L.handle_movie(MODE, message(1, "Forrest Gump"))   # AI off -> answer taken verbatim
    h = L.ddb_get(L._pk(MODE, CHAT), "history#s1")
    assert h["winner_title"] == "Forrest Gump"               # winner rewritten
    rp = _rating_polls()
    assert len(rp) == 1 and rp[0]["film_title"] == "Forrest Gump"


def test_morning_after_nothing_watched_removes_winner():
    _reset()
    _seed_winner()
    L.run_morning_after()
    L.handle_movie(MODE, message(1, "nope, we skipped it"))
    assert L.ddb_get(L._pk(MODE, CHAT), "history#s1") is None  # winner dropped
    assert _rating_polls() == []


def test_morning_after_idempotent_no_double_ask():
    _reset()
    _seed_winner()
    L.run_morning_after()
    n = len(SENT)
    assert L.run_morning_after()["body"].endswith("0 posted")   # already asked
    assert len(SENT) == n


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
    assert any("What did you end up watching" in t for t, _m in SENT)   # asks, not polls yet
    # the native EventBridge shape is recognised too
    assert L._is_scheduled_event({"source": "aws.events", "detail-type": "Scheduled Event"})
    assert not L._is_scheduled_event({"rawPath": "/movie"})


def test_wildcard_save_to_library_not_hat():
    g = _two_player_to_wildcard()
    wc_slug = g["wildcard"]["slug"]
    L.handle_movie(MODE, message(1, "not tonight but add it to my library"))
    g = L.get_game(CHAT)
    assert g["phase"] == "VETO" and g["wildcard"] is None
    assert wc_slug not in {e["slug"] for e in g["pool_all"]}          # not in tonight's hat
    assert L.get_film(CHAT, 0, wc_slug) is None                       # house copy removed
    assert any(f["slug"] == wc_slug for f in L.get_library(CHAT, 1))  # saved to player 1's library


def test_language_country_filter():
    f = L._empty_filter()
    L._merge_filter(f, {"include_languages": ["FR"]})       # codes lowercased on merge
    L._merge_filter(f, {"exclude_countries": ["US"]})
    assert f["include_languages"] == ["fr"] and f["exclude_countries"] == ["us"]
    P = L._passes_filter
    assert P({"language": "fr", "countries": ["FR"]}, f) is True
    assert P({"language": "en", "countries": ["FR"]}, f) is False        # wrong language
    assert P({"language": "fr", "countries": ["US", "FR"]}, f) is False  # excluded country
    assert P({"language": None, "countries": []}, f) is True             # unknown -> kept


def test_win_history_weights():
    _reset()
    L.ddb_put({"PK": L._pk(MODE, CHAT), "SK": "history#a", "winner_owner_id": 3,
               "watched_date": "2026-01-01T00:00:00+00:00"})
    L.ddb_put({"PK": L._pk(MODE, CHAT), "SK": "history#b", "winner_owner_id": 1,
               "watched_date": "2026-02-01T00:00:00+00:00"})   # player 1 won most recently
    last, ever = L._win_weight_context(CHAT)
    assert last == 1 and ever == {1, 3}
    players = [1, 2, 3]
    assert L._pick_weight(1, last, ever, players) == 0.85   # won last game
    assert L._pick_weight(2, last, ever, players) == 1.15   # never won
    assert L._pick_weight(3, last, ever, players) == 1.0    # won before, but not last
    assert L._pick_weight(0, last, ever, players) == 1.0    # house/wildcard neutral


def test_correct_last_winner_swaps_film_and_unwatches_original():
    _reset()
    L.add_to_library(CHAT, 1, "Film A")
    L.mark_watched(CHAT, 1, L._slugify("Film A"))     # crowned + marked watched
    pk = L._pk(MODE, CHAT)
    L.ddb_put({"PK": pk, "SK": "history#s1", "session_id": "s1",
               "winner_title": "Film A", "winner_slug": L._slugify("Film A"),
               "winner_owner_id": 1, "winner_year": "1950",
               "watched_date": _iso_at(NOW[0]), "participants": [1, 2],
               "pool": [], "ratings": {}, "morning_poll_posted": True})
    res = L.correct_last_winner(CHAT, "Forrest Gump")
    assert res["corrected"] and not res["removed"]
    assert res["new_title"] == "Forrest Gump" and res["in_library"] is False
    h = L.ddb_get(pk, "history#s1")
    assert h["winner_title"] == "Forrest Gump"
    assert h["winner_owner_id"] == 0                   # not on anyone's shelf
    assert h["morning_poll_posted"] is False           # corrected film gets a fresh poll
    # the announced pick is returned to player 1's shelf, un-watched
    assert L.get_film(CHAT, 1, L._slugify("Film A"))["watched"] is False


def test_correct_last_winner_in_a_players_library_marks_watched():
    _reset()
    pk = L._pk(MODE, CHAT)
    L.add_to_library(CHAT, 2, "Forrest Gump")          # player 2 already owns it
    L.ddb_put({"PK": pk, "SK": "history#s1", "session_id": "s1",
               "winner_title": "Film A", "winner_slug": L._slugify("Film A"),
               "winner_owner_id": 0, "winner_year": "1950",
               "watched_date": _iso_at(NOW[0]), "participants": [1, 2],
               "pool": [], "ratings": {}})
    res = L.correct_last_winner(CHAT, "Forrest Gump")
    assert res["in_library"] is True
    h = L.ddb_get(pk, "history#s1")
    assert h["winner_owner_id"] == 2
    assert L.get_film(CHAT, 2, L._slugify("Forrest Gump"))["watched"] is True


def test_correct_last_winner_no_replacement_removes_winner():
    _reset()
    pk = L._pk(MODE, CHAT)
    L.ddb_put({"PK": pk, "SK": "history#s1", "session_id": "s1",
               "winner_title": "Film A", "winner_slug": L._slugify("Film A"),
               "winner_owner_id": 0, "winner_year": "1950",
               "watched_date": _iso_at(NOW[0]), "participants": [1], "ratings": {}})
    res = L.correct_last_winner(CHAT)                   # watched nothing
    assert res["removed"] is True
    assert L.ddb_get(pk, "history#s1") is None          # winner dropped, no poll tomorrow


def _seed_library_winner(slug_title="Film A", owner=1, session="s1"):
    """A morning-ready winner that came from `owner`'s library (marked watched)."""
    pk = L._pk(MODE, CHAT)
    STORE[(pk, "chat")] = {"PK": pk, "SK": "chat", "mode": "movie", "chat_id": CHAT}
    L.remember_member(CHAT, owner, f"U{owner}", None)
    L.add_to_library(CHAT, owner, slug_title)
    slug = L._slugify(slug_title)
    L.mark_watched(CHAT, owner, slug)
    L.ddb_put({"PK": pk, "SK": f"history#{session}", "session_id": session,
               "winner_title": slug_title, "winner_slug": slug, "winner_owner_id": owner,
               "winner_year": "1950", "watched_date": _iso_at(NOW[0]),
               "participants": [owner], "ratings": {}})
    return slug


def test_shelf_prompt_fires_on_first_vote_then_removal():
    _reset()
    slug = _seed_library_winner()
    L._morning_after_for_chat(CHAT)
    L.handle_movie(MODE, message(1, "Film A"))           # confirm winner -> poll posts
    assert not any("take it off your shelf" in t for t, _m in SENT)   # not yet — no votes
    pid = _rating_polls()[0]["SK"]
    L.handle_movie(MODE, poll_answer(1, pid, [4]))        # a vote confirms it was watched
    assert any("take it off your shelf" in t for t, _m in SENT)
    # owner taps "remove" -> film is gone from their shelf
    L.handle_movie(MODE, cb(1, f"shelf_rm#1#{slug}"))
    assert L.get_film(CHAT, 1, slug) is None


def test_shelf_prompt_keep_and_non_owner_ignored():
    _reset()
    slug = _seed_library_winner()
    L._morning_after_for_chat(CHAT)
    L.handle_movie(MODE, message(1, "Film A"))
    pid = _rating_polls()[0]["SK"]
    L.handle_movie(MODE, poll_answer(1, pid, [3]))
    L.handle_movie(MODE, cb(2, f"shelf_rm#1#{slug}"))    # someone else taps -> ignored
    assert L.get_film(CHAT, 1, slug) is not None
    L.handle_movie(MODE, cb(1, f"shelf_keep#1#{slug}"))   # owner keeps it
    assert L.get_film(CHAT, 1, slug) is not None


def test_shelf_prompt_only_once_per_winner():
    _reset()
    slug = _seed_library_winner()
    L._morning_after_for_chat(CHAT)
    L.handle_movie(MODE, message(1, "Film A"))
    pid = _rating_polls()[0]["SK"]
    L.handle_movie(MODE, poll_answer(1, pid, [4]))
    L.handle_movie(MODE, poll_answer(1, pid, [5]))        # a second/changed vote
    prompts = [t for t, _m in SENT if "take it off your shelf" in t]
    assert len(prompts) == 1                              # asked once, not per vote


def test_no_shelf_prompt_for_house_winner_on_vote():
    _reset()
    pk = L._pk(MODE, CHAT)
    STORE[(pk, "chat")] = {"PK": pk, "SK": "chat", "mode": "movie", "chat_id": CHAT}
    L.ddb_put({"PK": pk, "SK": "history#s1", "session_id": "s1",
               "winner_title": "House Pick", "winner_slug": L._slugify("House Pick"),
               "winner_owner_id": 0, "winner_year": "1950", "watched_date": _iso_at(NOW[0]),
               "participants": [1], "ratings": {}})
    L._morning_after_for_chat(CHAT)
    L.handle_movie(MODE, message(1, "House Pick"))
    pid = _rating_polls()[0]["SK"]
    L.handle_movie(MODE, poll_answer(1, pid, [4]))
    assert not any("take it off your shelf" in t for t, _m in SENT)


# ---- one-off group migration (admin_migrate) ------------------------------ #
from decimal import Decimal as _D  # noqa: E402

OLD, NEW, CHAD_OLD, CHAD_NEW = -5184764745, -5160106592, 537834437, 8999003543
L._ddb_query_raw = fake_query
L._ddb_scan_raw = fake_scan


def _mig_seed():
    _reset()
    opk, npk = L._pk(MODE, OLD), L._pk(MODE, NEW)
    rows = [
        {"PK": opk, "SK": "chat", "chat_id": OLD},
        {"PK": opk, "SK": "dedupe#1"},
        {"PK": opk, "SK": "convo#log", "turns": []},
        {"PK": opk, "SK": "pending#1", "title": "x"},
        {"PK": opk, "SK": "watch_followup", "session_id": "s"},
        {"PK": opk, "SK": "game#current", "players": [CHAD_OLD]},
        {"PK": opk, "SK": f"member#{CHAD_OLD}", "user_id": CHAD_OLD},
        {"PK": opk, "SK": "member#42", "user_id": 42, "display_name": "Asa"},
        {"PK": opk, "SK": f"lib#{CHAD_OLD}#rear-window", "slug": "rear-window",
         "owner_id": _D(CHAD_OLD), "added_by_id": str(CHAD_OLD), "title": "Rear Window",
         "rating": "4.1"},
        {"PK": opk, "SK": f"lib#{CHAD_OLD}#mirror", "slug": "mirror",
         "owner_id": CHAD_OLD, "title": "Mirror"},
        {"PK": opk, "SK": "lib#42#poetry", "slug": "poetry", "owner_id": 42, "title": "Poetry"},
        {"PK": opk, "SK": "history#s1", "winner_owner_id": CHAD_OLD,
         "participants": [CHAD_OLD, 42], "players": {str(CHAD_OLD), "42"},
         "ratings": {str(CHAD_OLD): 5, "42": 3},
         "pool": [{"owner": str(CHAD_OLD), "slug": "mirror"}],
         "initiator": "1537834437"},                       # not an exact match: untouched
        {"PK": opk, "SK": f"rating#s1#{CHAD_OLD}", "user_id": CHAD_OLD, "stars": 5},
        {"PK": opk, "SK": "seedclaim#chad", "claimed_by": CHAD_OLD},
        {"PK": "ratingpoll", "SK": "p1", "chat_id": OLD,
         "participant_user_ids": [CHAD_OLD, 42]},
        {"PK": "ratingpoll", "SK": "p2", "chat_id": -999, "participant_user_ids": [CHAD_OLD]},
        {"PK": "pollmap#p3", "SK": "ref", "chat_id": _D(OLD), "film": {"owner": str(CHAD_OLD)}},
        # junk in the new group
        {"PK": npk, "SK": "chat", "chat_id": NEW},
        {"PK": npk, "SK": f"member#{CHAD_NEW}", "user_id": CHAD_NEW},
        {"PK": npk, "SK": "dedupe#9"},
        {"PK": npk, "SK": f"lib#{CHAD_NEW}#red-river", "slug": "red-river"},
        {"PK": npk, "SK": "lib#seed:asa#poetry", "slug": "poetry"},
        {"PK": npk, "SK": "seedclaim#chad", "claimed_by": CHAD_NEW},
        {"PK": npk, "SK": "history#junk"}, {"PK": npk, "SK": "rating#junk#1"},
        {"PK": npk, "SK": "pending#1"}, {"PK": npk, "SK": "game#current"},
        {"PK": npk, "SK": "watch_followup"}, {"PK": npk, "SK": "wildcardlog"},
        {"PK": npk, "SK": "convo#log"},
        {"PK": L._pk(MODE, CHAD_OLD), "SK": "lib#x", "owner_id": CHAD_OLD},   # DM partition
    ]
    for r in rows:
        STORE[(r["PK"], r["SK"])] = r


def test_remap_uids_sk_nested_and_map_keys():
    m = {CHAD_OLD: CHAD_NEW}
    assert L._remap_sk(f"lib#{CHAD_OLD}#mirror", m) == f"lib#{CHAD_NEW}#mirror"
    assert L._remap_sk(f"rating#s{CHAD_OLD}#{CHAD_OLD}", m) == f"rating#s{CHAD_OLD}#{CHAD_NEW}"
    v = L._remap_uids({str(CHAD_OLD): [CHAD_OLD, _D(CHAD_OLD), str(CHAD_OLD), 7,
                                       {"x": {CHAD_OLD}}], "k": "15378344370", "b": True}, m)
    assert v == {str(CHAD_NEW): [CHAD_NEW, _D(CHAD_NEW), str(CHAD_NEW), 7, {"x": {CHAD_NEW}}],
                 "k": "15378344370", "b": True}


def test_migrate_dry_run_writes_nothing_and_reports():
    _mig_seed()
    before = copy.deepcopy(STORE)
    rep = L.migrate_group()                     # defaults: dry run
    assert STORE == before
    assert rep["dry_run"] is True
    assert rep["chad_lib_under_new_uid"] == 2 and rep["new_group_lib_rows"] == 3
    assert rep["new_group_history_rows"] == 1 and rep["new_group_rating_rows"] == 1
    assert rep["C_updated"] == {"ratingpoll": 1, "pollmap": 1}
    assert rep["D_old_chat_row_deleted"] is True
    assert len(rep["samples"]) == 3
    json.dumps(rep)                             # console-printable
    # explicit non-False still dry
    assert L.migrate_group(dry_run="false")["dry_run"] is True and STORE == before


def test_migrate_step_a_keep_delete_lists():
    _mig_seed()
    rep = L.migrate_group(dry_run=False)
    npk = L._pk(MODE, NEW)
    assert rep["A_deleted"] == {"convo": 1, "game": 1, "history": 1, "lib": 2, "pending": 1,
                                "rating": 1, "seedclaim": 1, "watch_followup": 1,
                                "wildcardlog": 1}
    sks = {s for (p, s) in STORE if p == npk}
    assert {"chat", f"member#{CHAD_NEW}", "dedupe#9"} <= sks          # kept
    assert STORE[(npk, "chat")]["chat_id"] == NEW
    for gone in (f"lib#{CHAD_NEW}#red-river", "lib#seed:asa#poetry", "history#junk",
                 "rating#junk#1", "pending#1", "game#current", "watch_followup",
                 "wildcardlog", "convo#log"):
        assert gone not in sks, gone


def test_migrate_real_run_copies_remaps_and_is_rerunnable():
    _mig_seed()
    L.migrate_group(dry_run=False)
    opk, npk = L._pk(MODE, OLD), L._pk(MODE, NEW)
    sks = {s for (p, s) in STORE if p == npk}
    assert f"member#{CHAD_OLD}" not in sks and "member#42" in sks
    assert "seedclaim#chad" in sks and STORE[(npk, "seedclaim#chad")]["claimed_by"] == CHAD_NEW
    lib = STORE[(npk, f"lib#{CHAD_NEW}#rear-window")]
    assert lib["owner_id"] == _D(CHAD_NEW) and lib["added_by_id"] == str(CHAD_NEW)
    assert lib["rating"] == "4.1"
    h = STORE[(npk, "history#s1")]
    assert h["winner_owner_id"] == CHAD_NEW and h["participants"] == [CHAD_NEW, 42]
    assert h["players"] == {str(CHAD_NEW), "42"}
    assert h["ratings"] == {str(CHAD_NEW): 5, "42": 3}
    assert h["pool"][0]["owner"] == str(CHAD_NEW) and h["initiator"] == "1537834437"
    assert (npk, f"rating#s1#{CHAD_NEW}") in STORE
    assert STORE[("ratingpoll", "p1")]["chat_id"] == NEW
    assert STORE[("ratingpoll", "p1")]["participant_user_ids"] == [CHAD_NEW, 42]
    assert STORE[("ratingpoll", "p2")]["chat_id"] == -999                  # other chat
    assert STORE[("ratingpoll", "p2")]["participant_user_ids"] == [CHAD_OLD]
    assert STORE[("pollmap#p3", "ref")]["chat_id"] == NEW
    assert STORE[("pollmap#p3", "ref")]["film"]["owner"] == str(CHAD_NEW)
    # D: only the old chat row goes; backups stay; DM partition untouched
    assert (opk, "chat") not in STORE and (opk, f"lib#{CHAD_OLD}#mirror") in STORE
    assert STORE[(L._pk(MODE, CHAD_OLD), "lib#x")]["owner_id"] == CHAD_OLD
    snap = copy.deepcopy({k: v for k, v in STORE.items() if k[0] == npk})
    rep2 = L.migrate_group(dry_run=False)                                  # re-run
    assert {k: v for k, v in STORE.items() if k[0] == npk} == snap
    assert rep2["chad_lib_under_new_uid"] == 2 and rep2["new_group_lib_rows"] == 3


def test_admin_migrate_only_from_direct_invoke():
    _mig_seed()
    before = copy.deepcopy(STORE)
    out = L.lambda_handler({"admin_migrate": {}}, None)
    assert out["statusCode"] == 200 and json.loads(out["body"])["dry_run"] is True
    assert STORE == before
    called = []
    orig = L.migrate_group
    L.migrate_group = lambda **k: called.append(k) or {}
    try:
        for ev in ({"admin_migrate": {"dry_run": False}, "rawPath": "/movie"},
                   {"admin_migrate": {"dry_run": False}, "requestContext": {}}):
            L.lambda_handler(ev, None)
    finally:
        L.migrate_group = orig
    assert called == []


# ---- Fix 1: short-pool wording ------------------------------------------- #
def test_short_pool_blames_filter_only_when_it_removed_films():
    _short_pool_setup(["drama", "horror"])                 # filter trimmed 1 of 2
    assert any("only these qualify" in t for t, _ in SENT)
    _short_pool_setup(["drama"])                           # filter on, removed nothing
    assert not any("qualify" in t for t, _ in SENT)
    assert any("you have 1 film on your shelf" in t for t, _ in SENT)


# ---- Fix 2: one hint, one deadline reset --------------------------------- #
def test_active_player_free_text_gets_one_hint_and_one_reset():
    g = _short_pool_setup(["drama"])
    d0 = g["turn_deadline"]
    NOW[0] += 30
    L.handle_movie(MODE, message(1, "You have it backwards"))
    g = L.get_game(CHAT)
    hints = [t for t, _ in SENT if "didn't catch that" in t]
    assert len(hints) == 1 and "Play with these" in hints[0]
    assert g["turn_deadline"] == NOW[0] + L._TURN_WINDOW > d0 and g["selection"]["1"]["hinted"]
    d1 = g["turn_deadline"]
    NOW[0] += 30
    L.handle_movie(MODE, message(1, "Less than 180"))
    g = L.get_game(CHAT)
    assert len([t for t, _ in SENT if "didn't catch that" in t]) == 1
    assert g["turn_deadline"] == d1                          # not reset again
    NOW[0] = d1 + 1
    L.run_tick()
    assert any("didn't reply" in t for t, _ in SENT)


def test_full_slate_hint_names_marks():
    _two_player_to_selection()
    L.handle_movie(MODE, message(1, "hmm not sure"))
    assert any("reply 3 marks in order" in t for t, _ in SENT)


# ---- Fix 3: truthful shelf results --------------------------------------- #
def _shelf_ctx(uid):
    return {"chat_id": CHAT, "user_id": uid, "mode": MODE, "user_name": "U", "username": None}


def test_remove_not_found_on_unclaimed_seed_row():
    _reset()
    pk = L._pk(MODE, CHAT)
    STORE[(pk, "lib#seed:asa#woman-in-the-dunes")] = {
        "PK": pk, "SK": "lib#seed:asa#woman-in-the-dunes", "slug": "woman-in-the-dunes",
        "owner_id": "seed:asa", "title": "Woman in the Dunes"}
    res = L._dispatch_tool("remove_from_library", {"title": "woman in the dunes"}, _shelf_ctx(42))
    assert res["status"] == "not_found" and res["changed"] == 0 and res["removed"] is None
    assert (pk, "lib#seed:asa#woman-in-the-dunes") in STORE         # untouched
    assert "status \"ok\" AND changed > 0" in L.MOVIE_SYSTEM
    # the model reply is generated from this result; the code itself says nothing
    assert not any("Done" in t for t, _ in SENT)


def test_remove_own_row_case_insensitive_ok():
    _reset()
    L.add_to_library(CHAT, 42, "Woman in the Dunes")
    res = L._dispatch_tool("remove_from_library", {"title": "WOMAN IN THE DUNES"}, _shelf_ctx(42))
    assert res == {"status": "ok", "changed": 1, "removed": "Woman in the Dunes"}
    assert L.get_library(CHAT, 42) == []


def test_shelf_tools_report_status_and_changed():
    _reset()
    r = L._dispatch_tool("add_to_library", {"title": "Mirror"}, _shelf_ctx(1))
    assert r["status"] == "ok" and r["changed"] == 1
    r = L._dispatch_tool("add_to_library", {"title": "Mirror"}, _shelf_ctx(1))
    assert r["status"] == "noop" and r["changed"] == 0
    r = L._dispatch_tool("claim_library", {"name": "Nobody"}, _shelf_ctx(1))
    assert r["status"] == "not_found" and r["changed"] == 0
    r = L._dispatch_tool("seed_starter_libraries", {}, _shelf_ctx(1))
    assert r["status"] == "ok" and r["changed"] > 0
    r = L._dispatch_tool("claim_library", {"name": "Asa"}, _shelf_ctx(7))
    assert r["status"] == "ok" and r["changed"] == 6
    r = L._dispatch_tool("claim_library", {"name": "Asa"}, _shelf_ctx(8))
    assert r["status"] == "noop" and r["reason"] == "taken"
    r = L._dispatch_tool("correct_last_winner", {}, _shelf_ctx(1))
    assert r["status"] == "not_found" and r["changed"] == 0
    orig = L.remove_from_library
    L.remove_from_library = lambda *a: (_ for _ in ()).throw(RuntimeError("boom"))
    try:
        r = L._dispatch_tool("remove_from_library", {"title": "x"}, _shelf_ctx(1))
    finally:
        L.remove_from_library = orig
    assert r["status"] == "error" and r["changed"] == 0


# ---- admin shelf editing ------------------------------------------------- #
def _admin_setup(admin_ids):
    """Members 1 (Chad, @chad), 2 (Asa, @AsaFoxColorist); getChatMember stubbed so
    admin_ids are administrators. Returns the list of getChatMember calls."""
    _reset()
    L.remember_member(CHAT, 1, "Chad", "chad")
    L.remember_member(CHAT, 2, "Asa", "AsaFoxColorist")
    calls = []

    def fake_tg(token, method, payload):
        calls.append((method, payload))
        assert method == "getChatMember"
        st = "administrator" if payload["user_id"] in admin_ids else "member"
        return {"ok": True, "result": {"status": st}}
    L._tg_request = fake_tg
    L._token_for = lambda mode: "1234567890:AAFakeTokenForTestsOnly000000000000"
    return calls


_ORIG_TG, _ORIG_TOKEN_FOR = L._tg_request, L._token_for


def _admin_teardown():
    L._tg_request, L._token_for = _ORIG_TG, _ORIG_TOKEN_FOR


def test_admin_edits_another_members_shelf():
    calls = _admin_setup({1})
    try:
        r = L._dispatch_tool("add_to_library", {"title": "Poetry", "target": "@asafoxcolorist"},
                             _shelf_ctx(1))
        assert r["status"] == "ok" and r["changed"] == 1
        assert [f["title"] for f in L.get_library(CHAT, 2)] == ["Poetry"]
        assert L.get_library(CHAT, 1) == []
        r = L._dispatch_tool("remove_from_library", {"title": "poetry", "target": "Asa"},
                             _shelf_ctx(1))
        assert r["status"] == "ok" and r["changed"] == 1 and L.get_library(CHAT, 2) == []
        assert len(calls) == 1                     # second check served from the 10-min cache
        cache = STORE[(L._pk(MODE, CHAT), "admincache#1")]
        assert cache["is_admin"] is True and cache["ttl"] == NOW[0] + 600
        NOW[0] += 601                               # cache expired -> asks Telegram again
        assert L._is_group_admin(CHAT, 1) is True and len(calls) == 2
    finally:
        _admin_teardown()


def test_non_admin_cannot_edit_another_shelf():
    _admin_setup(set())
    try:
        L.add_to_library(CHAT, 1, "Mirror")
        before = copy.deepcopy({k: v for k, v in STORE.items() if k[1].startswith("lib#")})
        for tool, inp in (("add_to_library", {"title": "Poetry", "target": "chad"}),
                          ("remove_from_library", {"title": "Mirror", "target": "@chad"}),
                          ("correct_film", {"title": "Mirror", "year": 1975, "target": "Chad"})):
            r = L._dispatch_tool(tool, inp, _shelf_ctx(2))
            assert r == {"status": "forbidden", "changed": 0, "target": inp["target"],
                         "note": "only group admins can edit someone else's shelf"}, r
        assert {k: v for k, v in STORE.items() if k[1].startswith("lib#")} == before
        assert "only group admins can edit someone else's shelf" in L.MOVIE_SYSTEM
        # own shelf via own name is still fine for a non-admin
        r = L._dispatch_tool("add_to_library", {"title": "Poetry", "target": "Asa"}, _shelf_ctx(2))
        assert r["status"] == "ok"
    finally:
        _admin_teardown()


def test_unknown_target_returns_not_found():
    _admin_setup({1})
    try:
        r = L._dispatch_tool("add_to_library", {"title": "Poetry", "target": "@nobody"},
                             _shelf_ctx(1))
        assert r["status"] == "not_found" and r["changed"] == 0
        assert not any(k[1].startswith("lib#") for k in STORE)
    finally:
        _admin_teardown()


def test_correct_film_swaps_metadata_keeps_owner_added_watched():
    _admin_setup({1})
    pk = L._pk(MODE, CHAT)
    STORE[(pk, "lib#2#solaris")] = {
        "PK": pk, "SK": "lib#2#solaris", "slug": "solaris", "owner_id": 2,
        "title": "Solaris", "year": "2002", "tmdb_id": 2103, "runtime_min": 99,
        "genres": ["Science Fiction"], "rating": "3.1", "rating_scale": 5,
        "added_at": "2026-01-01T00:00:00+00:00", "watched": True, "description": "Soderbergh"}
    seen = {}

    def fake_lookup_1972(title, year=None):
        seen["q"] = (title, year)
        return {"found": True, "title": "Solaris", "year": "1972", "slug": "solaris-1972",
                "tmdb_id": 593, "runtime_min": 167, "genres": ["Drama", "Science Fiction"],
                "description": "Tarkovsky", "language": "ru", "countries": ["SU"],
                "rating": 4.2, "rating_scale": 5, "rt_rating": "93%", "alts": []}
    orig_lookup, orig_dir = L.lookup_film, L._tmdb_director_films
    L.lookup_film = fake_lookup_1972
    L._tmdb_director_films = lambda name: [{"title": "Solaris", "year": "1972"}]
    try:
        r = L._dispatch_tool("correct_film",
                             {"title": "solaris", "director": "Tarkovsky", "target": "Asa"},
                             _shelf_ctx(1))
    finally:
        L.lookup_film, L._tmdb_director_films = orig_lookup, orig_dir
        _admin_teardown()
    assert seen["q"] == ("Solaris", "1972")              # director hint pinned the year
    assert r["status"] == "ok" and r["changed"] == 1
    assert r["old"]["year"] == "2002" and r["new"]["year"] == "1972"
    assert (pk, "lib#2#solaris") not in STORE
    row = STORE[(pk, "lib#2#solaris-1972")]
    assert row["tmdb_id"] == 593 and row["runtime_min"] == 167 and row["rating"] == "4.2"
    assert row["description"] == "Tarkovsky" and row["language"] == "ru"
    assert row["owner_id"] == 2 and row["watched"] is True
    assert row["added_at"] == "2026-01-01T00:00:00+00:00"


def test_correct_film_not_on_shelf_is_not_found():
    _reset()
    r = L._dispatch_tool("correct_film", {"title": "Nope"}, _shelf_ctx(1))
    assert r["status"] == "not_found" and r["changed"] == 0



# ---- Letterboxd: film-fact lookups + admin_backfill_lb --------------------- #
def _lb_stub(results):
    """Stub the single Letterboxd fetch; records every tmdb_id fetched."""
    calls = []

    def fake(tmdb_id):
        calls.append(tmdb_id)
        return results.get(tmdb_id)
    return fake, calls


def _real_cached():
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "lf_real", os.path.join(os.path.dirname(__file__), "..", "lambda_function.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.lookup_film_cached


_REAL_CACHED_FN = _real_cached()


def _with_real_cached(fn):
    """Run fn with the module's real lookup_film_cached bound to L's globals."""
    real = types.FunctionType(_REAL_CACHED_FN.__code__, L.__dict__, "lookup_film_cached",
                              _REAL_CACHED_FN.__defaults__)
    saved = L.lookup_film_cached
    L.lookup_film_cached = real
    try:
        return fn()
    finally:
        L.lookup_film_cached = saved


def test_lookup_tool_is_offered_and_demands_lookup_for_film_facts():
    names = [t["toolSpec"]["name"] for t in L.MOVIE_TOOLS]
    assert "lookup_film" in names
    desc = next(t for t in L.MOVIE_TOOLS if t["toolSpec"]["name"] == "lookup_film")
    d = desc["toolSpec"]["description"]
    assert "Letterboxd" in d and "ALWAYS call" in d
    assert "FILM FACTS" in L.MOVIE_SYSTEM and "MUST call lookup_film" in L.MOVIE_SYSTEM
    assert "never say you can't pull" in L.MOVIE_SYSTEM


def test_film_fact_question_detection():
    q = "@SirWatchedALot_bot can you give Letterboxd score for varda's vagabond?"
    assert L._needs_film_lookup(q)
    assert L._needs_film_lookup("what's the imdb rating on Heat")
    assert not L._needs_film_lookup("what did I rate Persona")      # star polls: get_ratings
    assert not L._needs_film_lookup("anyone free saturday?")


def test_converse_forces_a_tool_when_film_fact_answered_from_memory():
    _reset()
    seen = []
    replies = [
        {"stopReason": "end_turn", "output": {"message": {"role": "assistant", "content": [
            {"text": "I don't have Letterboxd scores to hand"}]}}},
        {"stopReason": "tool_use", "output": {"message": {"role": "assistant", "content": [
            {"toolUse": {"toolUseId": "t1", "name": "lookup_film",
                         "input": {"title": "Vagabond", "year": 1985}}}]}}},
        {"stopReason": "end_turn", "output": {"message": {"role": "assistant", "content": [
            {"text": "Vagabond (1985) sits at 4.1/5 on Letterboxd."}]}}},
    ]

    class FakeBedrock:
        def converse(self, **kw):
            seen.append(kw["toolConfig"].get("toolChoice"))
            return replies[len(seen) - 1]
    saved_b, saved_l = L._bedrock, L.lookup_film_cached
    L._bedrock = FakeBedrock()
    L.lookup_film_cached = lambda t, y=None, refresh_lb=False: {
        "found": True, "title": "Vagabond", "year": "1985", "tmdb_id": 44018,
        "rating": 4.1, "rating_scale": 5, "rating_source": "letterboxd"}
    try:
        ctx = {"chat_id": CHAT, "user_id": 1, "mode": MODE}
        out = L.converse("sys", "Chad: letterboxd score for vagabond?", ctx,
                         require_tool=True)
    finally:
        L._bedrock, L.lookup_film_cached = saved_b, saved_l
    assert seen[0] is None and seen[1] == {"any": {}}
    assert "4.1/5 on Letterboxd" in out


def test_lookup_tool_labels_letterboxd_and_tmdb_fallback():
    _reset()
    saved = L.lookup_film_cached
    try:
        L.lookup_film_cached = lambda t, y=None, refresh_lb=False: {
            "found": True, "title": "Vagabond", "year": "1985", "rating": 4.05,
            "rating_scale": 5, "rating_source": "letterboxd"}
        r = L._dispatch_tool("lookup_film", {"title": "Vagabond"},
                             {"chat_id": CHAT, "user_id": 1, "mode": MODE})
        assert r["rating_label"].endswith("/5 on Letterboxd")
        L.lookup_film_cached = lambda t, y=None, refresh_lb=False: {
            "found": True, "title": "Obscure", "year": "1999", "rating": 7.2,
            "rating_scale": 10, "rating_source": "tmdb"}
        r = L._dispatch_tool("lookup_film", {"title": "Obscure"},
                             {"chat_id": CHAT, "user_id": 1, "mode": MODE})
        assert r["rating_label"] == "TMDB 7.2/10"
    finally:
        L.lookup_film_cached = saved


def test_cached_tmdb_fallback_gets_one_letterboxd_retry_then_daily_cap():
    _reset()
    key = L._film_cache_key("vagabond", None)
    STORE[(key, "ref")] = {"PK": key, "SK": "ref", "json": json.dumps({
        "found": True, "title": "Vagabond", "year": "1985", "tmdb_id": 44018,
        "rating": 7.6, "rating_scale": 10, "rating_source": "tmdb"})}
    fake, calls = _lb_stub({44018: 4.1})
    saved = L._letterboxd_rating
    L._letterboxd_rating = fake
    try:
        info = _with_real_cached(lambda: L.lookup_film_cached("vagabond", refresh_lb=True))
        assert info["rating"] == 4.1 and info["rating_scale"] == 5
        assert info["rating_source"] == "letterboxd" and calls == [44018]
        again = _with_real_cached(lambda: L.lookup_film_cached("vagabond", refresh_lb=True))
        assert again["rating_scale"] == 5 and calls == [44018]       # cached now, no fetch
        # a miss is not refetched again within the day
        STORE[(key, "ref")]["json"] = json.dumps({
            "found": True, "title": "Vagabond", "tmdb_id": 1, "rating": 7.0,
            "rating_scale": 10})
        L._letterboxd_rating, calls2 = _lb_stub({})
        _with_real_cached(lambda: L.lookup_film_cached("vagabond", refresh_lb=True))
        _with_real_cached(lambda: L.lookup_film_cached("vagabond", refresh_lb=True))
        assert calls2 == [1]
    finally:
        L._letterboxd_rating = saved


BF_CHAT = -5160106592


def _bf_rows():
    pk = L._pk(MODE, BF_CHAT)
    rows = [
        {"SK": "lib#1#vagabond", "title": "Vagabond", "year": "1985", "tmdb_id": 44018,
         "rating": "7.6", "rating_scale": 10},
        {"SK": "lib#1#heat", "title": "Heat", "year": "1995", "tmdb_id": 949,
         "rating": None, "rating_scale": None},
        {"SK": "lib#2#obscure", "title": "Obscure", "year": "1999", "tmdb_id": 7,
         "rating": "6.0", "rating_scale": 10},
        {"SK": "lib#2#done", "title": "Done", "year": "2000", "tmdb_id": 8,
         "rating": "4.2", "rating_scale": 5},
        {"SK": "history#x", "title": "Not a shelf row"},
    ]
    for r in rows:
        STORE[(pk, r["SK"])] = {"PK": pk, **r}
    return pk


def _bf_update(pk, sk, fields):
    STORE[(pk, sk)].update(fields)


def test_backfill_dry_run_lists_candidates_without_fetching():
    _reset()
    _bf_rows()
    fake, calls = _lb_stub({44018: 4.1})
    saved = L._letterboxd_rating
    L._letterboxd_rating = fake
    try:
        out = L.lambda_handler({"admin_backfill_lb": {"chat": BF_CHAT, "dry_run": True}}, None)
    finally:
        L._letterboxd_rating = saved
    rep = json.loads(out["body"])
    assert rep["dry_run"] is True and rep["candidates_total"] == 3
    assert sorted(c["title"] for c in rep["candidates"]) == ["Heat", "Obscure", "Vagabond"]
    assert calls == []


def test_backfill_updates_successes_and_leaves_failures_untouched():
    _reset()
    pk = _bf_rows()
    before_obscure = copy.deepcopy(STORE[(pk, "lib#2#obscure")])
    fake, calls = _lb_stub({44018: 4.1, 949: 4.3})
    saved = (L._letterboxd_rating, L.ddb_update_fields)
    L._letterboxd_rating, L.ddb_update_fields = fake, _bf_update
    try:
        out = L.lambda_handler({"admin_backfill_lb": {"chat": BF_CHAT, "limit": 50,
                                                       "dry_run": False}}, None)
    finally:
        L._letterboxd_rating, L.ddb_update_fields = saved
    rep = json.loads(out["body"])
    assert rep["candidates_total"] == 3 and rep["attempted"] == 3
    assert rep["updated"] == 2 and rep["failed"] == 1 and rep["remaining"] == 1
    assert rep["failed_titles"][0]["title"] == "Obscure"
    assert sorted(calls) == [7, 949, 44018]                 # exactly one fetch per title
    v = STORE[(pk, "lib#1#vagabond")]
    assert v["rating"] == "4.1" and v["rating_scale"] == 5 and v["lb_rating"] == "4.1"
    assert STORE[(pk, "lib#2#obscure")] == before_obscure
    assert STORE[(pk, "lib#2#done")]["rating"] == "4.2"     # non-candidate untouched


def test_backfill_respects_limit_and_reports_remaining():
    _reset()
    _bf_rows()
    fake, calls = _lb_stub({44018: 4.1, 949: 4.3, 7: 3.0})
    saved = (L._letterboxd_rating, L.ddb_update_fields)
    L._letterboxd_rating, L.ddb_update_fields = fake, _bf_update
    try:
        rep = L.backfill_lb(chat=BF_CHAT, limit=1, dry_run=False)
    finally:
        L._letterboxd_rating, L.ddb_update_fields = saved
    assert rep["attempted"] == 1 and rep["updated"] == 1 and rep["remaining"] == 2
    assert len(calls) == 1 and rep["next_start_after"]


def test_backfill_uses_cached_letterboxd_rating_without_fetching():
    _reset()
    pk = _bf_rows()
    key = L._film_cache_key("Vagabond", "1985")
    STORE[(key, "ref")] = {"PK": key, "SK": "ref", "json": json.dumps(
        {"found": True, "tmdb_id": 44018, "rating": 4.1, "rating_scale": 5})}
    fake, calls = _lb_stub({949: 4.3})
    saved = (L._letterboxd_rating, L.ddb_update_fields)
    L._letterboxd_rating, L.ddb_update_fields = fake, _bf_update
    try:
        rep = L.backfill_lb(chat=BF_CHAT, limit=50, dry_run=False)
    finally:
        L._letterboxd_rating, L.ddb_update_fields = saved
    assert 44018 not in calls                                # served from filmcache
    assert STORE[(pk, "lib#1#vagabond")]["rating"] == "4.1"
    assert rep["updated"] == 2 and rep["failed"] == 1


def test_backfill_only_from_direct_invoke():
    _reset()
    _bf_rows()
    called = []
    saved = L.backfill_lb
    L.backfill_lb = lambda **k: called.append(k) or {}
    try:
        for ev in ({"admin_backfill_lb": {"chat": BF_CHAT}, "rawPath": "/movie"},
                   {"admin_backfill_lb": {"chat": BF_CHAT}, "requestContext": {}}):
            L.lambda_handler(ev, None)
        assert called == []
        L.lambda_handler({"admin_backfill_lb": {"chat": BF_CHAT}}, None)
        assert len(called) == 1
    finally:
        L.backfill_lb = saved


# ---- shelf recognition without a seedclaim -------------------------------- #
def _dariia_setup(n_films):
    _reset()
    L.remember_member(CHAT, 393306645, "Dariia", "dariuozy")
    for i in range(n_films):
        L.add_to_library(CHAT, 393306645, f"Film {i}")


def test_member_with_rows_and_no_seedclaim_gets_shelf_listed():
    _dariia_setup(28)
    assert not any(s.startswith("seedclaim#") for (_p, s) in STORE)
    own = L._dispatch_tool("list_library", {}, _shelf_ctx(393306645))
    assert own["resolved"] and own["has_shelf"] and own["count"] == 28
    assert len(own["films"]) == 28 and "next_step" not in own
    for whose in ("Daria", "Dariia", "@dariuozy", "dariuozy"):     # by name / nickname
        r = L._dispatch_tool("list_library", {"whose": whose}, _shelf_ctx(1))
        assert r["resolved"] and r["count"] == 28, whose
        r = L._dispatch_tool("list_library", {"whose": whose}, _shelf_ctx(393306645))
        assert r["owner"] == "your" and r["count"] == 28, whose    # her own name -> own shelf
    assert L.resolve_owner(CHAT, "Daria")[0] == "393306645"
    assert "call list_library first" in L.MOVIE_SYSTEM
    assert "haven't claimed a shelf" not in L.MOVIE_SYSTEM


def test_member_with_zero_rows_and_matching_seed_is_offered_claim():
    _reset()
    L.remember_member(CHAT, 77, "Asa", "AsaFoxColorist")
    L.seed_starter_libraries(CHAT)
    r = L._dispatch_tool("list_library", {}, _shelf_ctx(77))
    assert r["has_shelf"] is False and r["count"] == 0
    assert r["starter_to_claim"] == "Asa" and "I'm Asa" in r["next_step"]
    L.claim_library(CHAT, "Asa", 77)
    r = L._dispatch_tool("list_library", {}, _shelf_ctx(77))
    assert r["has_shelf"] and r["count"] == 6 and "next_step" not in r


def test_member_with_zero_rows_and_no_seed_is_told_to_add():
    _dariia_setup(0)
    L.seed_starter_libraries(CHAT)                 # starters exist, none match Dariia
    r = L._dispatch_tool("list_library", {}, _shelf_ctx(393306645))
    assert r["has_shelf"] is False and r["starter_to_claim"] is None
    assert "add films" in r["next_step"]


def test_ratings_and_recommend_resolve_unclaimed_member_by_name():
    _dariia_setup(2)
    assert L.resolve_owner(CHAT, "@dariuozy") == ("393306645", "Dariia")
    r = L._dispatch_tool("get_ratings", {"whose": "Daria"}, _shelf_ctx(1))
    assert r.get("resolved") is not False and r["ratings"] == []
    r = L._dispatch_tool("recommend_films", {"whose": "Dariia"}, _shelf_ctx(1))
    assert r.get("resolved") is not False


def test_history_names_participants():
    _dariia_setup(0)
    pk = L._pk(MODE, CHAT)
    L.ddb_put({"PK": pk, "SK": "history#s1", "winner_title": "Mirror", "watched_date": "2026-11-01",
               "participants": [393306645]})
    assert L.get_history(CHAT)[0]["participant_names"] == ["Dariia"]


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
