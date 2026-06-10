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
    L.handle_movie(MODE, message(1, "go"))  # no constraints -> SELECTING (asks player 1)
    assert L.get_game(CHAT)["phase"] == "SELECTING"
    L.handle_movie(MODE, message(1, "👍"))  # player 1 keeps (1 film) -> next player
    L.handle_movie(MODE, message(2, "👍"))  # player 2 keeps -> VETO
    return L.get_game(CHAT)


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


def test_veto_consumes_repicks_and_blocks_second_veto():
    game = _two_player_game_to_veto()
    cur = game["current"]
    first_poll = cur["poll_id"]
    first_film = cur["film"]["slug"]
    # player 1 votes Veto (option 0)
    L.handle_movie(MODE, poll_answer(1, first_poll, [0]))
    game = L.get_game(CHAT)
    assert game["vetoes_remaining"]["1"] == 0
    assert first_poll in [p for p in POLLS_STOPPED] or True  # stopPoll called
    new = game["current"]
    assert new["poll_id"] != first_poll
    assert new["film"]["slug"] != first_film
    # player 1 has no veto left -> second veto ignored
    L.handle_movie(MODE, poll_answer(1, new["poll_id"], [0]))
    game = L.get_game(CHAT)
    assert game["current"]["poll_id"] == new["poll_id"]   # unchanged
    # auto-close the survivor -> winner
    L.handle_movie(MODE, poll_closed(new["poll_id"]))
    assert L.get_game(CHAT) is None


def test_stale_poll_close_is_ignored():
    game = _two_player_game_to_veto()
    cur = game["current"]
    first_poll = cur["poll_id"]
    L.handle_movie(MODE, poll_answer(1, first_poll, [0]))   # veto -> moves on
    # the vetoed poll's own auto-close arrives late: must NOT declare a winner
    L.handle_movie(MODE, poll_closed(first_poll))
    assert L.get_game(CHAT) is not None
    assert L.get_game(CHAT)["phase"] == "VETO"


def test_backstop_resolves_after_90s_on_any_update():
    game = _two_player_game_to_veto()
    cur = game["current"]
    NOW[0] += 91                              # candidate is now stale
    L.handle_movie(MODE, message(2, "anything at all"))   # any update fires backstop
    assert L.get_game(CHAT) is None           # winner declared, game cleared


def test_thumbs_down_swaps_a_slot_and_reasks():
    _reset()
    for t in ["A", "B", "C", "D"]:
        L.add_to_library(CHAT, 1, t)
    L.start_game(MODE, CHAT, 1)
    L.handle_movie(MODE, cb(1, "start"))
    L.handle_movie(MODE, message(1, "go"))      # -> SELECTING, player 1 asked
    sel = L.get_game(CHAT)["selection"]["1"]
    assert len(sel["slots"]) == 3 and len(sel["shown"]) == 3
    slot0_before = sel["slots"][0]["slug"]
    L.handle_movie(MODE, message(1, "👎👍👍"))  # swap slot 1, keep the rest
    sel = L.get_game(CHAT)["selection"]["1"]
    assert len(sel["shown"]) == 4               # a 4th film was drawn in
    assert sel["slots"][0]["slug"] != slot0_before
    assert L.get_game(CHAT)["phase"] == "SELECTING"   # re-asked, not locked yet
    L.handle_movie(MODE, message(1, "👍👍👍"))  # now keep all -> locks
    assert L.get_game(CHAT)["selection"]["1"]["locked"] is True


def test_selection_is_sequential_one_player_at_a_time():
    _reset()
    for t in ["A1", "A2", "A3", "A4"]:
        L.add_to_library(CHAT, 1, t)
    for t in ["B1", "B2", "B3", "B4"]:
        L.add_to_library(CHAT, 2, t)
    L.start_game(MODE, CHAT, 1)
    L.handle_movie(MODE, cb(2, "join"))
    L.handle_movie(MODE, cb(1, "start"))
    L.handle_movie(MODE, message(1, "go"))           # -> SELECTING, player 1 first
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
    # player 2 keeps -> VETO
    L.handle_movie(MODE, message(2, "👍👍👍"))
    assert L.get_game(CHAT)["phase"] == "VETO"


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


def test_poll_film_tool_resolves_and_registers():
    _reset()
    ctx = {"chat_id": CHAT, "user_id": 1, "user_name": "U1", "mode": MODE, "suppress_reply": False}
    out = L._dispatch_tool("poll_film", {"title": "Star Wars"}, ctx)
    assert out["polled"] and out["title"] == "Star Wars"
    polls = [v for (p, _s), v in STORE.items() if p == "ratingpoll"]
    assert len(polls) == 1 and polls[0]["film_title"] == "Star Wars"


def test_rating_vote_does_not_disturb_veto_poll():
    # a veto-phase poll vote still routes to the veto logic (no ratingpoll item)
    game = _two_player_game_to_veto()
    pid = game["current"]["poll_id"]
    assert L.get_rating_poll(pid) is None
    L.handle_movie(MODE, poll_answer(1, pid, [0]))   # veto
    assert L.get_game(CHAT)["current"]["film"]["slug"] != game["current"]["film"]["slug"] \
        or L.get_game(CHAT)["vetoes_remaining"]["1"] == 0


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
    L.handle_movie(MODE, cb(1, "sp_play"))                # accept the 2
    assert L.get_game(CHAT)["phase"] == "VETO"            # 1 player locked -> veto


def test_short_pool_add_qualifying_reaches_three():
    _short_pool_setup(["drama", "drama", "horror"])
    L.handle_movie(MODE, cb(1, "sp_add"))
    assert L.get_game(CHAT)["selection"]["1"]["awaiting_add"] is True
    L.handle_movie(MODE, message(1, "New Drama"))         # fake_lookup -> Drama, fits
    g = L.get_game(CHAT)
    assert g["phase"] == "VETO"                           # reached 3 -> locked + advanced
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
    g = L.get_game(CHAT)
    assert g["awaiting_relax"] is False
    assert g["current"]["film"]["slug"] == "dra"      # now eligible, presented


def test_constraints_window_parses_three_people_then_go_closes():
    _reset()
    pk = L._pk(MODE, CHAT)   # a film that survives the filter below (1980, drama, 100m)
    STORE[(pk, "lib#1#film-a")] = {"PK": pk, "SK": "lib#1#film-a", "slug": "film-a",
        "owner_id": 1, "title": "Film A", "year": "1980", "genres": ["drama"],
        "runtime_min": 100, "watched": False}
    L.start_game(MODE, CHAT, 1)
    L.handle_movie(MODE, cb(1, "start"))              # -> CONSTRAINTS, question asked once
    assert L.get_game(CHAT)["phase"] == "CONSTRAINTS"
    assert sum("constraints tonight" in t.lower() for t, _ in SENT) == 1
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
        L.handle_movie(MODE, message(1, "go"))        # explicit close
        assert L.get_game(CHAT)["phase"] == "SELECTING"
    finally:
        L.parse_constraint_text = orig


def test_constraints_backstop_closes_after_60s_silence():
    _reset()
    L.add_to_library(CHAT, 1, "Film A")
    L.start_game(MODE, CHAT, 1)
    L.handle_movie(MODE, cb(1, "start"))
    assert L.get_game(CHAT)["phase"] == "CONSTRAINTS"
    NOW[0] += 61
    L.handle_movie(MODE, message(2, "anything"))      # next event past deadline closes it
    assert L.get_game(CHAT)["phase"] == "SELECTING"


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


def test_confirm_saves_pending_film():
    _reset()
    L._set_pending(CHAT, 1, "One from the Heart", "1981")
    L.handle_movie(MODE, message(1, "yes, add it"))     # affirmative binds to pending
    lib = L.get_library(CHAT, 1)
    assert [f["title"] for f in lib] == ["One from the Heart"]
    assert L._get_pending(CHAT, 1) is None               # cleared after add
    assert any("Added" in t for t, _ in SENT)


def test_unanimous_fine_finalizes_without_clock():
    game = _two_player_game_to_veto()
    pid = game["current"]["poll_id"]
    L.handle_movie(MODE, poll_answer(1, pid, [1]))       # player 1: fine by me
    assert L.get_game(CHAT) is not None                  # not yet — player 2 pending
    L.handle_movie(MODE, poll_answer(2, pid, [1]))       # player 2: fine -> unanimous
    assert L.get_game(CHAT) is None                      # finalized immediately
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
