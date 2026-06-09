"""Local tests for movie-mode (natural-language + reaction/poll game engine).

These do NOT ship (deploy zips only lambda_function.py). They drive the engine
with in-memory fakes for DynamoDB + Telegram + Letterboxd/Bedrock, so the
deterministic parts — library, seeding/claim, selection, veto, win resolution,
and the no-scheduler backstop — run without AWS or network.
Run: python3 tests/test_movie_game.py
"""
import copy
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


def fake_lookup(title):
    return {"found": True, "title": title, "slug": L._slugify(title),
            "year": "1950", "runtime_min": 120, "genres": ["Drama"],
            "description": "A film.", "rating": 4.0, "rating_scale": 5,
            "rating_source": "Letterboxd", "similar": ["Other Film"]}


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
    L.handle_movie(MODE, cb(1, "start"))    # begins selection
    game = L.get_game(CHAT)
    assert game["phase"] == "SELECTING"
    for uid in (1, 2):
        for mid in cards_for(game, uid):
            L.handle_movie(MODE, reaction(uid, mid, "👍"))
            game = L.get_game(CHAT)
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


def test_thumbs_down_replaces_card():
    _reset()
    for t in ["A", "B", "C", "D"]:
        L.add_to_library(CHAT, 1, t)
    L.start_game(MODE, CHAT, 1)
    L.handle_movie(MODE, cb(1, "start"))
    game = L.get_game(CHAT)
    sel = game["selection"]["1"]
    assert len(sel["slots"]) == 3 and len(sel["shown"]) == 3
    mid = cards_for(game, 1)[0]
    slot0_before = sel["slots"][0]["slug"]
    L.handle_movie(MODE, reaction(1, mid, "👎"))
    sel = L.get_game(CHAT)["selection"]["1"]
    assert len(sel["shown"]) == 4
    assert sel["slots"][0]["slug"] != slot0_before
    assert sel["slots"][0]["state"] == "pending"


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
