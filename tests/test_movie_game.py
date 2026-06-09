"""Local tests for movie-night game logic.

These do NOT ship (deploy zips only lambda_function.py). They drive the handler
with in-memory fakes for DynamoDB + Telegram + Letterboxd/Bedrock, so the
deterministic parts — selection counts, veto rules, reply routing, idempotency —
are exercised without any AWS or network. Run: python3 tests/test_movie_game.py
"""
import copy
import os
import sys
import types

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Stub boto3 / botocore so the module imports with no AWS SDK installed. We never
# call the real clients — ddb_* and seen_update are monkeypatched below.
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
_mid = [1000]


# ---- in-memory fakes ------------------------------------------------------ #
def _reset():
    STORE.clear()
    SENT.clear()
    _mid[0] = 1000


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


def fake_lookup(title):
    return {"found": True, "title": title, "rating": 4.0, "rating_scale": 5,
            "rating_source": "Letterboxd", "year": "1950", "runtime_min": 120,
            "genres": ["Drama"], "letterboxd_url": "http://lb/x"}


L.ddb_get = fake_get
L.ddb_put = fake_put
L.ddb_delete = fake_delete
L.ddb_query = fake_query
L.seen_update = fake_seen
L.send_message = fake_send
L.lookup_film = fake_lookup
L.winner_note = lambda *a, **k: ""
L.AI_ENABLED = False


def upd(text, user_id, name, reply_to=None, update_id=None, username=None):
    return {
        "update_id": update_id, "chat_id": CHAT, "chat_title": "T",
        "chat_type": "group", "text": text, "user_id": user_id,
        "user_name": name, "username": username,
        "reply_to_message_id": reply_to, "migrate_to_chat_id": None,
        "migrate_from_chat_id": None, "raw_message": {},
    }


def deliver(u):
    """Mirror lambda_handler: dedupe on update_id, then dispatch."""
    if fake_seen(MODE, CHAT, u.get("update_id")):
        return
    L.handle_movie(MODE, u)


def card_mid_for(game, user_id):
    for mid, e in game["message_index"].items():
        if e["kind"] == "card" and str(e["user_id"]) == str(user_id):
            return int(mid)
    return None


def pick_mid(game):
    for mid, e in game["message_index"].items():
        if e["kind"] == "pick":
            return int(mid)
    return None


# ---- tests ---------------------------------------------------------------- #
def test_movie_adds_to_sender_library():
    _reset()
    deliver(upd("/movie Rear Window", 1, "Chad"))
    lib = L.get_library(CHAT, 1)
    assert len(lib) == 1 and lib[0]["title"] == "Rear Window", lib
    assert lib[0]["owner_id"] == 1
    # someone else's library is untouched
    assert L.get_library(CHAT, 2) == []


def test_idempotent_add():
    _reset()
    deliver(upd("/movie Tokyo Story", 1, "Chad", update_id=42))
    deliver(upd("/movie Tokyo Story", 1, "Chad", update_id=42))  # retry
    assert len(L.get_library(CHAT, 1)) == 1


def test_eligible_excludes_watched_and_veto_aware():
    lib = [
        {"film_id": "a", "title": "A", "watched": True, "vetoed_by": []},
        {"film_id": "b", "title": "B", "watched": False, "vetoed_by": [7]},
        {"film_id": "c", "title": "C", "watched": False, "vetoed_by": []},
    ]
    # 7 present -> B excluded (C remains), A always excluded (watched)
    elig = {f["film_id"] for f in L.eligible_films(lib, [7])}
    assert elig == {"c"}, elig
    # 7 absent -> B eligible again
    elig = {f["film_id"] for f in L.eligible_films(lib, [99])}
    assert elig == {"b", "c"}, elig
    # only the vetoed one unwatched, vetoer present -> fall back to it (better
    # to re-offer than offer nothing)
    lib2 = [{"film_id": "b", "title": "B", "watched": False, "vetoed_by": [7]}]
    elig = {f["film_id"] for f in L.eligible_films(lib2, [7])}
    assert elig == {"b"}, elig


def test_selection_count_and_stable_on_retry():
    _reset()
    for t in ["A", "B", "C", "D", "E"]:
        deliver(upd(f"/movie {t}", 1, "Chad"))
    deliver(upd("/movie Solo", 2, "Asa"))
    deliver(upd("/movienight", 1, "Chad", update_id=1))
    deliver(upd("/join", 2, "Asa", update_id=2))
    deliver(upd("/select", 1, "Chad", update_id=3))
    g1 = L.get_game(CHAT)
    assert len(g1["selections"]["1"]) == 3   # min(3, 5)
    assert len(g1["selections"]["2"]) == 1   # min(3, 1)
    # retried /select (same update_id) must not re-roll
    snap = copy.deepcopy(g1["selections"])
    deliver(upd("/select", 1, "Chad", update_id=3))
    assert L.get_game(CHAT)["selections"] == snap


def test_reply_routing_confirm_only_owner():
    _reset()
    for t in ["A", "B", "C"]:
        deliver(upd(f"/movie {t}", 1, "Chad"))
    deliver(upd("/movienight", 1, "Chad", update_id=1))
    deliver(upd("/select", 1, "Chad", update_id=2))
    g = L.get_game(CHAT)
    mid = card_mid_for(g, 1)
    assert mid is not None
    # a different user replying to Chad's card is ignored
    deliver(upd("👍", 2, "Asa", reply_to=mid, update_id=3))
    assert L.get_game(CHAT)["confirmed"] == []
    # the owner replying confirms
    deliver(upd("👍", 1, "Chad", reply_to=mid, update_id=4))
    assert 1 in L.get_game(CHAT)["confirmed"]


def test_emoji_swap_drops_thumbsdown_and_backfills():
    _reset()
    for t in ["A", "B", "C", "D"]:
        deliver(upd(f"/movie {t}", 1, "Chad"))
    deliver(upd("/movienight", 1, "Chad", update_id=1))
    deliver(upd("/select", 1, "Chad", update_id=2))
    g = L.get_game(CHAT)
    sel_before = list(g["selections"]["1"])
    mid = card_mid_for(g, 1)
    # keep #1, drop #2, keep #3 -> backfill to 3 from the library
    deliver(upd("👍 👎 👍", 1, "Chad", reply_to=mid, update_id=3))
    sel_after = L.get_game(CHAT)["selections"]["1"]
    assert len(sel_after) == 3
    assert sel_before[1] not in sel_after  # the 👎 film is gone


def test_veto_then_second_veto_rejected_then_finalize():
    _reset()
    deliver(upd("/movie OnlyA", 1, "Chad"))
    deliver(upd("/movie OnlyB", 2, "Asa"))
    deliver(upd("/movienight", 1, "Chad", update_id=1))
    deliver(upd("/join", 2, "Asa", update_id=2))
    deliver(upd("/select", 1, "Chad", update_id=3))
    deliver(upd("/lock", 1, "Chad", update_id=4))
    deliver(upd("/go", 1, "Chad", update_id=5))
    g = L.get_game(CHAT)
    first_pick = g["picked"]
    # Chad vetoes the current pick
    deliver(upd("/veto", 1, "Chad", update_id=6))
    g = L.get_game(CHAT)
    assert first_pick in g["vetoed_pile"]
    assert g["picked"] != first_pick
    assert g["vetoes_left"]["1"] == 0
    # the vetoed film recorded Chad as a vetoer in its library item
    owner = int(g["film_owner"][first_pick])
    vf = L.get_film(CHAT, owner, first_pick)
    assert 1 in vf["vetoed_by"]
    # second veto from Chad is rejected (state unchanged)
    pick_now = g["picked"]
    deliver(upd("/veto", 1, "Chad", update_id=7))
    assert L.get_game(CHAT)["picked"] == pick_now
    # finalize
    deliver(upd("/watch", 1, "Chad", update_id=8))
    assert L.get_game(CHAT) is None  # game cleared
    hist = [v for (_p, s), v in STORE.items() if s.startswith("history#")]
    assert len(hist) == 1 and hist[0]["winner_film_id"] == pick_now
    # winner marked watched in its owner's library
    wowner = int(hist[0]["winner_owner_id"])
    assert L.get_film(CHAT, wowner, pick_now)["watched"] is True


def test_claim_links_seed_library_to_user():
    _reset()
    pk = L._pk(MODE, CHAT)
    # simulate a seeded "Chad" library (2 films) under the placeholder owner
    for t in ["Red River", "Barry Lyndon"]:
        fid = t.replace(" ", "")
        STORE[(pk, f"lib#seed:chad#{fid}")] = {
            "PK": pk, "SK": f"lib#seed:chad#{fid}", "film_id": fid,
            "owner_id": "seed:chad", "seed_name": "Chad", "title": t,
            "year": "1948", "added_at": "x", "watched": False, "vetoed_by": [],
        }
    assert L.list_seed_names(CHAT) == ["chad"]
    # Chad (user 1) claims
    res = L.claim_library(CHAT, "Chad", 1)
    assert res == {"status": "ok", "moved": 2}, res
    lib = L.get_library(CHAT, 1)
    assert {f["title"] for f in lib} == {"Red River", "Barry Lyndon"}
    assert all(f["owner_id"] == 1 for f in lib)
    assert L.list_seed_names(CHAT) == []          # no longer a seed
    # someone else can't steal it
    assert L.claim_library(CHAT, "Chad", 2)["status"] == "taken"
    # re-claim by the same user is a no-op
    assert L.claim_library(CHAT, "Chad", 1)["moved"] == 0
    # unknown name
    assert L.claim_library(CHAT, "Nobody", 3)["status"] == "none"


def test_parse_confirm_tokens():
    assert L._parse_confirm_tokens("👍")[1] is True            # keep_all
    assert L._parse_confirm_tokens("y n y")[0] == [True, False, True]
    assert L._parse_confirm_tokens("👍👎👍")[0] == [True, False, True]
    assert L._parse_confirm_tokens("maybe")[0] == []           # unknown ignored


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
            failed += 1
            print(f"ERROR {t.__name__}: {type(e).__name__}: {e}")
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    sys.exit(1 if failed else 0)
