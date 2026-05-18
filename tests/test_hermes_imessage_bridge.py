import json
from pathlib import Path

from scripts.hermes_imessage_bridge import (
    HermesIMessageRouter,
    IMessage,
    normalize_sender,
    parse_sender_allowlist,
    read_messages,
    sender_allowed,
)


def make_router(tmp_path: Path) -> HermesIMessageRouter:
    return HermesIMessageRouter(
        allowed_senders={normalize_sender("+1 (555) 111-2222"), "anik@example.com"},
        queue_path=tmp_path / "queue.jsonl",
        audit_path=tmp_path / "audit.jsonl",
        pending_path=tmp_path / "pending.json",
    )


def read_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines()]


def test_sender_allowlist_normalizes_phone_and_email(monkeypatch):
    monkeypatch.setenv("HERMES_IMESSAGE_ALLOWED_SENDERS", "+1 (555) 111-2222, Anik@Example.com")

    allowed = parse_sender_allowlist()

    assert sender_allowed("5551112222", allowed)
    assert sender_allowed("+15551112222", allowed)
    assert sender_allowed("anik@example.com", allowed)
    assert not sender_allowed("5559990000", allowed)


def test_non_allowlisted_sender_is_ignored_and_not_replied(tmp_path):
    router = make_router(tmp_path)
    result = router.handle_message(IMessage(rowid=1, text="/status", sender="5559990000"))

    assert result.accepted is False
    assert result.reply is None
    assert result.reason == "sender_not_allowed"
    assert not (tmp_path / "queue.jsonl").exists()
    assert read_jsonl(tmp_path / "audit.jsonl")[0]["reason"] == "sender_not_allowed"


def test_safe_command_from_owner_is_queued(tmp_path):
    router = make_router(tmp_path)
    result = router.handle_message(IMessage(rowid=2, text="/ask check panda renders", sender="+15551112222"))

    assert result.accepted is True
    assert result.queued_id
    queue = read_jsonl(tmp_path / "queue.jsonl")
    assert queue[0]["command"] == "ask"
    assert queue[0]["args"] == "check panda renders"
    assert queue[0]["approved"] is True
    assert queue[0]["sender"] == "5551112222"


def test_group_chat_is_rejected_by_default(tmp_path):
    router = make_router(tmp_path)
    result = router.handle_message(
        IMessage(rowid=3, text="/ask hello", sender="5551112222", chat_identifier="chat123", room_name="Family"),
    )

    assert result.accepted is False
    assert result.reply == "Hermes ignores group chats."
    assert not (tmp_path / "queue.jsonl").exists()


def test_risky_restart_requires_same_sender_approval(tmp_path, monkeypatch):
    monkeypatch.setattr("scripts.hermes_imessage_bridge.random.randint", lambda _a, _b: 492100)
    router = make_router(tmp_path)

    request = router.handle_message(IMessage(rowid=4, text="/restart grid-hermes", sender="5551112222"))
    assert request.accepted is True
    assert request.approval_token == "492100"
    assert "approve 492100" in request.reply
    assert not (tmp_path / "queue.jsonl").exists()

    wrong_sender = router.handle_message(IMessage(rowid=5, text="approve 492100", sender="anik@example.com"))
    assert wrong_sender.accepted is False
    assert wrong_sender.reason == "approval_sender_mismatch"

    approval = router.handle_message(IMessage(rowid=6, text="approve 492100", sender="5551112222"))
    assert approval.accepted is True
    queue = read_jsonl(tmp_path / "queue.jsonl")
    assert queue[0]["command"] == "restart"
    assert queue[0]["args"] == "grid-hermes"
    assert queue[0]["approved"] is True


def test_restart_target_is_whitelisted(tmp_path):
    router = make_router(tmp_path)
    result = router.handle_message(IMessage(rowid=7, text="/restart sshd", sender="5551112222"))

    assert result.accepted is False
    assert "Restart target must be one of" in result.reply
    assert not (tmp_path / "pending.json").exists()


def test_read_messages_reads_new_inbound_rows(tmp_path):
    db_path = tmp_path / "chat.db"
    import sqlite3

    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE message (ROWID INTEGER PRIMARY KEY, date INTEGER, text TEXT, is_from_me INTEGER, handle_id INTEGER);
            CREATE TABLE handle (ROWID INTEGER PRIMARY KEY, id TEXT);
            CREATE TABLE chat (ROWID INTEGER PRIMARY KEY, chat_identifier TEXT, room_name TEXT);
            CREATE TABLE chat_message_join (message_id INTEGER, chat_id INTEGER);
            INSERT INTO handle VALUES (1, '+15551112222');
            INSERT INTO chat VALUES (1, '+15551112222', NULL);
            INSERT INTO message VALUES (10, 0, '/status', 0, 1);
            INSERT INTO message VALUES (11, 0, '/ignored outgoing', 1, 1);
            INSERT INTO chat_message_join VALUES (10, 1);
            INSERT INTO chat_message_join VALUES (11, 1);
            """
        )

    messages = read_messages(db_path, after_rowid=9)

    assert len(messages) == 1
    assert messages[0].rowid == 10
    assert messages[0].text == "/status"
    assert messages[0].sender == "+15551112222"
