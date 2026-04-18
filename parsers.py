"""
signaldesk/parsers.py
──────────────────────
Convert raw files/strings into RawInput objects.

Formats supported:
  Messages:  JSON array, plain text (one per line), Slack export
  Emails:    JSON array, plain text blocks
  Tickets:   JSON array, CSV
  Logs:      Any common log format
  Meetings:  Plain text transcript / notes
"""

from __future__ import annotations

import re
import csv
import json
import io
import uuid
from datetime import datetime
from typing import List, Optional, Dict, Any

from signaldesk.engine import RawInput, SourceType


# ─── Date parsing ─────────────────────────────────────────────────────────────

_DATE_FORMATS = [
    "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d",
    "%d/%m/%Y %H:%M", "%d/%m/%Y", "%m/%d/%Y %H:%M", "%m/%d/%Y",
    "%b %d %H:%M:%S", "%b %d %Y",
]

def _parse_dt(s: str) -> Optional[datetime]:
    if not s:
        return None
    s = str(s).strip()
    for fmt in _DATE_FORMATS:
        try:
            dt = datetime.strptime(s, fmt)
            if dt.year == 1900:
                dt = dt.replace(year=datetime.now().year)
            return dt
        except ValueError:
            continue
    return None

def _uid() -> str:
    return str(uuid.uuid4())[:8]


# ─── Message Parsers ──────────────────────────────────────────────────────────

def parse_messages_json(content: str) -> List[RawInput]:
    """
    Parse chat messages from JSON.
    Expected: array of {text/message/content, user/author/from, timestamp/ts, channel/source}
    """
    data = _load_json(content)
    if isinstance(data, dict):
        data = data.get("messages") or data.get("items") or [data]

    inputs = []
    for obj in data:
        if not isinstance(obj, dict):
            continue

        text = str(
            obj.get("text") or obj.get("message") or
            obj.get("content") or obj.get("body") or ""
        ).strip()
        if not text:
            continue

        author = str(
            obj.get("user") or obj.get("author") or
            obj.get("from") or obj.get("sender") or ""
        ).strip() or None

        ts  = _parse_dt(str(obj.get("timestamp") or obj.get("ts") or obj.get("time") or ""))
        src = str(obj.get("channel") or obj.get("source") or obj.get("room") or "").strip()

        inputs.append(RawInput(
            id=str(obj.get("id") or _uid()),
            content=text,
            source_type=SourceType.MESSAGE,
            source_name=src,
            author=author,
            timestamp=ts,
            metadata=obj,
        ))
    return inputs


def parse_messages_text(content: str) -> List[RawInput]:
    """
    Parse one message per line, or blocks separated by blank lines.
    Supports: "Alice: message text" prefix format.
    """
    inputs  = []
    blocks  = re.split(r'\n\s*\n', content.strip())

    for block in blocks:
        block = block.strip()
        if not block or len(block) < 4:
            continue

        # Try "Author: message" format
        m      = re.match(r'^([\w.\s@-]{2,30}):\s+(.+)$', block, re.DOTALL)
        author = m.group(1).strip() if m else None
        text   = m.group(2).strip() if m else block

        inputs.append(RawInput(
            id=_uid(),
            content=text,
            source_type=SourceType.MESSAGE,
            source_name="",
            author=author,
            timestamp=None,
        ))
    return inputs


# ─── Email Parsers ────────────────────────────────────────────────────────────

def parse_emails_json(content: str) -> List[RawInput]:
    data = _load_json(content)
    if isinstance(data, dict):
        data = data.get("emails") or data.get("messages") or [data]

    inputs = []
    for obj in data:
        if not isinstance(obj, dict):
            continue

        subject = str(obj.get("subject") or "")
        body    = str(obj.get("body") or obj.get("content") or obj.get("text") or "")
        text    = f"Subject: {subject}\n\n{body}".strip() if subject else body
        if not text:
            continue

        sender = str(obj.get("from") or obj.get("sender") or "")
        ts     = _parse_dt(str(obj.get("timestamp") or obj.get("date") or obj.get("sent_at") or ""))

        inputs.append(RawInput(
            id=str(obj.get("id") or obj.get("message_id") or _uid()),
            content=text,
            source_type=SourceType.EMAIL,
            source_name="email",
            author=_extract_email_addr(sender),
            timestamp=ts,
            metadata=obj,
        ))
    return inputs


def parse_emails_text(content: str) -> List[RawInput]:
    """Parse email blocks separated by '---' or blank lines with From: header."""
    blocks = re.split(r'\n\s*---+\s*\n|\n{3,}', content.strip())
    inputs = []

    for block in blocks:
        block = block.strip()
        if not block:
            continue

        def header(name: str) -> str:
            m = re.search(rf'^{name}:\s*(.+)$', block, re.IGNORECASE | re.MULTILINE)
            return m.group(1).strip() if m else ""

        subject = header("subject")
        sender  = header("from") or header("sender")
        ts      = _parse_dt(header("date") or header("sent"))
        body    = re.sub(r'^[\w\s]+:\s*.+$', '', block, flags=re.MULTILINE).strip()
        text    = f"Subject: {subject}\n\n{body}" if subject else body
        if not text.strip():
            continue

        inputs.append(RawInput(
            id=_uid(),
            content=text,
            source_type=SourceType.EMAIL,
            source_name="email",
            author=_extract_email_addr(sender),
            timestamp=ts,
        ))
    return inputs


# ─── Ticket Parsers ───────────────────────────────────────────────────────────

def parse_tickets_json(content: str) -> List[RawInput]:
    data = _load_json(content)
    if isinstance(data, dict):
        data = data.get("tickets") or data.get("issues") or data.get("items") or [data]

    inputs = []
    for obj in data:
        if not isinstance(obj, dict):
            continue

        title  = str(obj.get("title") or obj.get("summary") or obj.get("name") or "")
        desc   = str(obj.get("description") or obj.get("body") or obj.get("content") or "")
        status = str(obj.get("status") or obj.get("state") or "")
        prio   = str(obj.get("priority") or obj.get("severity") or "")

        # Build full text for signal extraction
        parts = []
        if title:
            parts.append(title)
        if status:
            parts.append(f"Status: {status}")
        if prio:
            parts.append(f"Priority: {prio}")
        if desc:
            parts.append(desc)
        text = "\n".join(parts)
        if not text.strip():
            continue

        ticket_id = str(obj.get("id") or obj.get("key") or obj.get("number") or _uid())
        author    = str(obj.get("assignee") or obj.get("reporter") or obj.get("owner") or "")
        ts        = _parse_dt(str(obj.get("created_at") or obj.get("updated_at") or obj.get("date") or ""))

        inputs.append(RawInput(
            id=ticket_id,
            content=text,
            source_type=SourceType.TICKET,
            source_name=ticket_id,
            author=author.strip() or None,
            timestamp=ts,
            metadata=obj,
        ))
    return inputs


def parse_tickets_csv(content: str) -> List[RawInput]:
    reader = csv.DictReader(io.StringIO(content.strip()))
    inputs = []
    COL = {
        "id":     ["id", "key", "ticket", "number", "issue_id"],
        "title":  ["title", "summary", "name", "subject"],
        "desc":   ["description", "body", "details", "content"],
        "status": ["status", "state"],
        "prio":   ["priority", "severity", "urgency"],
        "owner":  ["assignee", "owner", "reporter", "assigned_to"],
        "ts":     ["created_at", "updated_at", "date", "created"],
    }

    def find(row: dict, aliases: list) -> str:
        low = {k.lower().strip(): v for k, v in row.items()}
        for a in aliases:
            if a in low:
                return str(low[a] or "").strip()
        return ""

    for i, row in enumerate(reader):
        ticket_id = find(row, COL["id"]) or str(i + 1)
        title     = find(row, COL["title"])
        desc      = find(row, COL["desc"])
        status    = find(row, COL["status"])
        prio      = find(row, COL["prio"])
        owner     = find(row, COL["owner"])
        ts        = _parse_dt(find(row, COL["ts"]))

        parts = [p for p in [title, f"Status: {status}" if status else "",
                              f"Priority: {prio}" if prio else "", desc] if p]
        text  = "\n".join(parts)
        if not text.strip():
            continue

        inputs.append(RawInput(
            id=ticket_id, content=text,
            source_type=SourceType.TICKET, source_name=ticket_id,
            author=owner or None, timestamp=ts, metadata=dict(row),
        ))
    return inputs


# ─── Log Parser ───────────────────────────────────────────────────────────────

_LOG_PATTERNS = [
    re.compile(r'^(?P<ts>\d{4}-\d{2}-\d{2}[\sT]\d{2}:\d{2}:\d{2}[,.]?\d*)\s+'
               r'(?P<level>DEBUG|INFO|WARN|WARNING|ERROR|CRITICAL|FATAL)\s+'
               r'(?:(?P<src>[\w.]+):\s+)?(?P<msg>.+)$', re.IGNORECASE),
    re.compile(r'^\[(?P<ts>[\dT:\-. ]+)\]\s*\[(?P<level>\w+)\]\s*'
               r'(?:(?P<src>[\w.-]+):\s*)?(?P<msg>.+)$', re.IGNORECASE),
    re.compile(r'^(?P<level>ERROR|CRITICAL|WARN|WARNING|INFO|DEBUG|FATAL):\s*(?P<msg>.+)$',
               re.IGNORECASE),
]

def parse_logs(content: str) -> List[RawInput]:
    """Parse log lines — only ERROR/CRITICAL/WARN kept as potential signals."""
    inputs = []
    now    = datetime.now()

    for line in content.splitlines():
        line = line.strip()
        if not line:
            continue

        # JSON log
        if line.startswith("{"):
            try:
                obj   = json.loads(line)
                level = str(obj.get("level") or obj.get("severity") or "INFO").upper()
                msg   = str(obj.get("message") or obj.get("msg") or line)
                ts    = _parse_dt(str(obj.get("timestamp") or obj.get("time") or "")) or now
                src   = str(obj.get("service") or obj.get("source") or "")
            except Exception:
                level, msg, ts, src = "INFO", line, now, ""
        else:
            level, msg, ts, src = "INFO", line, now, ""
            for pat in _LOG_PATTERNS:
                m = pat.match(line)
                if m:
                    gd    = m.groupdict()
                    level = (gd.get("level") or "INFO").upper()
                    msg   = (gd.get("msg") or line).strip()
                    ts    = _parse_dt(gd.get("ts") or "") or now
                    src   = (gd.get("src") or "").strip()
                    break

        # Only keep error-level logs for signal extraction
        if level not in ("ERROR", "CRITICAL", "FATAL", "WARN", "WARNING"):
            continue

        inputs.append(RawInput(
            id=_uid(),
            content=f"{level}: {msg}",
            source_type=SourceType.LOG,
            source_name=src or "log",
            author=None,
            timestamp=ts,
            metadata={"level": level, "source": src},
        ))
    return inputs


# ─── Meeting Notes Parser ─────────────────────────────────────────────────────

def parse_meeting_notes(content: str) -> List[RawInput]:
    """
    Parse meeting notes / transcript into per-sentence or per-paragraph RawInputs.
    Speaker lines "Name: text" are split per speaker.
    """
    inputs  = []
    content = content.strip()

    # Check if structured with speaker labels
    has_speakers = bool(re.search(r'^[\w\s]{2,25}:\s+\w', content, re.MULTILINE))

    if has_speakers:
        # Split per speaker turn
        turns = re.split(r'(?m)^(?=[\w\s]{2,25}:\s)', content)
        for turn in turns:
            turn = turn.strip()
            if not turn:
                continue
            m      = re.match(r'^([\w\s]{2,25}):\s+(.+)$', turn, re.DOTALL)
            author = m.group(1).strip() if m else None
            text   = m.group(2).strip() if m else turn

            if len(text) < 10:
                continue

            inputs.append(RawInput(
                id=_uid(),
                content=text,
                source_type=SourceType.MEETING,
                source_name="meeting",
                author=author,
                timestamp=None,
            ))
    else:
        # Split into paragraphs
        paragraphs = re.split(r'\n\s*\n', content)
        for para in paragraphs:
            para = para.strip()
            if len(para) < 10:
                continue
            inputs.append(RawInput(
                id=_uid(),
                content=para,
                source_type=SourceType.MEETING,
                source_name="meeting",
                author=None,
                timestamp=None,
            ))
    return inputs


# ─── Universal auto-detect ────────────────────────────────────────────────────

def parse_auto(content: str, source_hint: str = "auto") -> List[RawInput]:
    """
    Auto-detect format and parse into RawInputs.
    source_hint: messages|emails|tickets|logs|meeting|auto
    """
    content = content.strip()
    if not content:
        return []

    if source_hint == "logs" or (source_hint == "auto" and _looks_like_log(content)):
        return parse_logs(content)
    if source_hint == "meeting":
        return parse_meeting_notes(content)
    if source_hint == "emails":
        if content.startswith("[") or content.startswith("{"):
            return parse_emails_json(content)
        return parse_emails_text(content)
    if source_hint == "tickets":
        if content.startswith("[") or content.startswith("{"):
            return parse_tickets_json(content)
        if "," in content.splitlines()[0]:
            return parse_tickets_csv(content)
    # Default: try JSON messages first, then plain text
    if content.startswith("[") or content.startswith("{"):
        return parse_messages_json(content)
    return parse_messages_text(content)


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _load_json(content: str) -> Any:
    content = content.strip()
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        # NDJSON
        items = []
        for line in content.splitlines():
            line = line.strip()
            if line:
                try:
                    items.append(json.loads(line))
                except Exception:
                    pass
        return items or []

def _looks_like_log(content: str) -> bool:
    first_lines = content.splitlines()[:5]
    patterns    = [r'\d{4}-\d{2}-\d{2}', r'\[ERROR\]', r'\[INFO\]',
                   r'\bERROR\b', r'\bCRITICAL\b', r'\bWARN\b']
    matches     = sum(1 for line in first_lines
                      for p in patterns if re.search(p, line, re.IGNORECASE))
    return matches >= 2

def _extract_email_addr(s: str) -> Optional[str]:
    if not s:
        return None
    m = re.search(r'<([^>]+)>', s)
    if m:
        return m.group(1).strip()
    m2 = re.search(r'[\w.+-]+@[\w.-]+\.\w+', s)
    if m2:
        return m2.group(0).strip()
    return s.strip() or None
