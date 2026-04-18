# 📡 SignalDesk — Work Signal Engine

> *Converts noise → actionable insights. Prioritizes what matters.*

SignalDesk scans your messages, emails, tickets, logs, and meeting notes — extracts the signals that actually need action — and hands you a ranked desk of what to address now, today, and this week.

---

## Quick Start

```bash
git clone https://github.com/OrbitScript/signaldesk
cd signaldesk

# Run with built-in demo data
python -m signaldesk --demo

# Get a standup briefing
python -m signaldesk --demo --format briefing

# Your own data
python -m signaldesk --messages slack_export.json --logs app.log --emails emails.json
```

---

## What It Does

### Before SignalDesk

```
[#incidents]  hey, prod api might be slow?
[#incidents]  Production API is DOWN. Getting 500s everywhere. Paging alice.
[#general]    Good morning!
[#deploys]    Deploy blocked — migration failing on step 4. Can't proceed.
[email]       Subject: URGENT: Client threatening to cancel
[#general]    ok
[#security]   potential JWT invalidation risk fyi
[logs]        CRITICAL: auth-service: JWT secret rotation failed
```

### After SignalDesk

```
📡 SignalDesk — 17 signals from 15 inputs (2 noise filtered)

  💀  CRITICAL (9)
  ──────────────────────────────────────────────
  🔥 Incident: Production API is DOWN              score 9.3
     → Declare incident. Assign IC. Update status page.
     Owners: alice, bob, charlie@co.com

  🚨 Escalation: Client threatening to cancel      score 9.0
     → Acknowledge. Respond with a clear plan immediately.
     Deadline: today 18:00

  🔴 Blocked: Deploy pipeline blocked on step 4    score 8.8
     → Unblock alice. Identify obstacle, remove or escalate.
```

---

## Signal Types

| Signal | What it means | Example |
|---|---|---|
| 🔥 `incident` | Something is broken right now | "prod is DOWN" |
| 🚨 `escalation` | Being raised to higher authority | "escalating to management" |
| 🔴 `blocker` | Work actively prevented | "completely blocked until..." |
| ⚖️ `decision` | A call needed before work proceeds | "need a decision on X" |
| ⏰ `deadline` | Time-bound commitment | "due EOD Friday" |
| ⚠️ `risk` | Something could go wrong | "security risk in auth" |
| 🔗 `dependency` | Waiting on another team/person | "waiting on backend team" |
| ✅ `action_item` | Named person must do specific thing | "@alice please review by..." |
| ℹ️ `fyi` | Informational, low urgency | "FYI: retro notes available" |

---

## Input Formats

### Messages (Slack/Teams/chat)
```json
[{
  "text": "Production API is DOWN. Getting 500s.",
  "user": "charlie@co.com",
  "channel": "#incidents",
  "timestamp": "2025-04-18T14:00:00"
}]
```

### Emails
```json
[{
  "from": "pm@co.com",
  "subject": "URGENT: Decision needed on payment provider",
  "body": "We need a decision ASAP or we miss the Q2 launch.",
  "timestamp": "2025-04-18T10:00:00"
}]
```

### Tickets (Jira/Linear/GitHub)
```csv
id,title,status,priority
PROJ-123,Deploy blocked — migration failing,open,critical
```

### Logs
```
2025-04-18 14:00:00 CRITICAL auth-service: JWT secret rotation failed
2025-04-18 14:01:00 ERROR db: Connection pool exhausted (200/200)
```

### Meeting notes
```
Alice: We're blocked on the auth decision.
Bob: I need sign-off before I can proceed.
PM: Action item for Bob — get approval by EOD today.
```

---

## CLI Reference

```bash
signaldesk --demo                           # built-in demo
signaldesk --messages msgs.json             # messages
signaldesk --emails emails.json             # emails
signaldesk --tickets tickets.csv            # tickets
signaldesk --logs app.log                   # logs
signaldesk --meeting notes.txt              # meeting notes
signaldesk --input anything.json            # auto-detect

signaldesk --format terminal     # default: rich terminal output
signaldesk --format briefing     # standup-style prose briefing
signaldesk --format json         # structured JSON
signaldesk --format markdown     # markdown for wikis/issues

signaldesk --group priority      # group by priority (default)
signaldesk --group type          # group by signal type
signaldesk --group owner         # group by owner

signaldesk --window 24           # only look at last 24h (default: 48)
signaldesk --owner alice@co.com  # filter for one person
signaldesk --output report.json  # save to file
signaldesk --verbose             # show full detail per signal
```

---

## Python API

```python
from signaldesk import SignalDeskEngine, RawInput, SourceType, parse_auto
from datetime import datetime

# Parse your inputs
inputs = parse_auto(open("slack_export.json").read(), source_hint="messages")
inputs += parse_auto(open("app.log").read(),          source_hint="logs")

# Process
engine = SignalDeskEngine()
desk   = engine.process(inputs, window_hours=24)

# Inspect
print(f"{len(desk.critical)} critical signals")
for signal in desk.critical:
    print(f"  [{signal.signal_type.value}] {signal.headline}")
    print(f"  → {signal.action}")
    print(f"  Owners: {signal.owners}")
```

---

## Scoring

Each signal is scored on three dimensions:

| Dimension | Weight | How it's calculated |
|---|---|---|
| **Urgency** (0-10) | 45% | Base pattern score + amplifiers (ASAP, today, overdue) |
| **Impact** (0-10) | 40% | Base score + amplifiers (production, revenue, customer) |
| **Confidence** (0-1) | 15% | Pattern match confidence |

**Composite score** = urgency×0.45 + impact×0.40 + confidence×10×0.15

Signals scoring ≥ 8.5 → **CRITICAL**. Incidents/escalations ≥ 7.5 → **CRITICAL**.

---

## Architecture

```
Raw inputs (messages, emails, tickets, logs, notes)
          ↓
      Parsers → List[RawInput]
          ↓
  SignalExtractor
    • Pattern matching (regex, 9 signal types)
    • Urgency / impact amplification
    • Owner extraction (@mentions, emails)
    • Deadline extraction (today/tomorrow/EOD)
    • Age decay (old messages less urgent)
          ↓
  SignalScorer  → assigns Priority
          ↓
  SignalDeduplicator → merges corroborated signals
          ↓
  SignalDesk (sorted, ranked, deduplicated)
          ↓
  Reporter (terminal | briefing | JSON | markdown)
```

---

## Zero Dependencies

Pure Python 3.8+. No pip installs needed.

```bash
python -m signaldesk --demo   # works immediately
```

---

## License

MIT
