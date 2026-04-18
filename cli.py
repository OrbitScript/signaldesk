#!/usr/bin/env python3
"""
signaldesk/cli.py — python -m signaldesk
"""
import sys, json, argparse
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import signaldesk
from signaldesk.engine import SignalDeskEngine, RawInput, SourceType
from signaldesk.parsers import (
    parse_messages_json, parse_messages_text,
    parse_emails_json, parse_emails_text,
    parse_tickets_json, parse_tickets_csv,
    parse_logs, parse_meeting_notes, parse_auto,
)
from signaldesk.reporter import (
    TerminalReporter, BriefingGenerator, JsonReporter, MarkdownReporter
)
from datetime import datetime, timedelta


def main():
    p = argparse.ArgumentParser(prog="signaldesk",
                                description="SignalDesk — Work Signal Engine")
    p.add_argument("--messages", help="Messages file (JSON or text)")
    p.add_argument("--emails",   help="Emails file (JSON or text)")
    p.add_argument("--tickets",  help="Tickets file (JSON or CSV)")
    p.add_argument("--logs",     help="Log file")
    p.add_argument("--meeting",  help="Meeting notes file")
    p.add_argument("--input",    help="Any file (auto-detect format)")
    p.add_argument("--format",   choices=["terminal","briefing","json","markdown"],
                   default="terminal")
    p.add_argument("--group",    choices=["priority","type","owner"],
                   default="priority")
    p.add_argument("--output",   help="Save report to file")
    p.add_argument("--window",   type=float, default=48,
                   help="Analysis window in hours (default: 48)")
    p.add_argument("--owner",    help="Filter signals for a specific owner")
    p.add_argument("--verbose",  "-v", action="store_true")
    p.add_argument("--demo",     action="store_true")
    p.add_argument("--version",  action="store_true")
    args = p.parse_args()

    if args.version:
        print(f"signaldesk {signaldesk.__version__}")
        return

    inputs: list = []

    if args.demo:
        inputs = _demo_inputs()
    else:
        loaders = [
            (args.messages, "messages"),
            (args.emails,   "emails"),
            (args.tickets,  "tickets"),
            (args.logs,     "logs"),
            (args.meeting,  "meeting"),
            (args.input,    "auto"),
        ]
        for path, hint in loaders:
            if not path:
                continue
            content = Path(path).read_text()
            parsed  = parse_auto(content, hint)
            inputs.extend(parsed)
            print(f"  Loaded {len(parsed)} inputs from {path} [{hint}]")

    if not inputs:
        print("\n  No inputs provided. Use --messages, --emails, --tickets, --logs, --meeting, or --demo\n")
        p.print_help()
        return

    engine = SignalDeskEngine()
    desk   = engine.process(inputs, window_hours=args.window)

    # Owner filter
    if args.owner:
        desk.signals = desk.for_owner(args.owner)

    if args.format == "briefing":
        gen = BriefingGenerator()
        text = gen.generate(desk)
        if args.output:
            Path(args.output).write_text(text)
            print(f"  Saved briefing to {args.output}")
        else:
            print(text)
    elif args.format == "json":
        rep = JsonReporter()
        if args.output:
            rep.save(desk, args.output)
        else:
            rep.print(desk)
    elif args.format == "markdown":
        rep = MarkdownReporter()
        if args.output:
            rep.save(desk, args.output)
        else:
            rep.print(desk)
    else:
        TerminalReporter(verbose=args.verbose, group_by=args.group).print(desk)
        if args.output:
            if args.output.endswith(".md"):
                MarkdownReporter().save(desk, args.output)
            else:
                JsonReporter().save(desk, args.output)


def _demo_inputs() -> list:
    now = datetime.now()
    raw = [
        # --- Slack-style messages ---
        (f"Hey team — production API is DOWN. Getting 500s on all endpoints. "
         f"Pagerduty alert just fired. @alice @bob need you on this NOW.",
         SourceType.MESSAGE, "#incidents", "charlie@co.com",
         now - timedelta(hours=1)),
        (f"Deploy pipeline is blocked — the migration script is failing on step 4. "
         f"Can't proceed until someone fixes the constraint violation in users table.",
         SourceType.MESSAGE, "#deployments", "alice@co.com",
         now - timedelta(hours=3)),
        (f"URGENT: Client Acme Corp is threatening to cancel their contract if we don't "
         f"deliver the dashboard feature by EOD Friday. Escalating to leadership.",
         SourceType.MESSAGE, "#account-acme", "dave@co.com",
         now - timedelta(hours=5)),
        (f"@sarah can you approve the infra cost increase? We're blocked on the AWS "
         f"quota increase until we get your sign-off. Deadline is tomorrow EOD.",
         SourceType.MESSAGE, "#infra", "bob@co.com",
         now - timedelta(hours=6)),
        (f"Heads up: noticed a potential security risk in the auth module — "
         f"JWT tokens aren't being invalidated on logout. Could be a data risk.",
         SourceType.MESSAGE, "#security", "eve@co.com",
         now - timedelta(hours=8)),
        (f"FYI: sprint retrospective notes are in Confluence. "
         f"No action needed, just sharing for visibility.",
         SourceType.MESSAGE, "#general", "frank@co.com",
         now - timedelta(hours=2)),
        (f"ok", SourceType.MESSAGE, "#general", "grace@co.com", now - timedelta(minutes=30)),
        (f"Morning everyone!", SourceType.MESSAGE, "#general", "henry@co.com",
         now - timedelta(hours=1)),

        # --- Emails ---
        (f"Subject: Re: Payment integration — decision needed on provider\n\n"
         f"Hi team, we've been going back and forth on Stripe vs Braintree for 3 weeks. "
         f"We need a decision this week or we'll miss the Q2 launch date. "
         f"Who has authority to make this call?",
         SourceType.EMAIL, "email", "pm@co.com", now - timedelta(hours=10)),
        (f"Subject: URGENT — database disk 90% full\n\n"
         f"Production DB disk is at 90% capacity and growing. "
         f"At current rate we'll hit 100% within 48 hours. "
         f"This needs immediate attention before it becomes an outage.",
         SourceType.EMAIL, "email", "sre@co.com", now - timedelta(hours=4)),
        (f"Subject: Waiting on API docs from backend team\n\n"
         f"Hi, the frontend team is completely blocked — we've been waiting 5 days "
         f"for the API documentation from the backend team. "
         f"Can't build the integration until we have the spec. @backend-team please respond.",
         SourceType.EMAIL, "email", "frontend@co.com", now - timedelta(hours=12)),

        # --- Log errors ---
        (f"ERROR: payment-service: Stripe webhook signature verification failed — "
         f"possible replay attack or misconfigured secret",
         SourceType.LOG, "payment-service", None, now - timedelta(hours=2)),
        (f"CRITICAL: auth-service: JWT secret key rotation failed — "
         f"all new tokens are being rejected",
         SourceType.LOG, "auth-service", None, now - timedelta(hours=1)),
        (f"ERROR: db-pool: Connection pool exhausted — 200/200 connections in use",
         SourceType.LOG, "db", None, now - timedelta(minutes=45)),

        # --- Meeting notes ---
        (f"Alice: We need to decide on the caching strategy before the end of this sprint. "
         f"If we don't make a call now, the performance work will be blocked.\n"
         f"Bob: I'm waiting on the benchmark results from the infra team before I can recommend.\n"
         f"Alice: The infra team hasn't responded in 3 days — that's a dependency we need to unblock.\n"
         f"PM: Action item for Bob — follow up with infra by EOD today and get those numbers.",
         SourceType.MEETING, "sprint-sync", None, now - timedelta(hours=7)),
    ]

    inputs = []
    for content, stype, src_name, author, ts in raw:
        inputs.append(RawInput(
            id=f"demo_{len(inputs)+1}",
            content=content,
            source_type=stype,
            source_name=src_name,
            author=author,
            timestamp=ts,
        ))
    return inputs


if __name__ == "__main__":
    main()
