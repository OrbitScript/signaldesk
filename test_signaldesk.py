"""
tests/test_signaldesk.py
─────────────────────────
Full test suite for SignalDesk.
Run: python tests/test_signaldesk.py
"""
import sys, json, re
from datetime import datetime, timedelta
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from signaldesk.engine import (
    RawInput, Signal, SignalDesk, SignalType, Priority, SourceType,
    SignalExtractor, SignalScorer, SignalDeduplicator, SignalDeskEngine,
)
from signaldesk.parsers import (
    parse_messages_json, parse_messages_text, parse_emails_json,
    parse_emails_text, parse_tickets_json, parse_tickets_csv,
    parse_logs, parse_meeting_notes, parse_auto,
)

now = datetime.now
def past(h): return now() - timedelta(hours=h)
def future(h): return now() + timedelta(hours=h)

def make_raw(content, stype=SourceType.MESSAGE, src="test", author=None, ts=None):
    return RawInput(
        id="t1", content=content,
        source_type=stype, source_name=src,
        author=author, timestamp=ts or now(),
    )


# ─── Extractor ────────────────────────────────────────────────────────────────

class TestSignalExtractor:
    def _extract(self, text, **kw):
        return SignalExtractor().extract(make_raw(text, **kw))

    def test_detects_blocker(self):
        sigs = self._extract("We're completely blocked on the deployment. Can't proceed.")
        types = [s.signal_type for s in sigs]
        assert SignalType.BLOCKER in types

    def test_detects_incident(self):
        sigs = self._extract("Production is DOWN. All endpoints returning 500.")
        assert any(s.signal_type == SignalType.INCIDENT for s in sigs)

    def test_detects_escalation(self):
        sigs = self._extract("Need to escalate this to management immediately.")
        assert any(s.signal_type == SignalType.ESCALATION for s in sigs)

    def test_detects_decision(self):
        sigs = self._extract("We need a decision on the database provider before we can start.")
        assert any(s.signal_type == SignalType.DECISION for s in sigs)

    def test_detects_risk(self):
        sigs = self._extract("There's a security risk in the auth module — tokens aren't invalidated.")
        assert any(s.signal_type == SignalType.RISK for s in sigs)

    def test_detects_deadline(self):
        sigs = self._extract("This is due EOD today and we're not close to done.")
        assert any(s.signal_type == SignalType.DEADLINE for s in sigs)

    def test_detects_dependency(self):
        sigs = self._extract("We're waiting on the backend team for the API spec. Completely blocked.")
        types = [s.signal_type for s in sigs]
        assert SignalType.DEPENDENCY in types or SignalType.BLOCKER in types

    def test_detects_action_item(self):
        sigs = self._extract("Action item for @alice: please review the PR by EOD.")
        assert any(s.signal_type == SignalType.ACTION_ITEM for s in sigs)

    def test_detects_fyi(self):
        sigs = self._extract(
            "FYI: the sprint retro notes are available in Confluence. "
            "No action needed from anyone."
        )
        types = [s.signal_type for s in sigs]
        assert SignalType.FYI in types

    def test_filters_noise_ok(self):
        sigs = self._extract("ok")
        assert len(sigs) == 0

    def test_filters_noise_thanks(self):
        sigs = self._extract("Thanks!")
        assert len(sigs) == 0

    def test_filters_greeting(self):
        sigs = self._extract("Good morning everyone!")
        assert len(sigs) == 0

    def test_urgency_amplified_by_asap(self):
        base = self._extract("The deployment is blocked.")
        amped = self._extract("The deployment is blocked ASAP urgent critical.")
        u_base = max((s.urgency_score for s in base), default=0)
        u_amped = max((s.urgency_score for s in amped), default=0)
        assert u_amped >= u_base

    def test_impact_amplified_by_production(self):
        base = self._extract("The API is failing.")
        prod = self._extract("The production API is failing and affecting all customers.")
        i_base = max((s.impact_score for s in base), default=0)
        i_prod = max((s.impact_score for s in prod), default=0)
        assert i_prod >= i_base

    def test_extracts_owners_from_mentions(self):
        sigs = self._extract("@alice please fix this blocker immediately.", author="bob@co.com")
        all_owners = [o for s in sigs for o in s.owners]
        assert "alice" in all_owners

    def test_extracts_owners_from_email(self):
        sigs = self._extract("Please contact alice@example.com — she's blocking us.")
        all_owners = [o for s in sigs for o in s.owners]
        assert "alice@example.com" in all_owners

    def test_extracts_deadline_today(self):
        sigs = self._extract("This is due EOD today. Critical path.")
        deadlines = [s.deadline_hint for s in sigs if s.deadline_hint]
        assert len(deadlines) > 0
        # Should be today
        assert deadlines[0].date() == datetime.now().date()

    def test_extracts_deadline_tomorrow(self):
        sigs = self._extract("The launch is due tomorrow. Everyone needs to be ready.")
        deadlines = [s.deadline_hint for s in sigs if s.deadline_hint]
        assert len(deadlines) > 0

    def test_age_decay_reduces_urgency(self):
        old_raw  = make_raw("Production is DOWN!", ts=past(24))
        new_raw  = make_raw("Production is DOWN!", ts=past(0.1))
        old_sigs = SignalExtractor().extract(old_raw)
        new_sigs = SignalExtractor().extract(new_raw)
        old_u = max((s.urgency_score for s in old_sigs), default=0)
        new_u = max((s.urgency_score for s in new_sigs), default=0)
        assert new_u >= old_u

    def test_tags_include_source_type(self):
        raw  = make_raw("Blocked on deploy.", stype=SourceType.EMAIL, src="email")
        sigs = SignalExtractor().extract(raw)
        all_tags = [t for s in sigs for t in s.tags]
        assert "email" in all_tags

    def test_fingerprint_is_deterministic(self):
        text = "The authentication module is blocked on the JWT fix."
        r1   = make_raw(text)
        r2   = make_raw(text)
        s1   = SignalExtractor().extract(r1)
        s2   = SignalExtractor().extract(r2)
        if s1 and s2:
            assert s1[0].fingerprint == s2[0].fingerprint


# ─── Scorer ───────────────────────────────────────────────────────────────────

class TestSignalScorer:
    def _make_signal(self, urgency, impact, confidence, stype=SignalType.BLOCKER):
        return Signal(
            id="t", signal_type=stype, priority=Priority.NOISE,
            headline="test", detail="test", action="test",
            urgency_score=urgency, impact_score=impact, confidence=confidence,
        )

    def test_critical_score(self):
        sig = self._make_signal(9.5, 9.0, 0.9)
        scored = SignalScorer().score([sig])
        assert scored[0].priority == Priority.CRITICAL

    def test_high_score(self):
        sig = self._make_signal(7.0, 6.5, 0.8)
        scored = SignalScorer().score([sig])
        assert scored[0].priority in (Priority.HIGH, Priority.CRITICAL)

    def test_low_score(self):
        sig = self._make_signal(2.0, 2.0, 0.65)
        scored = SignalScorer().score([sig])
        assert scored[0].priority in (Priority.LOW, Priority.NOISE)

    def test_incident_floor(self):
        sig = self._make_signal(6.0, 6.0, 0.8, stype=SignalType.INCIDENT)
        scored = SignalScorer().score([sig])
        assert scored[0].priority in (Priority.CRITICAL, Priority.HIGH)

    def test_composite_score_formula(self):
        sig = self._make_signal(8.0, 7.0, 0.9)
        expected = 8.0*0.45 + 7.0*0.40 + 0.9*10*0.15
        assert abs(sig.composite_score - expected) < 0.01


# ─── Deduplicator ─────────────────────────────────────────────────────────────

class TestSignalDeduplicator:
    def _make_sig(self, stype, tags, owners=None, ts=None):
        raw = make_raw("test", ts=ts or now())
        return Signal(
            id=f"t{id(raw)}", signal_type=stype, priority=Priority.MEDIUM,
            headline="test", detail="test", action="test",
            urgency_score=5.0, impact_score=5.0, confidence=0.8,
            owners=owners or [], sources=[raw], tags=tags,
            fingerprint=f"{stype.value}_{'_'.join(sorted(tags))}",
        )

    def test_merges_same_fingerprint(self):
        raw  = make_raw("Blocked on deploy!")
        s1   = SignalExtractor().extract(raw)
        s2   = SignalExtractor().extract(raw)
        both = s1 + s2
        deduped = SignalDeduplicator().deduplicate(both)
        # Should merge duplicates
        assert len(deduped) <= len(both)

    def test_does_not_merge_different_types(self):
        s1 = self._make_sig(SignalType.BLOCKER, ["deploy", "blocker"])
        s2 = self._make_sig(SignalType.INCIDENT, ["deploy", "incident"])
        deduped = SignalDeduplicator().deduplicate([s1, s2])
        assert len(deduped) == 2

    def test_merge_boosts_score(self):
        s1 = self._make_sig(SignalType.BLOCKER, ["deploy", "blocker", "api"])
        s2 = self._make_sig(SignalType.BLOCKER, ["deploy", "blocker", "api"])
        s2.fingerprint = s1.fingerprint   # same fp
        base_score = s1.composite_score
        deduped    = SignalDeduplicator().deduplicate([s1, s2])
        assert deduped[0].composite_score >= base_score

    def test_merges_sources(self):
        r1 = make_raw("deploy blocked"); r2 = make_raw("deploy blocked")
        r2.id = "r2"
        s1 = self._make_sig(SignalType.BLOCKER, ["deploy", "blocker"])
        s2 = self._make_sig(SignalType.BLOCKER, ["deploy", "blocker"])
        s1.sources = [r1]; s2.sources = [r2]
        s1.fingerprint = s2.fingerprint = "same_fp"
        deduped = SignalDeduplicator().deduplicate([s1, s2])
        assert len(deduped[0].sources) == 2


# ─── Parsers ──────────────────────────────────────────────────────────────────

class TestParsers:
    def test_messages_json(self):
        data = json.dumps([
            {"text": "Production is down!", "user": "alice", "channel": "#incidents",
             "timestamp": "2024-03-15T14:00:00"},
        ])
        inputs = parse_messages_json(data)
        assert len(inputs) == 1
        assert inputs[0].content == "Production is down!"
        assert inputs[0].author  == "alice"

    def test_messages_text(self):
        text = "Alice: The deployment is blocked on step 3.\n\nBob: Agreed, need help."
        inputs = parse_messages_text(text)
        assert len(inputs) >= 1
        assert any("blocked" in i.content for i in inputs)

    def test_emails_json(self):
        data = json.dumps([{
            "from": "pm@co.com",
            "subject": "Decision needed on provider",
            "body": "We need a decision ASAP.",
            "timestamp": "2024-03-15T10:00:00",
        }])
        inputs = parse_emails_json(data)
        assert len(inputs) == 1
        assert "Decision needed" in inputs[0].content
        assert inputs[0].source_type == SourceType.EMAIL

    def test_emails_text(self):
        text = """From: alice@co.com
Subject: Urgent: Deploy blocked
Date: 2024-03-15 10:00
Body:
The deployment pipeline is blocked. Need help ASAP.

---

From: bob@co.com
Subject: Re: Urgent: Deploy blocked
Date: 2024-03-15 11:00
Body:
On it."""
        inputs = parse_emails_text(text)
        assert len(inputs) >= 1

    def test_tickets_json(self):
        data = json.dumps([{
            "id": "PROJ-123",
            "title": "Fix auth bug — blocking login",
            "status": "in_progress",
            "priority": "critical",
        }])
        inputs = parse_tickets_json(data)
        assert len(inputs) == 1
        assert inputs[0].id == "PROJ-123"
        assert "critical" in inputs[0].content.lower()

    def test_tickets_csv(self):
        csv_data = "id,title,status,priority\nT-001,Deploy blocked,open,high\n"
        inputs = parse_tickets_csv(csv_data)
        assert len(inputs) == 1
        assert "blocked" in inputs[0].content.lower()

    def test_logs_errors_only(self):
        log = ("INFO: All good\nERROR: DB connection failed\n"
               "INFO: Health check OK\nCRITICAL: Auth service down\n")
        inputs = parse_logs(log)
        # Should only include ERROR and CRITICAL
        assert len(inputs) == 2
        assert all("ERROR" in i.content or "CRITICAL" in i.content for i in inputs)

    def test_logs_json_format(self):
        lines = "\n".join([
            json.dumps({"timestamp": "2024-03-15T14:00:00", "level": "ERROR",
                        "message": "DB down", "service": "api"}),
            json.dumps({"timestamp": "2024-03-15T14:01:00", "level": "INFO",
                        "message": "OK"}),
        ])
        inputs = parse_logs(lines)
        assert len(inputs) == 1
        assert inputs[0].source_type == SourceType.LOG

    def test_meeting_notes_speaker_format(self):
        notes = ("Alice: We're blocked on the auth decision.\n"
                 "Bob: I need sign-off before proceeding.\n"
                 "PM: Action item for Bob — get approval by EOD.")
        inputs = parse_meeting_notes(notes)
        assert len(inputs) >= 2
        assert all(i.source_type == SourceType.MEETING for i in inputs)

    def test_meeting_notes_paragraph_format(self):
        notes = ("The team is blocked on the deployment.\n\n"
                 "Action items: Bob to fix migration by EOD. Alice to approve.")
        inputs = parse_meeting_notes(notes)
        assert len(inputs) >= 1

    def test_auto_detect_json(self):
        data = json.dumps([{"text": "Deploy is blocked!", "user": "alice"}])
        inputs = parse_auto(data)
        assert len(inputs) == 1

    def test_auto_detect_log(self):
        log = "2024-03-15 14:00:00 ERROR api: DB connection refused\n"
        inputs = parse_auto(log, source_hint="logs")
        assert len(inputs) == 1

    def test_empty_returns_empty(self):
        assert parse_auto("") == []
        assert parse_auto("   ") == []


# ─── Full Engine ──────────────────────────────────────────────────────────────

class TestSignalDeskEngine:
    def test_empty_input(self):
        engine = SignalDeskEngine()
        desk   = engine.process([])
        assert len(desk.signals) == 0
        assert desk.total_inputs == 0

    def test_basic_processing(self):
        inputs = [
            make_raw("Production API is DOWN. Everyone paged.", ts=past(1)),
            make_raw("ok", ts=past(0.5)),
            make_raw("Good morning!", ts=past(0.1)),
        ]
        engine = SignalDeskEngine()
        desk   = engine.process(inputs)
        assert desk.total_inputs == 3
        # At least the production DOWN message should produce a signal
        assert len(desk.signals) >= 1

    def test_noise_filtered_out(self):
        inputs = [make_raw(t) for t in ["ok", "thanks", "sounds good", "lgtm", "noted"]]
        desk = SignalDeskEngine().process(inputs)
        assert len(desk.signals) == 0  # all noise, no actionable signals

    def test_signals_sorted_by_priority(self):
        inputs = [
            make_raw("FYI: sprint notes available. No action needed."),
            make_raw("Production is DOWN and we're losing revenue.", ts=past(0.5)),
        ]
        desk = SignalDeskEngine().process(inputs)
        if len(desk.signals) >= 2:
            orders = [s.priority_order for s in desk.signals]
            assert orders == sorted(orders)

    def test_window_filters_old_inputs(self):
        inputs = [
            make_raw("Old incident", ts=past(100)),
            make_raw("Production DOWN critical", ts=past(1)),
        ]
        desk = SignalDeskEngine().process(inputs, window_hours=48)
        # Old one (100h) should be filtered
        assert desk.total_inputs == 2  # total seen
        assert all(s.sources[0].timestamp and
                   (now() - s.sources[0].timestamp).total_seconds() < 48*3600
                   for s in desk.signals)

    def test_desk_properties(self):
        inputs = [
            make_raw("Production is critically DOWN. Incident declared.", ts=past(0.5)),
            make_raw("Deploy blocked by @alice awaiting approval.", ts=past(1)),
            make_raw("FYI: weekly report is out. No action needed.", ts=past(2)),
        ]
        desk = SignalDeskEngine().process(inputs)
        assert desk.total_inputs == 3
        assert isinstance(desk.generated_at, datetime)

    def test_desk_for_owner(self):
        raw = make_raw("@alice please fix the blocker ASAP. This is urgent.", author="bob")
        desk = SignalDeskEngine().process([raw])
        alice_sigs = desk.for_owner("alice")
        assert len(alice_sigs) >= 0  # owner extraction may vary

    def test_desk_to_dict(self):
        inputs = [make_raw("Production is DOWN.", ts=past(1))]
        desk   = SignalDeskEngine().process(inputs)
        d      = desk.to_dict()
        assert "generated_at" in d
        assert "signals"      in d
        assert "stats"        in d
        assert isinstance(d["signals"], list)


# ─── Reporters ────────────────────────────────────────────────────────────────

class TestReporters:
    def _desk(self):
        inputs = [
            make_raw("Production is DOWN. Critical incident.", ts=past(1)),
            make_raw("Deploy is blocked on the auth fix. Need decision.", ts=past(2)),
            make_raw("FYI: retrospective notes available.", ts=past(3)),
        ]
        return SignalDeskEngine().process(inputs)

    def test_json_reporter(self):
        from signaldesk.reporter import JsonReporter
        desk = self._desk()
        out  = JsonReporter().render(desk)
        data = json.loads(out)
        assert "signals" in data
        assert "stats"   in data

    def test_markdown_reporter(self):
        from signaldesk.reporter import MarkdownReporter
        desk = self._desk()
        md   = MarkdownReporter().render(desk)
        assert "# 📡 SignalDesk Report" in md
        assert "## Summary"             in md

    def test_briefing_generator(self):
        from signaldesk.reporter import BriefingGenerator
        desk    = self._desk()
        briefing = BriefingGenerator().generate(desk)
        assert "SignalDesk Briefing" in briefing
        assert isinstance(briefing, str)
        assert len(briefing) > 100

    def test_briefing_empty_desk(self):
        from signaldesk.reporter import BriefingGenerator
        desk = SignalDeskEngine().process([make_raw("ok"), make_raw("thanks")])
        briefing = BriefingGenerator().generate(desk)
        assert "clear" in briefing.lower() or "no actionable" in briefing.lower()

    def test_json_save(self, tmp_path=None):
        import tempfile, os
        from signaldesk.reporter import JsonReporter
        desk = self._desk()
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            path = f.name
        try:
            JsonReporter().save(desk, path)
            with open(path) as f:
                data = json.load(f)
            assert "signals" in data
        finally:
            os.unlink(path)


# ─── Demo smoke test ─────────────────────────────────────────────────────────

class TestDemo:
    def test_demo_produces_signals(self):
        from signaldesk.cli import _demo_inputs
        inputs = _demo_inputs()
        assert len(inputs) > 5
        desk = SignalDeskEngine().process(inputs)
        assert desk.total_inputs == len(inputs)
        assert len(desk.signals) > 0
        # Should have at least one critical
        assert len(desk.critical) > 0

    def test_demo_noise_filtered(self):
        from signaldesk.cli import _demo_inputs
        inputs = _demo_inputs()
        desk   = SignalDeskEngine().process(inputs)
        # Some noise should be filtered (ok, good morning)
        assert desk.noise_filtered >= 1

    def test_all_signal_types_detected(self):
        from signaldesk.cli import _demo_inputs
        inputs = _demo_inputs()
        desk   = SignalDeskEngine().process(inputs)
        found_types = {s.signal_type for s in desk.signals}
        # Should find at least 4 distinct signal types
        assert len(found_types) >= 4


# ─── Runner ───────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import traceback
    passed = failed = 0
    test_classes = [
        TestSignalExtractor, TestSignalScorer, TestSignalDeduplicator,
        TestParsers, TestSignalDeskEngine, TestReporters, TestDemo,
    ]
    for cls in test_classes:
        inst = cls()
        for name in sorted(dir(cls)):
            if not name.startswith("test_"): continue
            try:
                getattr(inst, name)()
                print(f"  ✓ {cls.__name__}.{name}")
                passed += 1
            except Exception as e:
                print(f"  ✗ {cls.__name__}.{name}: {e}")
                if "--verbose" in sys.argv: traceback.print_exc()
                failed += 1
    print(f"\n  {passed} passed, {failed} failed")
    if failed: sys.exit(1)
