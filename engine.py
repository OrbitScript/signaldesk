"""
signaldesk/engine.py
─────────────────────
Core signal extraction and scoring engine.

Pipeline:
  RawInput → SignalExtractor → [Signal] → SignalScorer → SignalDeduplicator
           → SignalDesk (ranked, deduplicated, actionable)

Signal types:
  BLOCKER       — something is actively preventing work
  DECISION      — a decision is needed before work can proceed
  RISK          — something could go wrong if unaddressed
  DEADLINE      — a time-bound commitment or cutoff
  DEPENDENCY    — work waiting on another team/person/task
  ESCALATION    — situation being raised to higher authority
  INCIDENT      — something is currently broken or failing
  ACTION_ITEM   — a named person must do a specific thing
  FYI           — informational, low urgency
"""

from __future__ import annotations

import re
import math
import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from enum import Enum


# ─── Enums ────────────────────────────────────────────────────────────────────

class SignalType(Enum):
    BLOCKER     = "blocker"
    DECISION    = "decision"
    RISK        = "risk"
    DEADLINE    = "deadline"
    DEPENDENCY  = "dependency"
    ESCALATION  = "escalation"
    INCIDENT    = "incident"
    ACTION_ITEM = "action_item"
    FYI         = "fyi"


class Priority(Enum):
    CRITICAL = "critical"   # act now, drop everything
    HIGH     = "high"       # act today
    MEDIUM   = "medium"     # act this week
    LOW      = "low"        # act when time permits
    NOISE    = "noise"      # not actionable


class SourceType(Enum):
    MESSAGE  = "message"    # Slack/Teams/chat
    EMAIL    = "email"
    TICKET   = "ticket"     # Jira/Linear/GitHub
    LOG      = "log"
    MEETING  = "meeting"    # meeting notes / transcript
    DOCUMENT = "document"
    MANUAL   = "manual"


# ─── Raw Input ────────────────────────────────────────────────────────────────

@dataclass
class RawInput:
    """A single piece of raw content from any source."""
    id:          str
    content:     str
    source_type: SourceType
    source_name: str                          = ""   # e.g. "#deployments", "PROJ-123"
    author:      Optional[str]               = None
    timestamp:   Optional[datetime]          = None
    metadata:    Dict[str, Any]              = field(default_factory=dict)

    @property
    def age_hours(self) -> float:
        if self.timestamp is None:
            return 0.0
        return (datetime.now() - self.timestamp).total_seconds() / 3600


# ─── Signal ───────────────────────────────────────────────────────────────────

@dataclass
class Signal:
    """
    An extracted, scored signal from one or more raw inputs.
    This is the core unit of SignalDesk.
    """
    id:             str
    signal_type:    SignalType
    priority:       Priority
    headline:       str                       # one-line summary
    detail:         str                       # fuller explanation
    action:         str                       # what needs to happen

    # Scoring
    urgency_score:  float   = 0.0            # 0-10: how time-sensitive
    impact_score:   float   = 0.0            # 0-10: how much it matters
    confidence:     float   = 0.0            # 0-1: how confident the detector is

    # Attribution
    owners:         List[str]                = field(default_factory=list)
    sources:        List[RawInput]           = field(default_factory=list)
    tags:           List[str]                = field(default_factory=list)

    # Timing
    detected_at:    datetime                 = field(default_factory=datetime.now)
    deadline_hint:  Optional[datetime]       = None   # extracted deadline if any

    # Dedup
    fingerprint:    str                      = ""      # for deduplication

    @property
    def composite_score(self) -> float:
        """Weighted composite: urgency × impact × confidence."""
        return round(
            (self.urgency_score * 0.45 +
             self.impact_score  * 0.40 +
             self.confidence    * 10   * 0.15),
            2
        )

    @property
    def priority_order(self) -> int:
        return {Priority.CRITICAL: 0, Priority.HIGH: 1,
                Priority.MEDIUM: 2, Priority.LOW: 3, Priority.NOISE: 4}[self.priority]

    def to_dict(self) -> Dict:
        return {
            "id":             self.id,
            "type":           self.signal_type.value,
            "priority":       self.priority.value,
            "headline":       self.headline,
            "detail":         self.detail,
            "action":         self.action,
            "urgency":        self.urgency_score,
            "impact":         self.impact_score,
            "confidence":     round(self.confidence, 2),
            "composite":      self.composite_score,
            "owners":         self.owners,
            "tags":           self.tags,
            "sources":        [{"id": s.id, "type": s.source_type.value,
                                 "name": s.source_name} for s in self.sources],
            "detected_at":    self.detected_at.isoformat(),
            "deadline_hint":  self.deadline_hint.isoformat() if self.deadline_hint else None,
        }


# ─── Desk (the final product) ─────────────────────────────────────────────────

@dataclass
class SignalDesk:
    """
    The prioritized, deduplicated desk of actionable signals.
    This is what the user sees.
    """
    generated_at:   datetime
    signals:        List[Signal]
    total_inputs:   int
    noise_filtered: int
    window_hours:   float

    @property
    def critical(self) -> List[Signal]:
        return [s for s in self.signals if s.priority == Priority.CRITICAL]

    @property
    def high(self) -> List[Signal]:
        return [s for s in self.signals if s.priority == Priority.HIGH]

    @property
    def medium(self) -> List[Signal]:
        return [s for s in self.signals if s.priority == Priority.MEDIUM]

    @property
    def low(self) -> List[Signal]:
        return [s for s in self.signals if s.priority == Priority.LOW]

    @property
    def by_type(self) -> Dict[SignalType, List[Signal]]:
        result: Dict[SignalType, List[Signal]] = {}
        for s in self.signals:
            result.setdefault(s.signal_type, []).append(s)
        return result

    @property
    def owners_affected(self) -> List[str]:
        seen = set()
        owners = []
        for s in self.signals:
            for o in s.owners:
                if o not in seen:
                    seen.add(o)
                    owners.append(o)
        return owners

    def for_owner(self, owner: str) -> List[Signal]:
        return [s for s in self.signals if owner in s.owners]

    def to_dict(self) -> Dict:
        return {
            "generated_at":   self.generated_at.isoformat(),
            "window_hours":   self.window_hours,
            "stats": {
                "total_signals":  len(self.signals),
                "critical":       len(self.critical),
                "high":           len(self.high),
                "medium":         len(self.medium),
                "low":            len(self.low),
                "noise_filtered": self.noise_filtered,
                "total_inputs":   self.total_inputs,
            },
            "signals": [s.to_dict() for s in self.signals],
        }


# ─── Signal Patterns ──────────────────────────────────────────────────────────
# Each pattern: (regex, signal_type, base_urgency, base_impact, base_confidence)

BLOCKER_PATTERNS = [
    (r'\b(blocked|blocking|can\'?t proceed|cannot proceed|stuck on|waiting on|held up|'
     r'can\'?t move forward|blocked by|blocked until)\b', 8.5, 8.0, 0.85),
    (r'\b(nothing can proceed|everything is blocked|all work stopped)\b', 9.5, 9.5, 0.9),
    (r'\b(need.{0,15}before.{0,20}can|must.{0,15}before.{0,20}can)\b', 6.5, 7.0, 0.7),
]

DECISION_PATTERNS = [
    (r'\b(need.{0,10}decision|decision needed|waiting for.{0,10}decision|'
     r'need.{0,10}approval|needs approval|approve|sign.?off|sign off)\b', 7.0, 7.5, 0.8),
    (r'\b(who.{0,10}decide|who should decide|who owns this decision|'
     r'need.{0,10}call on|need a call on)\b', 6.5, 7.0, 0.75),
    (r'\b(waiting for.{0,15}green light|waiting for.{0,10}ok|'
     r'waiting for.{0,10}go ahead|pending approval)\b', 7.0, 6.5, 0.8),
]

RISK_PATTERNS = [
    (r'\b(at risk|risk of|risky|might fail|could fail|danger of|'
     r'concern about|worried about|red flag|issue with|'
     r'potential problem|may break|could break)\b', 5.5, 7.0, 0.75),
    (r'\b(this will break|going to fail|will not work|won\'?t work|'
     r'heading for.{0,15}problem|on a path to)\b', 7.5, 8.0, 0.8),
    (r'\b(security risk|vulnerability|compliance risk|legal risk|data.{0,5}risk)\b', 8.0, 9.0, 0.85),
]

DEADLINE_PATTERNS = [
    (r'\b(due.{0,5}(today|tomorrow|tonight|this (morning|afternoon|evening)))\b', 9.5, 8.0, 0.9),
    (r'\b(due.{0,5}(monday|tuesday|wednesday|thursday|friday|saturday|sunday))\b', 7.0, 7.5, 0.85),
    (r'\b(deadline.{0,15}(today|tomorrow|this week|end of day|eod|eow))\b', 8.5, 8.0, 0.9),
    (r'\b(by.{0,5}(eod|eow|end of (day|week|month))|before.{0,5}(eod|eow))\b', 7.5, 7.5, 0.85),
    (r'\b(launch.{0,10}(tomorrow|today|this week|monday))\b', 8.5, 9.0, 0.85),
    (r'\b(going live.{0,10}(tomorrow|today|this week))\b', 8.5, 9.0, 0.85),
    (r'\bdue in (\d+) (hour|day|minute)s?\b', 8.0, 8.0, 0.85),
    (r'\b(client.{0,10}(expects|waiting|deadline)|customer.{0,10}deadline)\b', 8.0, 8.5, 0.8),
]

DEPENDENCY_PATTERNS = [
    (r'\b(waiting on|waiting for|depends on|dependent on|need.{0,15}from|'
     r'blocked by|requires.{0,15}from|need input from)\b', 6.0, 6.5, 0.8),
    (r'\b(hasn\'?t.{0,15}(replied|responded|got back)|'
     r'still waiting|no response from|haven\'?t heard from)\b', 6.5, 6.0, 0.8),
    (r'\b(can\'?t.{0,10}(start|continue|finish|proceed).{0,15}until|'
     r'only.{0,10}(start|continue).{0,15}once)\b', 7.0, 7.0, 0.8),
]

ESCALATION_PATTERNS = [
    (r'\b(escalat|escalating|escalated|need to escalate|going to escalate)\b', 8.0, 8.5, 0.9),
    (r'\b(loop in.{0,15}(manager|director|vp|cto|ceo|leadership|exec)|'
     r'cc.{0,10}(manager|boss|director))\b', 7.5, 8.0, 0.85),
    (r'\b(raise.{0,10}(concern|issue|flag)|flagging.{0,10}(concern|issue|risk))\b', 6.5, 7.0, 0.75),
    (r'\b(management.{0,10}(aware|involved|notified)|execs?.{0,10}(know|aware))\b', 7.0, 7.5, 0.8),
    (r'\b(client.{0,10}(complain|angry|upset|frustrated|threaten|escalat))\b', 9.0, 9.0, 0.85),
]

INCIDENT_PATTERNS = [
    (r'\b(down|outage|incident|p0|p1|pagerduty|sev[- ]?[012]|'
     r'prod.{0,10}(down|broken|failing|issue)|production.{0,10}(down|broken|failing))\b',
     9.0, 9.5, 0.9),
    (r'\b(on fire|everything.{0,10}broke|completely broken|'
     r'nothing.{0,10}working|system.{0,10}(down|crash|fail))\b', 9.0, 9.0, 0.85),
    (r'\b(500 error|database.{0,10}(down|fail|unreachable)|'
     r'api.{0,10}(down|returning.{0,10}error|failing))\b', 8.5, 9.0, 0.85),
    (r'\b(rollback|roll back|revert|reverting|deploying hotfix|emergency fix)\b', 8.0, 8.5, 0.85),
]

ACTION_ITEM_PATTERNS = [
    (r'\b(action item|todo|to do|task for|assigned to|you need to|'
     r'please.{0,20}(do|fix|check|review|update|send|get))\b', 5.0, 5.5, 0.75),
    (r'\b(can you.{0,30}\?|could you.{0,30}\?|would you.{0,30}\?)\b', 4.5, 5.0, 0.65),
    (r'\b(@\w+.{0,20}(please|can you|could you|need you to))\b', 6.0, 6.0, 0.75),
    (r'\b(follow up.{0,15}with|follow.?up.{0,15}(needed|required)|needs.{0,10}follow.?up)\b',
     5.5, 5.5, 0.7),
]

URGENCY_AMPLIFIERS = [
    (r'\b(urgent|urgently|asap|a\.s\.a\.p|immediately|right now|'
     r'as soon as possible|drop everything|stop everything)\b', 2.5),
    (r'\b(critical|emergency|crisis|fire|911)\b', 3.0),
    (r'\b(today|tonight|this morning|this afternoon|this evening|'
     r'end of day|eod|before.{0,5}cob)\b', 1.5),
    (r'\b(overdue|past due|missed|late|behind schedule)\b', 2.0),
    (r'\b(client|customer|exec|ceo|cto|board).{0,20}(wants|needs|asking|waiting)\b', 1.5),
]

IMPACT_AMPLIFIERS = [
    (r'\b(everyone|whole team|entire team|all of us|company.?wide)\b', 2.0),
    (r'\b(production|prod|live|customer.?facing|revenue|billing|payment)\b', 2.5),
    (r'\b(data loss|security breach|compliance|legal|regulatory)\b', 3.0),
    (r'\b(launch|release|go.?live|ship|deploy)\b', 1.5),
    (r'\b(million|k users|thousand users|enterprise client)\b', 2.0),
]

ALL_PATTERN_GROUPS = [
    (BLOCKER_PATTERNS,     SignalType.BLOCKER),
    (DECISION_PATTERNS,    SignalType.DECISION),
    (RISK_PATTERNS,        SignalType.RISK),
    (DEADLINE_PATTERNS,    SignalType.DEADLINE),
    (DEPENDENCY_PATTERNS,  SignalType.DEPENDENCY),
    (ESCALATION_PATTERNS,  SignalType.ESCALATION),
    (INCIDENT_PATTERNS,    SignalType.INCIDENT),
    (ACTION_ITEM_PATTERNS, SignalType.ACTION_ITEM),
]

# Compile patterns
_COMPILED: List[Tuple[re.Pattern, SignalType, float, float, float]] = []
for group, stype in ALL_PATTERN_GROUPS:
    for pattern, base_u, base_i, base_c in group:
        _COMPILED.append((re.compile(pattern, re.IGNORECASE), stype, base_u, base_i, base_c))

_URGENCY_AMP_COMPILED = [(re.compile(p, re.IGNORECASE), boost)
                          for p, boost in URGENCY_AMPLIFIERS]
_IMPACT_AMP_COMPILED  = [(re.compile(p, re.IGNORECASE), boost)
                          for p, boost in IMPACT_AMPLIFIERS]


# ─── Name / Person Extraction ─────────────────────────────────────────────────

_MENTION_RE = re.compile(r'@([\w.+-]+)')
_EMAIL_RE   = re.compile(r'\b[\w.+-]+@[\w.-]+\.\w+\b')
_NAME_POSSESSIVE = re.compile(
    r"\b(alice|bob|carol|dave|eve|frank|grace|henry|iris|jack|"
    r"kate|liam|mia|noah|olivia|paul|quinn|rachel|sam|tara|"
    r"uma|victor|wendy|xavier|yasmine|zoe|"
    r"[A-Z][a-z]+)\b", re.IGNORECASE
)

def _extract_owners(text: str, author: Optional[str] = None) -> List[str]:
    owners = []
    seen   = set()

    def add(o: str):
        o = o.strip().lower()
        if o and o not in seen and len(o) > 1:
            seen.add(o)
            owners.append(o)

    # @mentions
    for m in _MENTION_RE.findall(text):
        add(m)

    # email addresses
    for m in _EMAIL_RE.findall(text):
        add(m)

    # author
    if author:
        add(author)

    return owners[:5]   # cap at 5


# ─── Deadline Extraction ──────────────────────────────────────────────────────

_DEADLINE_HINT_PATTERNS = [
    (r'\bdue\s+(today)\b',     0),
    (r'\bdue\s+(tomorrow)\b',  1),
    (r'\b(today)\b',           0),
    (r'\b(tomorrow)\b',        1),
    (r'\b(end of day|eod)\b',  0),
    (r'\b(end of week|eow)\b', _days_until_friday := None),   # computed below
    (r'\bdue in (\d+) days?\b', None),   # variable
    (r'\bdue in (\d+) hours?\b', None),  # variable, fraction of day
]

def _extract_deadline(text: str) -> Optional[datetime]:
    now = datetime.now()
    text_lower = text.lower()

    if re.search(r'\b(today|end of day|eod|tonight)\b', text_lower):
        return now.replace(hour=18, minute=0, second=0, microsecond=0)

    if re.search(r'\b(tomorrow)\b', text_lower):
        return (now + timedelta(days=1)).replace(hour=18, minute=0, second=0, microsecond=0)

    if re.search(r'\b(end of week|eow|this friday|by friday)\b', text_lower):
        days_ahead = (4 - now.weekday()) % 7 or 7
        return (now + timedelta(days=days_ahead)).replace(hour=18, minute=0, second=0)

    m = re.search(r'\bdue in (\d+) days?\b', text_lower)
    if m:
        return now + timedelta(days=int(m.group(1)))

    m = re.search(r'\bdue in (\d+) hours?\b', text_lower)
    if m:
        return now + timedelta(hours=int(m.group(1)))

    # Day name: "due monday", "due thursday"
    days = {"monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3,
            "friday": 4, "saturday": 5, "sunday": 6}
    m = re.search(r'\bdue\s+(monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b',
                  text_lower)
    if m:
        target_day = days[m.group(1)]
        current_day = now.weekday()
        days_ahead = (target_day - current_day) % 7 or 7
        return (now + timedelta(days=days_ahead)).replace(hour=18, minute=0, second=0)

    return None


# ─── FYI / Noise Classifier ───────────────────────────────────────────────────

_FYI_PATTERNS = re.compile(
    r'\b(fyi|heads up|for your info|just letting you know|'
    r'keeping you posted|update:|status update:|for context|'
    r'no action needed|no action required|just sharing|'
    r'quick note|just a note|just wanted to share)\b',
    re.IGNORECASE
)

_NOISE_PATTERNS = re.compile(
    r'^(ok|okay|thanks|thank you|sounds good|got it|lgtm|'
    r'noted|ack|acknowledged|will do|sure|yep|yes|nope|no|'
    r'see you|ttyl|brb|bbl|afk|gtg|going offline|back in|'
    r'lunch|coffee|break|stepping out|be right back)\s*[!.]*$',
    re.IGNORECASE
)

_GREETING_PATTERNS = re.compile(
    r'^(good morning|good afternoon|good evening|hey|hi|hello|'
    r'howdy|yo|sup|what\'s up|how are you|how\'s everyone)\b',
    re.IGNORECASE
)


# ─── Signal Extractor ─────────────────────────────────────────────────────────

class SignalExtractor:
    """
    Scans raw text for signal patterns.
    Returns a list of candidate Signals (unscored).
    """

    def __init__(self, config: Dict[str, Any] = None):
        self.config       = config or {}
        self.min_confidence = self.config.get("min_confidence", 0.6)
        self._counter     = 0

    def _next_id(self, prefix: str = "sig") -> str:
        self._counter += 1
        return f"{prefix}_{self._counter:04d}"

    def extract(self, raw: RawInput) -> List[Signal]:
        """Extract all signals from a single RawInput."""
        text = raw.content.strip()
        if not text:
            return []

        # Quick noise filter
        if len(text) < 5:
            return []
        if _NOISE_PATTERNS.match(text):
            return []
        if _GREETING_PATTERNS.match(text) and len(text) < 40:
            return []

        signals = []
        matched_types = set()

        # Run all pattern groups
        for pattern, stype, base_u, base_i, base_c in _COMPILED:
            if not pattern.search(text):
                continue
            if stype in matched_types:
                # Already found this type — don't double-emit, but boost existing
                continue
            matched_types.add(stype)

            # Compute amplified scores
            urgency = min(10.0, base_u + self._urgency_boost(text))
            impact  = min(10.0, base_i + self._impact_boost(text))
            conf    = min(1.0, base_c)

            if conf < self.min_confidence:
                continue

            # Check if FYI
            if stype not in (SignalType.INCIDENT, SignalType.BLOCKER,
                              SignalType.ESCALATION) and _FYI_PATTERNS.search(text):
                stype   = SignalType.FYI
                urgency = min(urgency, 3.0)
                impact  = min(impact, 3.0)
                conf    = min(conf, 0.7)

            owners   = _extract_owners(text, raw.author)
            deadline = _extract_deadline(text)
            headline = self._extract_headline(text, stype)
            detail   = self._build_detail(text, stype)
            action   = self._suggest_action(stype, owners, deadline)
            tags     = self._extract_tags(text, stype, raw)

            # Age decay: older messages are less urgent
            age_decay = min(2.0, raw.age_hours * 0.1)
            urgency   = max(0.0, urgency - age_decay)

            sig = Signal(
                id=self._next_id(),
                signal_type=stype,
                priority=Priority.NOISE,     # will be set by scorer
                headline=headline,
                detail=detail,
                action=action,
                urgency_score=round(urgency, 2),
                impact_score=round(impact, 2),
                confidence=round(conf, 2),
                owners=owners,
                sources=[raw],
                tags=tags,
                deadline_hint=deadline,
                fingerprint=self._fingerprint(text, stype),
            )
            signals.append(sig)

        # If no pattern matched but text is long, check for FYI
        if not signals and len(text) > 30 and _FYI_PATTERNS.search(text):
            signals.append(Signal(
                id=self._next_id(),
                signal_type=SignalType.FYI,
                priority=Priority.LOW,
                headline=self._extract_headline(text, SignalType.FYI),
                detail=text[:200],
                action="Read when time permits.",
                urgency_score=1.5,
                impact_score=2.0,
                confidence=0.65,
                owners=_extract_owners(text, raw.author),
                sources=[raw],
                tags=["fyi"],
                fingerprint=self._fingerprint(text, SignalType.FYI),
            ))

        return signals

    def _urgency_boost(self, text: str) -> float:
        total = 0.0
        for pattern, boost in _URGENCY_AMP_COMPILED:
            if pattern.search(text):
                total += boost
        return min(total, 3.0)

    def _impact_boost(self, text: str) -> float:
        total = 0.0
        for pattern, boost in _IMPACT_AMP_COMPILED:
            if pattern.search(text):
                total += boost
        return min(total, 3.5)

    def _extract_headline(self, text: str, stype: SignalType) -> str:
        """Generate a concise headline from the text."""
        # Use first sentence or first 80 chars
        m = re.match(r'^([^.!?\n]{10,80})[.!?\n]?', text.strip())
        if m:
            headline = m.group(1).strip()
        else:
            headline = text[:80].strip()

        # Prepend type indicator
        prefix = {
            SignalType.BLOCKER:     "🔴 Blocked:",
            SignalType.DECISION:    "⚖️  Decision needed:",
            SignalType.RISK:        "⚠️  Risk:",
            SignalType.DEADLINE:    "⏰ Deadline:",
            SignalType.DEPENDENCY:  "🔗 Dependency:",
            SignalType.ESCALATION:  "🚨 Escalation:",
            SignalType.INCIDENT:    "🔥 Incident:",
            SignalType.ACTION_ITEM: "✅ Action:",
            SignalType.FYI:         "ℹ️  FYI:",
        }.get(stype, "")

        # Don't double-prefix
        if any(headline.startswith(e) for e in ["🔴", "⚖️", "⚠️", "⏰", "🔗", "🚨", "🔥", "✅", "ℹ️"]):
            return headline

        return f"{prefix} {headline}" if prefix else headline

    def _build_detail(self, text: str, stype: SignalType) -> str:
        """Return the most relevant portion of the text."""
        if len(text) <= 200:
            return text
        # Try to find the most relevant sentence
        sentences = re.split(r'[.!?\n]+', text)
        relevant = []
        for stype_group, st in ALL_PATTERN_GROUPS:
            if st != stype:
                continue
            for pattern, *_ in stype_group:
                for sent in sentences:
                    if re.search(pattern, sent, re.IGNORECASE) and sent not in relevant:
                        relevant.append(sent.strip())
        if relevant:
            return " ".join(relevant[:2])[:300]
        return text[:300]

    def _suggest_action(self, stype: SignalType, owners: List[str],
                         deadline: Optional[datetime]) -> str:
        owner_str = owners[0] if owners else "the responsible party"
        due_str   = ""
        if deadline:
            delta = deadline - datetime.now()
            if delta.total_seconds() < 3600:
                due_str = " immediately"
            elif delta.total_seconds() < 86400:
                due_str = " today"
            else:
                due_str = f" by {deadline.strftime('%a %b %-d')}"

        return {
            SignalType.BLOCKER:    f"Unblock {owner_str}{due_str}. Identify the specific obstacle and remove it or escalate.",
            SignalType.DECISION:   f"Get a decision from {owner_str}{due_str}. Schedule a sync if async isn't moving.",
            SignalType.RISK:       f"Assess and mitigate with {owner_str}. Document the risk and mitigation plan.",
            SignalType.DEADLINE:   f"Confirm delivery status with {owner_str}{due_str}. Escalate if at risk.",
            SignalType.DEPENDENCY: f"Follow up with {owner_str}{due_str}. Agree on a handoff date or find an alternative.",
            SignalType.ESCALATION: f"Acknowledge the escalation. Respond to {owner_str} with a clear plan{due_str}.",
            SignalType.INCIDENT:   f"Declare incident if not done. Assign IC. Update status page. Notify {owner_str}.",
            SignalType.ACTION_ITEM: f"Complete action item{due_str}. {owner_str.capitalize()} to confirm when done.",
            SignalType.FYI:        f"Read and acknowledge. No immediate action required.",
        }.get(stype, f"Review with {owner_str} and determine next steps.")

    def _extract_tags(self, text: str, stype: SignalType, raw: RawInput) -> List[str]:
        tags = [stype.value, raw.source_type.value]
        if raw.source_name:
            tags.append(raw.source_name.lstrip("#").lower()[:20])
        # Tech tags
        for tech in ["api", "database", "db", "auth", "deploy", "prod", "staging",
                     "payment", "billing", "security", "ci", "cd", "infrastructure"]:
            if re.search(rf'\b{tech}\b', text, re.IGNORECASE):
                tags.append(tech)
        return list(dict.fromkeys(tags))[:8]   # dedupe, cap at 8

    def _fingerprint(self, text: str, stype: SignalType) -> str:
        """Generate a fingerprint for deduplication."""
        # Normalize: lowercase, remove numbers/punctuation, keep only words
        normalized = re.sub(r'[^a-z\s]', '', text.lower())
        words = [w for w in normalized.split() if len(w) > 3][:15]
        key   = stype.value + " " + " ".join(sorted(words))
        return hashlib.md5(key.encode()).hexdigest()[:12]


# ─── Signal Scorer ────────────────────────────────────────────────────────────

class SignalScorer:
    """Assigns Priority to each signal based on composite score + type bonuses."""

    TYPE_PRIORITY_FLOOR = {
        SignalType.INCIDENT:    Priority.HIGH,
        SignalType.ESCALATION:  Priority.HIGH,
        SignalType.BLOCKER:     Priority.MEDIUM,
        SignalType.DEADLINE:    Priority.MEDIUM,
        SignalType.DECISION:    Priority.MEDIUM,
        SignalType.RISK:        Priority.MEDIUM,
        SignalType.DEPENDENCY:  Priority.LOW,
        SignalType.ACTION_ITEM: Priority.LOW,
        SignalType.FYI:         Priority.LOW,
    }

    def score(self, signals: List[Signal]) -> List[Signal]:
        for sig in signals:
            sig.priority = self._assign_priority(sig)
        return signals

    def _assign_priority(self, sig: Signal) -> Priority:
        cs = sig.composite_score

        # Critical: score ≥ 8.5, or incident/escalation with score ≥ 7.5
        if cs >= 8.5:
            return Priority.CRITICAL
        if cs >= 7.5 and sig.signal_type in (SignalType.INCIDENT, SignalType.ESCALATION,
                                               SignalType.BLOCKER):
            return Priority.CRITICAL

        # High: score ≥ 6.5
        if cs >= 6.5:
            return Priority.HIGH
        if cs >= 5.5 and sig.signal_type in (SignalType.INCIDENT, SignalType.ESCALATION):
            return Priority.HIGH

        # Medium: score ≥ 4.5, or type floor
        if cs >= 4.5:
            floor = self.TYPE_PRIORITY_FLOOR.get(sig.signal_type, Priority.LOW)
            if floor == Priority.MEDIUM:
                return Priority.MEDIUM
            return Priority.MEDIUM

        if cs >= 3.0:
            return Priority.LOW

        return Priority.NOISE


# ─── Signal Deduplicator ──────────────────────────────────────────────────────

class SignalDeduplicator:
    """
    Merges signals that represent the same underlying situation.
    Uses fingerprint similarity + type matching.
    """

    def __init__(self, similarity_threshold: float = 0.6):
        self.threshold = similarity_threshold

    def deduplicate(self, signals: List[Signal]) -> List[Signal]:
        if not signals:
            return []

        groups: List[List[Signal]] = []
        used    = set()

        for i, sig in enumerate(signals):
            if i in used:
                continue
            group = [sig]
            used.add(i)

            for j, other in enumerate(signals):
                if j in used or j <= i:
                    continue
                if self._should_merge(sig, other):
                    group.append(other)
                    used.add(j)

            groups.append(group)

        return [self._merge_group(g) for g in groups]

    def _should_merge(self, a: Signal, b: Signal) -> bool:
        # Different types never merge (unless one is a subtype)
        if a.signal_type != b.signal_type:
            return False

        # Same fingerprint = definitely same
        if a.fingerprint == b.fingerprint:
            return True

        # Jaccard similarity on tags
        ta = set(a.tags)
        tb = set(b.tags)
        if ta and tb:
            jaccard = len(ta & tb) / len(ta | tb)
            if jaccard >= self.threshold:
                return True

        # Same owners + same type + close in time
        if set(a.owners) & set(b.owners):
            at = a.sources[0].timestamp if a.sources else None
            bt = b.sources[0].timestamp if b.sources else None
            if at and bt:
                delta = abs((at - bt).total_seconds())
                if delta < 3600:  # within 1 hour
                    return True

        return False

    def _merge_group(self, group: List[Signal]) -> Signal:
        if len(group) == 1:
            return group[0]

        # Use the highest-scoring signal as base
        base = max(group, key=lambda s: s.composite_score)

        # Aggregate sources and owners
        all_sources = []
        all_owners  = []
        seen_src    = set()
        seen_own    = set()

        for sig in group:
            for src in sig.sources:
                if src.id not in seen_src:
                    seen_src.add(src.id)
                    all_sources.append(src)
            for o in sig.owners:
                if o not in seen_own:
                    seen_own.add(o)
                    all_owners.append(o)

        # Boost scores slightly for corroborated signals
        boost = min(1.0, 0.3 * (len(group) - 1))
        base.urgency_score = min(10.0, base.urgency_score + boost)
        base.impact_score  = min(10.0, base.impact_score  + boost)
        base.confidence    = min(1.0,  base.confidence + 0.05 * (len(group) - 1))
        base.sources       = all_sources
        base.owners        = all_owners
        base.tags          = list(dict.fromkeys(t for s in group for t in s.tags))[:10]
        if not base.headline.endswith(f"({len(group)}x)"):
            base.headline += f" ({len(group)}x corroborated)"

        return base


# ─── SignalDesk Engine ────────────────────────────────────────────────────────

class SignalDeskEngine:
    """
    Main orchestrator: raw inputs → SignalDesk.

    Usage:
        engine = SignalDeskEngine()
        desk   = engine.process(inputs)
        # desk.signals is sorted, deduplicated, prioritized
    """

    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self.extractor     = SignalExtractor(self.config.get("extractor", {}))
        self.scorer        = SignalScorer()
        self.deduplicator  = SignalDeduplicator(
            self.config.get("similarity_threshold", 0.6)
        )
        self.min_priority  = Priority[
            self.config.get("min_priority", "LOW").upper()
        ]

    def process(
        self,
        inputs: List[RawInput],
        window_hours: float = 48,
    ) -> SignalDesk:

        total_inputs = len(inputs)

        # Filter to time window
        cutoff = datetime.now() - timedelta(hours=window_hours)
        windowed = [
            r for r in inputs
            if r.timestamp is None or r.timestamp >= cutoff
        ]

        # Extract
        all_signals: List[Signal] = []
        for raw in windowed:
            extracted = self.extractor.extract(raw)
            all_signals.extend(extracted)

        # Score
        all_signals = self.scorer.score(all_signals)

        # Filter noise
        noise_count = sum(1 for s in all_signals if s.priority == Priority.NOISE)
        active = [s for s in all_signals if s.priority != Priority.NOISE]

        # Deduplicate
        deduped = self.deduplicator.deduplicate(active)

        # Re-score after merge (merged signals may have changed)
        deduped = self.scorer.score(deduped)

        # Filter by min priority
        priority_order = {Priority.CRITICAL: 0, Priority.HIGH: 1,
                          Priority.MEDIUM: 2, Priority.LOW: 3, Priority.NOISE: 4}
        min_order = priority_order[self.min_priority]
        filtered  = [s for s in deduped
                     if priority_order[s.priority] <= min_order]

        # Sort: priority first, then composite score
        filtered.sort(key=lambda s: (s.priority_order, -s.composite_score))

        return SignalDesk(
            generated_at=datetime.now(),
            signals=filtered,
            total_inputs=total_inputs,
            noise_filtered=noise_count,
            window_hours=window_hours,
        )
