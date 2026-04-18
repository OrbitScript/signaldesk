"""
signaldesk/reporter.py
───────────────────────
Reporters and briefing generator for SignalDesk.

TerminalReporter  — rich colorized terminal output
BriefingGenerator — standup-style morning briefing
JsonReporter      — structured JSON
MarkdownReporter  — markdown for wikis/issues
"""

from __future__ import annotations

import json
import shutil
import textwrap
from datetime import datetime
from typing import List, Dict

from signaldesk.engine import (
    SignalDesk, Signal, SignalType, Priority
)


# ─── ANSI ─────────────────────────────────────────────────────────────────────

class C:
    RST="\033[0m"; BOLD="\033[1m"; DIM="\033[2m"; ITALIC="\033[3m"
    RED="\033[31m"; GRN="\033[32m"; YLW="\033[33m"; BLU="\033[34m"
    MGT="\033[35m"; CYN="\033[36m"; GRY="\033[90m"
    BRED="\033[91m"; BGRN="\033[92m"; BYLW="\033[93m"; BBLU="\033[94m"
    BMGT="\033[95m"; BCYN="\033[96m"; BWHT="\033[97m"

def col(t, *c): return "".join(getattr(C, k) for k in c) + str(t) + C.RST
def tw(): return shutil.get_terminal_size((100,24)).columns
def hr(ch="─", c="GRY"): print(col(ch * tw(), c))
def wrap(text, width, indent=0):
    return textwrap.wrap(text, width=max(width, 30), subsequent_indent=" "*indent)


TYPE_ICONS = {
    SignalType.BLOCKER:     "🔴",
    SignalType.DECISION:    "⚖️ ",
    SignalType.RISK:        "⚠️ ",
    SignalType.DEADLINE:    "⏰",
    SignalType.DEPENDENCY:  "🔗",
    SignalType.ESCALATION:  "🚨",
    SignalType.INCIDENT:    "🔥",
    SignalType.ACTION_ITEM: "✅",
    SignalType.FYI:         "ℹ️ ",
}

TYPE_COLORS = {
    SignalType.BLOCKER:     ("BRED", "BOLD"),
    SignalType.DECISION:    ("BYLW", "BOLD"),
    SignalType.RISK:        ("BYLW",),
    SignalType.DEADLINE:    ("BMGT", "BOLD"),
    SignalType.DEPENDENCY:  ("BCYN",),
    SignalType.ESCALATION:  ("BRED", "BOLD"),
    SignalType.INCIDENT:    ("BRED", "BOLD"),
    SignalType.ACTION_ITEM: ("BGRN",),
    SignalType.FYI:         ("GRY",),
}

PRIORITY_COLORS = {
    Priority.CRITICAL: ("BRED", "BOLD"),
    Priority.HIGH:     ("BYLW", "BOLD"),
    Priority.MEDIUM:   ("BCYN",),
    Priority.LOW:      ("GRY",),
    Priority.NOISE:    ("GRY", "DIM"),
}

PRIORITY_ICONS = {
    Priority.CRITICAL: "💀",
    Priority.HIGH:     "🔴",
    Priority.MEDIUM:   "🟡",
    Priority.LOW:      "🟢",
    Priority.NOISE:    "·",
}

BANNER = """
  ███████╗██╗ ██████╗ ███╗   ██╗ █████╗ ██╗     ██████╗ ███████╗███████╗██╗  ██╗
  ██╔════╝██║██╔════╝ ████╗  ██║██╔══██╗██║     ██╔══██╗██╔════╝██╔════╝██║ ██╔╝
  ███████╗██║██║  ███╗██╔██╗ ██║███████║██║     ██║  ██║█████╗  ███████╗█████╔╝
  ╚════██║██║██║   ██║██║╚██╗██║██╔══██║██║     ██║  ██║██╔══╝  ╚════██║██╔═██╗
  ███████║██║╚██████╔╝██║ ╚████║██║  ██║███████╗██████╔╝███████╗███████║██║  ██╗
  ╚══════╝╚═╝ ╚═════╝ ╚═╝  ╚═══╝╚═╝  ╚═╝╚══════╝╚═════╝ ╚══════╝╚══════╝╚═╝  ╚═╝
"""


# ─── Terminal Reporter ────────────────────────────────────────────────────────

class TerminalReporter:
    """Full colorized terminal report."""

    def __init__(self, verbose: bool = False, group_by: str = "priority"):
        self.verbose  = verbose
        self.group_by = group_by   # "priority" | "type" | "owner"

    def print(self, desk: SignalDesk):
        self._banner()
        self._header(desk)
        self._summary(desk)

        if not desk.signals:
            print()
            print(col("  ✓  No actionable signals. Your desk is clear.", "BGRN", "BOLD"))
            print()
            return

        if self.group_by == "type":
            self._by_type(desk)
        elif self.group_by == "owner":
            self._by_owner(desk)
        else:
            self._by_priority(desk)

        self._footer(desk)

    def _banner(self):
        for line in BANNER.strip("\n").split("\n"):
            print(col(line, "BBLU"))
        print(col("  Work Signal Engine — noise → insight", "DIM"))
        print()

    def _header(self, desk: SignalDesk):
        hr()
        w = desk.window_hours
        window_str = f"{w:.0f}h" if w < 48 else f"{w/24:.0f}d"
        print(col(
            f"  {desk.generated_at.strftime('%Y-%m-%d %H:%M')}  ·  "
            f"Window: {window_str}  ·  "
            f"Inputs: {desk.total_inputs}  ·  "
            f"Noise filtered: {desk.noise_filtered}  ·  "
            f"Signals: {len(desk.signals)}",
            "GRY"
        ))
        hr()

    def _summary(self, desk: SignalDesk):
        parts = []
        if desk.critical:
            parts.append(col(f"  {len(desk.critical)} CRITICAL", "BRED", "BOLD"))
        if desk.high:
            parts.append(col(f"  {len(desk.high)} HIGH", "BYLW", "BOLD"))
        if desk.medium:
            parts.append(col(f"  {len(desk.medium)} MEDIUM", "BCYN"))
        if desk.low:
            parts.append(col(f"  {len(desk.low)} LOW", "GRY"))

        print()
        if parts:
            print("  Signals: " + "   ".join(parts))
        else:
            print(col("  No actionable signals.", "BGRN"))

        # Type breakdown
        type_counts: Dict[str, int] = {}
        for s in desk.signals:
            type_counts[s.signal_type.value] = type_counts.get(s.signal_type.value, 0) + 1

        if type_counts:
            type_parts = []
            for stype in SignalType:
                n = type_counts.get(stype.value, 0)
                if n:
                    icon = TYPE_ICONS.get(stype, "")
                    type_parts.append(f"{icon} {n} {stype.value}")
            print(col("  Types:   ", "GRY") + col("   ".join(type_parts), "DIM"))
        print()

    def _by_priority(self, desk: SignalDesk):
        for prio in [Priority.CRITICAL, Priority.HIGH, Priority.MEDIUM, Priority.LOW]:
            signals = [s for s in desk.signals if s.priority == prio]
            if not signals:
                continue
            colors = PRIORITY_COLORS[prio]
            icon   = PRIORITY_ICONS[prio]
            hr("─")
            print(col(f"  {icon}  {prio.value.upper()} ({len(signals)})", *colors))
            hr("─")
            print()
            for sig in signals:
                self._print_signal(sig)

    def _by_type(self, desk: SignalDesk):
        for stype in SignalType:
            signals = [s for s in desk.signals if s.signal_type == stype]
            if not signals:
                continue
            icon    = TYPE_ICONS.get(stype, "")
            colors  = TYPE_COLORS.get(stype, ("BWHT",))
            hr("─")
            print(col(f"  {icon}  {stype.value.upper().replace('_',' ')} ({len(signals)})", *colors))
            hr("─")
            print()
            for sig in signals:
                self._print_signal(sig)

    def _by_owner(self, desk: SignalDesk):
        seen     = set()
        owners   = []
        for s in desk.signals:
            for o in s.owners:
                if o not in seen:
                    seen.add(o)
                    owners.append(o)
        unowned = [s for s in desk.signals if not s.owners]

        for owner in owners:
            sigs = desk.for_owner(owner)
            hr("─")
            print(col(f"  👤  {owner} ({len(sigs)} signals)", "BCYN", "BOLD"))
            hr("─")
            print()
            for sig in sigs:
                self._print_signal(sig)

        if unowned:
            hr("─")
            print(col(f"  👤  (unattributed) ({len(unowned)} signals)", "GRY"))
            hr("─")
            print()
            for sig in unowned:
                self._print_signal(sig)

    def _print_signal(self, sig: Signal):
        prio_icon   = PRIORITY_ICONS[sig.priority]
        type_colors = TYPE_COLORS.get(sig.signal_type, ("BWHT",))

        # Headline
        print(col(f"  {prio_icon}  {sig.headline}", *type_colors))
        print(col(f"     #{sig.id}  ·  score {sig.composite_score:.1f}  ·  "
                  f"{sig.signal_type.value}  ·  conf {sig.confidence:.0%}", "GRY"))
        print()

        if self.verbose:
            # Detail
            for line in wrap(sig.detail, tw() - 8, indent=5):
                print("     " + col(line, "BWHT"))
            print()

        # Action
        for line in wrap(f"→ {sig.action}", tw() - 8, indent=7):
            print("     " + col(line, "BGRN"))
        print()

        # Owners
        if sig.owners:
            print(col(f"     Owners: {', '.join(sig.owners[:3])}", "CYN"))

        # Deadline
        if sig.deadline_hint:
            delta  = sig.deadline_hint - datetime.now()
            hours  = delta.total_seconds() / 3600
            due_str = (
                f"< 1h" if hours < 1
                else f"{hours:.0f}h"  if hours < 24
                else f"{hours/24:.0f}d"
            )
            print(col(f"     Deadline: {sig.deadline_hint.strftime('%a %b %-d %H:%M')} "
                      f"(in {due_str})", "BMGT"))

        # Sources
        if sig.sources:
            srcs = [f"{s.source_type.value}:{s.source_name or s.id}" for s in sig.sources[:3]]
            print(col(f"     Sources: {', '.join(srcs)}", "GRY"))

        # Tags
        if sig.tags:
            print(col("     " + "  ".join(f"#{t}" for t in sig.tags[:6]), "DIM"))

        print()
        hr("·")
        print()

    def _footer(self, desk: SignalDesk):
        hr()
        total = len(desk.signals)
        crit  = len(desk.critical)
        if crit:
            msg   = f"  {total} signal(s). {crit} CRITICAL — act now."
            color = "BRED"
        elif desk.high:
            msg   = f"  {total} signal(s). {len(desk.high)} high-priority — act today."
            color = "BYLW"
        else:
            msg   = f"  {total} signal(s). No critical issues."
            color = "BGRN"
        print()
        print(col(msg, color, "BOLD"))
        print()
        hr()
        print()


# ─── Briefing Generator ───────────────────────────────────────────────────────

class BriefingGenerator:
    """
    Generates a standup-style morning briefing from SignalDesk.
    Plain prose, designed to be read aloud or pasted into Slack.
    """

    def generate(self, desk: SignalDesk) -> str:
        now   = desk.generated_at
        lines = []

        # Header
        lines.append(f"📡 SignalDesk Briefing — {now.strftime('%A, %B %-d at %-I:%M %p')}")
        lines.append("=" * 55)

        if not desk.signals:
            lines.append("")
            lines.append("✅ Your desk is clear. No actionable signals detected.")
            lines.append(f"   Analyzed {desk.total_inputs} inputs, "
                         f"filtered {desk.noise_filtered} noise items.")
            return "\n".join(lines)

        # Lead paragraph
        crit = len(desk.critical)
        hi   = len(desk.high)
        tot  = len(desk.signals)
        parts = []
        if crit:
            parts.append(f"{crit} critical issue{'s' if crit > 1 else ''}")
        if hi:
            parts.append(f"{hi} high-priority item{'s' if hi > 1 else ''}")
        if not parts:
            parts.append(f"{tot} signal{'s' if tot > 1 else ''}")
        lines.append("")
        lines.append(f"Your desk has {' and '.join(parts)} requiring attention.")
        lines.append(f"({desk.total_inputs} inputs scanned, "
                     f"{desk.noise_filtered} noise items removed.)")

        # Critical section
        if desk.critical:
            lines.append("")
            lines.append("🚨 CRITICAL — ACT NOW:")
            for sig in desk.critical[:5]:
                owners_str = f" ({', '.join(sig.owners[:2])})" if sig.owners else ""
                lines.append(f"  • {self._plain_headline(sig)}{owners_str}")
                lines.append(f"    → {sig.action[:120]}")

        # High section
        if desk.high:
            lines.append("")
            lines.append("🔴 HIGH — ACT TODAY:")
            for sig in desk.high[:5]:
                owners_str = f" ({', '.join(sig.owners[:2])})" if sig.owners else ""
                lines.append(f"  • {self._plain_headline(sig)}{owners_str}")

        # Medium section
        if desk.medium:
            lines.append("")
            lines.append("🟡 MEDIUM — THIS WEEK:")
            for sig in desk.medium[:5]:
                lines.append(f"  • {self._plain_headline(sig)}")

        # By-owner summary
        owners = desk.owners_affected
        if owners:
            lines.append("")
            lines.append("👥 BY OWNER:")
            owner_count: Dict[str, int] = {}
            for s in desk.signals:
                for o in s.owners:
                    owner_count[o] = owner_count.get(o, 0) + 1
            for owner in sorted(owner_count, key=lambda o: -owner_count[o])[:5]:
                n         = owner_count[owner]
                owner_sigs = desk.for_owner(owner)
                crit_n    = sum(1 for s in owner_sigs if s.priority == Priority.CRITICAL)
                suffix    = f" ({crit_n} critical)" if crit_n else ""
                lines.append(f"  • {owner}: {n} signal{'s' if n > 1 else ''}{suffix}")

        # Deadlines today
        deadlines_today = [
            s for s in desk.signals
            if s.deadline_hint and
               (s.deadline_hint - now).total_seconds() < 86400
        ]
        if deadlines_today:
            lines.append("")
            lines.append("⏰ DUE TODAY:")
            for sig in deadlines_today[:3]:
                due = sig.deadline_hint.strftime("%-I:%M %p") if sig.deadline_hint else ""
                lines.append(f"  • {self._plain_headline(sig)} — by {due}")

        lines.append("")
        lines.append("─" * 55)
        lines.append(f"Generated by SignalDesk · {now.isoformat()[:19]}")
        return "\n".join(lines)

    def _plain_headline(self, sig: Signal) -> str:
        # Strip emoji prefix from headline
        text = re.sub(r'^[🔴⚖️⚠️⏰🔗🚨🔥✅ℹ️]+\s*\w+:\s*', '', sig.headline)
        return text.strip() or sig.headline

    def print(self, desk: SignalDesk):
        print(self.generate(desk))


import re   # make sure re is imported here too


# ─── JSON Reporter ────────────────────────────────────────────────────────────

class JsonReporter:
    def __init__(self, indent: int = 2):
        self.indent = indent

    def render(self, desk: SignalDesk) -> str:
        return json.dumps(desk.to_dict(), indent=self.indent, default=str)

    def print(self, desk: SignalDesk):
        print(self.render(desk))

    def save(self, desk: SignalDesk, path: str):
        with open(path, "w") as f:
            f.write(self.render(desk))
        print(f"  Saved JSON report to: {path}")


# ─── Markdown Reporter ────────────────────────────────────────────────────────

class MarkdownReporter:
    def render(self, desk: SignalDesk) -> str:
        lines = []
        lines.append("# 📡 SignalDesk Report")
        lines.append(f"*{desk.generated_at.strftime('%Y-%m-%d %H:%M')} · "
                     f"{len(desk.signals)} signals from {desk.total_inputs} inputs*\n")

        lines.append("## Summary\n")
        lines.append("| Priority | Count |")
        lines.append("|---|---|")
        lines.append(f"| 💀 Critical | {len(desk.critical)} |")
        lines.append(f"| 🔴 High     | {len(desk.high)} |")
        lines.append(f"| 🟡 Medium   | {len(desk.medium)} |")
        lines.append(f"| 🟢 Low      | {len(desk.low)} |")
        lines.append(f"| **Total**   | **{len(desk.signals)}** |")
        lines.append("")

        if not desk.signals:
            lines.append("✅ No actionable signals detected.")
            return "\n".join(lines)

        for prio in [Priority.CRITICAL, Priority.HIGH, Priority.MEDIUM, Priority.LOW]:
            signals = [s for s in desk.signals if s.priority == prio]
            if not signals:
                continue
            icon = PRIORITY_ICONS[prio]
            lines.append(f"\n## {icon} {prio.value.title()} ({len(signals)})\n")
            for sig in signals:
                type_icon = TYPE_ICONS.get(sig.signal_type, "")
                lines.append(f"### {type_icon} {sig.headline}")
                lines.append(f"*Score: {sig.composite_score:.1f} · "
                              f"Type: {sig.signal_type.value} · "
                              f"Confidence: {sig.confidence:.0%}*\n")
                lines.append(sig.detail)
                lines.append("")
                if sig.owners:
                    lines.append(f"**Owners:** {', '.join(sig.owners)}")
                if sig.deadline_hint:
                    lines.append(f"**Deadline:** {sig.deadline_hint.strftime('%Y-%m-%d %H:%M')}")
                lines.append(f"**Action:** {sig.action}")
                lines.append("")

        return "\n".join(lines)

    def print(self, desk: SignalDesk):
        print(self.render(desk))

    def save(self, desk: SignalDesk, path: str):
        with open(path, "w") as f:
            f.write(self.render(desk))
        print(f"  Saved Markdown report to: {path}")
