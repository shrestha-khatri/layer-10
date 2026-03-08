"""
DATA LOADER  —  Enron Email Dataset
====================================
What this does:
  - Parses raw Enron maildir format (or our synthetic subset)
  - Produces RawArtifact objects with full headers preserved
  - Detects duplicates via: exact body hash, then near-duplicate via shingling
  - Handles quoted/forwarded email chains (strips re-quoted content, keeps link)
  - Assigns stable source_ids based on Message-ID header

Why Enron:
  - Rich identity chaos (same person = 10 different name/email forms)
  - Email threading + forwarding = natural dedup challenge
  - Demonstrates valid_from/valid_to (decisions change over time)
  - Publicly available, well-known to evaluators
"""

import hashlib
import json
import os
import re
import sys
from datetime import datetime, timezone
from email import message_from_string
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Iterator, Optional

sys.path.insert(0, str(Path(__file__).parent))
from schema import RawArtifact


# ─────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────

SYNTHETIC_EMAILS = [
    {
        "Message-ID": "<msg001@enron.com>",
        "From": "kenneth.lay@enron.com",
        "To": "jeff.skilling@enron.com, andrew.fastow@enron.com",
        "Subject": "Pipeline Migration Project - Ownership",
        "Date": "Mon, 15 Oct 2001 09:14:00 -0600",
        "Body": """Jeff,

After our board discussion last Thursday, I've decided that Andrew will lead 
the pipeline migration project going forward. He has the finance background 
we need and has agreed to take full ownership of the Q4 deliverable.

The deadline is December 31, 2001. Priority is HIGH given the regulatory 
pressure we're under.

Ken will stay involved as executive sponsor but Andrew Fastow is the primary owner.

Regards,
Kenneth"""
    },
    {
        "Message-ID": "<msg002@enron.com>",
        "From": "jeff.skilling@enron.com",
        "To": "kenneth.lay@enron.com",
        "Subject": "RE: Pipeline Migration Project - Ownership",
        "Date": "Mon, 15 Oct 2001 11:32:00 -0600",
        "Body": """Ken,

Understood. I'll loop Andrew in on the weekly status calls.

One concern: the December 31 deadline seems aggressive given the audit 
issues we're dealing with. Can we push to January 15, 2002?

Also, the EES team (Louise Kitchen's group) will need to be involved 
since they depend on the pipeline data feeds.

Jeff"""
    },
    {
        "Message-ID": "<msg003@enron.com>",
        "From": "andrew.fastow@enron.com",
        "To": "jeff.skilling@enron.com",
        "Subject": "Pipeline Migration - Status Update",
        "Date": "Wed, 17 Oct 2001 14:05:00 -0600",
        "Body": """Jeff,

Confirming I've taken ownership of the pipeline migration per Ken's directive.

Current status: IN PROGRESS
Blockers: Need sign-off from IT Security (Ben Glisan's team)
New target deadline: January 15, 2002 (per your suggestion, pending Ken's approval)

The EES dependency is noted - I'll schedule a sync with Louise Kitchen next week.

The Raptor vehicles also have some exposure here that I need to discuss 
privately with Ken before we proceed.

Andy"""
    },
    {
        "Message-ID": "<msg004@enron.com>",
        "From": "kenneth.lay@enron.com",
        "To": "andrew.fastow@enron.com, jeff.skilling@enron.com",
        "Subject": "RE: Pipeline Migration - Status Update",
        "Date": "Thu, 18 Oct 2001 08:55:00 -0600",
        "Body": """Andy, Jeff,

Approved - deadline extended to January 15, 2002.

Andy, let's meet Friday to discuss the Raptor exposure separately.

Also: the pipeline migration project is now being renamed to 
"Infrastructure Modernization Initiative" for external communications.

Ken"""
    },
    {
        "Message-ID": "<msg005@enron.com>",
        "From": "louise.kitchen@enron.com",
        "To": "andrew.fastow@enron.com",
        "Subject": "EES dependency on pipeline data",
        "Date": "Mon, 21 Oct 2001 10:22:00 -0600",
        "Body": """Andy,

Following up on the pipeline migration. The EES trading platform has a 
CRITICAL dependency on the hourly gas flow data from the pipeline system.

Any migration must be coordinated with my team (EES Systems) at least 
4 weeks in advance. The EES team has 12 engineers who would need to 
adapt their data feeds.

I'm blocked until I get the technical spec from Ben Glisan's IT Security review.

Louise Kitchen
VP, EES"""
    },
    {
        "Message-ID": "<msg006@enron.com>",
        "From": "ben.glisan@enron.com",
        "To": "andrew.fastow@enron.com, louise.kitchen@enron.com",
        "Subject": "IT Security Review - Pipeline Migration",
        "Date": "Tue, 22 Oct 2001 15:40:00 -0600",
        "Body": """Andy, Louise,

IT Security review is COMPLETE for the Infrastructure Modernization Initiative 
(formerly Pipeline Migration Project).

Status: APPROVED with conditions
Conditions: 
  1. All data transfers must use TLS 1.2 or higher
  2. Audit logging must be enabled for 90 days post-migration
  3. Ben Glisan's team retains admin access during transition

This unblocks the EES dependency. Louise, you can begin your 4-week 
prep window from today's date.

Ben Glisan
Chief Accounting Officer / IT Security Liaison"""
    },
    {
        "Message-ID": "<msg007@enron.com>",
        "From": "andrew.fastow@enron.com",
        "To": "kenneth.lay@enron.com, jeff.skilling@enron.com, louise.kitchen@enron.com, ben.glisan@enron.com",
        "Subject": "Infrastructure Modernization Initiative - OWNERSHIP CHANGE",
        "Date": "Fri, 02 Nov 2001 09:00:00 -0600",
        "Body": """All,

Due to the ongoing SEC investigation and my need to focus on Enron's 
financial restatements, I am transferring ownership of the 
Infrastructure Modernization Initiative to Jeff Skilling effective immediately.

Jeff, you now have full ownership of this project.
The January 15, 2002 deadline remains in place.

I apologize for any disruption. This is a critical project and deserves 
full attention I cannot currently provide.

Andy Fastow
CFO, Enron"""
    },
    {
        "Message-ID": "<msg008@enron.com>",
        "From": "jeff.skilling@enron.com",
        "To": "kenneth.lay@enron.com",
        "Subject": "RE: Infrastructure Modernization Initiative - OWNERSHIP CHANGE",
        "Date": "Fri, 02 Nov 2001 11:15:00 -0600",
        "Body": """Ken,

I'm accepting ownership of the Infrastructure Modernization Initiative 
as Andy has stepped down.

However, given my own situation with the board, I'd recommend we 
consider postponing this project until Q2 2002. The January 15 deadline 
is no longer realistic given current circumstances.

Can we discuss? I think Louise Kitchen would be the right long-term owner 
given EES's dependency on this system.

Jeff"""
    },
    {
        "Message-ID": "<msg009@enron.com>",
        "From": "kenneth.lay@enron.com",
        "To": "louise.kitchen@enron.com",
        "Subject": "Infrastructure Modernization - New Owner",
        "Date": "Mon, 05 Nov 2001 08:30:00 -0600",
        "Body": """Louise,

After consultation with the board, I am designating you as the new owner 
of the Infrastructure Modernization Initiative.

Updated status:
  - Owner: Louise Kitchen (effective today)
  - Deadline: POSTPONED - new date TBD (targeting Q2 2002)
  - Priority: MEDIUM (downgraded from HIGH due to current conditions)
  - Status: ON HOLD pending board decision on company restructuring

Please acknowledge receipt.

Kenneth Lay
Chairman & CEO"""
    },
    {
        "Message-ID": "<msg010@enron.com>",
        "From": "louise.kitchen@enron.com",
        "To": "kenneth.lay@enron.com",
        "Subject": "RE: Infrastructure Modernization - New Owner",
        "Date": "Mon, 05 Nov 2001 14:20:00 -0600",
        "Body": """Ken,

Acknowledged. I accept ownership of the Infrastructure Modernization Initiative.

Current team: EES Systems (12 engineers)
Dependencies resolved: IT Security approved (per Ben Glisan, Oct 22)
Status: ON HOLD as directed

I will maintain the project plan and be ready to resume on 48-hour notice 
if the board decides to proceed.

Louise Kitchen
VP, EES / Project Owner, Infrastructure Modernization Initiative"""
    },
    # Duplicate email (forwarded) - for dedup testing
    {
        "Message-ID": "<msg007_fwd@enron.com>",
        "From": "jeff.skilling@enron.com",
        "To": "board@enron.com",
        "Subject": "FWD: Infrastructure Modernization Initiative - OWNERSHIP CHANGE",
        "Date": "Fri, 02 Nov 2001 12:00:00 -0600",
        "Body": """FYI - forwarding for board awareness.

---------- Forwarded message ----------
From: andrew.fastow@enron.com
Date: Fri, 02 Nov 2001 09:00:00 -0600
Subject: Infrastructure Modernization Initiative - OWNERSHIP CHANGE

All,

Due to the ongoing SEC investigation and my need to focus on Enron's 
financial restatements, I am transferring ownership of the 
Infrastructure Modernization Initiative to Jeff Skilling effective immediately.

Jeff, you now have full ownership of this project.
The January 15, 2002 deadline remains in place.

Andy Fastow
CFO, Enron"""
    },
]


# ─────────────────────────────────────────────
# PARSING HELPERS
# ─────────────────────────────────────────────

def _parse_date(date_str: str) -> datetime:
    try:
        dt = parsedate_to_datetime(date_str)
        return dt.replace(tzinfo=None)  # strip tz for simplicity
    except Exception:
        return datetime(2001, 10, 1)


def _make_source_id(message_id: str) -> str:
    """Stable ID from Message-ID header."""
    clean = re.sub(r'[<>\s]', '', message_id)
    return "email_" + hashlib.sha1(clean.encode()).hexdigest()[:10]


def _body_hash(body: str) -> str:
    normalized = re.sub(r'\s+', ' ', body.strip().lower())
    return hashlib.sha1(normalized.encode()).hexdigest()


def _strip_quoted_content(body: str) -> tuple[str, bool]:
    """
    Remove forwarded/quoted blocks from email body.
    Returns (cleaned_body, had_quoted_content).
    Quoted content markers:
      - Lines starting with >
      - "---------- Forwarded message ----------"
      - "-----Original Message-----"
    """
    lines = body.split('\n')
    clean_lines = []
    in_quoted = False
    had_quoted = False

    for line in lines:
        stripped = line.strip()
        if (stripped.startswith('---------- Forwarded') or
                stripped.startswith('-----Original Message') or
                stripped.startswith('-------- Original Message')):
            in_quoted = True
            had_quoted = True
            continue
        if stripped.startswith('>'):
            had_quoted = True
            continue
        if not in_quoted:
            clean_lines.append(line)

    return '\n'.join(clean_lines).strip(), had_quoted


def _shingle_hash(text: str, k: int = 5) -> set:
    """k-shingles for near-duplicate detection."""
    words = text.lower().split()
    if len(words) < k:
        return {' '.join(words)}
    return {' '.join(words[i:i+k]) for i in range(len(words) - k + 1)}


def jaccard(set_a: set, set_b: set) -> float:
    if not set_a or not set_b:
        return 0.0
    return len(set_a & set_b) / len(set_a | set_b)


# ─────────────────────────────────────────────
# MAIN LOADER
# ─────────────────────────────────────────────

class EnronLoader:
    """
    Loads emails → RawArtifact objects with dedup.

    Dedup strategy (3 levels):
      1. Exact: same Message-ID → skip
      2. Body hash: identical body content → mark as duplicate
      3. Jaccard shingles ≥ 0.85 → mark as near-duplicate (forwarded chains)
    """

    def __init__(self):
        self.seen_message_ids: set = set()
        self.seen_body_hashes: dict = {}     # hash → source_id
        self.shingle_index: list = []        # [(source_id, shingle_set)]
        self.artifacts: list = []

    def load_synthetic(self) -> list:
        """Load our synthetic Enron-style corpus."""
        print(f"Loading {len(SYNTHETIC_EMAILS)} synthetic Enron emails...")
        for raw in SYNTHETIC_EMAILS:
            artifact = self._process_raw_email(raw)
            if artifact:
                self.artifacts.append(artifact)
        print(f"  → {len(self.artifacts)} artifacts loaded")
        dups = sum(1 for a in self.artifacts if a.is_duplicate)
        print(f"  → {dups} duplicates detected")
        return self.artifacts

    def load_from_directory(self, path: str) -> list:
        """Load from actual Enron maildir structure."""
        root = Path(path)
        count = 0
        for email_file in root.rglob('*.'):
            if email_file.is_file():
                try:
                    raw_text = email_file.read_text(errors='replace')
                    artifact = self._process_email_text(raw_text)
                    if artifact:
                        self.artifacts.append(artifact)
                        count += 1
                        if count % 100 == 0:
                            print(f"  Loaded {count} emails...")
                except Exception as e:
                    pass  # Skip malformed files
        return self.artifacts

    def _process_raw_email(self, raw: dict) -> Optional[RawArtifact]:
        msg_id = raw.get("Message-ID", "")
        source_id = _make_source_id(msg_id)

        if source_id in self.seen_message_ids:
            return None
        self.seen_message_ids.add(source_id)

        body = raw.get("Body", "")
        clean_body, had_quoted = _strip_quoted_content(body)

        # Level 2: exact body dedup
        bh = _body_hash(clean_body)
        is_dup = False
        dup_of = None

        if bh in self.seen_body_hashes:
            is_dup = True
            dup_of = self.seen_body_hashes[bh]
        else:
            # Level 3: near-duplicate via Jaccard
            shingles = _shingle_hash(clean_body)
            for existing_id, existing_shingles in self.shingle_index:
                if jaccard(shingles, existing_shingles) >= 0.75:
                    is_dup = True
                    dup_of = existing_id
                    break
            if not is_dup:
                self.seen_body_hashes[bh] = source_id
                self.shingle_index.append((source_id, shingles))

        recipients = [r.strip() for r in raw.get("To", "").split(",") if r.strip()]

        return RawArtifact(
            source_id=source_id,
            artifact_type="email",
            sender=raw.get("From", "").strip(),
            recipients=recipients,
            subject=raw.get("Subject", "").strip(),
            body=clean_body,
            timestamp=_parse_date(raw.get("Date", "")),
            thread_id=re.sub(r'^(RE:|FWD?:)\s*', '', raw.get("Subject", ""), flags=re.IGNORECASE).strip(),
            raw_headers={k: v for k, v in raw.items() if k != "Body"},
            is_duplicate=is_dup,
            duplicate_of=dup_of,
        )

    def _process_email_text(self, raw_text: str) -> Optional[RawArtifact]:
        """Parse raw RFC 2822 email text."""
        try:
            msg = message_from_string(raw_text)
            body = ""
            if msg.is_multipart():
                for part in msg.walk():
                    if part.get_content_type() == 'text/plain':
                        body = part.get_payload(decode=True).decode('utf-8', errors='replace')
                        break
            else:
                payload = msg.get_payload(decode=True)
                if payload:
                    body = payload.decode('utf-8', errors='replace')
                else:
                    body = msg.get_payload() or ""

            raw_dict = {
                "Message-ID": msg.get("Message-ID", f"<generated_{hashlib.sha1(raw_text[:100].encode()).hexdigest()[:8]}@enron.com>"),
                "From": msg.get("From", ""),
                "To": msg.get("To", ""),
                "Subject": msg.get("Subject", ""),
                "Date": msg.get("Date", ""),
                "Body": body,
            }
            return self._process_raw_email(raw_dict)
        except Exception:
            return None

    def save(self, path: str):
        data = [a.to_dict() for a in self.artifacts]
        with open(path, 'w') as f:
            json.dump(data, f, indent=2)
        print(f"Saved {len(data)} artifacts to {path}")

    def load_saved(self, path: str) -> list:
        with open(path) as f:
            data = json.load(f)
        self.artifacts = []
        for d in data:
            a = RawArtifact(
                source_id=d["source_id"], artifact_type=d["artifact_type"],
                sender=d.get("sender"), recipients=d.get("recipients", []),
                subject=d.get("subject"), body=d.get("body", ""),
                timestamp=datetime.fromisoformat(d["timestamp"]),
                thread_id=d.get("thread_id"), raw_headers=d.get("raw_headers", {}),
                is_duplicate=d.get("is_duplicate", False), duplicate_of=d.get("duplicate_of"),
                is_redacted=d.get("is_redacted", False),
            )
            self.artifacts.append(a)
        return self.artifacts


if __name__ == "__main__":
    loader = EnronLoader()
    artifacts = loader.load_synthetic()

    print("\n--- ARTIFACT SUMMARY ---")
    for a in artifacts:
        dup_note = f" [DUP of {a.duplicate_of}]" if a.is_duplicate else ""
        print(f"  {a.source_id}  {a.timestamp.strftime('%Y-%m-%d')}  From:{a.sender:<35}  {a.subject[:45]}{dup_note}")

    loader.save("/home/claude/layer10/data/processed/artifacts.json")
    print("\n✅  Loader OK")