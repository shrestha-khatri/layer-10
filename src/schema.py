"""
LAYER10 MEMORY SCHEMA  —  zero external dependencies
=====================================================
Pure Python dataclasses + stdlib only.

Design principles:
  1. GROUNDING   : Every claim has char-level Evidence pointing to exact source text
  2. TIME        : valid_from/valid_to tracks when something WAS true vs IS true now
  3. REVERSIBILITY: Merges are soft — original IDs kept, MergeRecord logs why
  4. VERSIONING  : Every extraction stamped schema+model+prompt version
"""

from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional
import hashlib


class EntityType(str, Enum):
    PERSON      = "PERSON"
    PROJECT     = "PROJECT"
    COMPONENT   = "COMPONENT"
    TEAM        = "TEAM"
    DECISION    = "DECISION"
    UNKNOWN     = "UNKNOWN"

class ClaimType(str, Enum):
    ASSIGNED_TO     = "ASSIGNED_TO"
    REPORTED_BY     = "REPORTED_BY"
    MEMBER_OF       = "MEMBER_OF"
    HAS_STATUS      = "HAS_STATUS"
    HAS_DEADLINE    = "HAS_DEADLINE"
    HAS_PRIORITY    = "HAS_PRIORITY"
    DEPENDS_ON      = "DEPENDS_ON"
    DECIDED         = "DECIDED"
    MENTIONED       = "MENTIONED"

class MergeReason(str, Enum):
    EXACT_MATCH       = "EXACT_MATCH"
    ALIAS             = "ALIAS"
    EMAIL_NAME_MATCH  = "EMAIL_NAME_MATCH"
    FUZZY_MATCH       = "FUZZY_MATCH"
    MANUAL            = "MANUAL"


@dataclass
class Evidence:
    evidence_id:        str
    source_id:          str
    excerpt:            str         # Exact text from source — never a paraphrase
    char_start:         int
    char_end:           int
    source_timestamp:   datetime
    ingested_at:        datetime    = field(default_factory=datetime.utcnow)
    extraction_version: str         = "schema_v1"

    @staticmethod
    def make_id(source_id: str, char_start: int, char_end: int) -> str:
        return "ev_" + hashlib.sha1(f"{source_id}:{char_start}:{char_end}".encode()).hexdigest()[:12]

    def to_dict(self):
        return {
            "evidence_id": self.evidence_id, "source_id": self.source_id,
            "excerpt": self.excerpt, "char_start": self.char_start, "char_end": self.char_end,
            "source_timestamp": self.source_timestamp.isoformat(),
            "ingested_at": self.ingested_at.isoformat(),
            "extraction_version": self.extraction_version,
        }

    @classmethod
    def from_dict(cls, d):
        return cls(
            evidence_id=d["evidence_id"], source_id=d["source_id"],
            excerpt=d["excerpt"], char_start=d["char_start"], char_end=d["char_end"],
            source_timestamp=datetime.fromisoformat(d["source_timestamp"]),
            ingested_at=datetime.fromisoformat(d.get("ingested_at", datetime.utcnow().isoformat())),
            extraction_version=d.get("extraction_version", "schema_v1"),
        )


@dataclass
class Entity:
    entity_id:       str
    entity_type:     EntityType
    canonical_name:  str
    aliases:         list   = field(default_factory=list)
    email_addresses: list   = field(default_factory=list)
    merged_from:     list   = field(default_factory=list)   # audit trail
    first_seen:      datetime = field(default_factory=datetime.utcnow)
    last_seen:       datetime = field(default_factory=datetime.utcnow)
    evidence_ids:    list   = field(default_factory=list)
    metadata:        dict   = field(default_factory=dict)

    @staticmethod
    def make_id(name: str, entity_type: EntityType) -> str:
        return "ent_" + hashlib.sha1(f"{entity_type}:{name.lower().strip()}".encode()).hexdigest()[:12]

    def to_dict(self):
        return {
            "entity_id": self.entity_id, "entity_type": self.entity_type.value,
            "canonical_name": self.canonical_name, "aliases": self.aliases,
            "email_addresses": self.email_addresses, "merged_from": self.merged_from,
            "first_seen": self.first_seen.isoformat(), "last_seen": self.last_seen.isoformat(),
            "evidence_ids": self.evidence_ids, "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, d):
        return cls(
            entity_id=d["entity_id"], entity_type=EntityType(d["entity_type"]),
            canonical_name=d["canonical_name"], aliases=d.get("aliases", []),
            email_addresses=d.get("email_addresses", []), merged_from=d.get("merged_from", []),
            first_seen=datetime.fromisoformat(d["first_seen"]),
            last_seen=datetime.fromisoformat(d["last_seen"]),
            evidence_ids=d.get("evidence_ids", []), metadata=d.get("metadata", {}),
        )


@dataclass
class Claim:
    """
    TIME SEMANTICS:
      valid_from/valid_to = when this was TRUE (from document timestamp, not extraction time)
      superseded_by       = if overridden, points to the replacement claim_id
      is_current          = fast filter for present-state queries
    """
    claim_id:           str
    claim_type:         ClaimType
    subject_id:         str
    object_value:       str
    object_is_entity:   bool            = False
    valid_from:         datetime        = field(default_factory=datetime.utcnow)
    valid_to:           Optional[datetime] = None
    is_current:         bool            = True
    evidence:           list            = field(default_factory=list)  # list[Evidence]
    confidence:         float           = 1.0
    superseded_by:      Optional[str]   = None
    extracted_at:       datetime        = field(default_factory=datetime.utcnow)
    extraction_version: str             = "schema_v1"

    @staticmethod
    def make_id(subject_id: str, claim_type: ClaimType, object_value: str) -> str:
        return "clm_" + hashlib.sha1(f"{subject_id}:{claim_type}:{object_value}".encode()).hexdigest()[:12]

    def to_dict(self):
        return {
            "claim_id": self.claim_id, "claim_type": self.claim_type.value,
            "subject_id": self.subject_id, "object_value": self.object_value,
            "object_is_entity": self.object_is_entity,
            "valid_from": self.valid_from.isoformat(),
            "valid_to": self.valid_to.isoformat() if self.valid_to else None,
            "is_current": self.is_current,
            "evidence": [e.to_dict() if hasattr(e, 'to_dict') else e for e in self.evidence],
            "confidence": self.confidence, "superseded_by": self.superseded_by,
            "extracted_at": self.extracted_at.isoformat(),
            "extraction_version": self.extraction_version,
        }

    @classmethod
    def from_dict(cls, d):
        return cls(
            claim_id=d["claim_id"], claim_type=ClaimType(d["claim_type"]),
            subject_id=d["subject_id"], object_value=d["object_value"],
            object_is_entity=d.get("object_is_entity", False),
            valid_from=datetime.fromisoformat(d["valid_from"]),
            valid_to=datetime.fromisoformat(d["valid_to"]) if d.get("valid_to") else None,
            is_current=d.get("is_current", True),
            evidence=[Evidence.from_dict(e) if isinstance(e, dict) else e for e in d.get("evidence", [])],
            confidence=d.get("confidence", 1.0), superseded_by=d.get("superseded_by"),
            extracted_at=datetime.fromisoformat(d.get("extracted_at", datetime.utcnow().isoformat())),
            extraction_version=d.get("extraction_version", "schema_v1"),
        )


@dataclass
class RawArtifact:
    source_id:     str
    artifact_type: str
    sender:        Optional[str]
    recipients:    list            = field(default_factory=list)
    subject:       Optional[str]   = None
    body:          str             = ""
    timestamp:     datetime        = field(default_factory=datetime.utcnow)
    thread_id:     Optional[str]   = None
    raw_headers:   dict            = field(default_factory=dict)
    is_duplicate:  bool            = False
    duplicate_of:  Optional[str]   = None
    is_redacted:   bool            = False
    ingested_at:   datetime        = field(default_factory=datetime.utcnow)

    def to_dict(self):
        return {
            "source_id": self.source_id, "artifact_type": self.artifact_type,
            "sender": self.sender, "recipients": self.recipients,
            "subject": self.subject, "body": self.body,
            "timestamp": self.timestamp.isoformat(), "thread_id": self.thread_id,
            "raw_headers": self.raw_headers, "is_duplicate": self.is_duplicate,
            "duplicate_of": self.duplicate_of, "is_redacted": self.is_redacted,
            "ingested_at": self.ingested_at.isoformat(),
        }


@dataclass
class MergeRecord:
    """Full audit trail. To undo: restore loser entity + repoint claims."""
    merge_id:   str
    winner_id:  str
    loser_id:   str
    reason:     MergeReason
    similarity: float
    merged_at:  datetime = field(default_factory=datetime.utcnow)
    merged_by:  str      = "system"
    notes:      str      = ""

    def to_dict(self):
        return {
            "merge_id": self.merge_id, "winner_id": self.winner_id, "loser_id": self.loser_id,
            "reason": self.reason.value, "similarity": self.similarity,
            "merged_at": self.merged_at.isoformat(), "merged_by": self.merged_by, "notes": self.notes,
        }


@dataclass
class ContextPack:
    query:        str
    entities:     list  = field(default_factory=list)
    claims:       list  = field(default_factory=list)
    evidence_map: dict  = field(default_factory=dict)
    conflicts:    list  = field(default_factory=list)
    generated_at: datetime = field(default_factory=datetime.utcnow)

    def to_citation_text(self) -> str:
        lines = [f"Query: {self.query}", "=" * 50]
        for claim in self.claims:
            ev = claim.evidence[0] if claim.evidence else None
            citation = f"[{ev.source_id}:{ev.char_start}-{ev.char_end}]" if ev else "[NO EVIDENCE]"
            status = "CURRENT" if claim.is_current else f"HISTORICAL(until {claim.valid_to})"
            lines.append(f"  [{status}] {claim.claim_type.value}: {claim.subject_id} → \"{claim.object_value}\" conf={claim.confidence:.2f} {citation}")
        if self.conflicts:
            lines.append("\n⚠️  CONFLICTS:") 
            for c in self.conflicts:
                lines.append(f"  ↯ {c}")
        return "\n".join(lines)


if __name__ == "__main__":
    ts = datetime(2001, 10, 15, 9, 0)
    ev = Evidence(
        evidence_id=Evidence.make_id("email_001", 42, 97),
        source_id="email_001", excerpt="Ken will own the pipeline migration project",
        char_start=42, char_end=97, source_timestamp=ts,
    )
    ent = Entity(
        entity_id=Entity.make_id("Kenneth Lay", EntityType.PERSON),
        entity_type=EntityType.PERSON, canonical_name="Kenneth Lay",
        aliases=["Ken Lay", "Ken", "k.lay@enron.com"], first_seen=ts, last_seen=ts,
    )
    claim = Claim(
        claim_id=Claim.make_id(ent.entity_id, ClaimType.ASSIGNED_TO, "pipeline_migration"),
        claim_type=ClaimType.ASSIGNED_TO, subject_id=ent.entity_id,
        object_value="pipeline_migration", valid_from=ts, evidence=[ev], confidence=0.92,
    )
    assert Claim.from_dict(claim.to_dict()).claim_id == claim.claim_id
    pack = ContextPack(query="Who owns pipeline migration?", entities=[ent], claims=[claim])
    print(pack.to_citation_text())
    print("\n✅  Schema OK — zero dependencies")