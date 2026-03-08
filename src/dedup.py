"""
DEDUPLICATION + CANONICALIZATION ENGINE
=========================================
What this does:
  Three levels of dedup, each with full audit trail:

  Level 1 — ENTITY DEDUP
    • Exact match: same canonical_name + same type → merge
    • Email match: same email address seen in different name forms
    • Fuzzy match: Levenshtein similarity ≥ threshold
    → All merges logged in MergeRecord; REVERSIBLE

  Level 2 — CLAIM DEDUP  
    • Same subject + type + object → merge into one claim, pool evidence
    • Confidence boosted when multiple sources agree
    → Keeps ALL evidence, just under one canonical claim_id

  Level 3 — CONFLICT DETECTION
    • Same subject + type, DIFFERENT objects → CONFLICT
    → Represents "it used to be X, now it's Y" via valid_from/valid_to
    → Earlier claim gets valid_to set to newer claim's valid_from
    → This is the "claim diff" / timeline that makes us stand out

REVERSIBILITY:
  To undo a merge: use MergeRecord.loser_id to restore the entity,
  then repoint any claims that used winner_id but sourced from loser artifacts.
"""

import hashlib
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).parent))
from schema import (
    Entity, EntityType, Claim, ClaimType, Evidence,
    MergeRecord, MergeReason
)


# ─────────────────────────────────────────────
# STRING SIMILARITY (stdlib only)
# ─────────────────────────────────────────────

def levenshtein(s1: str, s2: str) -> int:
    """Standard Levenshtein distance, O(n*m)."""
    s1, s2 = s1.lower().strip(), s2.lower().strip()
    if s1 == s2: return 0
    if not s1: return len(s2)
    if not s2: return len(s1)
    
    prev = list(range(len(s2) + 1))
    for i, c1 in enumerate(s1):
        curr = [i + 1]
        for j, c2 in enumerate(s2):
            curr.append(min(prev[j] + (0 if c1 == c2 else 1),
                           curr[j] + 1, prev[j + 1] + 1))
        prev = curr
    return prev[-1]


def string_similarity(s1: str, s2: str) -> float:
    """Normalized similarity 0-1. 1.0 = identical."""
    s1, s2 = s1.lower().strip(), s2.lower().strip()
    if s1 == s2: return 1.0
    max_len = max(len(s1), len(s2))
    if max_len == 0: return 1.0
    return 1.0 - levenshtein(s1, s2) / max_len


def name_variations(name: str) -> list:
    """
    Generate common name variations for matching.
    "Kenneth Lay" → ["ken lay", "k lay", "ken", "k.lay"]
    """
    parts = name.lower().split()
    if len(parts) < 2:
        return [name.lower()]
    
    first, last = parts[0], parts[-1]
    return [
        name.lower(),
        f"{first} {last}",
        first,
        f"{first[0]} {last}",
        f"{first[0]}.{last}",
        last,
    ]


# ─────────────────────────────────────────────
# ENTITY CANONICALIZER
# ─────────────────────────────────────────────

# Known alias groups — in production this would come from a lookup table
# or be learned from the corpus. Hardcoded here for Enron.
KNOWN_ALIAS_GROUPS = [
    {
        "canonical": "Kenneth Lay",
        "aliases": ["ken lay", "ken", "k lay", "k.lay@enron.com", "kenneth.lay@enron.com",
                   "kenneth lay", "chairman & ceo", "chairman and ceo"],
        "email": "kenneth.lay@enron.com",
        "type": EntityType.PERSON,
    },
    {
        "canonical": "Jeff Skilling",
        "aliases": ["jeff skilling", "jeff", "j skilling", "jeff.skilling@enron.com"],
        "email": "jeff.skilling@enron.com",
        "type": EntityType.PERSON,
    },
    {
        "canonical": "Andrew Fastow",
        "aliases": ["andrew fastow", "andy fastow", "andy", "andrew.fastow@enron.com",
                   "cfo, enron", "cfo"],
        "email": "andrew.fastow@enron.com",
        "type": EntityType.PERSON,
    },
    {
        "canonical": "Louise Kitchen",
        "aliases": ["louise kitchen", "louise", "louise.kitchen@enron.com",
                   "vp, ees", "vp ees"],
        "email": "louise.kitchen@enron.com",
        "type": EntityType.PERSON,
    },
    {
        "canonical": "Ben Glisan",
        "aliases": ["ben glisan", "ben", "ben.glisan@enron.com",
                   "chief accounting officer"],
        "email": "ben.glisan@enron.com",
        "type": EntityType.PERSON,
    },
    {
        "canonical": "Infrastructure Modernization Initiative",
        "aliases": ["infrastructure modernization initiative", "pipeline migration project",
                   "pipeline migration", "infrastructure modernization",
                   "imi"],  # acronym
        "email": None,
        "type": EntityType.PROJECT,
    },
    {
        "canonical": "EES Systems",
        "aliases": ["ees systems", "ees team", "ees", "ees trading platform",
                   "louise kitchen's group"],
        "email": None,
        "type": EntityType.TEAM,
    },
    {
        "canonical": "IT Security",
        "aliases": ["it security", "ben glisan's team", "it security team",
                   "it security liaison"],
        "email": None,
        "type": EntityType.TEAM,
    },
]

# Build reverse lookup: alias → canonical info
ALIAS_LOOKUP = {}
for group in KNOWN_ALIAS_GROUPS:
    for alias in group["aliases"]:
        ALIAS_LOOKUP[alias.lower()] = group


class EntityCanonicalizer:
    """
    Merges duplicate entity mentions into canonical entities.
    
    Strategy:
      1. Known alias lookup (highest confidence, no threshold needed)
      2. Email address matching (very high confidence)
      3. Exact name match (high confidence)
      4. Fuzzy name match (similarity ≥ 0.82)
    
    All merges are soft — original entity_ids preserved in MergeRecord.
    """

    def __init__(self):
        self.canonical_entities: dict = {}   # entity_id → Entity
        self.alias_index: dict = {}          # alias_str → entity_id (canonical)
        self.email_index: dict = {}          # email → entity_id
        self.merge_log: list = []            # list[MergeRecord]
        self._merge_counter = 0

    def _next_merge_id(self) -> str:
        self._merge_counter += 1
        return f"mrg_{self._merge_counter:05d}"

    def canonicalize(self, entities: list) -> tuple[list, list]:
        """
        Takes raw entity list, returns (canonical_entities, merge_records).
        """
        # First pass: resolve via known alias groups
        for entity in entities:
            self._resolve_entity(entity)

        return list(self.canonical_entities.values()), self.merge_log

    def _resolve_entity(self, entity: Entity):
        """Resolve a single entity to its canonical form."""
        name_lower = entity.canonical_name.lower().strip()

        # 1. Known alias lookup
        if name_lower in ALIAS_LOOKUP:
            group = ALIAS_LOOKUP[name_lower]
            canonical_name = group["canonical"]
            canonical_id = Entity.make_id(canonical_name, group["type"])
            self._merge_into_canonical(entity, canonical_id, canonical_name,
                                       group["type"], group.get("email"),
                                       MergeReason.ALIAS, 1.0)
            return

        # 2. Email address matching
        for email in entity.email_addresses:
            if email.lower() in self.email_index:
                winner_id = self.email_index[email.lower()]
                winner = self.canonical_entities[winner_id]
                self._do_merge(winner, entity, MergeReason.EMAIL_NAME_MATCH, 0.95)
                return
            # Check alias lookup by email
            if email.lower() in ALIAS_LOOKUP:
                group = ALIAS_LOOKUP[email.lower()]
                canonical_name = group["canonical"]
                canonical_id = Entity.make_id(canonical_name, group["type"])
                self._merge_into_canonical(entity, canonical_id, canonical_name,
                                           group["type"], group.get("email"),
                                           MergeReason.EMAIL_NAME_MATCH, 0.98)
                return

        # 3. Exact name match (already in our index)
        if name_lower in self.alias_index:
            winner_id = self.alias_index[name_lower]
            winner = self.canonical_entities[winner_id]
            self._do_merge(winner, entity, MergeReason.EXACT_MATCH, 1.0)
            return

        # 4. Fuzzy name matching against existing canonicals
        best_match = None
        best_score = 0.0
        for variations in [name_variations(entity.canonical_name)]:
            for existing_id, existing in self.canonical_entities.items():
                if existing.entity_type != entity.entity_type:
                    continue
                # Compare all name variations
                for var in variations:
                    for ex_var in name_variations(existing.canonical_name):
                        score = string_similarity(var, ex_var)
                        if score > best_score:
                            best_score = score
                            best_match = existing

        if best_match and best_score >= 0.82 and best_match.entity_type == entity.entity_type:
            self._do_merge(best_match, entity, MergeReason.FUZZY_MATCH, best_score)
            return

        # No match — register as new canonical entity
        self._register_new(entity)

    def _register_new(self, entity: Entity):
        """Register entity as a new canonical."""
        self.canonical_entities[entity.entity_id] = entity
        # Index all aliases
        self.alias_index[entity.canonical_name.lower()] = entity.entity_id
        for alias in entity.aliases:
            self.alias_index[alias.lower()] = entity.entity_id
        for email in entity.email_addresses:
            self.email_index[email.lower()] = entity.entity_id

    def _merge_into_canonical(self, entity: Entity, canonical_id: str,
                               canonical_name: str, etype: EntityType,
                               email: str, reason: MergeReason, sim: float):
        """Merge entity into a known canonical (may not exist yet)."""
        if canonical_id not in self.canonical_entities:
            # Create canonical entity
            canonical = Entity(
                entity_id=canonical_id,
                entity_type=etype,
                canonical_name=canonical_name,
                aliases=list(ALIAS_LOOKUP.get(canonical_name.lower(), {}).get("aliases", [canonical_name])),
                email_addresses=[email] if email else [],
                first_seen=entity.first_seen,
                last_seen=entity.last_seen,
                merged_from=[],
            )
            self._register_new(canonical)
        
        winner = self.canonical_entities[canonical_id]
        self._do_merge(winner, entity, reason, sim)

    def _do_merge(self, winner: Entity, loser: Entity, reason: MergeReason, sim: float):
        """Perform the merge: absorb loser into winner."""
        if loser.entity_id == winner.entity_id:
            return  # Already the same entity

        # Update winner's metadata
        winner.merged_from.append(loser.entity_id)
        for alias in loser.aliases + [loser.canonical_name]:
            if alias not in winner.aliases:
                winner.aliases.append(alias)
        for email in loser.email_addresses:
            if email not in winner.email_addresses:
                winner.email_addresses.append(email)
        winner.evidence_ids.extend(loser.evidence_ids)
        if loser.first_seen < winner.first_seen:
            winner.first_seen = loser.first_seen
        if loser.last_seen > winner.last_seen:
            winner.last_seen = loser.last_seen

        # Index loser's names to winner
        self.alias_index[loser.canonical_name.lower()] = winner.entity_id
        for alias in loser.aliases:
            self.alias_index[alias.lower()] = winner.entity_id
        for email in loser.email_addresses:
            self.email_index[email.lower()] = winner.entity_id

        # Log the merge (REVERSIBILITY)
        record = MergeRecord(
            merge_id=self._next_merge_id(),
            winner_id=winner.entity_id,
            loser_id=loser.entity_id,
            reason=reason,
            similarity=sim,
            notes=f"'{loser.canonical_name}' → '{winner.canonical_name}'",
        )
        self.merge_log.append(record)

    def remap_entity_id(self, entity_id: str) -> str:
        """Map a raw entity_id to its canonical form (for claim repointing)."""
        if entity_id in self.canonical_entities:
            return entity_id
        # Check if it was merged
        for winner_id, winner in self.canonical_entities.items():
            if entity_id in winner.merged_from:
                return winner_id
        # Try alias lookup by entity_id pattern
        for eid, canonical in self.canonical_entities.items():
            if entity_id in canonical.merged_from:
                return eid
        return entity_id  # unchanged


# ─────────────────────────────────────────────
# CLAIM DEDUPLICATOR
# ─────────────────────────────────────────────

class ClaimDeduplicator:
    """
    Merges duplicate claims and detects conflicts.
    
    DEDUP: Same (subject, type, object) → one claim with pooled evidence.
    CONFLICT: Same (subject, type) but different object AND overlapping time
              → "used to be X, now Y" timeline.
              
    This is the KEY differentiator: we don't just pick a winner.
    We preserve BOTH claims with valid_from/valid_to showing the transition.
    """

    def __init__(self, canonicalizer: EntityCanonicalizer):
        self.canonicalizer = canonicalizer
        self.canonical_claims: dict = {}     # claim_id → Claim
        self.conflict_log: list = []         # list of conflict dicts

    def deduplicate(self, claims: list) -> tuple[list, list]:
        """
        Returns (canonical_claims, conflicts).
        """
        # First remap all entity IDs to canonical form
        remapped = []
        for claim in claims:
            new_subject = self.canonicalizer.remap_entity_id(claim.subject_id)
            new_object = (self.canonicalizer.remap_entity_id(claim.object_value)
                         if claim.object_is_entity else claim.object_value)
            
            new_claim_id = Claim.make_id(new_subject, claim.claim_type, new_object)
            claim.claim_id = new_claim_id
            claim.subject_id = new_subject
            claim.object_value = new_object
            remapped.append(claim)

        # Sort by timestamp so we process in order (important for timeline)
        remapped.sort(key=lambda c: c.valid_from)

        for claim in remapped:
            self._process_claim(claim)

        return list(self.canonical_claims.values()), self.conflict_log

    def _process_claim(self, new_claim: Claim):
        """Process a single claim — dedup or conflict."""
        # Check for exact match (same subject + type + object)
        if new_claim.claim_id in self.canonical_claims:
            # DEDUP: pool evidence, boost confidence
            existing = self.canonical_claims[new_claim.claim_id]
            self._pool_evidence(existing, new_claim)
            return

        # Check for conflict (same subject + type, different object)
        conflict_key = f"{new_claim.subject_id}:{new_claim.claim_type.value}"
        conflicting = self._find_conflicts(new_claim)

        if conflicting:
            # This is the TIMELINE feature
            for old_claim in conflicting:
                if old_claim.is_current and old_claim.object_value != new_claim.object_value:
                    # Mark old claim as historical
                    old_claim.valid_to = new_claim.valid_from
                    old_claim.is_current = False
                    old_claim.superseded_by = new_claim.claim_id

                    self.conflict_log.append({
                        "type": "SUPERSEDED",
                        "claim_type": new_claim.claim_type.value,
                        "subject_id": new_claim.subject_id,
                        "old_value": old_claim.object_value,
                        "new_value": new_claim.object_value,
                        "changed_at": new_claim.valid_from.isoformat(),
                        "old_claim_id": old_claim.claim_id,
                        "new_claim_id": new_claim.claim_id,
                        "old_evidence": old_claim.evidence[0].excerpt if old_claim.evidence else "",
                        "new_evidence": new_claim.evidence[0].excerpt if new_claim.evidence else "",
                    })

        self.canonical_claims[new_claim.claim_id] = new_claim

    def _find_conflicts(self, claim: Claim) -> list:
        """Find existing claims with same subject+type but different object."""
        conflicts = []
        for existing in self.canonical_claims.values():
            if (existing.subject_id == claim.subject_id and
                    existing.claim_type == claim.claim_type and
                    existing.object_value != claim.object_value and
                    existing.is_current):
                conflicts.append(existing)
        return conflicts

    def _pool_evidence(self, existing: Claim, duplicate: Claim):
        """Merge evidence from duplicate into existing claim."""
        existing_ev_ids = {e.evidence_id for e in existing.evidence}
        for ev in duplicate.evidence:
            if ev.evidence_id not in existing_ev_ids:
                existing.evidence.append(ev)
                existing_ev_ids.add(ev.evidence_id)
        
        # Boost confidence when multiple sources agree
        n = len(existing.evidence)
        existing.confidence = min(0.99, existing.confidence + 0.02 * (n - 1))
        
        # Update validity window
        if duplicate.valid_from < existing.valid_from:
            existing.valid_from = duplicate.valid_from


# ─────────────────────────────────────────────
# MAIN DEDUP PIPELINE
# ─────────────────────────────────────────────

def run_dedup(raw_entities: list, raw_claims: list) -> tuple[list, list, list, list]:
    """
    Full dedup pipeline.
    Returns: (canonical_entities, canonical_claims, merge_records, conflict_log)
    """
    print(f"\nDeduplicating {len(raw_entities)} entity mentions, {len(raw_claims)} claims...")

    # Step 1: Entity canonicalization
    canon = EntityCanonicalizer()
    canonical_entities, merge_records = canon.canonicalize(raw_entities)
    print(f"  → {len(canonical_entities)} canonical entities (from {len(raw_entities)} mentions)")
    print(f"  → {len(merge_records)} merges performed")

    # Step 2: Claim deduplication + conflict detection
    dedup = ClaimDeduplicator(canon)
    canonical_claims, conflicts = dedup.deduplicate(raw_claims)
    print(f"  → {len(canonical_claims)} canonical claims (from {len(raw_claims)} raw)")
    print(f"  → {len(conflicts)} conflicts/supersessions detected")

    return canonical_entities, canonical_claims, merge_records, conflicts


if __name__ == "__main__":
    sys.path.insert(0, '/home/claude/layer10/src')
    from loader import EnronLoader
    from extractor import extract_rule_based

    loader = EnronLoader()
    artifacts = loader.load_synthetic()

    all_entities, all_claims = [], []
    for a in artifacts:
        if not a.is_duplicate:
            ents, clms = extract_rule_based(a)
            all_entities.extend(ents)
            all_claims.extend(clms)

    canonical_entities, canonical_claims, merge_records, conflicts = run_dedup(all_entities, all_claims)

    print("\n--- CANONICAL ENTITIES ---")
    for e in sorted(canonical_entities, key=lambda x: x.canonical_name):
        merged_note = f"  (merged from {len(e.merged_from)} others)" if e.merged_from else ""
        print(f"  [{e.entity_type.value:10s}] {e.canonical_name}{merged_note}")
        if e.aliases and len(e.aliases) > 1:
            print(f"             aliases: {e.aliases[:5]}")

    print("\n--- MERGE LOG ---")
    for m in merge_records:
        print(f"  {m.reason.value:20s} '{m.notes}'  sim={m.similarity:.2f}")

    print("\n--- CLAIM TIMELINE (conflicts/supersessions) ---")
    for c in conflicts:
        print(f"  ⟳ {c['claim_type']:15s} [{c['subject_id'][:16]}]")
        print(f"    WAS: \"{c['old_value'][:40]}\"")
        print(f"    NOW: \"{c['new_value'][:40]}\"")
        print(f"    AT:  {c['changed_at']}")

    print("\n--- CURRENT CLAIMS ---")
    for claim in [c for c in canonical_claims if c.is_current]:
        ev = claim.evidence[0] if claim.evidence else None
        print(f"  {claim.claim_type.value:20s} subj={claim.subject_id[:16]}  obj=\"{claim.object_value[:35]}\"")

    print("\n✅  Dedup OK")