"""
RETRIEVAL ENGINE
=================
Given a query, returns a grounded ContextPack.

Strategy (hybrid):
  1. Entity name matching — find entities mentioned in query
  2. FTS5 keyword search — find evidence containing query terms  
  3. Claim expansion — for each matched entity, pull current claims
  4. Conflict injection — surface any superseded claims (historical)
  5. Rank + prune — confidence × recency, cap at top-K

Every returned item is grounded:
  - Each claim has ≥1 Evidence with source_id + char offsets
  - Conflicts shown explicitly with timeline
  - No hallucination: if we can't ground it, we don't return it
"""

import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).parent))
from schema import Entity, Claim, Evidence, ContextPack, ClaimType
from store import MemoryGraphStore

DB_PATH = "/home/claude/layer10/data/memory.db"


class RetrievalEngine:

    def __init__(self, store: MemoryGraphStore):
        self.store = store

    def query(self, question: str, top_k: int = 8, current_only: bool = False) -> ContextPack:
        """
        Main retrieval entry point.
        Returns a ContextPack with grounded claims + evidence.
        """
        # Step 1: Entity extraction from query
        matched_entities = self._match_entities(question)

        # Step 2: FTS keyword search on evidence
        fts_results = self._fts_search(question)

        # Step 3: Expand to claims
        all_claims = []
        evidence_map = {}

        # From entity matches
        for entity in matched_entities:
            claims = self.store.get_claims_for_entity(entity.entity_id, current_only=current_only)
            for claim in claims:
                self._index_evidence(claim, evidence_map)
                all_claims.append(claim)

        # From FTS matches
        for fts_row in fts_results:
            ev_id = fts_row["evidence_id"]
            claim_id = fts_row.get("claim_id") or fts_row.get("claim_id")
            # Get the claim
            rows = self.store.conn.execute(
                "SELECT * FROM claims WHERE claim_id = (SELECT claim_id FROM evidence WHERE evidence_id = ?)",
                (ev_id,)
            ).fetchall()
            for row in rows:
                claim = self.store._row_to_claim(row)
                if not any(c.claim_id == claim.claim_id for c in all_claims):
                    self._index_evidence(claim, evidence_map)
                    all_claims.append(claim)

        # Step 4: Detect conflicts
        conflicts = self._find_relevant_conflicts(matched_entities, question)

        # Step 5: Rank claims
        ranked_claims = self._rank_claims(all_claims, question)[:top_k]

        return ContextPack(
            query=question,
            entities=matched_entities,
            claims=ranked_claims,
            evidence_map=evidence_map,
            conflicts=conflicts,
        )

    def _match_entities(self, question: str) -> list:
        """Find entities referenced in the question."""
        question_lower = question.lower()
        matched = []
        seen_ids = set()

        # Try progressively shorter n-grams from the question
        words = re.findall(r'\b\w+\b', question_lower)
        for n in range(min(len(words), 4), 0, -1):
            for i in range(len(words) - n + 1):
                ngram = ' '.join(words[i:i+n])
                if len(ngram) < 3:
                    continue
                results = self.store.search_entities(ngram)
                for entity in results:
                    if entity.entity_id not in seen_ids:
                        matched.append(entity)
                        seen_ids.add(entity.entity_id)

        return matched[:5]  # cap at 5 entities

    def _fts_search(self, question: str) -> list:
        """FTS5 search on evidence excerpts."""
        # Extract meaningful keywords (skip stopwords)
        stopwords = {'who', 'what', 'when', 'where', 'why', 'how', 'is', 'are',
                    'was', 'the', 'a', 'an', 'of', 'to', 'for', 'on', 'in',
                    'and', 'or', 'does', 'did', 'has', 'have', 'do'}
        words = [w for w in re.findall(r'\b\w{3,}\b', question.lower()) if w not in stopwords]
        
        if not words:
            return []
        
        # Try with all keywords, then progressively fewer
        for n in range(len(words), 0, -1):
            try:
                fts_query = ' OR '.join(words[:n])
                results = self.store.full_text_search(fts_query, limit=5)
                if results:
                    return results
            except Exception:
                continue
        return []

    def _index_evidence(self, claim: Claim, evidence_map: dict):
        """Add claim's evidence to the lookup map."""
        for ev in claim.evidence:
            evidence_map[ev.evidence_id] = ev

    def _find_relevant_conflicts(self, entities: list, question: str) -> list:
        """Pull conflicts relevant to the matched entities."""
        conflicts = []
        for entity in entities:
            entity_conflicts = self.store.get_conflicts(entity.entity_id)
            conflicts.extend(entity_conflicts)
        return conflicts[:5]

    def _rank_claims(self, claims: list, question: str) -> list:
        """
        Score claims by:
          - is_current: current claims score higher
          - confidence: higher confidence = higher score
          - recency: more recent valid_from = higher score
          - keyword overlap with query
        """
        question_words = set(re.findall(r'\b\w{3,}\b', question.lower()))
        
        def score(claim: Claim) -> float:
            s = 0.0
            s += 2.0 if claim.is_current else 0.5
            s += claim.confidence
            
            # Recency bonus (normalize to 0-1 range for 2001 dates)
            try:
                days_old = (datetime(2002, 1, 1) - claim.valid_from).days
                s += max(0, (365 - days_old) / 365)
            except Exception:
                pass

            # Keyword match in evidence
            for ev in claim.evidence:
                ev_words = set(re.findall(r'\b\w{3,}\b', ev.excerpt.lower()))
                overlap = len(question_words & ev_words)
                s += overlap * 0.3

            return s

        seen_ids = set()
        unique = [c for c in claims if c.claim_id not in seen_ids and not seen_ids.add(c.claim_id)]
        return sorted(unique, key=lambda c: score(c), reverse=True)

    def format_context_pack(self, pack: ContextPack) -> str:
        """Human-readable formatted context pack for display."""
        lines = [
            f"╔══════════════════════════════════════════════════════════════",
            f"║ QUERY: {pack.query}",
            f"║ Generated: {pack.generated_at.strftime('%Y-%m-%d %H:%M')}",
            f"╚══════════════════════════════════════════════════════════════",
            "",
        ]

        if pack.entities:
            lines.append(f"ENTITIES MATCHED ({len(pack.entities)}):")
            for e in pack.entities:
                merged = f" [merged from {len(e.merged_from)} aliases]" if e.merged_from else ""
                lines.append(f"  • {e.canonical_name} [{e.entity_type.value}]{merged}")
            lines.append("")

        if pack.claims:
            lines.append(f"GROUNDED CLAIMS ({len(pack.claims)}):")
            for claim in pack.claims:
                status = "✓ CURRENT" if claim.is_current else f"✗ HISTORICAL (until {claim.valid_to})"
                lines.append(f"\n  [{status}] {claim.claim_type.value}")
                lines.append(f"  Subject  : {claim.subject_id}")
                lines.append(f"  Value    : {claim.object_value}")
                lines.append(f"  Confidence: {claim.confidence:.2f}")
                lines.append(f"  Valid from: {claim.valid_from.strftime('%Y-%m-%d')}")
                for ev in claim.evidence[:2]:
                    lines.append(f"  Evidence  : \"{ev.excerpt[:100]}\"")
                    lines.append(f"             [{ev.source_id} @ chars {ev.char_start}-{ev.char_end}]")

        if pack.conflicts:
            lines.append(f"\n⚠️  CONFLICTS / TIMELINE ({len(pack.conflicts)}):")
            for c in pack.conflicts:
                lines.append(f"  ⟳ {c.get('claim_type', 'CLAIM')} changed at {c.get('changed_at', '?')[:10]}")
                lines.append(f"    WAS: \"{c.get('old_value', '?')[:50]}\"")
                lines.append(f"    NOW: \"{c.get('new_value', '?')[:50]}\"")

        return "\n".join(lines)


# ─────────────────────────────────────────────
# EXAMPLE CONTEXT PACKS FOR SUBMISSION
# ─────────────────────────────────────────────

EXAMPLE_QUERIES = [
    "Who owns the Infrastructure Modernization Initiative?",
    "What is the current status of the pipeline migration project?",
    "What is the deadline for the infrastructure project?",
    "Who is on the EES team and what do they depend on?",
    "What decisions did Kenneth Lay make about the project?",
]


def run_example_queries(store: MemoryGraphStore) -> list:
    engine = RetrievalEngine(store)
    results = []
    for q in EXAMPLE_QUERIES:
        pack = engine.query(q)
        formatted = engine.format_context_pack(pack)
        results.append({"query": q, "pack": pack, "formatted": formatted})
        print(formatted)
        print()
    return results


if __name__ == "__main__":
    store = MemoryGraphStore(DB_PATH)
    engine = RetrievalEngine(store)

    print("=== RETRIEVAL ENGINE TEST ===\n")
    for q in EXAMPLE_QUERIES:
        pack = engine.query(q)
        print(engine.format_context_pack(pack))
        print("\n" + "─" * 60 + "\n")
    
    store.close()
    print("✅  Retrieval OK")