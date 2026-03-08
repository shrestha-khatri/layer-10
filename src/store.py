"""
MEMORY GRAPH STORE  —  SQLite-backed
======================================
Why SQLite (not Neo4j):
  - Zero infrastructure, runs anywhere, fully reproducible
  - FTS5 for full-text search on evidence excerpts
  - Same logical model as a graph: entities=nodes, claims=edges
  - Transactions guarantee idempotent ingestion
  - WAL mode handles concurrent reads

Schema:
  artifacts      — immutable source records
  entities       — canonical entity nodes
  claims         — typed, time-bounded edges with evidence
  evidence       — grounding pointers (many-to-one with claims)
  merge_log      — full audit trail for entity merges
  conflicts      — claim supersession history

Idempotency:
  All inserts use INSERT OR REPLACE — safe to re-run pipeline.
  Reprocessing: bump extraction_version; old versions stay in DB for audit.
"""

import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).parent))
from schema import (
    Entity, EntityType, Claim, ClaimType, Evidence,
    RawArtifact, MergeRecord, ContextPack
)

DB_PATH = str(Path(__file__).parent.parent / "data" / "memory.db")


class MemoryGraphStore:
    """
    Persistent memory graph backed by SQLite.
    
    Key capabilities:
      - Idempotent ingestion (safe to re-run)
      - Full-text search on evidence excerpts
      - Time-range queries (current vs historical)
      - Conflict/timeline queries
      - Merge audit trail
    """

    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self._create_schema()

    def _create_schema(self):
        self.conn.executescript("""
        CREATE TABLE IF NOT EXISTS artifacts (
            source_id       TEXT PRIMARY KEY,
            artifact_type   TEXT NOT NULL,
            sender          TEXT,
            recipients      TEXT,        -- JSON array
            subject         TEXT,
            body            TEXT,
            timestamp       TEXT NOT NULL,
            thread_id       TEXT,
            raw_headers     TEXT,        -- JSON
            is_duplicate    INTEGER DEFAULT 0,
            duplicate_of    TEXT,
            is_redacted     INTEGER DEFAULT 0,
            ingested_at     TEXT
        );

        CREATE TABLE IF NOT EXISTS entities (
            entity_id       TEXT PRIMARY KEY,
            entity_type     TEXT NOT NULL,
            canonical_name  TEXT NOT NULL,
            aliases         TEXT,        -- JSON array
            email_addresses TEXT,        -- JSON array
            merged_from     TEXT,        -- JSON array of entity_ids
            first_seen      TEXT,
            last_seen       TEXT,
            evidence_ids    TEXT,        -- JSON array
            metadata        TEXT         -- JSON
        );

        CREATE INDEX IF NOT EXISTS idx_entity_type ON entities(entity_type);
        CREATE INDEX IF NOT EXISTS idx_entity_name ON entities(canonical_name);

        CREATE TABLE IF NOT EXISTS claims (
            claim_id            TEXT PRIMARY KEY,
            claim_type          TEXT NOT NULL,
            subject_id          TEXT NOT NULL,
            object_value        TEXT NOT NULL,
            object_is_entity    INTEGER DEFAULT 0,
            valid_from          TEXT NOT NULL,
            valid_to            TEXT,
            is_current          INTEGER DEFAULT 1,
            confidence          REAL DEFAULT 1.0,
            superseded_by       TEXT,
            extracted_at        TEXT,
            extraction_version  TEXT,
            FOREIGN KEY(subject_id) REFERENCES entities(entity_id)
        );

        CREATE INDEX IF NOT EXISTS idx_claim_subject ON claims(subject_id);
        CREATE INDEX IF NOT EXISTS idx_claim_type ON claims(claim_type);
        CREATE INDEX IF NOT EXISTS idx_claim_current ON claims(is_current);
        CREATE INDEX IF NOT EXISTS idx_claim_object ON claims(object_value);

        CREATE TABLE IF NOT EXISTS evidence (
            evidence_id         TEXT PRIMARY KEY,
            claim_id            TEXT NOT NULL,
            source_id           TEXT NOT NULL,
            excerpt             TEXT NOT NULL,
            char_start          INTEGER,
            char_end            INTEGER,
            source_timestamp    TEXT,
            ingested_at         TEXT,
            extraction_version  TEXT,
            FOREIGN KEY(claim_id) REFERENCES claims(claim_id),
            FOREIGN KEY(source_id) REFERENCES artifacts(source_id)
        );

        CREATE INDEX IF NOT EXISTS idx_evidence_claim ON evidence(claim_id);
        CREATE INDEX IF NOT EXISTS idx_evidence_source ON evidence(source_id);

        -- Full-text search on evidence excerpts
        CREATE VIRTUAL TABLE IF NOT EXISTS evidence_fts USING fts5(
            evidence_id UNINDEXED,
            excerpt,
            content='evidence',
            content_rowid='rowid'
        );

        CREATE TABLE IF NOT EXISTS merge_log (
            merge_id    TEXT PRIMARY KEY,
            winner_id   TEXT NOT NULL,
            loser_id    TEXT NOT NULL,
            reason      TEXT NOT NULL,
            similarity  REAL,
            merged_at   TEXT,
            merged_by   TEXT,
            notes       TEXT
        );

        CREATE TABLE IF NOT EXISTS conflicts (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            conflict_type   TEXT NOT NULL,   -- SUPERSEDED | CONTRADICTED
            claim_type      TEXT NOT NULL,
            subject_id      TEXT NOT NULL,
            old_value       TEXT,
            new_value       TEXT,
            changed_at      TEXT,
            old_claim_id    TEXT,
            new_claim_id    TEXT,
            old_evidence    TEXT,
            new_evidence    TEXT
        );
        """)
        self.conn.commit()

    # ─────────────────────────────────────────
    # INGESTION
    # ─────────────────────────────────────────

    def ingest_artifact(self, artifact: RawArtifact):
        self.conn.execute("""
            INSERT OR REPLACE INTO artifacts VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            artifact.source_id, artifact.artifact_type, artifact.sender,
            json.dumps(artifact.recipients), artifact.subject, artifact.body,
            artifact.timestamp.isoformat(), artifact.thread_id,
            json.dumps(artifact.raw_headers),
            int(artifact.is_duplicate), artifact.duplicate_of,
            int(artifact.is_redacted), artifact.ingested_at.isoformat(),
        ))

    def ingest_entity(self, entity: Entity):
        self.conn.execute("""
            INSERT OR REPLACE INTO entities VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (
            entity.entity_id, entity.entity_type.value, entity.canonical_name,
            json.dumps(entity.aliases), json.dumps(entity.email_addresses),
            json.dumps(entity.merged_from),
            entity.first_seen.isoformat(), entity.last_seen.isoformat(),
            json.dumps(entity.evidence_ids), json.dumps(entity.metadata),
        ))

    def ingest_claim(self, claim: Claim):
        self.conn.execute("""
            INSERT OR REPLACE INTO claims VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            claim.claim_id, claim.claim_type.value, claim.subject_id,
            claim.object_value, int(claim.object_is_entity),
            claim.valid_from.isoformat(),
            claim.valid_to.isoformat() if claim.valid_to else None,
            int(claim.is_current), claim.confidence, claim.superseded_by,
            claim.extracted_at.isoformat(), claim.extraction_version,
        ))

        # Ingest evidence
        for ev in claim.evidence:
            self.ingest_evidence(ev, claim.claim_id)

    def ingest_evidence(self, ev: Evidence, claim_id: str):
        self.conn.execute("""
            INSERT OR REPLACE INTO evidence VALUES (?,?,?,?,?,?,?,?,?)
        """, (
            ev.evidence_id, claim_id, ev.source_id, ev.excerpt,
            ev.char_start, ev.char_end,
            ev.source_timestamp.isoformat(), ev.ingested_at.isoformat(),
            ev.extraction_version,
        ))
        # Update FTS index
        self.conn.execute("""
            INSERT OR REPLACE INTO evidence_fts(evidence_id, excerpt) VALUES (?,?)
        """, (ev.evidence_id, ev.excerpt))

    def ingest_merge(self, record: MergeRecord):
        self.conn.execute("""
            INSERT OR REPLACE INTO merge_log VALUES (?,?,?,?,?,?,?,?)
        """, (
            record.merge_id, record.winner_id, record.loser_id,
            record.reason.value, record.similarity,
            record.merged_at.isoformat(), record.merged_by, record.notes,
        ))

    def ingest_conflict(self, conflict: dict):
        self.conn.execute("""
            INSERT INTO conflicts 
            (conflict_type, claim_type, subject_id, old_value, new_value,
             changed_at, old_claim_id, new_claim_id, old_evidence, new_evidence)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (
            conflict.get("type", "SUPERSEDED"),
            conflict["claim_type"], conflict["subject_id"],
            conflict.get("old_value"), conflict.get("new_value"),
            conflict.get("changed_at"),
            conflict.get("old_claim_id"), conflict.get("new_claim_id"),
            conflict.get("old_evidence"), conflict.get("new_evidence"),
        ))

    def commit(self):
        self.conn.commit()

    def ingest_all(self, artifacts, entities, claims, merge_records, conflicts):
        """Full pipeline ingest with progress reporting."""
        print(f"\nIngesting into graph store: {self.db_path}")
        for a in artifacts:
            self.ingest_artifact(a)
        print(f"  ✓ {len(artifacts)} artifacts")

        for e in entities:
            self.ingest_entity(e)
        print(f"  ✓ {len(entities)} entities")

        for c in claims:
            self.ingest_claim(c)
        print(f"  ✓ {len(claims)} claims")

        for m in merge_records:
            self.ingest_merge(m)
        print(f"  ✓ {len(merge_records)} merge records")

        # Clear old conflicts before re-inserting (idempotency)
        self.conn.execute("DELETE FROM conflicts")
        for cf in conflicts:
            self.ingest_conflict(cf)
        print(f"  ✓ {len(conflicts)} conflicts")

        self.commit()
        print("  ✓ Committed")

    # ─────────────────────────────────────────
    # QUERIES
    # ─────────────────────────────────────────

    def get_entity(self, entity_id: str) -> Optional[Entity]:
        row = self.conn.execute(
            "SELECT * FROM entities WHERE entity_id = ?", (entity_id,)
        ).fetchone()
        return self._row_to_entity(row) if row else None

    def search_entities(self, name: str) -> list:
        rows = self.conn.execute("""
            SELECT * FROM entities 
            WHERE lower(canonical_name) LIKE lower(?)
               OR lower(aliases) LIKE lower(?)
            LIMIT 10
        """, (f"%{name}%", f"%{name}%")).fetchall()
        return [self._row_to_entity(r) for r in rows]

    def get_claims_for_entity(self, entity_id: str, current_only: bool = False) -> list:
        query = "SELECT * FROM claims WHERE subject_id = ?"
        params = [entity_id]
        if current_only:
            query += " AND is_current = 1"
        query += " ORDER BY valid_from DESC"
        rows = self.conn.execute(query, params).fetchall()
        return [self._row_to_claim(r) for r in rows]

    def get_timeline(self, entity_id: str, claim_type: str = None) -> list:
        """
        Get the full history of claims for an entity — current + historical.
        This powers the claim diff / timeline visualization.
        """
        query = """
            SELECT c.*, e.excerpt, e.source_id, e.char_start, e.char_end, e.source_timestamp
            FROM claims c
            LEFT JOIN evidence e ON e.claim_id = c.claim_id
            WHERE c.subject_id = ?
        """
        params = [entity_id]
        if claim_type:
            query += " AND c.claim_type = ?"
            params.append(claim_type)
        query += " ORDER BY c.valid_from ASC"
        
        rows = self.conn.execute(query, params).fetchall()
        result = []
        for row in rows:
            d = dict(row)
            result.append({
                "claim_id": d["claim_id"],
                "claim_type": d["claim_type"],
                "object_value": d["object_value"],
                "valid_from": d["valid_from"],
                "valid_to": d["valid_to"],
                "is_current": bool(d["is_current"]),
                "confidence": d["confidence"],
                "superseded_by": d["superseded_by"],
                "excerpt": d.get("excerpt", ""),
                "source_id": d.get("source_id", ""),
            })
        return result

    def full_text_search(self, query: str, limit: int = 10) -> list:
        """Search evidence excerpts using FTS5."""
        rows = self.conn.execute("""
            SELECT e.*, c.subject_id, c.claim_type, c.object_value, c.is_current, c.confidence
            FROM evidence_fts fts
            JOIN evidence e ON e.evidence_id = fts.evidence_id
            JOIN claims c ON c.claim_id = e.claim_id
            WHERE evidence_fts MATCH ?
            ORDER BY rank
            LIMIT ?
        """, (query, limit)).fetchall()
        return [dict(r) for r in rows]

    def get_conflicts(self, subject_id: str = None) -> list:
        query = "SELECT * FROM conflicts"
        params = []
        if subject_id:
            query += " WHERE subject_id = ?"
            params.append(subject_id)
        query += " ORDER BY changed_at DESC"
        return [dict(r) for r in self.conn.execute(query, params).fetchall()]

    def get_graph_data(self) -> dict:
        """Return full graph as node/edge lists for visualization."""
        entities = self.conn.execute("SELECT * FROM entities").fetchall()
        claims = self.conn.execute(
            "SELECT c.*, e.excerpt, e.source_id FROM claims c LEFT JOIN evidence e ON e.claim_id = c.claim_id"
        ).fetchall()
        merges = self.conn.execute("SELECT * FROM merge_log").fetchall()
        conflicts = self.conn.execute("SELECT * FROM conflicts").fetchall()

        return {
            "entities": [dict(e) for e in entities],
            "claims": [dict(c) for c in claims],
            "merges": [dict(m) for m in merges],
            "conflicts": [dict(c) for c in conflicts],
        }

    def stats(self) -> dict:
        return {
            "artifacts": self.conn.execute("SELECT COUNT(*) FROM artifacts").fetchone()[0],
            "entities": self.conn.execute("SELECT COUNT(*) FROM entities").fetchone()[0],
            "claims_total": self.conn.execute("SELECT COUNT(*) FROM claims").fetchone()[0],
            "claims_current": self.conn.execute("SELECT COUNT(*) FROM claims WHERE is_current=1").fetchone()[0],
            "claims_historical": self.conn.execute("SELECT COUNT(*) FROM claims WHERE is_current=0").fetchone()[0],
            "evidence": self.conn.execute("SELECT COUNT(*) FROM evidence").fetchone()[0],
            "merges": self.conn.execute("SELECT COUNT(*) FROM merge_log").fetchone()[0],
            "conflicts": self.conn.execute("SELECT COUNT(*) FROM conflicts").fetchone()[0],
        }

    # ─────────────────────────────────────────
    # HELPERS
    # ─────────────────────────────────────────

    def _row_to_entity(self, row) -> Entity:
        d = dict(row)
        return Entity(
            entity_id=d["entity_id"], entity_type=EntityType(d["entity_type"]),
            canonical_name=d["canonical_name"],
            aliases=json.loads(d.get("aliases") or "[]"),
            email_addresses=json.loads(d.get("email_addresses") or "[]"),
            merged_from=json.loads(d.get("merged_from") or "[]"),
            first_seen=datetime.fromisoformat(d["first_seen"]),
            last_seen=datetime.fromisoformat(d["last_seen"]),
            evidence_ids=json.loads(d.get("evidence_ids") or "[]"),
            metadata=json.loads(d.get("metadata") or "{}"),
        )

    def _row_to_claim(self, row) -> Claim:
        d = dict(row)
        # Load evidence for this claim
        ev_rows = self.conn.execute(
            "SELECT * FROM evidence WHERE claim_id = ?", (d["claim_id"],)
        ).fetchall()
        evidence = []
        for er in ev_rows:
            ed = dict(er)
            evidence.append(Evidence(
                evidence_id=ed["evidence_id"], source_id=ed["source_id"],
                excerpt=ed["excerpt"], char_start=ed["char_start"], char_end=ed["char_end"],
                source_timestamp=datetime.fromisoformat(ed["source_timestamp"]),
                ingested_at=datetime.fromisoformat(ed["ingested_at"]),
                extraction_version=ed["extraction_version"],
            ))
        return Claim(
            claim_id=d["claim_id"], claim_type=ClaimType(d["claim_type"]),
            subject_id=d["subject_id"], object_value=d["object_value"],
            object_is_entity=bool(d["object_is_entity"]),
            valid_from=datetime.fromisoformat(d["valid_from"]),
            valid_to=datetime.fromisoformat(d["valid_to"]) if d.get("valid_to") else None,
            is_current=bool(d["is_current"]), evidence=evidence,
            confidence=d["confidence"], superseded_by=d.get("superseded_by"),
            extracted_at=datetime.fromisoformat(d["extracted_at"]),
            extraction_version=d["extraction_version"],
        )

    def close(self):
        self.conn.close()


if __name__ == "__main__":
    import os
    # Clean slate for test
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    sys.path.insert(0, str(Path(__file__).parent))
    from loader import EnronLoader
    from extractor import extract_rule_based
    from dedup import run_dedup

    loader = EnronLoader()
    artifacts = loader.load_synthetic()
    all_entities, all_claims = [], []
    for a in artifacts:
        if not a.is_duplicate:
            ents, clms = extract_rule_based(a)
            all_entities.extend(ents)
            all_claims.extend(clms)

    canonical_entities, canonical_claims, merge_records, conflicts = run_dedup(all_entities, all_claims)

    store = MemoryGraphStore(DB_PATH)
    store.ingest_all(artifacts, canonical_entities, canonical_claims, merge_records, conflicts)

    print("\n--- GRAPH STATS ---")
    for k, v in store.stats().items():
        print(f"  {k:25s}: {v}")

    print("\n--- FTS SEARCH: 'pipeline ownership' ---")
    results = store.full_text_search("pipeline ownership")
    for r in results[:3]:
        print(f"  [{r['source_id']}] {r['claim_type']} → \"{r['excerpt'][:80]}\"")

    print("\n--- TIMELINE: Infrastructure Modernization Initiative ---")
    proj_entities = store.search_entities("Infrastructure")
    if proj_entities:
        proj = proj_entities[0]
        timeline = store.get_timeline(proj.entity_id, "HAS_STATUS")
        print(f"  Status history for '{proj.canonical_name}':")
        for t in timeline:
            status = "→ CURRENT" if t["is_current"] else f"→ until {t['valid_to']}"
            print(f"    {t['valid_from'][:10]}  {t['object_value']:15s}  {status}")
            print(f"              \"{t['excerpt'][:60]}\"")

    print("\n✅  Graph store OK")